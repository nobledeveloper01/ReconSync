package store

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// Integration tests run against a real Postgres, never a mock. Set
// RECONSYNC_TEST_DATABASE_URL to enable them; they skip cleanly without it.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("RECONSYNC_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("RECONSYNC_TEST_DATABASE_URL not set; skipping Postgres integration tests")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	applySchema(t, pool)
	return pool
}

func applySchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	for _, name := range []string{"0001_init.down.sql", "0001_init.up.sql"} {
		sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
}

func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	// audit_records is deliberately excluded: it is append-only and its
	// statement-level trigger rejects TRUNCATE.
	_, err := pool.Exec(context.Background(),
		`TRUNCATE transactions, reconciliation_rules, webhook_deliveries,
		 webhook_endpoints, api_keys, tenants RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func TestPostgresStore(t *testing.T) {
	pool := testPool(t)
	runConformance(t, func(t *testing.T) Store {
		truncate(t, pool)
		return NewPostgres(pool)
	})
}

// The audit trail's immutability must hold at the database, not in application
// code that could be bypassed.
func TestAuditRecordsAreAppendOnly(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx,
		`INSERT INTO audit_records (tenant_id, event_type, occurred_at, hash)
		 VALUES ('tnt_audit', 'reversal.triggered', now(), 'sha256:seed')`)
	if err != nil {
		t.Fatalf("insert audit record: %v", err)
	}

	for _, tc := range []struct{ name, sql string }{
		{"update", `UPDATE audit_records SET hash = 'tampered' WHERE tenant_id = 'tnt_audit'`},
		{"delete", `DELETE FROM audit_records WHERE tenant_id = 'tnt_audit'`},
		{"truncate", `TRUNCATE audit_records`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, tc.sql); err == nil {
				t.Errorf("%s on audit_records succeeded; it must be rejected", tc.name)
			}
		})
	}
}

// The critical race from §11.9: a credit arriving while the detection sweep is
// claiming the same transaction. Exactly one must win, and the loser must not
// corrupt the result.
func TestCreditRacingDetection(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	const rounds = 40
	var creditWins, sweepWins int
	for i := 0; i < rounds; i++ {
		truncate(t, pool)
		s := NewPostgres(pool)

		if err := s.EnsureTenant(ctx, tenantA, "A", "test"); err != nil {
			t.Fatalf("EnsureTenant: %v", err)
		}
		// Window already closed, so the sweep is eligible to claim it right now.
		if _, err := s.UpsertDebits(ctx, tenantA, []*domain.Transaction{debit(tenantA, "RACE", -time.Second)}); err != nil {
			t.Fatalf("UpsertDebits: %v", err)
		}

		var (
			wg          sync.WaitGroup
			creditErr   error
			claimedRows int
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, creditErr = s.ApplyCredit(ctx, tenantA, "RACE", domain.StatusCompleted, time.Now().UTC())
		}()
		go func() {
			defer wg.Done()
			claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 10)
			if err != nil {
				t.Errorf("ClaimExpired: %v", err)
				return
			}
			claimedRows = len(claimed)
		}()
		wg.Wait()

		final, err := s.Get(ctx, tenantA, "RACE")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		creditWon := creditErr == nil
		sweepWon := claimedRows == 1

		if creditWon == sweepWon {
			t.Fatalf("round %d: credit won=%v sweep won=%v — exactly one must win", i, creditWon, sweepWon)
		}
		switch {
		case creditWon && final.Status != domain.StatusCompleted:
			t.Fatalf("round %d: credit won but status is %s", i, final.Status)
		case sweepWon && final.Status != domain.StatusOrphaned:
			t.Fatalf("round %d: sweep won but status is %s", i, final.Status)
		}
		if creditWon {
			creditWins++
		} else {
			sweepWins++
		}
	}

	// If one side always wins, the schedule is deterministic and this test is
	// asserting nothing about the race it claims to cover.
	t.Logf("credit won %d, sweep won %d of %d rounds", creditWins, sweepWins, rounds)
	if creditWins == 0 || sweepWins == 0 {
		t.Skipf("race never interleaved (credit %d / sweep %d); test had no signal this run",
			creditWins, sweepWins)
	}
}

// Five schedulers competing for the same rows must partition them, never
// double-claim. This is what SKIP LOCKED is load-bearing for.
func TestClaimExpiredIsSafeAcrossSchedulers(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	truncate(t, pool)

	s := NewPostgres(pool)
	if err := s.EnsureTenant(ctx, tenantA, "A", "test"); err != nil {
		t.Fatalf("EnsureTenant: %v", err)
	}

	const total = 50
	batch := make([]*domain.Transaction, 0, total)
	for i := 0; i < total; i++ {
		batch = append(batch, debit(tenantA, "EXP"+itoa(i), -time.Minute))
	}
	if _, err := s.UpsertDebits(ctx, tenantA, batch); err != nil {
		t.Fatalf("UpsertDebits: %v", err)
	}

	const schedulers = 5
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[string]int)
	)
	now := time.Now().UTC()
	for i := 0; i < schedulers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := s.ClaimExpired(ctx, now, total)
			if err != nil {
				t.Errorf("ClaimExpired: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, c := range claimed {
				seen[c.TransactionID]++
			}
		}()
	}
	wg.Wait()

	for id, count := range seen {
		if count != 1 {
			t.Errorf("%s claimed %d times, want exactly 1", id, count)
		}
	}
	if len(seen) != total {
		t.Errorf("claimed %d distinct transactions, want %d", len(seen), total)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
