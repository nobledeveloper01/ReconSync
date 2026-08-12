package tests

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

const (
	tenantA = "tnt_a"
	tenantB = "tnt_b"
)

// Mirrors of unexported package limits. If either side changes, the tests that
// probe the boundary fail, which is the intent.
const (
	maxIdentifierLen = 255
	maxMetadataKeys  = 50
	maxScanLen       = 4096
	maxScanDepth     = 8
)

// Torn down in reverse order, then rebuilt, so each run starts from a known
// schema regardless of what the previous one left behind.
var migrationFiles = []string{
	"0004_ingest_health.down.sql",
	"0003_api_key_scopes.down.sql",
	"0002_pending_credits.down.sql",
	"0001_init.down.sql",
	"0001_init.up.sql",
	"0002_pending_credits.up.sql",
	"0003_api_key_scopes.up.sql",
	"0004_ingest_health.up.sql",
}

// testPool connects to the database named by RECONSYNC_TEST_DATABASE_URL and
// resets the schema. Integration tests skip cleanly when it is unset.
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

	for _, name := range migrationFiles {
		sql, err := os.ReadFile(filepath.Join("..", "migrations", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	return pool
}

func truncate(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	// audit_records is excluded: it is append-only and rejects TRUNCATE.
	_, err := pool.Exec(context.Background(),
		`TRUNCATE transactions, pending_credits, reconciliation_rules, webhook_deliveries,
		 webhook_endpoints, api_keys, ingest_health, tenants RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func seedTenants(t *testing.T, s store.Store, ids ...string) {
	t.Helper()
	if len(ids) == 0 {
		ids = []string{tenantA, tenantB}
	}
	ctx := context.Background()
	for _, id := range ids {
		if err := s.EnsureTenant(ctx, id, id, "test"); err != nil {
			t.Fatalf("EnsureTenant(%s): %v", id, err)
		}
	}
}

// newDebitTxn builds a storable transaction whose window opens now and closes
// after the given offset. A negative offset means the window is already shut.
func newDebitTxn(tenantID, txnID string, window time.Duration) *domain.Transaction {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return &domain.Transaction{
		TenantID:             tenantID,
		TransactionID:        txnID,
		IdempotencyKey:       "key-" + tenantID + "-" + txnID,
		TransactionType:      "transfer",
		Provider:             "paystack",
		AmountMinor:          5_000_000,
		Currency:             "NGN",
		Status:               domain.StatusPendingDebit,
		DebitAt:              now,
		ExpectedCompletionAt: now.Add(window),
		CustomerRefHash:      "hash_9931",
		Metadata:             map[string]any{"channel": "mobile"},
	}
}

// newExpiredTxn builds a transaction whose window opened and closed in the past,
// the way a real one does.
//
// newDebitTxn with a negative offset puts expected_completion_at *before*
// debit_at, which never happens in production and inverts any range derived from
// the window. Tests that care about the window itself need this instead.
func newExpiredTxn(tenantID, txnID string, openedAgo, window time.Duration) *domain.Transaction {
	t := newDebitTxn(tenantID, txnID, window)
	t.DebitAt = time.Now().UTC().Add(-openedAgo).Truncate(time.Millisecond)
	t.ExpectedCompletionAt = t.DebitAt.Add(window)
	return t
}

func newCreditEvent(tenantID, txnID string, status domain.CreditStatus) *domain.CreditEvent {
	return &domain.CreditEvent{
		TenantID:          tenantID,
		TransactionID:     txnID,
		IdempotencyKey:    "ck-" + tenantID + "-" + txnID,
		CreditAt:          time.Now().UTC().Truncate(time.Millisecond),
		ProviderReference: "ps_ref_1",
		Status:            status,
	}
}

func newDebitEvent(tenantID, txnID string) *domain.DebitEvent {
	return &domain.DebitEvent{
		TenantID:        tenantID,
		TransactionID:   txnID,
		IdempotencyKey:  "dk-" + tenantID + "-" + txnID,
		TransactionType: "transfer",
		Provider:        "paystack",
		AmountMinor:     5_000_000,
		Currency:        "NGN",
		DebitAt:         time.Now().UTC().Truncate(time.Millisecond),
		CustomerRef:     "usr_9931",
		Metadata:        map[string]any{"channel": "mobile"},
	}
}

func mustUpsert(t *testing.T, s store.Store, txns ...*domain.Transaction) store.UpsertResult {
	t.Helper()
	res, err := s.UpsertDebits(context.Background(), txns[0].TenantID, txns)
	if err != nil {
		t.Fatalf("UpsertDebits: %v", err)
	}
	return res
}

func txnIDs(txns []*domain.Transaction) []string {
	out := make([]string, len(txns))
	for i, t := range txns {
		out[i] = t.TransactionID
	}
	return out
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}
