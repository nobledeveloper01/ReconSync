package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// One suite, run against every implementation, so the in-memory fake cannot
// drift from Postgres and quietly make unit tests meaningless.
func runConformance(t *testing.T, newStore func(t *testing.T) Store) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(t *testing.T, s Store)
	}{
		{"UpsertDebits", testUpsertDebits},
		{"UpsertDebitsIsIdempotent", testUpsertIdempotent},
		{"UpsertDedupesWithinBatch", testUpsertDedupesWithinBatch},
		{"UpsertRejectsTenantMismatch", testUpsertTenantMismatch},
		{"GetIsTenantScoped", testGetIsTenantScoped},
		{"ApplyCreditSuccess", testApplyCreditSuccess},
		{"ApplyCreditUnknown", testApplyCreditUnknown},
		{"ApplyCreditFailed", testApplyCreditFailed},
		{"ApplyCreditRejectsReplayOnCompleted", testApplyCreditReplay},
		{"ApplyCreditNotFound", testApplyCreditNotFound},
		{"ClaimExpired", testClaimExpired},
		{"ClaimExpiredIsNotRepeatable", testClaimExpiredOnce},
		{"ClaimExpiredRespectsLimit", testClaimExpiredLimit},
		{"ListByStatus", testListByStatus},
		{"TenantIsolation", testTenantIsolation},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

const (
	tenantA = "tnt_a"
	tenantB = "tnt_b"
)

func seedTenants(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{tenantA, tenantB} {
		if err := s.EnsureTenant(ctx, id, id, "test"); err != nil {
			t.Fatalf("EnsureTenant(%s): %v", id, err)
		}
	}
}

func debit(tenantID, txnID string, window time.Duration) *domain.Transaction {
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

func mustUpsert(t *testing.T, s Store, txns ...*domain.Transaction) UpsertResult {
	t.Helper()
	res, err := s.UpsertDebits(context.Background(), txns[0].TenantID, txns)
	if err != nil {
		t.Fatalf("UpsertDebits: %v", err)
	}
	return res
}

func testUpsertDebits(t *testing.T, s Store) {
	seedTenants(t, s)
	res := mustUpsert(t, s, debit(tenantA, "TX1", time.Minute), debit(tenantA, "TX2", time.Minute))

	if len(res.Inserted) != 2 {
		t.Fatalf("inserted %d, want 2", len(res.Inserted))
	}
	if len(res.Duplicates) != 0 {
		t.Errorf("duplicates %v, want none", res.Duplicates)
	}

	got, err := s.Get(context.Background(), tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusPendingDebit {
		t.Errorf("status = %s, want pending_debit", got.Status)
	}
	if got.AmountMinor != 5_000_000 {
		t.Errorf("amount = %d, want 5000000", got.AmountMinor)
	}
	if got.Currency != "NGN" {
		t.Errorf("currency = %q, want NGN", got.Currency)
	}
	if got.Metadata["channel"] != "mobile" {
		t.Errorf("metadata = %v, want channel=mobile", got.Metadata)
	}
}

func testUpsertIdempotent(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "TX1", time.Minute))

	// Same event again — the normal SDK retry, not an error.
	res := mustUpsert(t, s, debit(tenantA, "TX1", time.Minute))
	if len(res.Inserted) != 0 {
		t.Errorf("inserted %v on replay, want none", res.Inserted)
	}
	if len(res.Duplicates) != 1 {
		t.Errorf("duplicates %v, want 1", res.Duplicates)
	}
}

func testUpsertDedupesWithinBatch(t *testing.T, s Store) {
	seedTenants(t, s)
	res := mustUpsert(t, s, debit(tenantA, "TX1", time.Minute), debit(tenantA, "TX1", time.Minute))
	if len(res.Inserted) != 1 {
		t.Errorf("inserted %v, want exactly 1", res.Inserted)
	}
	if len(res.Duplicates) != 1 {
		t.Errorf("duplicates %v, want 1", res.Duplicates)
	}
}

func testUpsertTenantMismatch(t *testing.T, s Store) {
	seedTenants(t, s)
	rogue := debit(tenantB, "TX1", time.Minute)
	if _, err := s.UpsertDebits(context.Background(), tenantA, []*domain.Transaction{rogue}); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("err = %v, want ErrTenantMismatch", err)
	}
}

func testGetIsTenantScoped(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "TX1", time.Minute))

	// Must be ErrNotFound, never a permission error — 403 confirms existence.
	if _, err := s.Get(context.Background(), tenantB, "TX1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant Get returned %v, want ErrNotFound", err)
	}
}

func testApplyCreditSuccess(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "TX1", time.Minute))

	creditAt := time.Now().UTC()
	got, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusCompleted, creditAt)
	if err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
	if got.CreditAt == nil {
		t.Fatal("credit_at not set")
	}
}

func testApplyCreditUnknown(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "TX1", time.Minute))

	got, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusPendingUnknown, time.Now().UTC())
	if err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}
	if got.Status != domain.StatusPendingUnknown {
		t.Errorf("status = %s, want pending_unknown", got.Status)
	}
	if !got.Status.IsOpen() {
		t.Error("pending_unknown must remain open for the scheduler")
	}
}

func testApplyCreditFailed(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "TX1", time.Minute))

	got, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusOrphaned, time.Now().UTC())
	if err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}
	if got.Status != domain.StatusOrphaned {
		t.Errorf("status = %s, want orphaned", got.Status)
	}
	if got.DetectedAt == nil {
		t.Error("detected_at must be set when a transaction becomes orphaned")
	}
}

func testApplyCreditReplay(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "TX1", time.Minute))

	if _, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusCompleted, time.Now().UTC()); err != nil {
		t.Fatalf("first credit: %v", err)
	}

	// §10: a replayed credit must not move a settled transaction.
	_, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusOrphaned, time.Now().UTC())
	var ite domain.InvalidTransitionError
	if !errors.As(err, &ite) {
		t.Fatalf("replay returned %v (%T), want InvalidTransitionError", err, err)
	}
	if ite.From != domain.StatusCompleted {
		t.Errorf("error From = %s, want completed", ite.From)
	}

	after, err := s.Get(context.Background(), tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if after.Status != domain.StatusCompleted {
		t.Errorf("status is %s after rejected replay, want completed", after.Status)
	}
}

func testApplyCreditNotFound(t *testing.T, s Store) {
	seedTenants(t, s)
	_, err := s.ApplyCredit(context.Background(), tenantA, "NOPE", domain.StatusCompleted, time.Now().UTC())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func testClaimExpired(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s,
		debit(tenantA, "EXPIRED", -time.Minute), // window already closed
		debit(tenantA, "FRESH", time.Hour),
	)

	claimed, err := s.ClaimExpired(context.Background(), time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1", len(claimed))
	}
	if claimed[0].TransactionID != "EXPIRED" {
		t.Errorf("claimed %s, want EXPIRED", claimed[0].TransactionID)
	}
	if claimed[0].Status != domain.StatusOrphaned {
		t.Errorf("status = %s, want orphaned", claimed[0].Status)
	}
	if claimed[0].DetectedAt == nil {
		t.Error("detected_at must be set on claim")
	}

	fresh, err := s.Get(context.Background(), tenantA, "FRESH")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fresh.Status != domain.StatusPendingDebit {
		t.Errorf("in-window transaction moved to %s", fresh.Status)
	}
}

func testClaimExpiredOnce(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "EXPIRED", -time.Minute))
	ctx := context.Background()
	now := time.Now().UTC()

	first, err := s.ClaimExpired(ctx, now, 100)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim got %d, want 1", len(first))
	}

	// Already claimed, so no longer open — a second sweep must not re-detect it.
	second, err := s.ClaimExpired(ctx, now, 100)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("second claim got %d, want 0 — detection is not idempotent", len(second))
	}
}

func testClaimExpiredLimit(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s,
		debit(tenantA, "E1", -3*time.Minute),
		debit(tenantA, "E2", -2*time.Minute),
		debit(tenantA, "E3", -time.Minute),
	)

	claimed, err := s.ClaimExpired(context.Background(), time.Now().UTC(), 2)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed))
	}
	// Oldest deadline first: the transaction closest to its regulatory limit.
	if claimed[0].TransactionID != "E1" {
		t.Errorf("first claimed %s, want E1 (oldest deadline)", claimed[0].TransactionID)
	}
}

func testListByStatus(t *testing.T, s Store) {
	seedTenants(t, s)
	mustUpsert(t, s, debit(tenantA, "TX1", time.Minute), debit(tenantA, "TX2", time.Minute))
	if _, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusCompleted, time.Now().UTC()); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}

	pending, err := s.ListByStatus(context.Background(), tenantA, domain.StatusPendingDebit, 10)
	if err != nil {
		t.Fatalf("ListByStatus: %v", err)
	}
	if len(pending) != 1 || pending[0].TransactionID != "TX2" {
		t.Errorf("pending = %v, want [TX2]", ids(pending))
	}
}

// Required CI gate (§8.1): tenant B must not see tenant A's data by any route.
func testTenantIsolation(t *testing.T, s Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, debit(tenantA, "SECRET", time.Minute))

	if _, err := s.Get(ctx, tenantB, "SECRET"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get as tenant B: %v, want ErrNotFound", err)
	}

	listed, err := s.ListByStatus(ctx, tenantB, domain.StatusPendingDebit, 100)
	if err != nil {
		t.Fatalf("ListByStatus as tenant B: %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("tenant B listed %v, want none", ids(listed))
	}

	if _, err := s.ApplyCredit(ctx, tenantB, "SECRET", domain.StatusCompleted, time.Now().UTC()); !errors.Is(err, ErrNotFound) {
		t.Errorf("ApplyCredit as tenant B: %v, want ErrNotFound", err)
	}

	// And A's record is untouched by B's attempts.
	got, err := s.Get(ctx, tenantA, "SECRET")
	if err != nil {
		t.Fatalf("Get as tenant A: %v", err)
	}
	if got.Status != domain.StatusPendingDebit {
		t.Errorf("tenant B's attempt changed A's transaction to %s", got.Status)
	}
}

func ids(txns []*domain.Transaction) []string {
	out := make([]string, len(txns))
	for i, t := range txns {
		out[i] = t.TransactionID
	}
	return out
}

func TestMemoryStore(t *testing.T) {
	runConformance(t, func(*testing.T) Store { return NewMemory() })
}
