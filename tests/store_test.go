package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// One suite, run against every implementation, so the in-memory fake cannot
// drift from Postgres and quietly make unit tests meaningless.
func runConformance(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(t *testing.T, s store.Store)
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
		{"ClaimExpiredMovesAmbiguousToSuspect", testClaimExpiredSuspect},
		{"ListByStatus", testListByStatus},
		{"ParkAndPeekCredit", testParkAndPeekCredit},
		{"ParkCreditFirstVerdictWins", testParkCreditFirstWins},
		{"PeekParkedCreditsIsTenantScoped", testPeekParkedTenantScoped},
		{"PeekDoesNotRemove", testPeekDoesNotRemove},
		{"DeleteParkedCreditIsIdempotent", testDeleteParkedIdempotent},
		{"ReversalLifecycle", testReversalLifecycle},
		{"ReversalCompletedRequiresPending", testReversalCompletedRequiresPending},
		{"APIKeyRoundTrip", testAPIKeyRoundTrip},
		{"APIKeyRevocation", testAPIKeyRevocation},
		{"TenantIsolation", testTenantIsolation},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, newStore(t))
		})
	}
}

func TestMemoryStore(t *testing.T) {
	runConformance(t, func(*testing.T) store.Store { return store.NewMemory() })
}

func TestPostgresStore(t *testing.T) {
	pool := testPool(t)
	runConformance(t, func(t *testing.T) store.Store {
		truncate(t, pool)
		return store.NewPostgres(pool)
	})
}

func testUpsertDebits(t *testing.T, s store.Store) {
	seedTenants(t, s)
	res := mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute), newDebitTxn(tenantA, "TX2", time.Minute))

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

func testUpsertIdempotent(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))

	// The same event again — a normal SDK retry, not an error.
	res := mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))
	if len(res.Inserted) != 0 {
		t.Errorf("inserted %v on replay, want none", res.Inserted)
	}
	if len(res.Duplicates) != 1 {
		t.Errorf("duplicates %v, want 1", res.Duplicates)
	}
}

func testUpsertDedupesWithinBatch(t *testing.T, s store.Store) {
	seedTenants(t, s)
	res := mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute), newDebitTxn(tenantA, "TX1", time.Minute))
	if len(res.Inserted) != 1 {
		t.Errorf("inserted %v, want exactly 1", res.Inserted)
	}
	if len(res.Duplicates) != 1 {
		t.Errorf("duplicates %v, want 1", res.Duplicates)
	}
}

func testUpsertTenantMismatch(t *testing.T, s store.Store) {
	seedTenants(t, s)
	rogue := newDebitTxn(tenantB, "TX1", time.Minute)
	if _, err := s.UpsertDebits(context.Background(), tenantA, []*domain.Transaction{rogue}); !errors.Is(err, store.ErrTenantMismatch) {
		t.Fatalf("err = %v, want ErrTenantMismatch", err)
	}
}

func testGetIsTenantScoped(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))

	// Must be ErrNotFound, never a permission error — 403 confirms existence.
	if _, err := s.Get(context.Background(), tenantB, "TX1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cross-tenant Get returned %v, want ErrNotFound", err)
	}
}

func testApplyCreditSuccess(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))

	got, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusCompleted, time.Now().UTC())
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

func testApplyCreditUnknown(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))

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

func testApplyCreditFailed(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))

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

func testApplyCreditReplay(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))

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

func testApplyCreditNotFound(t *testing.T, s store.Store) {
	seedTenants(t, s)
	_, err := s.ApplyCredit(context.Background(), tenantA, "NOPE", domain.StatusCompleted, time.Now().UTC())
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func testClaimExpired(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s,
		newDebitTxn(tenantA, "EXPIRED", -time.Minute),
		newDebitTxn(tenantA, "FRESH", time.Hour),
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

func testClaimExpiredOnce(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
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

func testClaimExpiredLimit(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s,
		newDebitTxn(tenantA, "E1", -3*time.Minute),
		newDebitTxn(tenantA, "E2", -2*time.Minute),
		newDebitTxn(tenantA, "E3", -time.Minute),
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

// An ambiguous transaction that outlives its window becomes suspect, not
// orphaned — reversing a credit that actually succeeded pays the customer twice.
func testClaimExpiredSuspect(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "AMBIG", -time.Minute))

	if _, err := s.ApplyCredit(ctx, tenantA, "AMBIG", domain.StatusPendingUnknown, time.Now().UTC()); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}

	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1", len(claimed))
	}
	if claimed[0].Status != domain.StatusSuspect {
		t.Errorf("status = %s, want suspect", claimed[0].Status)
	}
}

func testListByStatus(t *testing.T, s store.Store) {
	seedTenants(t, s)
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute), newDebitTxn(tenantA, "TX2", time.Minute))
	if _, err := s.ApplyCredit(context.Background(), tenantA, "TX1", domain.StatusCompleted, time.Now().UTC()); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}

	pending, err := s.ListByStatus(context.Background(), tenantA, domain.StatusPendingDebit, 10)
	if err != nil {
		t.Fatalf("ListByStatus: %v", err)
	}
	if len(pending) != 1 || pending[0].TransactionID != "TX2" {
		t.Errorf("pending = %v, want [TX2]", txnIDs(pending))
	}
}

func testParkAndPeekCredit(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	if err := s.ParkCredit(ctx, tenantA, newCreditEvent(tenantA, "TX1", domain.CreditSuccess)); err != nil {
		t.Fatalf("ParkCredit: %v", err)
	}

	got, err := s.PeekParkedCredits(ctx, tenantA, []string{"TX1", "TX-absent"})
	if err != nil {
		t.Fatalf("PeekParkedCredits: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("peeked %d credits, want 1", len(got))
	}
	if got[0].TransactionID != "TX1" || got[0].Status != domain.CreditSuccess {
		t.Errorf("peeked %+v, want TX1/success", got[0])
	}
}

func testParkCreditFirstWins(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	if err := s.ParkCredit(ctx, tenantA, newCreditEvent(tenantA, "TX1", domain.CreditUnknown)); err != nil {
		t.Fatalf("first park: %v", err)
	}
	// A duplicate must not overwrite the verdict already recorded.
	if err := s.ParkCredit(ctx, tenantA, newCreditEvent(tenantA, "TX1", domain.CreditSuccess)); err != nil {
		t.Fatalf("second park: %v", err)
	}

	got, err := s.PeekParkedCredits(ctx, tenantA, []string{"TX1"})
	if err != nil {
		t.Fatalf("PeekParkedCredits: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("peeked %d credits, want 1", len(got))
	}
	if got[0].Status != domain.CreditUnknown {
		t.Errorf("status = %s, want unknown — a duplicate overwrote the first verdict", got[0].Status)
	}
}

func testPeekParkedTenantScoped(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	if err := s.ParkCredit(ctx, tenantA, newCreditEvent(tenantA, "TX1", domain.CreditSuccess)); err != nil {
		t.Fatalf("ParkCredit: %v", err)
	}
	got, err := s.PeekParkedCredits(ctx, tenantB, []string{"TX1"})
	if err != nil {
		t.Fatalf("PeekParkedCredits as tenant B: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("tenant B peeked %d of tenant A's parked credits", len(got))
	}
}

// Peeking must leave the credit in place. If a read removed it, a failed apply
// would lose it and the transaction would later be reversed despite its credit
// having succeeded.
func testPeekDoesNotRemove(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	if err := s.ParkCredit(ctx, tenantA, newCreditEvent(tenantA, "TX1", domain.CreditSuccess)); err != nil {
		t.Fatalf("ParkCredit: %v", err)
	}
	for i := 0; i < 3; i++ {
		got, err := s.PeekParkedCredits(ctx, tenantA, []string{"TX1"})
		if err != nil {
			t.Fatalf("peek %d: %v", i, err)
		}
		if len(got) != 1 {
			t.Fatalf("peek %d returned %d credits, want 1 — peeking consumed it", i, len(got))
		}
	}

	if err := s.DeleteParkedCredit(ctx, tenantA, "TX1"); err != nil {
		t.Fatalf("DeleteParkedCredit: %v", err)
	}
	got, err := s.PeekParkedCredits(ctx, tenantA, []string{"TX1"})
	if err != nil {
		t.Fatalf("peek after delete: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("peeked %d after delete, want 0", len(got))
	}
}

func testDeleteParkedIdempotent(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	// Two workers racing the same resolved credit must both succeed.
	if err := s.DeleteParkedCredit(ctx, tenantA, "TX-absent"); err != nil {
		t.Errorf("deleting an absent parked credit errored: %v", err)
	}
	if err := s.ParkCredit(ctx, tenantA, newCreditEvent(tenantA, "TX1", domain.CreditSuccess)); err != nil {
		t.Fatalf("ParkCredit: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := s.DeleteParkedCredit(ctx, tenantA, "TX1"); err != nil {
			t.Errorf("delete %d: %v", i, err)
		}
	}
}

// orphaned -> reversal_pending -> reversal_completed, the full §4.2 tail.
func testReversalLifecycle(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", -time.Minute))

	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Status != domain.StatusOrphaned {
		t.Fatalf("claimed %d, want 1 orphaned", len(claimed))
	}

	triggered := time.Now().UTC()
	pending, err := s.MarkReversalPending(ctx, tenantA, "TX1", triggered)
	if err != nil {
		t.Fatalf("MarkReversalPending: %v", err)
	}
	if pending.Status != domain.StatusReversalPending {
		t.Errorf("status = %s, want reversal_pending", pending.Status)
	}
	if pending.ReversalTriggeredAt == nil {
		t.Error("reversal_triggered_at not set")
	}

	done, err := s.MarkReversalCompleted(ctx, tenantA, "TX1", time.Now().UTC())
	if err != nil {
		t.Fatalf("MarkReversalCompleted: %v", err)
	}
	if done.Status != domain.StatusReversalCompleted {
		t.Errorf("status = %s, want reversal_completed", done.Status)
	}
	if done.ReversalCompletedAt == nil {
		t.Error("reversal_completed_at not set")
	}
	// Terminal: a replayed confirmation must not move it again.
	if _, err := s.MarkReversalCompleted(ctx, tenantA, "TX1", time.Now().UTC()); err == nil {
		t.Error("a second reversal confirmation was accepted")
	}
}

func testReversalCompletedRequiresPending(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "TX1", time.Minute))

	// Still pending_debit: confirming a reversal nobody asked for must be refused.
	_, err := s.MarkReversalCompleted(ctx, tenantA, "TX1", time.Now().UTC())
	var ite domain.InvalidTransitionError
	if !errors.As(err, &ite) {
		t.Fatalf("got %v (%T), want InvalidTransitionError", err, err)
	}

	if _, err := s.MarkReversalCompleted(ctx, tenantA, "ABSENT", time.Now().UTC()); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown transaction: %v, want ErrNotFound", err)
	}
}

func testAPIKeyRoundTrip(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	key, err := auth.Generate(auth.EnvLive)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := s.CreateAPIKey(ctx, tenantA, "key_1", key, []string{"events:write"}); err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	rec, err := s.APIKeyByPrefix(ctx, key.Prefix)
	if err != nil {
		t.Fatalf("APIKeyByPrefix: %v", err)
	}
	if rec.TenantID != tenantA || rec.ID != "key_1" {
		t.Errorf("record = %+v, want tenant %s / key_1", rec, tenantA)
	}
	if rec.Environment != auth.EnvLive {
		t.Errorf("environment = %q, want live", rec.Environment)
	}
	if len(rec.Scopes) != 1 || rec.Scopes[0] != "events:write" {
		t.Errorf("scopes = %v, want [events:write]", rec.Scopes)
	}
	// Only the hash is stored — the secret must not be recoverable.
	if rec.Hash != key.Hash || strings.Contains(rec.Hash, key.Secret) {
		t.Error("stored hash does not match, or contains the plaintext secret")
	}

	if err := s.TouchAPIKey(ctx, "key_1"); err != nil {
		t.Errorf("TouchAPIKey: %v", err)
	}
	if _, err := s.APIKeyByPrefix(ctx, "rs_live_zzzz"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown prefix: %v, want ErrNotFound", err)
	}
}

func testAPIKeyRevocation(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	key, err := auth.Generate(auth.EnvTest)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := s.CreateAPIKey(ctx, tenantA, "key_1", key, nil); err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	// Another tenant must not be able to revoke this key.
	if err := s.RevokeAPIKey(ctx, tenantB, "key_1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("cross-tenant revoke: %v, want ErrNotFound", err)
	}

	if err := s.RevokeAPIKey(ctx, tenantA, "key_1"); err != nil {
		t.Fatalf("RevokeAPIKey: %v", err)
	}
	rec, err := s.APIKeyByPrefix(ctx, key.Prefix)
	if err != nil {
		t.Fatalf("APIKeyByPrefix: %v", err)
	}
	if rec.RevokedAt == nil {
		t.Error("revoked_at not set after revocation")
	}

	// Revoking twice must not silently succeed and look like a fresh revocation.
	if err := s.RevokeAPIKey(ctx, tenantA, "key_1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("second revoke: %v, want ErrNotFound", err)
	}
}

// Required CI gate (§8.1): tenant B must not reach tenant A's data by any route.
func testTenantIsolation(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "SECRET", time.Minute))

	if _, err := s.Get(ctx, tenantB, "SECRET"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get as tenant B: %v, want ErrNotFound", err)
	}

	listed, err := s.ListByStatus(ctx, tenantB, domain.StatusPendingDebit, 100)
	if err != nil {
		t.Fatalf("ListByStatus as tenant B: %v", err)
	}
	if len(listed) != 0 {
		t.Errorf("tenant B listed %v, want none", txnIDs(listed))
	}

	if _, err := s.ApplyCredit(ctx, tenantB, "SECRET", domain.StatusCompleted, time.Now().UTC()); !errors.Is(err, store.ErrNotFound) {
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
// claiming the same transaction. Exactly one must win.
func TestCreditRacingDetection(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	const rounds = 40
	var creditWins, sweepWins int

	for i := 0; i < rounds; i++ {
		truncate(t, pool)
		s := store.NewPostgres(pool)
		seedTenants(t, s, tenantA)

		// Window already closed, so the sweep is eligible immediately.
		if _, err := s.UpsertDebits(ctx, tenantA, []*domain.Transaction{newDebitTxn(tenantA, "RACE", -time.Second)}); err != nil {
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
		t.Skipf("race never interleaved (credit %d / sweep %d); no signal this run", creditWins, sweepWins)
	}
}

// Five schedulers competing for the same rows must partition them, never
// double-claim. This is what SKIP LOCKED is load-bearing for.
func TestClaimExpiredIsSafeAcrossSchedulers(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	truncate(t, pool)

	s := store.NewPostgres(pool)
	seedTenants(t, s, tenantA)

	const total = 50
	batch := make([]*domain.Transaction, 0, total)
	for i := 0; i < total; i++ {
		batch = append(batch, newDebitTxn(tenantA, fmt.Sprintf("EXP%03d", i), -time.Minute))
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
