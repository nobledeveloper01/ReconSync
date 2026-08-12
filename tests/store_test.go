package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
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
		{"MarkSettledClosesAnOrphan", testMarkSettled},
		{"MarkUncertainRaisesInvestigation", testMarkUncertain},
		{"ReversalCompletedRequiresPending", testReversalCompletedRequiresPending},
		{"APIKeyRoundTrip", testAPIKeyRoundTrip},
		{"APIKeyRevocation", testAPIKeyRevocation},
		{"WebhookEndpoints", testWebhookEndpoints},
		{"DeliveryQueue", testDeliveryQueue},
		{"DeliveryLeasePreventsDoubleClaim", testDeliveryLease},
		{"DeliveryReplay", testDeliveryReplay},
		{"RulesRoundTrip", testRulesRoundTrip},
		{"RulesMatchAnyIsStoredAsNull", testRulesMatchAny},
		{"RulesAreTenantScoped", testRulesTenantScoped},
		{"IngestHealthRoundTrip", testIngestHealthRoundTrip},
		{"IngestHealthAccumulates", testIngestHealthAccumulates},
		{"ClaimExpiredRoutesGapToSuspect", testClaimExpiredGapToSuspect},
		{"SilentTenants", testSilentTenants},
		{"LowVolumeTenantIsNotSilent", testLowVolumeTenantIsNotSilent},
		{"SilenceCheckDisabled", testSilenceCheckDisabled},
		{"ClaimExpiredSkipsTenants", testClaimExpiredSkipsTenants},
		{"SyncSilenceEpisodes", testSyncSilenceEpisodes},
		{"SilenceIsDatedFromTheLastEvent", testSilenceIsDatedFromTheLastEvent},
		{"SilenceEpisodesAreIndependent", testSilenceEpisodesAreIndependent},
		{"ReversalClaims", testReversalClaims},
		{"ReleaseAndReclaim", testReleaseAndReclaim},
		{"AuditChainAppend", testAuditChainAppend},
		{"AuditRoundTripsContent", testAuditRoundTripsContent},
		{"AuditChainsAreTenantScoped", testAuditChainsAreTenantScoped},
		{"CheckpointRoundTrip", testCheckpointRoundTrip},
		{"CountByStatus", testCountByStatus},
		{"ListReversalCandidates", testListReversalCandidates},
		{"ProviderStats", testProviderStats},
		{"Exposure", testExposure},
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

// ADR-0005: the rail confirmed arrival, so the orphan closes without a reversal.
func testMarkSettled(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX1", 5*time.Minute, time.Minute))

	if _, err := s.ClaimExpired(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	settled, err := s.MarkSettled(ctx, tenantA, "TX1", time.Now().UTC())
	if err != nil {
		t.Fatalf("MarkSettled: %v", err)
	}
	if settled.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", settled.Status)
	}
	if settled.CreditAt == nil {
		t.Error("credit_at not recorded")
	}

	// Completed is absorbing, so a second answer changes nothing.
	if _, err := s.MarkSettled(ctx, tenantA, "TX1", time.Now().UTC()); err == nil {
		t.Error("a settled transaction was settled again")
	}
}

func testMarkUncertain(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX1", 5*time.Minute, time.Minute))

	if _, err := s.ClaimExpired(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	got, err := s.MarkUncertain(ctx, tenantA, "TX1", time.Now().UTC())
	if err != nil {
		t.Fatalf("MarkUncertain: %v", err)
	}
	if got.Status != domain.StatusSuspect {
		t.Errorf("status = %s, want suspect", got.Status)
	}

	// Once a reversal is dispatched neither answer may cancel it.
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX2", 5*time.Minute, time.Minute))
	if _, err := s.ClaimExpired(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if _, err := s.MarkReversalPending(ctx, tenantA, "TX2", time.Now().UTC()); err != nil {
		t.Fatalf("MarkReversalPending: %v", err)
	}
	if _, err := s.MarkUncertain(ctx, tenantA, "TX2", time.Now().UTC()); err == nil {
		t.Error("a dispatched reversal was downgraded to suspect")
	}
	if _, err := s.MarkSettled(ctx, tenantA, "TX2", time.Now().UTC()); err == nil {
		t.Error("a dispatched reversal was cancelled by a settled answer")
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

func seedEndpoint(t *testing.T, s store.Store, tenantID, id string) {
	t.Helper()
	err := s.CreateEndpoint(context.Background(), tenantID, &store.WebhookEndpoint{
		ID:        id,
		TenantID:  tenantID,
		URL:       "https://" + tenantID + ".example.com/hook",
		SecretRef: "kms://test",
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
}

func testWebhookEndpoints(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	seedEndpoint(t, s, tenantA, "we_a")
	seedEndpoint(t, s, tenantB, "we_b")

	eps, err := s.ListEndpoints(ctx, tenantA)
	if err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	}
	if len(eps) != 1 || eps[0].ID != "we_a" {
		t.Fatalf("tenant A sees %d endpoints, want just we_a", len(eps))
	}
	// The secret itself is never stored, only a reference to it.
	if eps[0].SecretRef != "kms://test" {
		t.Errorf("secret_ref = %q", eps[0].SecretRef)
	}

	rogue := &store.WebhookEndpoint{ID: "we_x", TenantID: tenantB, URL: "https://x/", SecretRef: "kms://x"}
	if err := s.CreateEndpoint(ctx, tenantA, rogue); !errors.Is(err, store.ErrTenantMismatch) {
		t.Errorf("cross-tenant create: %v, want ErrTenantMismatch", err)
	}
}

func testDeliveryQueue(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	seedEndpoint(t, s, tenantA, "we_a")

	id, err := s.EnqueueDelivery(ctx, tenantA, &store.PendingDelivery{
		TenantID:      tenantA,
		EndpointID:    "we_a",
		TransactionID: "TX1",
		EventType:     "reversal.triggered",
		Payload:       []byte(`{"event":"reversal.triggered"}`),
	})
	if err != nil {
		t.Fatalf("EnqueueDelivery: %v", err)
	}

	due, err := s.ClaimDueDeliveries(ctx, time.Now().UTC(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("claimed %d, want 1", len(due))
	}
	if due[0].ID != id || due[0].URL == "" || due[0].SecretRef != "kms://test" {
		t.Errorf("claimed %+v, want the endpoint joined in", due[0])
	}
	// Compared as JSON, not bytes: Postgres stores this column as JSONB and
	// normalises whitespace. That is safe because the signature is computed at
	// send time over the bytes actually transmitted, never over what was queued.
	var payload map[string]any
	if err := json.Unmarshal(due[0].Payload, &payload); err != nil {
		t.Fatalf("stored payload is not valid JSON: %v (%s)", err, due[0].Payload)
	}
	if payload["event"] != "reversal.triggered" {
		t.Errorf("payload = %s", due[0].Payload)
	}

	code := 200
	if err := s.RecordDeliveryOutcome(ctx, id, store.DeliveryOutcome{
		Status: "delivered", ResponseCode: &code, ResponseBody: "ok", DurationMS: 12,
	}); err != nil {
		t.Fatalf("RecordDeliveryOutcome: %v", err)
	}

	delivered, err := s.ListDeliveries(ctx, tenantA, "delivered", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(delivered) != 1 || delivered[0].Attempt != 1 {
		t.Fatalf("delivered = %+v, want one record at attempt 1", delivered)
	}

	// Tenant B must not see it, and an unknown id must not silently succeed.
	if other, err := s.ListDeliveries(ctx, tenantB, "", 10); err != nil || len(other) != 0 {
		t.Errorf("tenant B saw %d deliveries (err=%v)", len(other), err)
	}
	if err := s.RecordDeliveryOutcome(ctx, 999999, store.DeliveryOutcome{Status: "delivered"}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown delivery: %v, want ErrNotFound", err)
	}
}

// The lease is what stops two dispatcher replicas delivering the same webhook.
func testDeliveryLease(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	seedEndpoint(t, s, tenantA, "we_a")

	if _, err := s.EnqueueDelivery(ctx, tenantA, &store.PendingDelivery{
		TenantID: tenantA, EndpointID: "we_a", TransactionID: "TX1",
		EventType: "reversal.triggered", Payload: []byte(`{}`),
	}); err != nil {
		t.Fatalf("EnqueueDelivery: %v", err)
	}

	now := time.Now().UTC()
	first, err := s.ClaimDueDeliveries(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim got %d, want 1", len(first))
	}

	second, err := s.ClaimDueDeliveries(ctx, now, time.Minute, 10)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 0 {
		t.Errorf("second claim got %d, want 0 — the lease did not hold", len(second))
	}

	// Once the lease lapses a worker that died mid-attempt releases its claim.
	lapsed, err := s.ClaimDueDeliveries(ctx, now.Add(2*time.Minute), time.Minute, 10)
	if err != nil {
		t.Fatalf("claim after lease: %v", err)
	}
	if len(lapsed) != 1 {
		t.Errorf("after the lease lapsed got %d, want 1 — a dead worker would strand it", len(lapsed))
	}
}

func testDeliveryReplay(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	seedEndpoint(t, s, tenantA, "we_a")

	id, err := s.EnqueueDelivery(ctx, tenantA, &store.PendingDelivery{
		TenantID: tenantA, EndpointID: "we_a", TransactionID: "TX1",
		EventType: "reversal.triggered", Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("EnqueueDelivery: %v", err)
	}

	// A pending delivery is not replayable — only a dead-lettered one.
	if err := s.ReplayDelivery(ctx, tenantA, id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("replay of a pending delivery: %v, want ErrNotFound", err)
	}

	if err := s.RecordDeliveryOutcome(ctx, id, store.DeliveryOutcome{Status: "dead_letter", DurationMS: 5}); err != nil {
		t.Fatalf("RecordDeliveryOutcome: %v", err)
	}
	if err := s.ReplayDelivery(ctx, tenantB, id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("cross-tenant replay: %v, want ErrNotFound", err)
	}
	if err := s.ReplayDelivery(ctx, tenantA, id); err != nil {
		t.Fatalf("ReplayDelivery: %v", err)
	}

	due, err := s.ClaimDueDeliveries(ctx, time.Now().UTC(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	if len(due) != 1 || due[0].Attempt != 0 {
		t.Errorf("after replay: %d due, attempt %v — want 1 at attempt 0", len(due), due)
	}
}

func testRulesRoundTrip(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	if got, err := s.ListRules(ctx, tenantA); err != nil || len(got) != 0 {
		t.Fatalf("ListRules on a fresh tenant = %v, %v; want empty", got, err)
	}

	id, err := s.CreateRule(ctx, tenantA, &rules.Rule{
		TransactionType: "transfer",
		Provider:        "paystack",
		Currency:        "NGN",
		MinAmountMinor:  rules.Amount(1000),
		MaxAmountMinor:  rules.Amount(500000),
		WindowSeconds:   120,
		Action:          rules.ActionInvestigate,
		Priority:        5,
		Enabled:         true,
	})
	if err != nil {
		t.Fatalf("CreateRule: %v", err)
	}

	got, err := s.ListRules(ctx, tenantA)
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("listed %d rules, want 1", len(got))
	}

	r := got[0]
	if r.ID != id {
		t.Errorf("id = %d, want %d", r.ID, id)
	}
	if r.TransactionType != "transfer" || r.Provider != "paystack" || r.Currency != "NGN" {
		t.Errorf("criteria = %+v", r)
	}
	if r.WindowSeconds != 120 || r.Action != rules.ActionInvestigate || r.Priority != 5 {
		t.Errorf("window/action/priority = %d/%s/%d", r.WindowSeconds, r.Action, r.Priority)
	}
	if r.MinAmountMinor == nil || *r.MinAmountMinor != 1000 {
		t.Errorf("min amount = %v, want 1000", r.MinAmountMinor)
	}
	if r.MaxAmountMinor == nil || *r.MaxAmountMinor != 500000 {
		t.Errorf("max amount = %v, want 500000", r.MaxAmountMinor)
	}

	// The stored rule must resolve the same way the in-memory one does.
	if res := rules.NewSet(got).Resolve(&domain.Transaction{
		TransactionType: "transfer", Provider: "paystack", Currency: "NGN", AmountMinor: 5000,
	}); res.Window != 120*time.Second {
		t.Errorf("resolved window = %s, want 120s", res.Window)
	}

	if err := s.DeleteRule(ctx, tenantA, id); err != nil {
		t.Fatalf("DeleteRule: %v", err)
	}
	if err := s.DeleteRule(ctx, tenantA, id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("second delete: %v, want ErrNotFound", err)
	}
	if got, _ := s.ListRules(ctx, tenantA); len(got) != 0 {
		t.Errorf("listed %d rules after delete, want 0", len(got))
	}
}

// An empty criterion means "matches anything". Currency matters most: the column
// is CHAR(3), so an empty string would be stored as three spaces and match
// nothing ever.
func testRulesMatchAny(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	if _, err := s.CreateRule(ctx, tenantA, &rules.Rule{WindowSeconds: 90, Enabled: true}); err != nil {
		t.Fatalf("CreateRule: %v", err)
	}

	got, err := s.ListRules(ctx, tenantA)
	if err != nil {
		t.Fatalf("ListRules: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("listed %d rules, want 1", len(got))
	}
	for _, field := range []struct{ name, value string }{
		{"transaction_type", got[0].TransactionType},
		{"provider", got[0].Provider},
		{"currency", got[0].Currency},
	} {
		if field.value != "" {
			t.Errorf("%s round-tripped as %q, want empty", field.name, field.value)
		}
	}
	if got[0].Action != rules.ActionAutoReverse {
		t.Errorf("action defaulted to %q, want auto_reverse", got[0].Action)
	}

	// And it must actually match a transaction with unrelated criteria.
	res := rules.NewSet(got).Resolve(&domain.Transaction{
		TransactionType: "bill_payment", Provider: "flutterwave", Currency: "USD", AmountMinor: 99,
	})
	if res.Window != 90*time.Second {
		t.Errorf("wildcard rule resolved to %s, want 90s", res.Window)
	}
}

func testRulesTenantScoped(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	id, err := s.CreateRule(ctx, tenantA, &rules.Rule{WindowSeconds: 60, Enabled: true})
	if err != nil {
		t.Fatalf("CreateRule: %v", err)
	}

	if got, _ := s.ListRules(ctx, tenantB); len(got) != 0 {
		t.Errorf("tenant B listed %d of tenant A's rules", len(got))
	}
	if err := s.DeleteRule(ctx, tenantB, id); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("cross-tenant delete: %v, want ErrNotFound", err)
	}
	if got, _ := s.ListRules(ctx, tenantA); len(got) != 1 {
		t.Error("tenant B's delete removed tenant A's rule")
	}
}

func testIngestHealthRoundTrip(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	bucket := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Minute)

	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantA, Bucket: bucket, Received: 100},
		{TenantID: tenantA, Bucket: bucket.Add(time.Minute), Received: 50, Dropped: 3},
		{TenantID: tenantB, Bucket: bucket, Received: 7},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	// The gap is in the second minute only.
	clean, err := s.HasIngestGap(ctx, tenantA, bucket, bucket)
	if err != nil {
		t.Fatalf("HasIngestGap: %v", err)
	}
	if clean {
		t.Error("reported a gap in a minute that lost nothing")
	}

	gap, err := s.HasIngestGap(ctx, tenantA, bucket, bucket.Add(time.Minute))
	if err != nil {
		t.Fatalf("HasIngestGap: %v", err)
	}
	if !gap {
		t.Error("missed a gap inside the range")
	}

	// Tenant B lost nothing, and must not inherit tenant A's gap.
	otherGap, err := s.HasIngestGap(ctx, tenantB, bucket, bucket.Add(time.Minute))
	if err != nil {
		t.Fatalf("HasIngestGap: %v", err)
	}
	if otherGap {
		t.Error("tenant B inherited tenant A's gap")
	}

	act, err := s.IngestActivity(ctx, tenantA, bucket, bucket.Add(time.Minute))
	if err != nil {
		t.Fatalf("IngestActivity: %v", err)
	}
	if act.Received != 150 {
		t.Errorf("received = %d, want 150", act.Received)
	}
	if act.ActiveBuckets != 2 {
		t.Errorf("active buckets = %d, want 2", act.ActiveBuckets)
	}
}

// Samples carry deltas, so two flushes into the same minute must sum. Two
// replicas writing concurrently depend on this.
func testIngestHealthAccumulates(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	bucket := time.Now().UTC().Truncate(time.Minute)

	for i := 0; i < 3; i++ {
		if err := s.RecordIngestHealth(ctx, []store.IngestSample{
			{TenantID: tenantA, Bucket: bucket, Received: 10, Dropped: 1},
		}); err != nil {
			t.Fatalf("RecordIngestHealth %d: %v", i, err)
		}
	}

	act, err := s.IngestActivity(ctx, tenantA, bucket, bucket)
	if err != nil {
		t.Fatalf("IngestActivity: %v", err)
	}
	if act.Received != 30 {
		t.Errorf("received = %d, want 30 — writes overwrote instead of accumulating", act.Received)
	}
}

// The whole point of B1: a transaction whose window overlaps a hole in our own
// view must not auto-reverse. This runs against both implementations because the
// rule lives in SQL for Postgres and in Go for the fake.
func testClaimExpiredGapToSuspect(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	gapTxn := newExpiredTxn(tenantA, "TX-GAP", 5*time.Minute, time.Minute)
	cleanTxn := newExpiredTxn(tenantA, "TX-CLEAN", 5*time.Minute, time.Minute)
	mustUpsert(t, s, gapTxn, cleanTxn)

	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantA, Bucket: gapTxn.DebitAt, Dropped: 1},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed))
	}

	// Both share the same window, so the gap covers both. What matters is that a
	// gap produces suspect rather than orphaned.
	for _, c := range claimed {
		if c.Status != domain.StatusSuspect {
			t.Errorf("%s = %s, want suspect — a gap covered its window", c.TransactionID, c.Status)
		}
	}
}

func testCountByStatus(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	mustUpsert(t, s,
		newDebitTxn(tenantA, "OPEN-1", time.Hour),
		newDebitTxn(tenantA, "OPEN-2", time.Hour),
		newDebitTxn(tenantA, "SETTLED", time.Hour),
	)
	if _, err := s.ApplyCredit(ctx, tenantA, "SETTLED", domain.StatusCompleted, now); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}
	mustUpsert(t, s, newDebitTxn(tenantB, "OTHER", time.Hour))

	counts, err := s.CountByStatus(ctx, tenantA, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if counts[domain.StatusPendingDebit] != 2 {
		t.Errorf("pending = %d, want 2", counts[domain.StatusPendingDebit])
	}
	if counts[domain.StatusCompleted] != 1 {
		t.Errorf("completed = %d, want 1", counts[domain.StatusCompleted])
	}

	// The period is half-open, so a window that ends before the debit sees none.
	empty, err := s.CountByStatus(ctx, tenantA, now.Add(-48*time.Hour), now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("out-of-period counts = %v, want none", empty)
	}

	// Tenant B's transaction must not appear in tenant A's totals.
	total := 0
	for _, n := range counts {
		total += n
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
}

// Only transactions that were actually detected can be measured against a
// reversal SLA, and fetching only those is what keeps the report cheap.
func testListReversalCandidates(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	mustUpsert(t, s,
		newExpiredTxn(tenantA, "DETECTED", 5*time.Minute, time.Minute),
		newExpiredTxn(tenantA, "DETECTED-2", 4*time.Minute, time.Minute),
		newDebitTxn(tenantA, "HEALTHY", time.Hour),
	)
	if _, err := s.ClaimExpired(ctx, now, 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	got, err := s.ListReversalCandidates(ctx, tenantA, now.Add(-time.Hour), now.Add(time.Hour), 0)
	if err != nil {
		t.Fatalf("ListReversalCandidates: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("candidates = %v, want the two detected", txnIDs(got))
	}
	if got[0].DetectedAt == nil {
		t.Error("candidate has no detected_at")
	}

	// The limit must be exact: the report probes for overflow by asking for one
	// more than it can handle, so a store that returns extra rows would turn a
	// complete report into one falsely marked incomplete, and one that returns
	// too few would hide the overflow entirely.
	capped, err := s.ListReversalCandidates(ctx, tenantA, now.Add(-time.Hour), now.Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("ListReversalCandidates limited: %v", err)
	}
	if len(capped) != 1 {
		t.Errorf("limit 1 returned %d rows", len(capped))
	}

	if other, err := s.ListReversalCandidates(ctx, tenantB, now.Add(-time.Hour), now.Add(time.Hour), 0); err != nil || len(other) != 0 {
		t.Errorf("tenant B saw %d of tenant A's candidates (err=%v)", len(other), err)
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

	// Raw insert rather than AppendAudit: this test is about the database
	// enforcing immutability, not about the chain being well formed.
	_, err := pool.Exec(ctx,
		`INSERT INTO audit_records (tenant_id, seq, event_type, occurred_at, hash)
		 VALUES ('tnt_immutable', 1, 'reversal.triggered', now(), 'sha256:seed')
		 ON CONFLICT (tenant_id, seq) DO NOTHING`)
	if err != nil {
		t.Fatalf("insert audit record: %v", err)
	}

	for _, tc := range []struct{ name, sql string }{
		{"update", `UPDATE audit_records SET hash = 'tampered' WHERE tenant_id = 'tnt_immutable'`},
		{"delete", `DELETE FROM audit_records WHERE tenant_id = 'tnt_immutable'`},
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

		// Both succeeding is legal since ADR-0005 added orphaned -> completed:
		// the sweep claims it, then the credit settles it anyway. That is the
		// point of the edge — a credit that turns up after detection but before
		// dispatch should settle the transaction, not be refused.
		//
		// What must never happen is neither succeeding, which would mean the
		// transaction sat expired and untouched.
		if !creditWon && !sweepWon {
			t.Fatalf("round %d: neither the credit nor the sweep touched the transaction", i)
		}

		// Whatever the interleaving, a credit that succeeded means the money
		// arrived, and the final state must say so.
		switch {
		case creditWon && final.Status != domain.StatusCompleted:
			t.Fatalf("round %d: the credit succeeded but status is %s", i, final.Status)
		case !creditWon && final.Status != domain.StatusOrphaned:
			t.Fatalf("round %d: the credit was refused so the sweep owns it, but status is %s", i, final.Status)
		}
		if creditWon {
			creditWins++
		} else {
			sweepWins++
		}
	}

	// A lopsided split is expected now and is not a defect. Since ADR-0005 the
	// credit is legal from both pending_debit and orphaned, so it succeeds
	// whichever order the two operations land in. What this test still proves is
	// that concurrent access never corrupts the row or leaves it untouched.
	//
	// The "exactly one wins" guard has not disappeared, it has moved: once a
	// reversal is dispatched the transaction is reversal_pending and a credit
	// must be refused. That boundary is covered by
	// TestCreditCannotCancelADispatchedReversalUnderRace below.
	t.Logf("credit won %d, sweep won %d of %d rounds", creditWins, sweepWins, rounds)
}

// The boundary that must still hold absolutely: once a reversal has been
// dispatched, a credit arriving concurrently must never cancel it. The
// customer's system may already have moved the money back.
func TestCreditCannotCancelADispatchedReversalUnderRace(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	const rounds = 40
	for i := 0; i < rounds; i++ {
		truncate(t, pool)
		s := store.NewPostgres(pool)
		seedTenants(t, s, tenantA)

		if _, err := s.UpsertDebits(ctx, tenantA, []*domain.Transaction{
			newExpiredTxn(tenantA, "RACE", 5*time.Minute, time.Minute),
		}); err != nil {
			t.Fatalf("UpsertDebits: %v", err)
		}
		if _, err := s.ClaimExpired(ctx, time.Now().UTC(), 10); err != nil {
			t.Fatalf("ClaimExpired: %v", err)
		}

		var (
			wg          sync.WaitGroup
			creditErr   error
			dispatchErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, creditErr = s.ApplyCredit(ctx, tenantA, "RACE", domain.StatusCompleted, time.Now().UTC())
		}()
		go func() {
			defer wg.Done()
			_, dispatchErr = s.MarkReversalPending(ctx, tenantA, "RACE", time.Now().UTC())
		}()
		wg.Wait()

		final, err := s.Get(ctx, tenantA, "RACE")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}

		// Exactly one must win here, because completed and reversal_pending have
		// no edge between them in either direction.
		if (creditErr == nil) == (dispatchErr == nil) {
			t.Fatalf("round %d: credit err=%v dispatch err=%v — exactly one must win", i, creditErr, dispatchErr)
		}
		switch {
		case creditErr == nil && final.Status != domain.StatusCompleted:
			t.Fatalf("round %d: credit won but status is %s", i, final.Status)
		case dispatchErr == nil && final.Status != domain.StatusReversalPending:
			t.Fatalf("round %d: dispatch won but status is %s", i, final.Status)
		}
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
