package tests

import (
	"context"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/provider"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// answering is a StatusProvider that returns a fixed outcome.
type answering struct {
	name    string
	outcome provider.Outcome
	err     error
}

func (a *answering) Name() string { return a.name }

func (a *answering) Query(context.Context, provider.Ref) (provider.Status, error) {
	return provider.Status{Outcome: a.outcome, Detail: "stub"}, a.err
}

func registryAnswering(t *testing.T, outcome provider.Outcome) *provider.Registry {
	t.Helper()
	r := provider.NewRegistry()
	if err := r.Register(&answering{name: "paystack", outcome: outcome}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	return r
}

// corroboratingSetup builds a store with one expired transaction on a rail, plus
// an endpoint that would receive any reversal.
func corroboratingSetup(t *testing.T) (store.Store, *domain.Transaction) {
	t.Helper()

	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	txn := newExpiredTxn(tenantA, "TX-1", 5*time.Minute, time.Minute)
	txn.Provider = "paystack"
	mustUpsert(t, s, txn)
	return s, txn
}

func sweepWith(t *testing.T, s store.Store, reg *provider.Registry) service.SweepResult {
	t.Helper()

	d, err := service.NewDetector(s, service.DetectorOptions{Providers: reg})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	res, err := d.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	return res
}

func assertNoReversalQueued(t *testing.T, s store.Store) {
	t.Helper()

	deliveries, err := s.ListDeliveries(context.Background(), tenantA, "", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	for _, d := range deliveries {
		if d.EventType == "reversal.triggered" {
			t.Fatal("a reversal was queued when it should not have been")
		}
	}
}

// The rail says the money arrived. Reversing now would take back funds the
// destination already has.
func TestCorroborationSettledSendsNoReversal(t *testing.T) {
	s, _ := corroboratingSetup(t)

	res := sweepWith(t, s, registryAnswering(t, provider.Settled))
	if res.Settled != 1 {
		t.Errorf("settled = %d, want 1", res.Settled)
	}
	if res.Queued != 0 {
		t.Errorf("queued %d webhooks, want 0", res.Queued)
	}

	got, err := s.Get(context.Background(), tenantA, "TX-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
	if got.CreditAt == nil {
		t.Error("credit_at not recorded when the rail confirmed arrival")
	}
	assertNoReversalQueued(t, s)
}

// The rail confirms failure, so the orphan now has evidence behind it.
func TestCorroborationFailedConfirmsTheOrphan(t *testing.T) {
	for _, outcome := range []provider.Outcome{provider.Failed, provider.NotFound} {
		t.Run(string(outcome), func(t *testing.T) {
			s, _ := corroboratingSetup(t)

			res := sweepWith(t, s, registryAnswering(t, outcome))
			if res.Corroborated != 1 {
				t.Errorf("corroborated = %d, want 1", res.Corroborated)
			}
			if res.Queued != 1 {
				t.Errorf("queued = %d, want 1", res.Queued)
			}

			got, err := s.Get(context.Background(), tenantA, "TX-1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Status != domain.StatusReversalPending {
				t.Errorf("status = %s, want reversal_pending", got.Status)
			}
		})
	}
}

// No answer means no verdict. A guess here moves real money.
func TestCorroborationUnknownRaisesInvestigationNotReversal(t *testing.T) {
	s, _ := corroboratingSetup(t)

	res := sweepWith(t, s, registryAnswering(t, provider.Unknown))
	if res.Uncertain != 1 {
		t.Errorf("uncertain = %d, want 1", res.Uncertain)
	}

	got, err := s.Get(context.Background(), tenantA, "TX-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusSuspect {
		t.Errorf("status = %s, want suspect", got.Status)
	}
	assertNoReversalQueued(t, s)
}

// An adapter that errors has told us nothing, so it must behave exactly like
// Unknown rather than letting the sweep fail or guess.
func TestCorroborationAdapterErrorIsTreatedAsUnknown(t *testing.T) {
	s, _ := corroboratingSetup(t)

	r := provider.NewRegistry()
	if err := r.Register(&answering{
		name:    "paystack",
		outcome: provider.Failed,
		err:     context.DeadlineExceeded,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	res := sweepWith(t, s, r)
	if res.Uncertain != 1 {
		t.Errorf("uncertain = %d, want 1", res.Uncertain)
	}
	assertNoReversalQueued(t, s)
}

// A rail with no adapter answers Unknown, which would stop reversals entirely.
// That is why corroboration is opt-in per deployment.
func TestCorroborationUnregisteredRailStopsReversals(t *testing.T) {
	s, _ := corroboratingSetup(t)

	res := sweepWith(t, s, provider.NewRegistry())
	if res.Uncertain != 1 {
		t.Errorf("uncertain = %d, want 1", res.Uncertain)
	}
	assertNoReversalQueued(t, s)
}

// With no registry configured, detection behaves exactly as it did before.
func TestNoRegistryLeavesDetectionUnchanged(t *testing.T) {
	s, _ := corroboratingSetup(t)

	res := sweepWith(t, s, nil)
	if res.Queued != 1 {
		t.Errorf("queued = %d, want 1", res.Queued)
	}
	if res.Settled != 0 || res.Uncertain != 0 || res.Corroborated != 0 {
		t.Errorf("corroboration counters moved with no registry: %+v", res)
	}

	got, err := s.Get(context.Background(), tenantA, "TX-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusReversalPending {
		t.Errorf("status = %s, want reversal_pending", got.Status)
	}
}

// A transaction already heading for a human is not asked about again.
func TestCorroborationSkipsSuspectTransactions(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	ctx := context.Background()

	txn := newExpiredTxn(tenantA, "TX-AMBIG", 5*time.Minute, time.Minute)
	txn.Provider = "paystack"
	mustUpsert(t, s, txn)
	if _, err := s.ApplyCredit(ctx, tenantA, "TX-AMBIG", domain.StatusPendingUnknown, time.Now().UTC()); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}

	// The rail would say settled, but a suspect transaction is not re-asked.
	res := sweepWith(t, s, registryAnswering(t, provider.Settled))
	if res.Settled != 0 {
		t.Errorf("settled = %d, want 0 — suspect transactions are not corroborated", res.Settled)
	}
	if res.Suspect != 1 {
		t.Errorf("suspect = %d, want 1", res.Suspect)
	}

	got, err := s.Get(ctx, tenantA, "TX-AMBIG")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusSuspect {
		t.Errorf("status = %s, want suspect", got.Status)
	}
}

// Once a reversal is queued the transaction is reversal_pending, and a late
// "actually it settled" must not silently cancel something the customer's system
// may already have acted on.
func TestSettledCannotCancelADispatchedReversal(t *testing.T) {
	s, _ := corroboratingSetup(t)
	ctx := context.Background()

	if res := sweepWith(t, s, nil); res.Queued != 1 {
		t.Fatalf("queued = %d, want 1", res.Queued)
	}

	if _, err := s.MarkSettled(ctx, tenantA, "TX-1", time.Now().UTC()); err == nil {
		t.Fatal("a dispatched reversal was cancelled by a late settled answer")
	}

	got, err := s.Get(ctx, tenantA, "TX-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusReversalPending {
		t.Errorf("status = %s, want reversal_pending", got.Status)
	}
}

// A late credit event arriving after detection but before dispatch settles the
// transaction rather than being refused — ADR-0005's second effect.
func TestLateCreditSettlesAnOrphanBeforeDispatch(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()

	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-LATE", 5*time.Minute, time.Minute))
	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Status != domain.StatusOrphaned {
		t.Fatalf("claimed %v, want one orphan", claimed)
	}

	if _, err := s.ApplyCredit(ctx, tenantA, "TX-LATE", domain.StatusCompleted, time.Now().UTC()); err != nil {
		t.Fatalf("late credit on an orphan was refused: %v", err)
	}

	got, err := s.Get(ctx, tenantA, "TX-LATE")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
}
