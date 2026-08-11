package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/correlate"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

func newEngine(t *testing.T, ruleSet *rules.Set) (*correlate.Engine, store.Store) {
	t.Helper()

	s := store.NewMemory()
	seedTenants(t, s, tenantA)
	if ruleSet == nil {
		ruleSet = rules.NewSet(nil)
	}

	e, err := correlate.New(s, correlate.Options{
		Rules: func(context.Context, string) (*rules.Set, error) { return ruleSet, nil },
		Salt:  func(_ context.Context, id string) (string, error) { return "salt_for_" + id, nil },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return e, s
}

func TestNewRequiresDependencies(t *testing.T) {
	ok := func(context.Context, string) (*rules.Set, error) { return rules.NewSet(nil), nil }
	salt := func(context.Context, string) (string, error) { return "s", nil }

	if _, err := correlate.New(nil, correlate.Options{Rules: ok, Salt: salt}); err == nil {
		t.Error("accepted a nil store")
	}
	if _, err := correlate.New(store.NewMemory(), correlate.Options{Salt: salt}); err == nil {
		t.Error("accepted a nil rule provider")
	}
	if _, err := correlate.New(store.NewMemory(), correlate.Options{Rules: ok}); err == nil {
		t.Error("accepted a nil salt provider")
	}
}

func TestApplyStoresDebit(t *testing.T) {
	e, s := newEngine(t, nil)
	ctx := context.Background()

	res, err := e.Apply(ctx, tenantA, []domain.Event{domain.NewDebitEvent(newDebitEvent(tenantA, "TX1"))})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.DebitsStored != 1 {
		t.Fatalf("stored %d debits, want 1", res.DebitsStored)
	}

	got, err := s.Get(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusPendingDebit {
		t.Errorf("status = %s, want pending_debit", got.Status)
	}
	if w := got.Window(); w != rules.DefaultWindow {
		t.Errorf("window = %s, want %s", w, rules.DefaultWindow)
	}
	// The raw customer reference must never reach storage.
	if got.CustomerRefHash == "usr_9931" || got.CustomerRefHash == "" {
		t.Errorf("customer_ref_hash = %q, want a hash of the reference", got.CustomerRefHash)
	}
}

func TestApplyUsesMatchingRuleWindow(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 90, Action: rules.ActionAutoReverse, Enabled: true},
	})
	e, s := newEngine(t, set)
	ctx := context.Background()

	if _, err := e.Apply(ctx, tenantA, []domain.Event{domain.NewDebitEvent(newDebitEvent(tenantA, "TX1"))}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	got, err := s.Get(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if w := got.Window(); w != 90*time.Second {
		t.Errorf("window = %s, want 90s", w)
	}
}

func TestApplySettlesDebitAndCreditInOneBatch(t *testing.T) {
	e, s := newEngine(t, nil)
	ctx := context.Background()

	// A fast transaction: both legs land in the same flush.
	res, err := e.Apply(ctx, tenantA, []domain.Event{
		domain.NewDebitEvent(newDebitEvent(tenantA, "TX1")),
		domain.NewCreditEvent(newCreditEvent(tenantA, "TX1", domain.CreditSuccess)),
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.DebitsStored != 1 || res.CreditsApplied != 1 {
		t.Fatalf("stored=%d applied=%d, want 1/1", res.DebitsStored, res.CreditsApplied)
	}

	got, err := s.Get(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
}

func TestApplyOrdersDebitsBeforeCreditsWithinABatch(t *testing.T) {
	e, s := newEngine(t, nil)
	ctx := context.Background()

	// Credit listed first, but it must still correlate — position in a batch is
	// arrival order, not causal order.
	res, err := e.Apply(ctx, tenantA, []domain.Event{
		domain.NewCreditEvent(newCreditEvent(tenantA, "TX1", domain.CreditSuccess)),
		domain.NewDebitEvent(newDebitEvent(tenantA, "TX1")),
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.CreditsApplied != 1 {
		t.Fatalf("applied %d credits, want 1 (parked=%d)", res.CreditsApplied, res.CreditsParked)
	}

	got, err := s.Get(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}
}

func TestApplyParksCreditUntilDebitArrives(t *testing.T) {
	e, s := newEngine(t, nil)
	ctx := context.Background()

	// Credit arrives in its own earlier batch (§3.2 A2).
	res, err := e.Apply(ctx, tenantA, []domain.Event{domain.NewCreditEvent(newCreditEvent(tenantA, "TX1", domain.CreditSuccess))})
	if err != nil {
		t.Fatalf("Apply credit: %v", err)
	}
	if res.CreditsParked != 1 {
		t.Fatalf("parked %d, want 1", res.CreditsParked)
	}
	if _, err := s.Get(ctx, tenantA, "TX1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a parked credit created a transaction: %v", err)
	}

	// The debit turns up later and the parked credit is applied with it.
	res, err = e.Apply(ctx, tenantA, []domain.Event{domain.NewDebitEvent(newDebitEvent(tenantA, "TX1"))})
	if err != nil {
		t.Fatalf("Apply debit: %v", err)
	}
	if res.DebitsStored != 1 || res.CreditsApplied != 1 {
		t.Fatalf("stored=%d applied=%d, want 1/1", res.DebitsStored, res.CreditsApplied)
	}

	got, err := s.Get(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed — the parked credit was not applied", got.Status)
	}

	// The parked credit must be gone once resolved, not applied twice.
	parked, err := s.PeekParkedCredits(ctx, tenantA, []string{"TX1"})
	if err != nil {
		t.Fatalf("PeekParkedCredits: %v", err)
	}
	if len(parked) != 0 {
		t.Errorf("%d credits still parked after being applied", len(parked))
	}
}

func TestApplyCreditVerdicts(t *testing.T) {
	cases := map[domain.CreditStatus]domain.Status{
		domain.CreditSuccess: domain.StatusCompleted,
		domain.CreditUnknown: domain.StatusPendingUnknown,
		domain.CreditFailed:  domain.StatusOrphaned, // ADR-0001
	}

	for verdict, want := range cases {
		t.Run(string(verdict), func(t *testing.T) {
			e, s := newEngine(t, nil)
			ctx := context.Background()

			if _, err := e.Apply(ctx, tenantA, []domain.Event{
				domain.NewDebitEvent(newDebitEvent(tenantA, "TX1")),
				domain.NewCreditEvent(newCreditEvent(tenantA, "TX1", verdict)),
			}); err != nil {
				t.Fatalf("Apply: %v", err)
			}

			got, err := s.Get(ctx, tenantA, "TX1")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Status != want {
				t.Errorf("status = %s, want %s", got.Status, want)
			}
		})
	}
}

func TestApplyIgnoresReplayedCreditOnSettledTransaction(t *testing.T) {
	e, s := newEngine(t, nil)
	ctx := context.Background()

	if _, err := e.Apply(ctx, tenantA, []domain.Event{
		domain.NewDebitEvent(newDebitEvent(tenantA, "TX1")),
		domain.NewCreditEvent(newCreditEvent(tenantA, "TX1", domain.CreditSuccess)),
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// §10: a replayed credit must not move a settled transaction, and must not
	// fail the batch either.
	replay := newCreditEvent(tenantA, "TX1", domain.CreditFailed)
	res, err := e.Apply(ctx, tenantA, []domain.Event{domain.NewCreditEvent(replay)})
	if err != nil {
		t.Fatalf("Apply replay: %v", err)
	}
	if res.CreditsIgnored != 1 {
		t.Errorf("ignored %d, want 1", res.CreditsIgnored)
	}

	got, err := s.Get(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s after replay, want completed", got.Status)
	}
}

func TestApplyDeduplicatesRepeatedDebit(t *testing.T) {
	e, _ := newEngine(t, nil)
	ctx := context.Background()

	if _, err := e.Apply(ctx, tenantA, []domain.Event{domain.NewDebitEvent(newDebitEvent(tenantA, "TX1"))}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	res, err := e.Apply(ctx, tenantA, []domain.Event{domain.NewDebitEvent(newDebitEvent(tenantA, "TX1"))})
	if err != nil {
		t.Fatalf("Apply replay: %v", err)
	}
	if res.DebitsStored != 0 || res.DebitsDuplicate != 1 {
		t.Errorf("stored=%d duplicate=%d, want 0/1", res.DebitsStored, res.DebitsDuplicate)
	}
}

func TestApplyRejectsBadEventsWithoutFailingTheBatch(t *testing.T) {
	e, s := newEngine(t, nil)
	ctx := context.Background()

	bad := newDebitEvent(tenantA, "TX-BAD")
	bad.AmountMinor = 0

	carded := newDebitEvent(tenantA, "TX-CARD")
	carded.Metadata = map[string]any{"note": "4111 1111 1111 1111"}

	wrongTenant := newDebitEvent(tenantA, "TX-OTHER")
	wrongTenant.TenantID = "tnt_other"

	res, err := e.Apply(ctx, tenantA, []domain.Event{
		domain.NewDebitEvent(bad),
		domain.NewDebitEvent(carded),
		domain.NewDebitEvent(wrongTenant),
		{}, // empty envelope
		domain.NewDebitEvent(newDebitEvent(tenantA, "TX-GOOD")),
	})
	if err != nil {
		t.Fatalf("Apply returned a batch error for per-event problems: %v", err)
	}
	if len(res.Rejections) != 4 {
		t.Errorf("rejections = %d, want 4", len(res.Rejections))
	}
	// The valid event in the batch must still be stored.
	if res.DebitsStored != 1 {
		t.Errorf("stored %d, want 1 — a bad event discarded the good one", res.DebitsStored)
	}
	if _, err := s.Get(ctx, tenantA, "TX-GOOD"); err != nil {
		t.Errorf("valid event not stored: %v", err)
	}
	for _, id := range []string{"TX-BAD", "TX-CARD", "TX-OTHER"} {
		if _, err := s.Get(ctx, tenantA, id); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%s was stored despite rejection", id)
		}
	}
}

func TestApplyEmptyBatch(t *testing.T) {
	e, _ := newEngine(t, nil)
	res, err := e.Apply(context.Background(), tenantA, nil)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.DebitsStored != 0 || len(res.Rejections) != 0 {
		t.Errorf("empty batch produced %+v", res)
	}
}

func TestHashCustomerRef(t *testing.T) {
	if got := correlate.HashCustomerRef("salt", ""); got != "" {
		t.Errorf("empty ref hashed to %q, want empty", got)
	}

	a := correlate.HashCustomerRef("salt_a", "usr_1")
	b := correlate.HashCustomerRef("salt_b", "usr_1")
	if a == b {
		t.Error("the same reference hashed identically under different tenant salts")
	}
	if a != correlate.HashCustomerRef("salt_a", "usr_1") {
		t.Error("hash is not deterministic")
	}
	if a == "usr_1" {
		t.Error("hash returned the plaintext")
	}
}
