package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/correlate"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// The whole path: Submit -> worker batching -> per-tenant grouping ->
// correlation -> Postgres.
func TestPipelineThroughCorrelationToPostgres(t *testing.T) {
	pool := testPool(t)
	truncate(t, pool)
	s := store.NewPostgres(pool)
	ctx := context.Background()

	const (
		e2eA = "tnt_e2e_a"
		e2eB = "tnt_e2e_b"
	)
	seedTenants(t, s, e2eA, e2eB)

	ruleSet := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 120, Action: rules.ActionAutoReverse, Enabled: true},
	})
	engine, err := correlate.New(s, correlate.Options{
		Rules: func(context.Context, string) (*rules.Set, error) { return ruleSet, nil },
		Salt:  func(_ context.Context, id string) (string, error) { return "salt_" + id, nil },
	})
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}

	var (
		mu       sync.Mutex
		batchErr error
	)
	p, err := pipeline.New(pipeline.HandlerFunc(
		func(ctx context.Context, tenantID string, events []domain.Event) error {
			res, err := engine.Apply(ctx, tenantID, events)
			if err != nil {
				return err
			}
			if len(res.Rejections) > 0 {
				mu.Lock()
				batchErr = fmt.Errorf("unexpected rejection: %v", res.Rejections[0].Err)
				mu.Unlock()
			}
			return nil
		}), pipeline.Config{
		Workers:       4,
		BatchSize:     25,
		FlushInterval: 20 * time.Millisecond,
		BufferSize:    4000,
	})
	if err != nil {
		t.Fatalf("New pipeline: %v", err)
	}
	p.Start(ctx)

	// 200 transactions per tenant: even ones settle, odd ones stay open.
	const perTenant = 200
	now := time.Now().UTC().Truncate(time.Millisecond)

	for _, tenantID := range []string{e2eA, e2eB} {
		for i := 0; i < perTenant; i++ {
			txnID := fmt.Sprintf("TX-%s-%03d", tenantID, i)

			d := newDebitEvent(tenantID, txnID)
			d.DebitAt = now
			d.AmountMinor = int64(1000 + i)
			if err := p.Submit(domain.NewDebitEvent(d)); err != nil {
				t.Fatalf("submit debit: %v", err)
			}

			if i%2 == 0 {
				c := newCreditEvent(tenantID, txnID, domain.CreditSuccess)
				c.CreditAt = now.Add(time.Second)
				if err := p.Submit(domain.NewCreditEvent(c)); err != nil {
					t.Fatalf("submit credit: %v", err)
				}
			}
		}
	}

	p.Close()

	mu.Lock()
	if batchErr != nil {
		mu.Unlock()
		t.Fatalf("correlation rejected an event: %v", batchErr)
	}
	mu.Unlock()

	stats := p.Stats()
	if stats.Dropped != 0 {
		t.Errorf("dropped %d events", stats.Dropped)
	}
	if stats.HandlerErrors != 0 {
		t.Errorf("%d handler errors", stats.HandlerErrors)
	}

	for _, tenantID := range []string{e2eA, e2eB} {
		completed, err := s.ListByStatus(ctx, tenantID, domain.StatusCompleted, 1000)
		if err != nil {
			t.Fatalf("ListByStatus: %v", err)
		}
		if len(completed) != perTenant/2 {
			t.Errorf("%s: %d completed, want %d", tenantID, len(completed), perTenant/2)
		}

		pending, err := s.ListByStatus(ctx, tenantID, domain.StatusPendingDebit, 1000)
		if err != nil {
			t.Fatalf("ListByStatus: %v", err)
		}
		if len(pending) != perTenant/2 {
			t.Errorf("%s: %d still pending, want %d", tenantID, len(pending), perTenant/2)
		}
	}

	// The rule window must have been applied, not the default.
	sample, err := s.Get(ctx, e2eA, fmt.Sprintf("TX-%s-001", e2eA))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if w := sample.Window(); w != 120*time.Second {
		t.Errorf("window = %s, want 120s", w)
	}

	// And the open half must be detectable once the window closes.
	claimed, err := s.ClaimExpired(ctx, now.Add(121*time.Second), 1000)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != perTenant {
		t.Errorf("claimed %d orphans across both tenants, want %d", len(claimed), perTenant)
	}
	for _, c := range claimed {
		if c.Status != domain.StatusOrphaned {
			t.Fatalf("%s claimed as %s, want orphaned", c.TransactionID, c.Status)
		}
	}
}

// A credit that arrives in an earlier batch than its debit must still settle.
func TestOutOfOrderCreditSettlesAgainstPostgres(t *testing.T) {
	pool := testPool(t)
	truncate(t, pool)
	s := store.NewPostgres(pool)
	ctx := context.Background()

	const tenantID = "tnt_ooo"
	seedTenants(t, s, tenantID)

	engine, err := correlate.New(s, correlate.Options{
		Rules: func(context.Context, string) (*rules.Set, error) { return rules.NewSet(nil), nil },
		Salt:  func(context.Context, string) (string, error) { return "salt", nil },
	})
	if err != nil {
		t.Fatalf("New engine: %v", err)
	}

	apply := func(events ...domain.Event) correlate.Result {
		t.Helper()
		res, err := engine.Apply(ctx, tenantID, events)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		return res
	}

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Batch 1: the credit alone. Nothing to correlate against yet.
	c := newCreditEvent(tenantID, "TX-OOO", domain.CreditSuccess)
	c.CreditAt = now
	if res := apply(domain.NewCreditEvent(c)); res.CreditsParked != 1 {
		t.Fatalf("parked %d, want 1", res.CreditsParked)
	}

	// Batch 2: the debit turns up and the parked credit settles it.
	d := newDebitEvent(tenantID, "TX-OOO")
	d.DebitAt = now.Add(-time.Second)
	res := apply(domain.NewDebitEvent(d))
	if res.DebitsStored != 1 || res.CreditsApplied != 1 {
		t.Fatalf("stored=%d applied=%d, want 1/1", res.DebitsStored, res.CreditsApplied)
	}

	got, err := s.Get(ctx, tenantID, "TX-OOO")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed", got.Status)
	}

	// It must not then be detectable as an orphan.
	claimed, err := s.ClaimExpired(ctx, now.Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	for _, cl := range claimed {
		if cl.TransactionID == "TX-OOO" {
			t.Error("a settled transaction was claimed as an orphan")
		}
	}
}
