package tests

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
)

// countingLoader records how often storage was actually hit.
type countingLoader struct {
	mu    sync.Mutex
	loads int
	set   []rules.Rule
	err   error
}

func (l *countingLoader) load(context.Context, string) ([]rules.Rule, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.loads++
	if l.err != nil {
		return nil, l.err
	}
	return l.set, nil
}

func (l *countingLoader) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.loads
}

func (l *countingLoader) setRules(rs []rules.Rule, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.set, l.err = rs, err
}

func transferTxn() *domain.Transaction {
	return &domain.Transaction{TransactionType: "transfer", Provider: "paystack", Currency: "NGN"}
}

func TestProviderRequiresLoader(t *testing.T) {
	if _, err := rules.NewProvider(nil, rules.ProviderOptions{}); err == nil {
		t.Error("accepted a nil loader")
	}
}

// The window is resolved for every debit, so an uncached provider would put a
// query in front of each one.
func TestProviderCachesWithinTTL(t *testing.T) {
	loader := &countingLoader{set: []rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 120, Enabled: true},
	}}
	p, err := rules.NewProvider(loader.load, rules.ProviderOptions{TTL: time.Minute})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	for i := 0; i < 50; i++ {
		set, err := p.Resolve(context.Background(), tenantA)
		if err != nil {
			t.Fatalf("Resolve %d: %v", i, err)
		}
		if got := set.Resolve(transferTxn()).Window; got != 120*time.Second {
			t.Fatalf("window = %s, want 120s", got)
		}
	}

	if loader.count() != 1 {
		t.Errorf("%d loads for 50 resolves, want 1", loader.count())
	}
}

func TestProviderReloadsAfterTTL(t *testing.T) {
	loader := &countingLoader{set: []rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 120, Enabled: true},
	}}

	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	p, err := rules.NewProvider(loader.load, rules.ProviderOptions{
		TTL: 15 * time.Second,
		Now: func() time.Time { return time.Unix(0, now.Load()) },
	})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if _, err := p.Resolve(context.Background(), tenantA); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	// A rule change must take effect without a restart.
	loader.setRules([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 45, Enabled: true},
	}, nil)
	now.Add(int64(20 * time.Second))

	set, err := p.Resolve(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("Resolve after TTL: %v", err)
	}
	if got := set.Resolve(transferTxn()).Window; got != 45*time.Second {
		t.Errorf("window = %s, want 45s — the change did not take effect", got)
	}
	if loader.count() != 2 {
		t.Errorf("%d loads, want 2", loader.count())
	}
}

// Rejecting a debit because a configuration read timed out would lose
// reconciliation coverage over something that does not matter.
func TestProviderServesStaleWhenTheLoaderFails(t *testing.T) {
	loader := &countingLoader{set: []rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 120, Enabled: true},
	}}

	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	p, err := rules.NewProvider(loader.load, rules.ProviderOptions{
		TTL: 15 * time.Second,
		Now: func() time.Time { return time.Unix(0, now.Load()) },
	})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if _, err := p.Resolve(context.Background(), tenantA); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	loader.setRules(nil, errors.New("database unreachable"))
	now.Add(int64(time.Minute))

	set, err := p.Resolve(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("Resolve with a failing loader: %v — should have served the cached set", err)
	}
	if got := set.Resolve(transferTxn()).Window; got != 120*time.Second {
		t.Errorf("window = %s, want the cached 120s", got)
	}
}

// With nothing cached there is no safe fallback, so the error must surface.
func TestProviderFailsWhenNothingIsCached(t *testing.T) {
	loader := &countingLoader{err: errors.New("database unreachable")}
	p, err := rules.NewProvider(loader.load, rules.ProviderOptions{})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if _, err := p.Resolve(context.Background(), tenantA); err == nil {
		t.Error("a cold cache with a failing loader returned no error")
	}
}

func TestProviderInvalidate(t *testing.T) {
	loader := &countingLoader{set: []rules.Rule{{ID: 1, WindowSeconds: 120, Enabled: true}}}
	p, err := rules.NewProvider(loader.load, rules.ProviderOptions{TTL: time.Hour})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if _, err := p.Resolve(context.Background(), tenantA); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	p.Invalidate(tenantA)
	if _, err := p.Resolve(context.Background(), tenantA); err != nil {
		t.Fatalf("Resolve after Invalidate: %v", err)
	}

	if loader.count() != 2 {
		t.Errorf("%d loads, want 2 — Invalidate did not drop the entry", loader.count())
	}
}

func TestProviderIsPerTenant(t *testing.T) {
	loader := &countingLoader{set: []rules.Rule{{ID: 1, WindowSeconds: 120, Enabled: true}}}
	p, err := rules.NewProvider(loader.load, rules.ProviderOptions{TTL: time.Hour})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	for _, tenant := range []string{tenantA, tenantB, tenantA, tenantB} {
		if _, err := p.Resolve(context.Background(), tenant); err != nil {
			t.Fatalf("Resolve(%s): %v", tenant, err)
		}
	}
	// One load each, not one shared entry across tenants.
	if loader.count() != 2 {
		t.Errorf("%d loads for two tenants, want 2", loader.count())
	}
}

func TestProviderIsConcurrencySafe(t *testing.T) {
	loader := &countingLoader{set: []rules.Rule{{ID: 1, WindowSeconds: 120, Enabled: true}}}
	p, err := rules.NewProvider(loader.load, rules.ProviderOptions{TTL: time.Millisecond})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if _, err := p.Resolve(context.Background(), tenantA); err != nil {
					t.Errorf("Resolve: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
