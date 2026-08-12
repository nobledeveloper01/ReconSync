package rules

import (
	"context"
	"errors"
	"sync"
	"time"
)

// DefaultCacheTTL bounds how stale a rule set can be, and therefore how long a
// change takes to take effect (§3.2 B2 requires within 30s, without a restart).
const DefaultCacheTTL = 15 * time.Second

// Loader reads a tenant's rules from storage.
type Loader func(ctx context.Context, tenantID string) ([]Rule, error)

// Provider resolves a tenant's rule set, caching briefly.
//
// The window is resolved on the ingest path for every debit, so an uncached
// lookup would put a query in front of each one. The cache trades a bounded
// amount of staleness for that, which is the right way round: a rule change
// landing 15 seconds late costs nothing, a query per event costs throughput.
type Provider struct {
	load Loader
	ttl  time.Duration
	now  func() time.Time

	mu     sync.RWMutex
	cached map[string]entry
}

type entry struct {
	set       *Set
	expiresAt time.Time
}

// ProviderOptions configures a Provider.
type ProviderOptions struct {
	TTL time.Duration
	Now func() time.Time
}

// NewProvider builds a caching Provider around a Loader.
func NewProvider(load Loader, opts ProviderOptions) (*Provider, error) {
	if load == nil {
		return nil, errors.New("rules: loader is required")
	}
	p := &Provider{load: load, ttl: opts.TTL, now: opts.Now, cached: make(map[string]entry)}
	if p.ttl <= 0 {
		p.ttl = DefaultCacheTTL
	}
	if p.now == nil {
		p.now = time.Now
	}
	return p, nil
}

// Resolve returns the tenant's current rule set.
func (p *Provider) Resolve(ctx context.Context, tenantID string) (*Set, error) {
	p.mu.RLock()
	e, ok := p.cached[tenantID]
	p.mu.RUnlock()

	if ok && p.now().Before(e.expiresAt) {
		return e.set, nil
	}

	loaded, err := p.load(ctx, tenantID)
	if err != nil {
		// Serving a stale set beats failing the request: the alternative is
		// rejecting a debit because a rules query timed out, which would lose
		// reconciliation coverage over a configuration read.
		if ok {
			return e.set, nil
		}
		return nil, err
	}

	set := NewSet(loaded)
	p.mu.Lock()
	p.cached[tenantID] = entry{set: set, expiresAt: p.now().Add(p.ttl)}
	p.mu.Unlock()
	return set, nil
}

// Invalidate drops a tenant's cached rules so the next resolve reloads.
func (p *Provider) Invalidate(tenantID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cached, tenantID)
}
