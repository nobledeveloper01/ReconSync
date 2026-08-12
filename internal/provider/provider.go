// Package provider asks the rail what actually happened, instead of inferring
// failure from silence.
//
// Detection concludes a transaction failed because no credit event arrived. That
// is the weakest evidence in the system: silence has several causes and only one
// of them is failure. A provider that can be asked directly turns "we heard
// nothing" into "we asked, and here is what we found".
package provider

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Outcome is what the rail says about a transaction.
type Outcome string

const (
	// Settled means the provider confirms the money arrived. The transaction
	// must not be reversed.
	Settled Outcome = "settled"

	// Failed means the provider confirms the credit leg did not happen.
	Failed Outcome = "failed"

	// NotFound means the provider has no record of it at all, which for a
	// transfer we believe we initiated is itself evidence of failure.
	NotFound Outcome = "not_found"

	// Unknown means we could not get an answer — unreachable, timed out,
	// malformed, or a status we do not recognise.
	//
	// Unknown is a first-class outcome, never an error to be swallowed. Every
	// failure path in every adapter must produce Unknown rather than guessing,
	// because a guess here becomes a real money movement.
	Unknown Outcome = "unknown"
)

// Conclusive reports whether an outcome is strong enough to act on.
func (o Outcome) Conclusive() bool {
	return o == Settled || o == Failed || o == NotFound
}

func (o Outcome) String() string { return string(o) }

// Ref identifies the transaction being asked about. Providers accept different
// identifiers, so all of them travel together and each adapter picks what it
// needs.
type Ref struct {
	TenantID      string
	TransactionID string
	Provider      string
	ProviderRef   string
	AmountMinor   int64
	Currency      string
}

// Status is one provider's answer.
type Status struct {
	Outcome    Outcome
	Provider   string
	Reference  string
	ObservedAt time.Time

	// Detail is a short human-readable note for the evidence trail. It must
	// never carry the raw response, which may contain customer data.
	Detail string
}

// StatusProvider answers the only question that matters: did the credit leg
// actually happen?
//
// Implementations must never return an error and a conclusive outcome together,
// and must return Unknown for anything they are not certain about.
type StatusProvider interface {
	Name() string
	Query(ctx context.Context, ref Ref) (Status, error)
}

// ErrNoProvider means nothing is registered for that rail.
var ErrNoProvider = errors.New("provider: no status provider registered")

// Registry maps a rail name to its adapter.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]StatusProvider
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]StatusProvider)}
}

// Register adds an adapter. A second registration for the same name replaces
// the first, so configuration reloads are not an error.
func (r *Registry) Register(p StatusProvider) error {
	if p == nil {
		return errors.New("provider: cannot register nil")
	}
	if p.Name() == "" {
		return errors.New("provider: adapter has no name")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[p.Name()] = p
	return nil
}

// Names lists the registered rails, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.providers))
	for name := range r.providers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Query asks the adapter for a transaction's rail.
//
// With no adapter registered the answer is Unknown, not an error: a tenant using
// a rail we cannot query is a normal state, and it must degrade to "we do not
// know" rather than break the sweep.
func (r *Registry) Query(ctx context.Context, ref Ref) Status {
	r.mu.RLock()
	p, ok := r.providers[ref.Provider]
	r.mu.RUnlock()

	if !ok {
		return Status{
			Outcome:    Unknown,
			Provider:   ref.Provider,
			ObservedAt: time.Now().UTC(),
			Detail:     "no status provider registered for this rail",
		}
	}

	status, err := p.Query(ctx, ref)
	if err != nil {
		// An adapter that errors has told us nothing. Never let that become a
		// verdict.
		return Status{
			Outcome:    Unknown,
			Provider:   ref.Provider,
			ObservedAt: time.Now().UTC(),
			Detail:     "provider query failed: " + err.Error(),
		}
	}

	// Defensive: an adapter returning an unrecognised outcome is a bug, and the
	// safe reading of a bug is that we do not know.
	switch status.Outcome {
	case Settled, Failed, NotFound, Unknown:
	default:
		return Status{
			Outcome:    Unknown,
			Provider:   ref.Provider,
			ObservedAt: time.Now().UTC(),
			Detail:     "adapter returned an unrecognised outcome",
		}
	}

	if status.Provider == "" {
		status.Provider = ref.Provider
	}
	if status.ObservedAt.IsZero() {
		status.ObservedAt = time.Now().UTC()
	}
	return status
}
