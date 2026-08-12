// Package evidence records why a verdict was reached.
//
// A bare "orphaned" tells a receiver nothing about how much to trust it. The
// same word covers "the window closed and we asked the rail, which confirmed the
// transfer failed" and "the window closed and we have no other information at
// all". Those deserve different responses, so the verdict carries its reasoning.
package evidence

import "sort"

// Signal is one observation supporting a verdict.
type Signal struct {
	// Name is a stable identifier a receiver can branch on.
	Name string `json:"signal"`

	// Value is what was observed, in human-readable form.
	Value string `json:"value"`

	// Weight is how much this signal contributes to confidence that a reversal
	// is warranted, between 0 and 1.
	Weight float64 `json:"weight"`
}

// Signal names. Stable strings: receivers branch on them.
const (
	// SignalWindowExpired is the baseline observation — the debit's window
	// closed with no credit. On its own it is only silence, which is why it
	// carries well under half the available confidence.
	SignalWindowExpired = "window_expired"

	// SignalIngestIntact means our own view of the tenant's stream had no gaps
	// across the window, so the absence of a credit is real rather than ours.
	SignalIngestIntact = "ingest_intact"

	// SignalProviderFailed means the rail confirmed the credit leg failed. This
	// is the only signal that is evidence rather than inference.
	SignalProviderFailed = "provider_failed"

	// SignalProviderNotFound means the rail has no record of a transfer we
	// believe we initiated.
	SignalProviderNotFound = "provider_not_found"

	// SignalProviderUnreachable means we asked and could not get an answer. It
	// carries no weight — it is recorded so the receiver can see we tried.
	SignalProviderUnreachable = "provider_unreachable"

	// SignalIngestGap means we dropped events over this window, so nothing can
	// be concluded from the missing credit.
	SignalIngestGap = "ingest_gap"
)

// Weights are chosen so that silence alone can never reach certainty.
//
// A window closing with no credit is the weakest evidence in the system and
// tops out at 0.70 on its own. Only the rail confirming failure takes a verdict
// into the range where auto-reversal is clearly safe. That ordering is the whole
// point: it makes "we guessed" and "we checked" different numbers.
const (
	WeightWindowExpired     = 0.55
	WeightIngestIntact      = 0.15
	WeightProviderFailed    = 0.30
	WeightProviderNotFound  = 0.25
	WeightProviderUnreached = 0.0
	WeightIngestGap         = 0.0
)

// Set is the accumulated evidence for one verdict.
type Set struct {
	signals []Signal
}

// New builds an empty set.
func New() *Set { return &Set{} }

// Add records a signal. A nil set is a no-op, so callers need not branch.
func (s *Set) Add(name, value string, weight float64) *Set {
	if s == nil {
		return s
	}
	s.signals = append(s.signals, Signal{Name: name, Value: value, Weight: weight})
	return s
}

// Signals returns the recorded signals, heaviest first, so the reason that
// mattered most reads first.
func (s *Set) Signals() []Signal {
	if s == nil {
		return nil
	}
	out := make([]Signal, len(s.signals))
	copy(out, s.signals)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Weight > out[j].Weight })
	return out
}

// Has reports whether a signal was recorded.
func (s *Set) Has(name string) bool {
	if s == nil {
		return false
	}
	for _, sig := range s.signals {
		if sig.Name == name {
			return true
		}
	}
	return false
}

// Confidence is how sure we are that reversing is the right action, from 0 to 1.
//
// It is a plain sum of weights, clamped. Deliberately not a probability: it is a
// disclosure of what we checked, and inventing a statistical model over signals
// we have not calibrated would dress a guess up as a measurement.
func (s *Set) Confidence() float64 {
	if s == nil {
		return 0
	}
	var total float64
	for _, sig := range s.signals {
		total += sig.Weight
	}
	switch {
	case total < 0:
		return 0
	case total > 1:
		return 1
	default:
		return round2(total)
	}
}

// round2 keeps the published number to two decimals, so a receiver comparing
// against a threshold is not tripped by float noise.
func round2(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}
