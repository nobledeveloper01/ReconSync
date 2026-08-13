// Package metrics is the observability the background loops were missing.
//
// The product's promise is a bound on how long a failed transfer goes
// unnoticed, and until now nothing measured whether that bound was being met.
// Worse, the two loops that deliver it run in goroutines: if one died, the
// process stayed healthy, /readyz stayed green, and detection silently stopped.
// Ingest counters would have kept climbing the whole time.
//
// The metric that matters most is therefore not a rate but a timestamp — when
// the sweep last completed. Everything else is diagnosis; that one is the alarm.
package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Registry holds process-wide counters. The zero value is usable.
//
// Hand-rolled atomics rather than a metrics client: §7.3 treats every
// dependency as something a customer's security team must approve, and this
// needs six counters and two gauges.
type Registry struct {
	// Detection.
	sweeps         atomic.Uint64
	sweepFailures  atomic.Uint64
	detected       atomic.Uint64
	reversalsQueue atomic.Uint64
	suspect        atomic.Uint64
	noTarget       atomic.Uint64
	settledByRail  atomic.Uint64
	atRisk         atomic.Uint64
	silentTenants  atomic.Int64

	// Dispatch.
	delivered  atomic.Uint64
	retried    atomic.Uint64
	deadLetter atomic.Uint64

	mu            sync.RWMutex
	lastSweep     time.Time
	lastDispatch  time.Time
	lastSweepLag  time.Duration
	sweepObserved bool
}

// New returns an empty Registry.
func New() *Registry { return &Registry{} }

// SweepResult is what one detection sweep did.
type SweepResult struct {
	Claimed       int
	Queued        int
	Suspect       int
	NoTarget      int
	SettledByRail int
	AtRisk        int
	SilentTenants int
	Lag           time.Duration
}

// RecordSweep records a completed detection sweep.
func (r *Registry) RecordSweep(at time.Time, res SweepResult) {
	if r == nil {
		return
	}
	r.sweeps.Add(1)
	r.detected.Add(nonNegative(res.Claimed))
	r.reversalsQueue.Add(nonNegative(res.Queued))
	r.suspect.Add(nonNegative(res.Suspect))
	r.noTarget.Add(nonNegative(res.NoTarget))
	r.settledByRail.Add(nonNegative(res.SettledByRail))
	r.atRisk.Add(nonNegative(res.AtRisk))
	r.silentTenants.Store(int64(res.SilentTenants))

	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastSweep = at
	r.lastSweepLag = res.Lag
	r.sweepObserved = true
}

// RecordSweepFailure records a sweep that errored.
//
// Deliberately does not update the last-sweep timestamp. A loop that runs and
// fails every time is not a loop that is working, and letting it keep the
// freshness gauge alive would hide exactly the outage this exists to catch.
func (r *Registry) RecordSweepFailure() {
	if r == nil {
		return
	}
	r.sweepFailures.Add(1)
}

// RecordDispatch records one dispatch round.
func (r *Registry) RecordDispatch(at time.Time, delivered, retried, deadLettered int) {
	if r == nil {
		return
	}
	r.delivered.Add(nonNegative(delivered))
	r.retried.Add(nonNegative(retried))
	r.deadLetter.Add(nonNegative(deadLettered))

	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastDispatch = at
}

// nonNegative converts a count for a counter.
//
// A negative count is a bug in the caller, and converting one straight to
// uint64 would add about eighteen quintillion to a counter that can only go up
// — permanently destroying the metric at the exact moment something is already
// wrong. Clamping keeps the counter usable while the bug is found.
func nonNegative(n int) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n)
}

// Snapshot is a consistent read of every metric.
type Snapshot struct {
	Sweeps                uint64
	SweepFailures         uint64
	Detected              uint64
	ReversalsQueued       uint64
	Suspect               uint64
	NoTarget              uint64
	SettledByRail         uint64
	AtRisk                uint64
	SilentTenants         int64
	Delivered             uint64
	Retried               uint64
	DeadLettered          uint64
	SecondsSinceLastSweep float64
	SweepLagSeconds       float64

	// SweepObserved is false before the first sweep completes. Reporting zero
	// seconds since a sweep that never happened would read as perfect health
	// on a process whose detector never started.
	SweepObserved  bool
	DispatchActive bool
}

// Read takes a snapshot as of now.
func (r *Registry) Read(now time.Time) Snapshot {
	if r == nil {
		return Snapshot{}
	}

	r.mu.RLock()
	lastSweep, lastDispatch, lag, observed := r.lastSweep, r.lastDispatch, r.lastSweepLag, r.sweepObserved
	r.mu.RUnlock()

	s := Snapshot{
		Sweeps:          r.sweeps.Load(),
		SweepFailures:   r.sweepFailures.Load(),
		Detected:        r.detected.Load(),
		ReversalsQueued: r.reversalsQueue.Load(),
		Suspect:         r.suspect.Load(),
		NoTarget:        r.noTarget.Load(),
		SettledByRail:   r.settledByRail.Load(),
		AtRisk:          r.atRisk.Load(),
		SilentTenants:   r.silentTenants.Load(),
		Delivered:       r.delivered.Load(),
		Retried:         r.retried.Load(),
		DeadLettered:    r.deadLetter.Load(),
		SweepLagSeconds: lag.Seconds(),
		SweepObserved:   observed,
		DispatchActive:  !lastDispatch.IsZero(),
	}
	if observed {
		s.SecondsSinceLastSweep = now.Sub(lastSweep).Seconds()
	}
	return s
}
