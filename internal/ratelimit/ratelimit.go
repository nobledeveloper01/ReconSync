// Package ratelimit bounds how much of a shared deployment one tenant can take.
//
// Deliberately not applied to ingest. A debit that is refused is a debit
// ReconSync never observes, and a transaction it never observes is one whose
// failure it can never detect — the customer believes they are covered and is
// not, silently and on our signature. That is the same reasoning that keeps
// licence expiry away from ingest. Ingest is protected by backpressure instead,
// which is temporary, paired with Retry-After, and retried by every SDK.
//
// What is limited here is the work that is expensive for everyone else: reports
// that scan a tenant's history, and fire drills that send real webhooks to real
// endpoints.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter is a token bucket per key.
type Limiter struct {
	rate  float64 // tokens per second
	burst float64

	mu      sync.Mutex
	buckets map[string]*bucket
	// idle is how long an untouched bucket is kept. Without eviction the map
	// grows with every key ever seen, which turns the thing meant to protect
	// the process into a way to exhaust it.
	idle time.Duration
}

type bucket struct {
	tokens float64
	last   time.Time
}

// New builds a limiter allowing `burst` at once, refilling to `rate` per period.
//
// A rate of zero disables it: every call is allowed. That is the honest way to
// turn a limit off, rather than setting a number so large it looks like a limit
// and is not.
func New(rate float64, period time.Duration, burst int) *Limiter {
	if period <= 0 {
		period = time.Second
	}
	if burst < 1 {
		burst = 1
	}
	return &Limiter{
		rate:    rate / period.Seconds(),
		burst:   float64(burst),
		buckets: make(map[string]*bucket),
		idle:    10 * time.Minute,
	}
}

// Allow reports whether the key may proceed, and how long to wait if not.
func (l *Limiter) Allow(key string, now time.Time) (bool, time.Duration) {
	if l == nil || l.rate <= 0 {
		return true, 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.buckets[key]
	if !ok {
		// A new key starts full, so a tenant's first request is never refused
		// for a bucket that has had no time to fill.
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
		l.evict(now)
	}

	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * l.rate
		if b.tokens > l.burst {
			b.tokens = l.burst
		}
		b.last = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}

	// Rounded up, and never zero: telling a client to retry in no time at all
	// invites a hot loop.
	wait := time.Duration((1-b.tokens)/l.rate*float64(time.Second)) + time.Second
	return false, wait.Truncate(time.Second)
}

// evict drops buckets nobody has touched. The caller holds the lock.
func (l *Limiter) evict(now time.Time) {
	if len(l.buckets) <= 1024 {
		return
	}
	for key, b := range l.buckets {
		if now.Sub(b.last) > l.idle {
			delete(l.buckets, key)
		}
	}
}
