// Package health records how intact our own view of each tenant's event stream
// was, so detection can tell "no credit arrived" apart from "we did not see it".
package health

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// DefaultFlushInterval is how often accumulated counters reach storage. Shorter
// than the shortest sensible reconciliation window, so a gap is durable before
// the transactions it affects are swept.
const DefaultFlushInterval = 5 * time.Second

// Recorder accumulates per-tenant ingest counters in memory and flushes them
// periodically.
//
// Writing on each event would put a database round-trip on the ingest path, and
// writing on each *drop* would add load at exactly the moment the system is
// already overloaded. So counts accumulate under a mutex — cheap next to the
// work each event already does — and land in batches.
type Recorder struct {
	store store.HealthStore
	log   *slog.Logger
	now   func() time.Time
	every time.Duration

	mu      sync.Mutex
	pending map[key]*counts
}

type key struct {
	tenantID string
	bucket   time.Time
}

type counts struct {
	received      int64
	dropped       int64
	handlerErrors int64
}

// Options configures a Recorder.
type Options struct {
	Logger        *slog.Logger
	Now           func() time.Time
	FlushInterval time.Duration
}

// New builds a Recorder.
func New(s store.HealthStore, opts Options) (*Recorder, error) {
	if s == nil {
		return nil, errors.New("health: store is required")
	}
	r := &Recorder{store: s, log: opts.Logger, now: opts.Now, every: opts.FlushInterval,
		pending: make(map[key]*counts)}
	if r.log == nil {
		r.log = slog.Default()
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.every <= 0 {
		r.every = DefaultFlushInterval
	}
	return r, nil
}

func (r *Recorder) bump(tenantID string, f func(*counts)) {
	if tenantID == "" {
		return
	}
	k := key{tenantID: tenantID, bucket: r.now().UTC().Truncate(time.Minute)}

	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.pending[k]
	if !ok {
		c = &counts{}
		r.pending[k] = c
	}
	f(c)
}

// Accepted records an event taken onto the pipeline.
func (r *Recorder) Accepted(tenantID string) {
	r.bump(tenantID, func(c *counts) { c.received++ })
}

// Dropped records an event refused by backpressure. This is the counter that
// makes a later reversal untrustworthy.
func (r *Recorder) Dropped(tenantID string) {
	r.bump(tenantID, func(c *counts) { c.dropped++ })
}

// BatchFailed records events lost because a batch could not be applied.
func (r *Recorder) BatchFailed(tenantID string, events int) {
	if events <= 0 {
		return
	}
	r.bump(tenantID, func(c *counts) { c.handlerErrors += int64(events) })
}

// Flush writes accumulated counters. On failure the counts are put back so the
// next flush retries them — losing them would silently disable the protection
// they exist to provide.
func (r *Recorder) Flush(ctx context.Context) error {
	r.mu.Lock()
	pending := r.pending
	r.pending = make(map[key]*counts)
	r.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	samples := make([]store.IngestSample, 0, len(pending))
	for k, c := range pending {
		samples = append(samples, store.IngestSample{
			TenantID:      k.tenantID,
			Bucket:        k.bucket,
			Received:      c.received,
			Dropped:       c.dropped,
			HandlerErrors: c.handlerErrors,
		})
	}

	if err := r.store.RecordIngestHealth(ctx, samples); err != nil {
		r.restore(pending)
		return err
	}
	return nil
}

// restore merges un-flushed counts back into the pending set, adding to anything
// that accumulated while the flush was in flight.
func (r *Recorder) restore(pending map[key]*counts) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for k, c := range pending {
		existing, ok := r.pending[k]
		if !ok {
			r.pending[k] = c
			continue
		}
		existing.received += c.received
		existing.dropped += c.dropped
		existing.handlerErrors += c.handlerErrors
	}
}

// Run flushes until the context is cancelled, then flushes once more.
func (r *Recorder) Run(ctx context.Context) {
	ticker := time.NewTicker(r.every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// WithoutCancel: the shutdown signal must not cancel the write that
			// records what we dropped on the way down.
			if err := r.Flush(context.WithoutCancel(ctx)); err != nil {
				r.log.ErrorContext(ctx, "final ingest health flush failed",
					slog.String("error", err.Error()))
			}
			return
		case <-ticker.C:
			if err := r.Flush(ctx); err != nil {
				// Logged, never swallowed: a recorder failing silently means
				// detection silently loses its guard against our own gaps.
				r.log.ErrorContext(ctx, "ingest health flush failed",
					slog.String("error", err.Error()))
			}
		}
	}
}
