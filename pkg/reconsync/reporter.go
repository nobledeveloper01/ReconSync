package reconsync

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Reporter reports transactions without ever standing in the way of one.
//
// This exists because of where the calls sit. Reporting a debit happens inside
// the code path that moves a customer's money, and the naive integration —
// `if err := client.ReportDebit(ctx, d); err != nil { return err }` — makes a
// reconciliation service having a bad afternoon into a reason the customer's
// transfer fails. That is a strictly worse outcome than not reconciling it.
//
// So the enqueue never blocks, never returns an error, and a full buffer drops
// the report rather than waiting. What it does not do is drop silently: a
// dropped debit is a transaction ReconSync will never see and therefore can
// never detect the failure of, which is exactly the blind spot the server
// models as an ingest gap. OnDrop is where that becomes your alert.
type Reporter struct {
	client *Client
	queue  chan func(context.Context) error

	dropped  atomic.Int64
	failed   atomic.Int64
	sent     atomic.Int64
	onDrop   func(kind, transactionID string)
	onError  func(kind, transactionID string, err error)
	sendWait time.Duration
	workers  int

	// A read-write mutex rather than a flag, because a flag cannot make the
	// check and the send atomic: Close could land between them and the send
	// would panic on a closed channel — inside a payment path, which is the
	// one place this library must never do that.
	wg     sync.WaitGroup
	mu     sync.RWMutex
	closed bool
}

// ReporterOption configures a Reporter.
type ReporterOption func(*Reporter)

// WithBuffer sets how many reports may be waiting. The default of 1024 is
// roughly a few seconds of a busy tenant's traffic — long enough to ride out a
// restart, short enough to bound memory.
func WithBuffer(n int) ReporterOption {
	return func(r *Reporter) {
		if n > 0 {
			r.queue = make(chan func(context.Context) error, n)
		}
	}
}

// OnDrop is called when the buffer is full and a report is discarded.
//
// Wire this to an alert. Sustained drops mean ReconSync's view of your traffic
// has a hole in it, and a hole is indistinguishable from a quiet day unless
// something says otherwise.
func OnDrop(f func(kind, transactionID string)) ReporterOption {
	return func(r *Reporter) { r.onDrop = f }
}

// OnError is called when a report was sent and refused, after retries.
func OnError(f func(kind, transactionID string, err error)) ReporterOption {
	return func(r *Reporter) { r.onError = f }
}

// WithWorkers sets how many reports are in flight at once.
func WithWorkers(n int) ReporterOption {
	return func(r *Reporter) {
		if n > 0 {
			r.workers = n
		}
	}
}

// NewReporter starts the background workers. Call Close before exiting, or
// whatever is still queued is lost.
func NewReporter(c *Client, opts ...ReporterOption) *Reporter {
	r := &Reporter{
		client:   c,
		queue:    make(chan func(context.Context) error, 1024),
		sendWait: 10 * time.Second,
		workers:  2,
	}
	for _, o := range opts {
		o(r)
	}

	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.run()
	}
	return r
}

func (r *Reporter) run() {
	defer r.wg.Done()
	for task := range r.queue {
		// Its own timeout, not the caller's: the caller's request context is
		// long gone by the time this runs, and inheriting a cancelled context
		// would fail every report.
		ctx, cancel := context.WithTimeout(context.Background(), r.sendWait)
		err := task(ctx)
		cancel()

		if err != nil {
			r.failed.Add(1)
			continue
		}
		r.sent.Add(1)
	}
}

// ReportDebit queues a debit and returns immediately.
//
// It returns whether the report was queued. Callers are free to ignore that —
// the point of this type is that a false is not a reason to fail a payment —
// but it is there for the caller who wants to record it inline.
func (r *Reporter) ReportDebit(d Debit) bool {
	return r.enqueue("debit", d.TransactionID, func(ctx context.Context) error {
		_, err := r.client.ReportDebit(ctx, d)
		if err != nil && r.onError != nil {
			r.onError("debit", d.TransactionID, err)
		}
		return err
	})
}

// ReportCredit queues a verdict and returns immediately.
func (r *Reporter) ReportCredit(c Credit) bool {
	return r.enqueue("credit", c.TransactionID, func(ctx context.Context) error {
		err := r.client.ReportCredit(ctx, c)
		if err != nil && r.onError != nil {
			r.onError("credit", c.TransactionID, err)
		}
		return err
	})
}

// ReportReversalCompleted queues a reversal confirmation.
func (r *Reporter) ReportReversalCompleted(transactionID string, at time.Time) bool {
	return r.enqueue("reversal", transactionID, func(ctx context.Context) error {
		err := r.client.ReportReversalCompleted(ctx, transactionID, at)
		if err != nil && r.onError != nil {
			r.onError("reversal", transactionID, err)
		}
		return err
	})
}

func (r *Reporter) enqueue(kind, transactionID string, task func(context.Context) error) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		r.drop(kind, transactionID)
		return false
	}

	select {
	case r.queue <- task:
		return true
	default:
		// Full. Dropping is the deliberate choice: blocking here would apply
		// backpressure from a reconciliation service onto a payment path.
		r.drop(kind, transactionID)
		return false
	}
}

func (r *Reporter) drop(kind, transactionID string) {
	r.dropped.Add(1)
	if r.onDrop != nil {
		r.onDrop(kind, transactionID)
	}
}

// Stats reports what the reporter has done, for your own metrics.
type Stats struct {
	Sent    int64
	Failed  int64
	Dropped int64
	Queued  int
}

// Stats returns a snapshot.
func (r *Reporter) Stats() Stats {
	return Stats{
		Sent:    r.sent.Load(),
		Failed:  r.failed.Load(),
		Dropped: r.dropped.Load(),
		Queued:  len(r.queue),
	}
}

// ErrDrainTimeout means Close gave up with reports still queued.
var ErrDrainTimeout = errors.New("reconsync: closed with reports still queued")

// Close stops accepting reports and waits for the queue to drain.
//
// Call it on shutdown, before the process exits. Whatever is still queued when
// ctx expires is lost, and the error says so — which is the difference between
// a clean deploy and a silent gap in the record across every rolling restart.
func (r *Reporter) Close(ctx context.Context) error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.queue)
	}
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ErrDrainTimeout
	}
}
