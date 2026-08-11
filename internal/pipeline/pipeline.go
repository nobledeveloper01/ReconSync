// Package pipeline moves ingested events to a handler in batches (§4.3).
//
// Every stage is bounded. The ingest channel has a fixed capacity, batches have
// a fixed size, and a full queue is reported to the caller rather than absorbed,
// so load shows up as a 429 instead of unbounded memory growth.
package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// Handler consumes a batch of one tenant's events.
type Handler interface {
	Handle(ctx context.Context, tenantID string, events []domain.Event) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, tenantID string, events []domain.Event) error

func (f HandlerFunc) Handle(ctx context.Context, tenantID string, events []domain.Event) error {
	return f(ctx, tenantID, events)
}

var (
	// ErrBackpressure means the queue is full. The ingest handler turns this into
	// 429 with Retry-After rather than blocking the caller.
	ErrBackpressure = errors.New("pipeline: at capacity")

	// ErrClosed means Submit was called after Close.
	ErrClosed = errors.New("pipeline: closed")
)

// Defaults from §4.3.
const (
	DefaultBufferSize    = 10_000
	DefaultBatchSize     = 100
	DefaultFlushInterval = 200 * time.Millisecond
)

// Config tunes the pipeline. Zero values take the defaults.
type Config struct {
	BufferSize    int
	Workers       int
	BatchSize     int
	FlushInterval time.Duration
}

func (c *Config) setDefaults() {
	if c.BufferSize <= 0 {
		c.BufferSize = DefaultBufferSize
	}
	if c.Workers <= 0 {
		// This stage is I/O bound, so it oversubscribes cores deliberately.
		c.Workers = runtime.GOMAXPROCS(0) * 4
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = DefaultFlushInterval
	}
}

// Stats is a counter snapshot.
type Stats struct {
	Received      uint64
	Dropped       uint64
	Flushed       uint64
	Batches       uint64
	HandlerErrors uint64
	QueueDepth    int
}

// Pipeline batches events from many producers onto a handler.
type Pipeline struct {
	cfg     Config
	handler Handler
	ingest  chan domain.Event

	// mu guards closed and serialises Submit against Close, so a send can never
	// race a channel close.
	mu     sync.RWMutex
	closed bool

	wg      sync.WaitGroup
	started atomic.Bool

	received      atomic.Uint64
	dropped       atomic.Uint64
	flushed       atomic.Uint64
	batches       atomic.Uint64
	handlerErrors atomic.Uint64
}

// New builds a pipeline. Start must be called before events are consumed.
func New(h Handler, cfg Config) (*Pipeline, error) {
	if h == nil {
		return nil, errors.New("pipeline: handler is required")
	}
	cfg.setDefaults()
	return &Pipeline{
		cfg:     cfg,
		handler: h,
		ingest:  make(chan domain.Event, cfg.BufferSize),
	}, nil
}

// Start launches the worker pool. Calling it more than once is a no-op.
func (p *Pipeline) Start(ctx context.Context) {
	if !p.started.CompareAndSwap(false, true) {
		return
	}
	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.worker(ctx)
		}()
	}
}

// Submit enqueues an event without blocking.
//
// A full queue returns ErrBackpressure immediately. The alternative — blocking
// until space frees — would push latency back onto the customer's transaction
// path, which is the one thing this system must never do.
func (p *Pipeline) Submit(ev domain.Event) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrClosed
	}

	select {
	case p.ingest <- ev:
		p.received.Add(1)
		return nil
	default:
		p.dropped.Add(1)
		return ErrBackpressure
	}
}

// Close stops accepting events, drains what is queued and waits for the final
// flush. Safe to call more than once.
func (p *Pipeline) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.ingest)
	p.mu.Unlock()

	p.wg.Wait()
}

// Stats returns a snapshot of the counters.
func (p *Pipeline) Stats() Stats {
	return Stats{
		Received:      p.received.Load(),
		Dropped:       p.dropped.Load(),
		Flushed:       p.flushed.Load(),
		Batches:       p.batches.Load(),
		HandlerErrors: p.handlerErrors.Load(),
		QueueDepth:    len(p.ingest),
	}
}

func (p *Pipeline) worker(ctx context.Context) {
	batch := make([]domain.Event, 0, p.cfg.BatchSize)
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case ev, ok := <-p.ingest:
			if !ok {
				// Channel closed by Close: flush what is in hand and stop.
				p.flush(context.WithoutCancel(ctx), batch)
				return
			}
			batch = append(batch, ev)
			if len(batch) >= p.cfg.BatchSize {
				p.flush(ctx, batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			// Bounds latency for low-volume tenants that would otherwise wait for
			// a full batch.
			if len(batch) > 0 {
				p.flush(ctx, batch)
				batch = batch[:0]
			}

		case <-ctx.Done():
			// WithoutCancel: the shutdown signal must not cancel the flush it
			// triggered.
			p.flush(context.WithoutCancel(ctx), batch)
			return
		}
	}
}

// flush groups a batch by tenant and hands each group to the handler.
//
// Grouping is what keeps every write tenant-scoped, so no store call ever spans
// tenants. A handler failure is counted and skipped rather than propagated: one
// tenant's database error must not take down the pool serving everyone else.
func (p *Pipeline) flush(ctx context.Context, batch []domain.Event) {
	if len(batch) == 0 {
		return
	}

	byTenant := make(map[string][]domain.Event)
	for _, ev := range batch {
		id := ev.TenantID()
		byTenant[id] = append(byTenant[id], ev)
	}

	for tenantID, events := range byTenant {
		p.batches.Add(1)
		if err := p.handler.Handle(ctx, tenantID, events); err != nil {
			p.handlerErrors.Add(1)
			continue
		}
		p.flushed.Add(uint64(len(events)))
	}
}
