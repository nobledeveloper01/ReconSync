package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
)

// recorder captures what the handler was given.
type recorder struct {
	mu    sync.Mutex
	calls []handlerCall
	err   error
	block chan struct{} // when set, Handle waits on it
}

type handlerCall struct {
	tenantID string
	events   []domain.Event
}

func (r *recorder) Handle(_ context.Context, tenantID string, events []domain.Event) error {
	if r.block != nil {
		<-r.block
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]domain.Event, len(events))
	copy(cp, events)
	r.calls = append(r.calls, handlerCall{tenantID: tenantID, events: cp})
	return r.err
}

func (r *recorder) snapshot() []handlerCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]handlerCall, len(r.calls))
	copy(out, r.calls)
	return out
}

func (r *recorder) totalEvents() int {
	n := 0
	for _, c := range r.snapshot() {
		n += len(c.events)
	}
	return n
}

func pipelineEvent(tenantID, txnID string) domain.Event {
	return domain.NewDebitEvent(newDebitEvent(tenantID, txnID))
}

func TestPipelineRequiresHandler(t *testing.T) {
	if _, err := pipeline.New(nil, pipeline.Config{}); err == nil {
		t.Error("accepted a nil handler")
	}
}

func TestPipelineDefaultsAreApplied(t *testing.T) {
	// A zero Config must produce a working pipeline rather than a zero-capacity
	// channel that refuses everything.
	rec := &recorder{}
	p, err := pipeline.New(rec, pipeline.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Close()

	if err := p.Submit(pipelineEvent(tenantA, "TX1")); err != nil {
		t.Fatalf("Submit with default config: %v", err)
	}
	waitFor(t, func() bool { return rec.totalEvents() == 1 },
		"default flush interval never fired")

	if pipeline.DefaultBufferSize <= 0 || pipeline.DefaultBatchSize <= 0 || pipeline.DefaultFlushInterval <= 0 {
		t.Error("exported defaults must be positive")
	}
}

func TestFlushesOnBatchSize(t *testing.T) {
	rec := &recorder{}
	// A long interval so only the size trigger can fire.
	p, err := pipeline.New(rec, pipeline.Config{Workers: 1, BatchSize: 3, FlushInterval: time.Hour, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Close()

	for i := 0; i < 3; i++ {
		if err := p.Submit(pipelineEvent(tenantA, "TX")); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	waitFor(t, func() bool { return rec.totalEvents() == 3 }, "batch was not flushed at batch size")

	if calls := rec.snapshot(); len(calls) != 1 {
		t.Fatalf("handler called %d times, want 1", len(calls))
	}
}

func TestFlushesOnInterval(t *testing.T) {
	rec := &recorder{}
	// A batch size that will never be reached, so only the ticker can flush.
	p, err := pipeline.New(rec, pipeline.Config{Workers: 1, BatchSize: 1000, FlushInterval: 10 * time.Millisecond, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Close()

	if err := p.Submit(pipelineEvent(tenantA, "TX1")); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	waitFor(t, func() bool { return rec.totalEvents() == 1 },
		"a partial batch was never flushed; low-volume tenants would wait forever")
}

func TestBackpressureIsReportedNotAbsorbed(t *testing.T) {
	rec := &recorder{block: make(chan struct{})}
	// One worker held inside Handle and a buffer of 1: everything past that must
	// be refused rather than queued.
	p, err := pipeline.New(rec, pipeline.Config{Workers: 1, BatchSize: 1, FlushInterval: time.Hour, BufferSize: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())

	var refused int
	for i := 0; i < 200; i++ {
		if err := p.Submit(pipelineEvent(tenantA, "TX")); errors.Is(err, pipeline.ErrBackpressure) {
			refused++
		}
	}

	if refused == 0 {
		t.Fatal("no submission was refused; the queue absorbed unbounded load")
	}
	if got := p.Stats().Dropped; got != uint64(refused) {
		t.Errorf("dropped counter = %d, want %d", got, refused)
	}

	close(rec.block)
	p.Close()
}

func TestSubmitAfterCloseIsRefused(t *testing.T) {
	p, err := pipeline.New(&recorder{}, pipeline.Config{Workers: 1, BatchSize: 10, FlushInterval: time.Hour, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	p.Close()

	if err := p.Submit(pipelineEvent(tenantA, "TX1")); !errors.Is(err, pipeline.ErrClosed) {
		t.Errorf("Submit after Close = %v, want ErrClosed", err)
	}
}

func TestCloseDrainsQueuedEvents(t *testing.T) {
	rec := &recorder{}
	// Neither trigger can fire on its own, so only the drain-on-close path can
	// deliver these events.
	p, err := pipeline.New(rec, pipeline.Config{Workers: 1, BatchSize: 1000, FlushInterval: time.Hour, BufferSize: 100})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())

	const n = 25
	for i := 0; i < n; i++ {
		if err := p.Submit(pipelineEvent(tenantA, "TX")); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	p.Close()

	if got := rec.totalEvents(); got != n {
		t.Errorf("delivered %d events, want %d — Close lost buffered events", got, n)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	p, err := pipeline.New(&recorder{}, pipeline.Config{Workers: 2, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	p.Close()
	p.Close() // must not panic on a second close of the channel
}

func TestFlushGroupsByTenant(t *testing.T) {
	rec := &recorder{}
	p, err := pipeline.New(rec, pipeline.Config{Workers: 1, BatchSize: 4, FlushInterval: time.Hour, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Close()

	for _, tenantID := range []string{tenantA, tenantB, tenantA, tenantB} {
		if err := p.Submit(pipelineEvent(tenantID, "TX")); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	waitFor(t, func() bool { return len(rec.snapshot()) == 2 }, "batch was not split per tenant")

	// No handler call may mix tenants — that is what keeps every store write
	// tenant-scoped.
	for _, c := range rec.snapshot() {
		if len(c.events) != 2 {
			t.Errorf("tenant %s got %d events, want 2", c.tenantID, len(c.events))
		}
		for _, ev := range c.events {
			if ev.TenantID() != c.tenantID {
				t.Errorf("event for %s delivered under tenant %s", ev.TenantID(), c.tenantID)
			}
		}
	}
}

func TestHandlerErrorDoesNotStopThePipeline(t *testing.T) {
	rec := &recorder{err: errors.New("database unavailable")}
	p, err := pipeline.New(rec, pipeline.Config{Workers: 1, BatchSize: 1, FlushInterval: time.Hour, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())
	defer p.Close()

	for i := 0; i < 3; i++ {
		if err := p.Submit(pipelineEvent(tenantA, "TX")); err != nil {
			t.Fatalf("Submit: %v", err)
		}
	}

	waitFor(t, func() bool { return p.Stats().HandlerErrors == 3 },
		"a handler error stopped the worker pool")

	if got := p.Stats().Flushed; got != 0 {
		t.Errorf("flushed = %d, want 0 — failed batches must not count as delivered", got)
	}
}

func TestContextCancellationFlushesInFlightWork(t *testing.T) {
	rec := &recorder{}
	p, err := pipeline.New(rec, pipeline.Config{Workers: 1, BatchSize: 1000, FlushInterval: time.Hour, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	if err := p.Submit(pipelineEvent(tenantA, "TX1")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitFor(t, func() bool { return p.Stats().QueueDepth == 0 }, "worker never picked up the event")

	cancel()

	waitFor(t, func() bool { return rec.totalEvents() == 1 },
		"cancellation discarded the in-flight batch instead of flushing it")
	p.Close()
}

func TestConcurrentSubmitsAreSafe(t *testing.T) {
	rec := &recorder{}
	p, err := pipeline.New(rec, pipeline.Config{Workers: 4, BatchSize: 10, FlushInterval: 5 * time.Millisecond, BufferSize: 5000})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	p.Start(context.Background())

	const producers, each = 8, 100
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				_ = p.Submit(pipelineEvent(tenantA, "TX"))
			}
		}()
	}
	wg.Wait()
	p.Close()

	stats := p.Stats()
	if stats.Received+stats.Dropped != producers*each {
		t.Errorf("received %d + dropped %d != %d submitted",
			stats.Received, stats.Dropped, producers*each)
	}
	// Everything accepted must have been delivered by the time Close returns.
	if got := uint64(rec.totalEvents()); got != stats.Received {
		t.Errorf("delivered %d, accepted %d", got, stats.Received)
	}
}

func TestStartIsIdempotent(t *testing.T) {
	rec := &recorder{}
	p, err := pipeline.New(rec, pipeline.Config{Workers: 2, BatchSize: 1, FlushInterval: time.Hour, BufferSize: 10})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	p.Start(ctx)
	p.Start(ctx) // must not double the pool

	if err := p.Submit(pipelineEvent(tenantA, "TX1")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitFor(t, func() bool { return rec.totalEvents() == 1 }, "event not handled")
	p.Close()
}
