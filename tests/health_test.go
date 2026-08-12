package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/health"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// failingHealthStore lets a flush fail on demand.
type failingHealthStore struct {
	store.Store
	mu      sync.Mutex
	fail    bool
	samples []store.IngestSample
}

func (f *failingHealthStore) RecordIngestHealth(ctx context.Context, samples []store.IngestSample) error {
	f.mu.Lock()
	shouldFail := f.fail
	f.mu.Unlock()

	if shouldFail {
		return errors.New("database unreachable")
	}
	f.mu.Lock()
	f.samples = append(f.samples, samples...)
	f.mu.Unlock()
	return f.Store.RecordIngestHealth(ctx, samples)
}

func (f *failingHealthStore) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

func newRecorder(t *testing.T, s store.HealthStore) *health.Recorder {
	t.Helper()
	r, err := health.New(s, health.Options{})
	if err != nil {
		t.Fatalf("health.New: %v", err)
	}
	return r
}

func TestHealthNewRequiresStore(t *testing.T) {
	if _, err := health.New(nil, health.Options{}); err == nil {
		t.Error("accepted a nil store")
	}
}

func TestRecorderAccumulatesAndFlushes(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	r := newRecorder(t, s)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		r.Accepted(tenantA)
	}
	r.Dropped(tenantA)
	r.Dropped(tenantA)
	r.BatchFailed(tenantA, 3)
	r.Accepted(tenantB)

	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	now := time.Now().UTC()
	gap, err := s.HasIngestGap(ctx, tenantA, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("HasIngestGap: %v", err)
	}
	if !gap {
		t.Error("dropped events did not produce a gap")
	}

	// Tenant B lost nothing, so its view is intact.
	gapB, err := s.HasIngestGap(ctx, tenantB, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("HasIngestGap: %v", err)
	}
	if gapB {
		t.Error("tenant B has a gap it never earned")
	}

	act, err := s.IngestActivity(ctx, tenantA, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("IngestActivity: %v", err)
	}
	if act.Received != 5 {
		t.Errorf("received = %d, want 5", act.Received)
	}
}

func TestRecorderFlushIsEmptyWhenNothingHappened(t *testing.T) {
	r := newRecorder(t, store.NewMemory())
	if err := r.Flush(context.Background()); err != nil {
		t.Errorf("empty flush: %v", err)
	}
}

// Losing the counts would silently disable the protection they exist to
// provide, so a failed flush must put them back.
func TestRecorderRetainsCountsWhenFlushFails(t *testing.T) {
	base := store.NewMemory()
	seedTenants(t, base)
	failing := &failingHealthStore{Store: base}

	r := newRecorder(t, failing)
	ctx := context.Background()

	failing.setFail(true)
	r.Dropped(tenantA)
	r.Dropped(tenantA)
	if err := r.Flush(ctx); err == nil {
		t.Fatal("Flush returned nil despite the store failing")
	}

	// Nothing recorded yet, so no gap is visible.
	now := time.Now().UTC()
	if gap, _ := base.HasIngestGap(ctx, tenantA, now.Add(-time.Minute), now.Add(time.Minute)); gap {
		t.Error("a failed flush still recorded a gap")
	}

	// The next flush must carry the retained counts.
	failing.setFail(false)
	if err := r.Flush(ctx); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if gap, _ := base.HasIngestGap(ctx, tenantA, now.Add(-time.Minute), now.Add(time.Minute)); !gap {
		t.Error("counts were lost when the first flush failed")
	}
}

func TestRecorderIgnoresEmptyTenantAndZeroBatches(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	r := newRecorder(t, s)

	// A malformed event has no tenant; it must not create a phantom bucket.
	r.Accepted("")
	r.Dropped("")
	r.BatchFailed(tenantA, 0)

	if err := r.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	now := time.Now().UTC()
	if gap, _ := s.HasIngestGap(context.Background(), tenantA, now.Add(-time.Minute), now.Add(time.Minute)); gap {
		t.Error("a zero-event batch failure recorded a gap")
	}
}

func TestRecorderIsConcurrencySafe(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	r := newRecorder(t, s)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Accepted(tenantA)
				r.Dropped(tenantB)
			}
		}()
	}
	wg.Wait()

	if err := r.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	now := time.Now().UTC()
	act, err := s.IngestActivity(context.Background(), tenantA, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("IngestActivity: %v", err)
	}
	if act.Received != 1600 {
		t.Errorf("received = %d, want 1600 — counts were lost under concurrency", act.Received)
	}
}

// The point of the whole mechanism: a transaction whose window overlaps a gap in
// our own view must not auto-reverse.
func TestGapSendsTransactionToSuspectNotOrphaned(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()

	// Two transactions whose windows opened 5 minutes ago and have closed.
	gapTxn := newExpiredTxn(tenantA, "TX-GAP", 5*time.Minute, time.Minute)
	cleanTxn := newExpiredTxn(tenantB, "TX-CLEAN", 5*time.Minute, time.Minute)
	mustUpsert(t, s, gapTxn)
	mustUpsert(t, s, cleanTxn)

	// A drop inside tenant A's window. Written directly so the bucket is exact
	// rather than dependent on when the test happens to run.
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{{
		TenantID: tenantA,
		Bucket:   gapTxn.DebitAt,
		Dropped:  1,
	}}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 100)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed %d, want 2", len(claimed))
	}

	got := map[string]domain.Status{}
	for _, c := range claimed {
		got[c.TransactionID] = c.Status
	}
	if got["TX-GAP"] != domain.StatusSuspect {
		t.Errorf("TX-GAP = %s, want suspect — we dropped events over its window", got["TX-GAP"])
	}
	if got["TX-CLEAN"] != domain.StatusOrphaned {
		t.Errorf("TX-CLEAN = %s, want orphaned — that tenant's view was intact", got["TX-CLEAN"])
	}
}

// A suspect transaction raises an investigation, never a reversal.
func TestGapProducesNoReversalWebhook(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	txn := newExpiredTxn(tenantA, "TX-GAP", 5*time.Minute, time.Minute)
	mustUpsert(t, s, txn)

	// A batch that failed to apply is the same kind of hole as a drop.
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{{
		TenantID:      tenantA,
		Bucket:        txn.DebitAt,
		HandlerErrors: 12,
	}}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	res, err := newDetector(t, s).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Suspect != 1 {
		t.Errorf("suspect = %d, want 1", res.Suspect)
	}

	stored, err := s.Get(ctx, tenantA, "TX-GAP")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Status != domain.StatusSuspect {
		t.Fatalf("status = %s, want suspect", stored.Status)
	}

	deliveries, err := s.ListDeliveries(ctx, tenantA, "", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	for _, d := range deliveries {
		if d.EventType == "reversal.triggered" {
			t.Error("a reversal was queued for a transaction we could not vouch for")
		}
	}
}

// A gap outside the transaction's window says nothing about it.
func TestGapOutsideTheWindowDoesNotSuppress(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()

	// The window opened and closed two hours ago; the gap is an hour later.
	txn := newExpiredTxn(tenantA, "TX-OLD", 2*time.Hour, time.Minute)
	mustUpsert(t, s, txn)

	if err := s.RecordIngestHealth(ctx, []store.IngestSample{{
		TenantID: tenantA,
		Bucket:   txn.DebitAt.Add(time.Hour),
		Dropped:  1,
	}}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed %d, want 1", len(claimed))
	}
	if claimed[0].Status != domain.StatusOrphaned {
		t.Errorf("status = %s, want orphaned — the gap did not overlap this window", claimed[0].Status)
	}
}

// The pipeline must report which tenant lost events, since the aggregate
// counters cannot say who or when.
func TestPipelineReportsPerTenantOutcomes(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	r := newRecorder(t, s)
	ctx := context.Background()

	// One worker held inside the handler and a buffer of one, so the queue
	// fills and everything after that is refused.
	blocked := make(chan struct{})
	p, err := pipeline.New(
		pipeline.HandlerFunc(func(context.Context, string, []domain.Event) error {
			<-blocked
			return nil
		}),
		pipeline.Config{Workers: 1, BatchSize: 1, FlushInterval: time.Hour, BufferSize: 1, Observer: r},
	)
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}
	p.Start(ctx)
	// Close before unblocking would deadlock: Close waits for the parked worker.
	defer p.Close()
	defer close(blocked)

	var refused int
	for i := 0; i < 100; i++ {
		if err := p.Submit(pipelineEvent(tenantA, "TX")); err != nil {
			refused++
		}
	}
	if refused == 0 {
		t.Fatal("nothing was refused; the queue never filled")
	}

	if err := r.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	now := time.Now().UTC()
	gap, err := s.HasIngestGap(ctx, tenantA, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("HasIngestGap: %v", err)
	}
	if !gap {
		t.Error("backpressure drops did not reach the health store")
	}
}
