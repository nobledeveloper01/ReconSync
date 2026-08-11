package tests

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

const testSecret = "whsec_service_test"

func testSecrets(context.Context, string) (string, error) { return testSecret, nil }

// localSender reaches loopback, which the production guard refuses.
func localSender() *webhook.Sender {
	return webhook.NewSender(webhook.SenderOptions{
		Client: webhook.NewClient(webhook.TransportOptions{AllowPrivateAddresses: true}),
	})
}

func newEndpoint(t *testing.T, s store.Store, tenantID, id, url string, events ...string) {
	t.Helper()
	err := s.CreateEndpoint(context.Background(), tenantID, &store.WebhookEndpoint{
		ID:        id,
		TenantID:  tenantID,
		URL:       url,
		SecretRef: "kms://test",
		Events:    events,
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
}

func newDetector(t *testing.T, s store.Store) *service.Detector {
	t.Helper()
	d, err := service.NewDetector(s, service.DetectorOptions{})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	return d
}

func newDispatcher(t *testing.T, s store.Store) *service.Dispatcher {
	t.Helper()
	d, err := service.NewDispatcher(s, service.DispatcherOptions{
		Sender:  localSender(),
		Secrets: testSecrets,
	})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	return d
}

// --- detection ---

func TestDetectorQueuesReversalForOrphans(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute), newDebitTxn(tenantA, "FRESH", time.Hour))

	res, err := newDetector(t, s).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Claimed != 1 || res.Queued != 1 {
		t.Fatalf("claimed=%d queued=%d, want 1/1", res.Claimed, res.Queued)
	}

	// The orphan moves to reversal_pending only because a delivery was queued.
	txn, err := s.Get(ctx, tenantA, "EXPIRED")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if txn.Status != domain.StatusReversalPending {
		t.Errorf("status = %s, want reversal_pending", txn.Status)
	}
	if txn.ReversalTriggeredAt == nil {
		t.Error("reversal_triggered_at not set")
	}

	deliveries, err := s.ListDeliveries(ctx, tenantA, "pending", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("queued %d deliveries, want 1", len(deliveries))
	}
	if deliveries[0].EventType != string(webhook.EventReversalTriggered) {
		t.Errorf("event = %q, want reversal.triggered", deliveries[0].EventType)
	}

	// The in-window transaction is untouched.
	fresh, err := s.Get(ctx, tenantA, "FRESH")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if fresh.Status != domain.StatusPendingDebit {
		t.Errorf("in-window transaction moved to %s", fresh.Status)
	}
}

// An ambiguous transaction raises an investigation, never an automatic
// reversal: reversing a credit that actually succeeded pays the customer twice.
func TestDetectorRaisesSuspectNotReversal(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "AMBIG", -time.Minute))
	if _, err := s.ApplyCredit(ctx, tenantA, "AMBIG", domain.StatusPendingUnknown, time.Now().UTC()); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}

	res, err := newDetector(t, s).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Suspect != 1 {
		t.Errorf("suspect = %d, want 1", res.Suspect)
	}

	txn, err := s.Get(ctx, tenantA, "AMBIG")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if txn.Status != domain.StatusSuspect {
		t.Errorf("status = %s, want suspect — an ambiguous transaction must not auto-reverse", txn.Status)
	}

	deliveries, err := s.ListDeliveries(ctx, tenantA, "", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(deliveries) != 1 || deliveries[0].EventType != string(webhook.EventTransactionSuspect) {
		t.Errorf("deliveries = %v, want one transaction.suspect", deliveries)
	}
}

func TestDetectorSkipsBackfill(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	ctx := context.Background()

	txn := newDebitTxn(tenantA, "OLD", -time.Minute)
	txn.IsBackfill = true
	mustUpsert(t, s, txn)

	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	// §3.2 A3: backfilled events are correlated but never notify.
	deliveries, err := s.ListDeliveries(ctx, tenantA, "", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(deliveries) != 0 {
		t.Errorf("backfilled transaction queued %d deliveries, want 0", len(deliveries))
	}
}

func TestDetectorRespectsEventSubscription(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	// Subscribed only to suspect events, so a reversal must not be delivered here.
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook",
		string(webhook.EventTransactionSuspect))
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
	res, err := newDetector(t, s).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Queued != 0 || res.NoTarget != 1 {
		t.Errorf("queued=%d noTarget=%d, want 0/1", res.Queued, res.NoTarget)
	}

	// With nothing queued, the transaction stays orphaned rather than claiming
	// a reversal is pending.
	txn, err := s.Get(ctx, tenantA, "EXPIRED")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if txn.Status != domain.StatusOrphaned {
		t.Errorf("status = %s, want orphaned", txn.Status)
	}
}

func TestDetectorIsTenantScoped(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_a", "https://a.example.com/hook")
	newEndpoint(t, s, tenantB, "we_b", "https://b.example.com/hook")
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "A-EXPIRED", -time.Minute))
	mustUpsert(t, s, newDebitTxn(tenantB, "B-EXPIRED", -time.Minute))

	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	for _, tc := range []struct{ tenant, txn string }{{tenantA, "A-EXPIRED"}, {tenantB, "B-EXPIRED"}} {
		deliveries, err := s.ListDeliveries(ctx, tc.tenant, "", 10)
		if err != nil {
			t.Fatalf("ListDeliveries: %v", err)
		}
		if len(deliveries) != 1 {
			t.Fatalf("%s: %d deliveries, want 1", tc.tenant, len(deliveries))
		}
		if deliveries[0].TransactionID != tc.txn {
			t.Errorf("%s got a delivery for %s, want %s", tc.tenant, deliveries[0].TransactionID, tc.txn)
		}
	}
}

func TestNewDetectorRequiresStore(t *testing.T) {
	if _, err := service.NewDetector(nil, service.DetectorOptions{}); err == nil {
		t.Error("accepted a nil store")
	}
}

// --- dispatch ---

func TestDispatcherDeliversAndSigns(t *testing.T) {
	var (
		hits      atomic.Int32
		verifyErr atomic.Value
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, _ := readAll(r)
		if err := webhook.Verify(testSecret, r.Header.Get("X-ReconSync-Signature"), body, time.Now(), webhook.DefaultTolerance); err != nil {
			verifyErr.Store(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", srv.URL)
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	res, err := newDispatcher(t, s).Dispatch(ctx)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if res.Delivered != 1 {
		t.Fatalf("delivered=%d claimed=%d, want 1", res.Delivered, res.Claimed)
	}
	if hits.Load() != 1 {
		t.Errorf("endpoint hit %d times, want 1", hits.Load())
	}
	if err, ok := verifyErr.Load().(error); ok && err != nil {
		t.Errorf("receiver could not verify the signature: %v", err)
	}

	delivered, err := s.ListDeliveries(ctx, tenantA, "delivered", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(delivered) != 1 {
		t.Fatalf("%d delivered records, want 1", len(delivered))
	}
	if delivered[0].ResponseCode == nil || *delivered[0].ResponseCode != http.StatusOK {
		t.Errorf("response code = %v, want 200", delivered[0].ResponseCode)
	}
}

func TestDispatcherRetriesThenDeadLetters(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", srv.URL)
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	// A clock the test advances between rounds, so the whole backoff schedule
	// runs in milliseconds instead of six real hours. It has to move: the same
	// clock both schedules the next retry and decides what is due.
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())

	dispatcher, err := service.NewDispatcher(s, service.DispatcherOptions{
		Sender:  localSender(),
		Secrets: testSecrets,
		Now:     func() time.Time { return time.Unix(0, clock.Load()).UTC() },
	})
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}

	var dead int
	for round := 0; round < webhook.MaxAttempts+2 && dead == 0; round++ {
		res, err := dispatcher.Dispatch(ctx)
		if err != nil {
			t.Fatalf("Dispatch round %d: %v", round, err)
		}
		dead += res.DeadLetters

		// Past the longest backoff, so the next retry is due.
		clock.Add(int64(7 * time.Hour))
	}
	if dead != 1 {
		t.Fatalf("delivery never dead-lettered after %d attempts", hits.Load())
	}
	if int(hits.Load()) != webhook.MaxAttempts {
		t.Errorf("endpoint hit %d times, want %d", hits.Load(), webhook.MaxAttempts)
	}

	// The transaction must say the reversal failed — that is what the DLQ alert
	// and the operator's queue key off.
	txn, err := s.Get(ctx, tenantA, "EXPIRED")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if txn.Status != domain.StatusReversalFailed {
		t.Errorf("status = %s, want reversal_failed", txn.Status)
	}

	dlq, err := s.ListDeliveries(ctx, tenantA, "dead_letter", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(dlq) != 1 {
		t.Fatalf("%d dead letters, want 1", len(dlq))
	}
}

// A 4xx is the client's own request being wrong; burning six hours of retries
// on it just repeats the rejection.
func TestDispatcherDeadLettersPermanentFailureImmediately(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", srv.URL)
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	res, err := newDispatcher(t, s).Dispatch(ctx)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if res.DeadLetters != 1 {
		t.Fatalf("deadLetters = %d, want 1 on the first attempt", res.DeadLetters)
	}
	if hits.Load() != 1 {
		t.Errorf("endpoint hit %d times, want exactly 1", hits.Load())
	}
}

func TestDispatcherLeasePreventsDoubleDelivery(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", srv.URL)
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	d := newDispatcher(t, s)
	for i := 0; i < 3; i++ {
		if _, err := d.Dispatch(ctx); err != nil {
			t.Fatalf("Dispatch %d: %v", i, err)
		}
	}
	// Once delivered it leaves the queue; repeated rounds must not re-send.
	if hits.Load() != 1 {
		t.Errorf("endpoint hit %d times across 3 rounds, want 1", hits.Load())
	}
}

func TestReplayReturnsDeadLetterToTheQueue(t *testing.T) {
	var succeed atomic.Bool
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if succeed.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusBadRequest) // permanent: dead-letters at once
	}))
	defer srv.Close()

	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", srv.URL)
	ctx := context.Background()

	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	d := newDispatcher(t, s)
	if _, err := d.Dispatch(ctx); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	dlq, err := s.ListDeliveries(ctx, tenantA, "dead_letter", 10)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(dlq) != 1 {
		t.Fatalf("%d dead letters, want 1", len(dlq))
	}

	// Runbook §11.5 step 5: fix the endpoint, then replay.
	succeed.Store(true)
	if err := s.ReplayDelivery(ctx, tenantA, dlq[0].ID); err != nil {
		t.Fatalf("ReplayDelivery: %v", err)
	}
	res, err := d.Dispatch(ctx)
	if err != nil {
		t.Fatalf("Dispatch after replay: %v", err)
	}
	if res.Delivered != 1 {
		t.Errorf("delivered = %d after replay, want 1", res.Delivered)
	}

	// Another tenant must not be able to replay it.
	if err := s.ReplayDelivery(ctx, tenantB, dlq[0].ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("cross-tenant replay: %v, want ErrNotFound", err)
	}
}

func TestDispatcherDisabledEndpointIsNotDelivered(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()

	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	mustUpsert(t, s, newDebitTxn(tenantA, "EXPIRED", -time.Minute))
	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	// Disable it after queueing: the claim must skip it.
	eps, err := s.ListEndpoints(ctx, tenantA)
	if err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	}
	eps[0].Enabled = false
	if err := s.CreateEndpoint(ctx, tenantA, &store.WebhookEndpoint{
		ID: "we_2", TenantID: tenantA, URL: "https://other.example.com/hook", SecretRef: "kms://test", Enabled: false,
	}); err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}

	due, err := s.ClaimDueDeliveries(ctx, time.Now().UTC(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	for _, d := range due {
		if d.EndpointID == "we_2" {
			t.Error("claimed a delivery for a disabled endpoint")
		}
	}
}

func TestNewDispatcherValidatesOptions(t *testing.T) {
	if _, err := service.NewDispatcher(nil, service.DispatcherOptions{Secrets: testSecrets}); err == nil {
		t.Error("accepted a nil store")
	}
	if _, err := service.NewDispatcher(store.NewMemory(), service.DispatcherOptions{}); err == nil {
		t.Error("accepted a nil secret resolver")
	}
}
