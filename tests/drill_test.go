package tests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/drill"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// drillFixture wires a runner against a receiver under the test's control.
type drillFixture struct {
	store  *store.Memory
	runner *drill.Runner
	got    chan *http.Request
	body   chan []byte
}

func newDrillFixture(t *testing.T, handler http.HandlerFunc) *drillFixture {
	t.Helper()

	f := &drillFixture{
		store: store.NewMemory(),
		got:   make(chan *http.Request, 8),
		body:  make(chan []byte, 8),
	}
	seedTenants(t, f.store)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		f.got <- r
		f.body <- body
		handler(w, r)
	}))
	t.Cleanup(srv.Close)

	newEndpoint(t, f.store, tenantA, "we_1", srv.URL)

	runner, err := drill.New(drill.Options{
		Store: f.store,
		// The SSRF guard refuses loopback by design, and the receiver is on
		// loopback, so the drill gets the same sender with that guard relaxed.
		Sender: webhook.NewSender(webhook.SenderOptions{
			Client: webhook.NewClient(webhook.TransportOptions{AllowPrivateAddresses: true}),
		}),
		Secrets: func(context.Context, string) (string, error) { return "whsec_test", nil },
	})
	if err != nil {
		t.Fatalf("drill.New: %v", err)
	}
	f.runner = runner
	return f
}

func ok(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

// The single most important property: a drill must be impossible to mistake for
// a real reversal. A handler that acts on this would move money it should not.
func TestDrillIsUnmistakablySynthetic(t *testing.T) {
	f := newDrillFixture(t, ok)

	rep, err := f.runner.Run(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	req := <-f.got
	body := <-f.body

	// Before parsing: a handler can refuse on the header alone.
	if req.Header.Get(webhook.DrillHeader) != "true" {
		t.Errorf("%s = %q, want true", webhook.DrillHeader, req.Header.Get(webhook.DrillHeader))
	}

	// And after parsing, in the payload itself.
	var env webhook.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if !env.Data.Drill {
		t.Error("payload does not carry drill: true")
	}
	if !strings.HasPrefix(env.Data.TransactionID, drill.TransactionPrefix) {
		t.Errorf("transaction_id = %q, want the %q prefix", env.Data.TransactionID, drill.TransactionPrefix)
	}
	if !env.Data.Advisory {
		t.Error("drill is not marked advisory")
	}
	// A receiver deduplicating on the delivery header must not see every drill
	// as the same delivery.
	if id := req.Header.Get(webhook.DeliveryHeader); id != env.Data.TransactionID {
		t.Errorf("%s = %q, want the drill's own id %q", webhook.DeliveryHeader, id, env.Data.TransactionID)
	}
	if rep.Notice == "" {
		t.Error("report does not restate what a drill is")
	}
}

// A real reversal must never acquire the drill marker by accident, or the flag
// would train handlers to ignore genuine reversals.
func TestRealReversalCarriesNoDrillMarker(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()

	var seen atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(webhook.DrillHeader) != "" {
			t.Errorf("a real reversal carried %s", webhook.DrillHeader)
		}
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		if strings.Contains(string(body), `"drill"`) {
			t.Errorf("a real reversal payload contains a drill field: %s", body)
		}
		seen.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	newEndpoint(t, s, tenantA, "we_1", srv.URL)
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-REAL", 5*time.Minute, time.Minute))

	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, err := newDispatcher(t, s).Dispatch(ctx); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !seen.Load() {
		t.Fatal("the real reversal was never delivered, so nothing was checked")
	}
}

// A drill that left rows behind would contaminate the compliance report it
// exists to support.
func TestDrillWritesNothing(t *testing.T) {
	f := newDrillFixture(t, ok)
	ctx := context.Background()

	rep, err := f.runner.Run(ctx, tenantA)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	<-f.got
	<-f.body

	if _, err := f.store.Get(ctx, tenantA, rep.TransactionID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("the drill created transaction %s", rep.TransactionID)
	}
	deliveries, err := f.store.ListDeliveries(ctx, tenantA, "", 100)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(deliveries) != 0 {
		t.Errorf("the drill queued %d deliveries", len(deliveries))
	}

	// And no state to clean up afterwards, so it is safe to run repeatedly.
	counts, err := f.store.CountByStatus(ctx, tenantA, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("CountByStatus: %v", err)
	}
	if len(counts) != 0 {
		t.Errorf("the drill left transactions behind: %v", counts)
	}
}

// The whole point is finding a broken handler before a real incident does.
func TestDrillReportsABrokenHandler(t *testing.T) {
	f := newDrillFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nil pointer dereference in reversal handler", http.StatusInternalServerError)
	})

	rep, err := f.runner.Run(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	<-f.got
	<-f.body

	if rep.Failed != 1 || rep.Passed != 0 {
		t.Fatalf("passed=%d failed=%d, want 0/1", rep.Passed, rep.Failed)
	}
	res := rep.Results[0]
	if res.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", res.StatusCode)
	}
	// The response body is what makes the report actionable rather than a
	// red light with no explanation.
	if !strings.Contains(res.ResponseBody, "nil pointer") {
		t.Errorf("response body not reported: %q", res.ResponseBody)
	}
	if !strings.Contains(res.Diagnosis, "drill exists to find") {
		t.Errorf("diagnosis = %q", res.Diagnosis)
	}
}

// A rejected signature and a broken handler need different fixes, so they get
// different diagnoses.
func TestDrillDistinguishesFailureModes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   string
	}{
		{"stale secret", http.StatusUnauthorized, "signing secret"},
		{"wrong path", http.StatusNotFound, "not there"},
		{"handler error", http.StatusInternalServerError, "drill exists to find"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDrillFixture(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			})
			rep, err := f.runner.Run(context.Background(), tenantA)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			<-f.got
			<-f.body
			if !strings.Contains(rep.Results[0].Diagnosis, tc.want) {
				t.Errorf("diagnosis = %q, want it to mention %q", rep.Results[0].Diagnosis, tc.want)
			}
		})
	}
}

// No endpoint is a real finding, not an error: it means a genuine reversal would
// have nowhere to go either.
func TestDrillWithNoEndpointIsAFinding(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)

	runner, err := drill.New(drill.Options{
		Store:   s,
		Sender:  webhook.NewSender(webhook.SenderOptions{}),
		Secrets: func(context.Context, string) (string, error) { return "whsec_test", nil },
	})
	if err != nil {
		t.Fatalf("drill.New: %v", err)
	}

	if _, err := runner.Run(context.Background(), tenantA); !errors.Is(err, drill.ErrNoEndpoint) {
		t.Errorf("err = %v, want ErrNoEndpoint", err)
	}
}

// stubRunner stands in for a real drill at the HTTP boundary.
type stubRunner struct {
	report drill.Report
	err    error
}

func (s stubRunner) Run(context.Context, string) (drill.Report, error) { return s.report, s.err }

func TestIngestFireDrill(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{drills: stubRunner{report: drill.Report{
		TenantID:      tenantA,
		TransactionID: drill.TransactionPrefix + "abc",
		Endpoints:     1,
		Failed:        1,
		Results:       []drill.Result{{EndpointID: "we_1", StatusCode: 500, Diagnosis: "your handler errored"}},
		Notice:        "synthetic transaction",
	}}})

	w := f.do(t, http.MethodPost, "/v1/fire-drill", f.keyA, nil)
	// A failing drill is a successful test that found a problem. A 5xx would
	// point the operator at us rather than at their handler.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	if body["failed"] != float64(1) {
		t.Errorf("failed = %v, want 1", body["failed"])
	}
	if body["notice"] == nil {
		t.Error("no notice explaining what a drill is")
	}

	// Nothing registered is a finding, not a server fault.
	f2 := newIngestFixture(t, fixtureOpts{drills: stubRunner{err: drill.ErrNoEndpoint}})
	if w := f2.do(t, http.MethodPost, "/v1/fire-drill", f2.keyA, nil); w.Code != http.StatusConflict {
		t.Errorf("no endpoint = %d, want 409", w.Code)
	}

	// A deployment without drills configured says so rather than pretending.
	f3 := newIngestFixture(t, fixtureOpts{})
	if w := f3.do(t, http.MethodPost, "/v1/fire-drill", f3.keyA, nil); w.Code != http.StatusNotImplemented {
		t.Errorf("unconfigured = %d, want 501", w.Code)
	}

	// And it needs a key like everything else under /v1.
	if w := f.do(t, http.MethodPost, "/v1/fire-drill", "", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated = %d, want 401", w.Code)
	}
}

// Each run gets its own id, so two drills can never be confused with each other
// or with a real transaction.
func TestDrillIDsAreUnique(t *testing.T) {
	f := newDrillFixture(t, ok)
	ctx := context.Background()

	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		rep, err := f.runner.Run(ctx, tenantA)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		<-f.got
		<-f.body
		if seen[rep.TransactionID] {
			t.Fatalf("reused transaction id %s", rep.TransactionID)
		}
		seen[rep.TransactionID] = true
	}
}
