package tests

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/metrics"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// scrape returns the /metrics body.
func scrape(t *testing.T, f *ingestFixture) string {
	t.Helper()
	w := f.do(t, http.MethodGet, "/metrics", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", w.Code)
	}
	return w.Body.String()
}

// metricValue pulls one sample out of the exposition text.
func metricValue(t *testing.T, body, name string) (string, bool) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") {
			return strings.TrimSpace(strings.TrimPrefix(line, name+" ")), true
		}
	}
	return "", false
}

// The gap this closes: ingest counters climb whether or not anything is being
// detected, so a dead detector looked exactly like a healthy process.
func TestMetricsReportTheDetectionLoop(t *testing.T) {
	reg := metrics.New()
	f := newIngestFixture(t, fixtureOpts{metrics: reg})
	ctx := context.Background()

	// Before any sweep, the freshness gauge must be absent rather than zero.
	// Zero seconds since a sweep that never happened reads as perfect health on
	// a process whose detector never started, and the alert would never fire.
	if _, ok := metricValue(t, scrape(t, f), "reconsync_seconds_since_last_sweep"); ok {
		t.Error("reported time since a sweep that never happened")
	}

	newEndpoint(t, f.store, tenantA, "we_1", "https://customer.example.com/hook")
	mustUpsert(t, f.store, newExpiredTxn(tenantA, "TX-1", 5*time.Minute, time.Minute))

	d, err := service.NewDetector(f.store, service.DetectorOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	if _, err := d.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	body := scrape(t, f)
	for name, want := range map[string]string{
		"reconsync_detection_sweeps_total":      "1",
		"reconsync_transactions_detected_total": "1",
		"reconsync_reversals_queued_total":      "1",
	} {
		got, ok := metricValue(t, body, name)
		if !ok {
			t.Errorf("%s missing from /metrics", name)
			continue
		}
		if got != want {
			t.Errorf("%s = %s, want %s", name, got, want)
		}
	}

	// Now that a sweep has happened, both gauges appear.
	if _, ok := metricValue(t, body, "reconsync_seconds_since_last_sweep"); !ok {
		t.Error("no reconsync_seconds_since_last_sweep after a sweep")
	}
	// The transaction's window closed four minutes before it was claimed, so
	// the lag is the real number the SLO is written against — not zero.
	lag, ok := metricValue(t, body, "reconsync_detection_lag_seconds")
	if !ok {
		t.Fatal("no reconsync_detection_lag_seconds")
	}
	if lag == "0.000" {
		t.Errorf("detection lag = %s, want the age of the overdue transaction", lag)
	}
}

// A loop that runs and fails every time is not a working loop, and letting it
// refresh the freshness gauge would hide exactly the outage the alert is for.
func TestMetricsDoNotLetAFailingSweepLookFresh(t *testing.T) {
	reg := metrics.New()
	f := newIngestFixture(t, fixtureOpts{metrics: reg})

	reg.RecordSweepFailure()
	reg.RecordSweepFailure()

	body := scrape(t, f)
	if got, _ := metricValue(t, body, "reconsync_detection_sweep_failures_total"); got != "2" {
		t.Errorf("failures = %s, want 2", got)
	}
	if _, ok := metricValue(t, body, "reconsync_seconds_since_last_sweep"); ok {
		t.Error("a sweep that only ever failed refreshed the freshness gauge")
	}
}

func TestMetricsReportTheDispatchLoop(t *testing.T) {
	reg := metrics.New()
	f := newIngestFixture(t, fixtureOpts{metrics: reg})

	reg.RecordDispatch(time.Now().UTC(), 3, 2, 1)

	body := scrape(t, f)
	for name, want := range map[string]string{
		"reconsync_deliveries_delivered_total":     "3",
		"reconsync_deliveries_retrying_total":      "2",
		"reconsync_deliveries_dead_lettered_total": "1",
	} {
		if got, _ := metricValue(t, body, name); got != want {
			t.Errorf("%s = %s, want %s", name, got, want)
		}
	}
}

// Suppressed detection is the state an operator most needs to see, because
// nothing is being watched while it lasts.
func TestMetricsReportSuppressedTenants(t *testing.T) {
	reg := metrics.New()
	f := newIngestFixture(t, fixtureOpts{metrics: reg})
	ctx := context.Background()

	busyThenSilent(t, f.store, tenantA, time.Now().UTC(), 10*time.Minute)

	d, err := service.NewDetector(f.store, service.DetectorOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	if _, err := d.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if got, _ := metricValue(t, scrape(t, f), "reconsync_silent_tenants"); got != "1" {
		t.Errorf("silent tenants = %s, want 1", got)
	}
}

// A nil registry must be safe: the loops are usable without metrics, and every
// existing test constructs them that way.
func TestMetricsAreOptional(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 5*time.Minute, time.Minute))

	d, err := service.NewDetector(s, service.DetectorOptions{})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	if _, err := d.Sweep(context.Background()); err != nil {
		t.Fatalf("Sweep with no metrics registry: %v", err)
	}

	// And reading a nil registry does not panic either.
	var nilReg *metrics.Registry
	if snap := nilReg.Read(time.Now()); snap.Sweeps != 0 {
		t.Errorf("nil registry reported %d sweeps", snap.Sweeps)
	}
}

// A negative count is a caller bug, and converting one straight to uint64 would
// add eighteen quintillion to a counter that can only go up — destroying the
// metric at the moment something is already wrong.
func TestMetricsClampNegativeCounts(t *testing.T) {
	reg := metrics.New()
	reg.RecordSweep(time.Now().UTC(), metrics.SweepResult{Claimed: -1, Queued: -5})
	reg.RecordDispatch(time.Now().UTC(), -1, -1, -1)

	snap := reg.Read(time.Now().UTC())
	for name, got := range map[string]uint64{
		"detected":      snap.Detected,
		"queued":        snap.ReversalsQueued,
		"delivered":     snap.Delivered,
		"dead_lettered": snap.DeadLettered,
	} {
		if got != 0 {
			t.Errorf("%s = %d, want 0 rather than a wrapped negative", name, got)
		}
	}
}
