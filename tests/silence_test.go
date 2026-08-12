package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// busyThenSilent records a tenant sending steadily for an hour, then stopping.
func busyThenSilent(t *testing.T, s store.Store, tenantID string, now time.Time, quietFor time.Duration) {
	t.Helper()

	samples := make([]store.IngestSample, 0, 60)
	for i := 1; i <= 60; i++ {
		samples = append(samples, store.IngestSample{
			TenantID: tenantID,
			Bucket:   now.Add(-quietFor).Add(-time.Duration(i) * time.Minute),
			Received: 500,
		})
	}
	if err := s.RecordIngestHealth(context.Background(), samples); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}
}

func silenceParams() store.SilenceParams {
	return store.SilenceParams{
		Quiet:            5 * time.Minute,
		Baseline:         time.Hour,
		MinActiveBuckets: 10,
	}
}

func testSilentTenants(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	// Tenant A was busy and has gone quiet. Tenant B is still sending.
	busyThenSilent(t, s, tenantA, now, 10*time.Minute)
	busyThenSilent(t, s, tenantB, now, 10*time.Minute)
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantB, Bucket: now.Add(-time.Minute), Received: 300},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	silent, err := s.SilentTenants(ctx, now, silenceParams())
	if err != nil {
		t.Fatalf("SilentTenants: %v", err)
	}
	if len(silent) != 1 || silent[0] != tenantA {
		t.Fatalf("silent = %v, want [%s]", silent, tenantA)
	}
}

// A tenant that sends four events a day is not broken. Suppressing their
// detection would be a bug, not a safeguard.
func testLowVolumeTenantIsNotSilent(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	// Three active minutes in the baseline hour, well under MinActiveBuckets.
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantA, Bucket: now.Add(-40 * time.Minute), Received: 1},
		{TenantID: tenantA, Bucket: now.Add(-30 * time.Minute), Received: 1},
		{TenantID: tenantA, Bucket: now.Add(-20 * time.Minute), Received: 1},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	silent, err := s.SilentTenants(ctx, now, silenceParams())
	if err != nil {
		t.Fatalf("SilentTenants: %v", err)
	}
	if len(silent) != 0 {
		t.Errorf("silent = %v, want none — a low-volume tenant is not a broken one", silent)
	}
}

func testSilenceCheckDisabled(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()
	busyThenSilent(t, s, tenantA, now, 10*time.Minute)

	silent, err := s.SilentTenants(ctx, now, store.SilenceParams{})
	if err != nil {
		t.Fatalf("SilentTenants: %v", err)
	}
	if len(silent) != 0 {
		t.Errorf("silent = %v, want none when the check is disabled", silent)
	}
}

// The interlock: with several replicas sweeping the same tenants, exactly one
// opens the episode and only that one alerts.
func testSyncSilenceEpisodes(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	first, err := s.SyncSilenceEpisodes(ctx, []string{tenantA}, now)
	if err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}
	if len(first.Opened) != 1 || first.Opened[0].TenantID != tenantA {
		t.Fatalf("opened = %+v, want one episode for %s", first.Opened, tenantA)
	}
	if len(first.Recovered) != 0 {
		t.Errorf("recovered = %+v on the first call", first.Recovered)
	}

	// A second sweep — or a second replica — must not claim it again.
	second, err := s.SyncSilenceEpisodes(ctx, []string{tenantA}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}
	if len(second.Opened) != 0 {
		t.Errorf("the same episode was opened twice: %+v", second.Opened)
	}

	// Events resume. The episode closes once, carrying the time it started.
	recovered, err := s.SyncSilenceEpisodes(ctx, nil, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}
	if len(recovered.Recovered) != 1 || recovered.Recovered[0].TenantID != tenantA {
		t.Fatalf("recovered = %+v, want one episode for %s", recovered.Recovered, tenantA)
	}
	if !recovered.Recovered[0].SilentSince.Equal(now) {
		t.Errorf("silent_since = %v, want the time the episode opened (%v)",
			recovered.Recovered[0].SilentSince, now)
	}

	// Closing is idempotent, and the tenant is re-armed for the next episode.
	again, err := s.SyncSilenceEpisodes(ctx, nil, now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}
	if len(again.Recovered) != 0 {
		t.Errorf("recovered twice: %+v", again.Recovered)
	}
	reopened, err := s.SyncSilenceEpisodes(ctx, []string{tenantA}, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}
	if len(reopened.Opened) != 1 {
		t.Errorf("a tenant that broke twice was only alerted once: %+v", reopened.Opened)
	}
}

// Telling a tenant they went quiet "just now" after a three-hour outage would
// understate it by three hours, so the episode is dated from their last event.
func testSilenceIsDatedFromTheLastEvent(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)

	lastEvent := now.Add(-3 * time.Hour)
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantA, Bucket: lastEvent, Received: 500},
		{TenantID: tenantA, Bucket: lastEvent.Add(-time.Minute), Received: 500},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	change, err := s.SyncSilenceEpisodes(ctx, []string{tenantA}, now)
	if err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}
	if len(change.Opened) != 1 {
		t.Fatalf("opened = %+v, want one episode", change.Opened)
	}

	// The minute after their last event is when the silence began.
	want := lastEvent.Add(time.Minute)
	if got := change.Opened[0].SilentSince.UTC(); !got.Equal(want) {
		t.Errorf("silent_since = %v, want %v — the episode must be dated from their last event, not from when we noticed",
			got, want)
	}
}

// One tenant recovering must not close another's open episode.
func testSilenceEpisodesAreIndependent(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	if _, err := s.SyncSilenceEpisodes(ctx, []string{tenantA, tenantB}, now); err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}

	change, err := s.SyncSilenceEpisodes(ctx, []string{tenantA}, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("SyncSilenceEpisodes: %v", err)
	}
	if len(change.Recovered) != 1 || change.Recovered[0].TenantID != tenantB {
		t.Fatalf("recovered = %+v, want only %s", change.Recovered, tenantB)
	}
	if len(change.Opened) != 0 {
		t.Errorf("re-opened a still-silent tenant: %+v", change.Opened)
	}
}

func testClaimExpiredSkipsTenants(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-A", 5*time.Minute, time.Minute))
	mustUpsert(t, s, newExpiredTxn(tenantB, "TX-B", 5*time.Minute, time.Minute))

	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 100, store.SkipTenants(tenantA))
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TransactionID != "TX-B" {
		t.Fatalf("claimed %v, want only TX-B", txnIDs(claimed))
	}

	// The skipped transaction stays open, so it can settle normally once the
	// tenant recovers and sends the credit it owes.
	skipped, err := s.Get(ctx, tenantA, "TX-A")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if skipped.Status != domain.StatusPendingDebit {
		t.Errorf("skipped transaction is %s, want pending_debit", skipped.Status)
	}
}

// The scenario this exists to prevent: a tenant's integration breaks with
// hundreds of debits in flight, and we fire a reversal for every one of them
// into a system that is already down.
func TestSilentTenantDoesNotGetMassReversed(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	const inFlight = 200
	batch := make([]*domain.Transaction, 0, inFlight)
	for i := 0; i < inFlight; i++ {
		batch = append(batch, newExpiredTxn(tenantA, fmt.Sprintf("TX-%03d", i), 6*time.Minute, time.Minute))
	}
	if _, err := s.UpsertDebits(ctx, tenantA, batch); err != nil {
		t.Fatalf("UpsertDebits: %v", err)
	}

	// They were sending steadily, then stopped ten minutes ago.
	busyThenSilent(t, s, tenantA, now, 10*time.Minute)

	d, err := service.NewDetector(s, service.DetectorOptions{})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if res.Claimed != 0 {
		t.Errorf("claimed %d transactions from a silent tenant, want 0", res.Claimed)
	}
	if len(res.SilentTenants) != 1 || res.SilentTenants[0] != tenantA {
		t.Errorf("silent tenants = %v, want [%s]", res.SilentTenants, tenantA)
	}

	deliveries, err := s.ListDeliveries(ctx, tenantA, "", 1000)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	for _, d := range deliveries {
		if d.EventType != string(webhook.EventIntegrationSilent) {
			t.Fatalf("queued %s into a tenant that is already down, want only the silence alert", d.EventType)
		}
	}

	// Nothing was consumed: every transaction is still open and can settle when
	// they recover.
	pending, err := s.ListByStatus(ctx, tenantA, domain.StatusPendingDebit, 1000)
	if err != nil {
		t.Fatalf("ListByStatus: %v", err)
	}
	if len(pending) != inFlight {
		t.Errorf("%d transactions still pending, want %d", len(pending), inFlight)
	}
}

// Once the tenant starts sending again, detection resumes normally.
func TestDetectionResumesWhenTheTenantRecovers(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 6*time.Minute, time.Minute))
	busyThenSilent(t, s, tenantA, now, 10*time.Minute)

	d, err := service.NewDetector(s, service.DetectorOptions{})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	if res, err := d.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	} else if res.Claimed != 0 {
		t.Fatalf("claimed %d while silent, want 0", res.Claimed)
	}

	// They come back.
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantA, Bucket: now, Received: 400},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep after recovery: %v", err)
	}
	if len(res.SilentTenants) != 0 {
		t.Errorf("still reported silent after recovery: %v", res.SilentTenants)
	}
	if res.Claimed != 1 {
		t.Errorf("claimed %d after recovery, want 1", res.Claimed)
	}
}

// Suppressing detection without telling anyone is the worst of both worlds:
// nothing is being watched and nobody knows.
func TestSilentTenantIsAlerted(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	busyThenSilent(t, s, tenantA, now, 10*time.Minute)

	d, err := service.NewDetector(s, service.DetectorOptions{})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.SilenceAlerts != 1 {
		t.Fatalf("silence alerts = %d, want 1", res.SilenceAlerts)
	}

	due, err := s.ClaimDueDeliveries(ctx, time.Now().UTC(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("queued %d deliveries, want 1", len(due))
	}
	alert := due[0]
	if alert.EventType != string(webhook.EventIntegrationSilent) {
		t.Fatalf("event = %s, want %s", alert.EventType, webhook.EventIntegrationSilent)
	}
	// An event about the stream concerns no transaction, and inventing one
	// would put a transaction that does not exist into their delivery log.
	if alert.TransactionID != "" {
		t.Errorf("transaction_id = %q, want empty for an integration event", alert.TransactionID)
	}

	var body map[string]any
	if err := json.Unmarshal(alert.Payload, &body); err != nil {
		t.Fatalf("payload: %v", err)
	}
	data, _ := body["data"].(map[string]any)
	if data["advisory"] != true {
		t.Error("silence alert is not marked advisory")
	}
	// The part that costs them money is that we stopped judging anything.
	if data["detection_suspended"] != true {
		t.Error("alert does not say detection is suspended")
	}
	if data["actionable"] == "" || data["actionable"] == nil {
		t.Error("alert says nothing about what to do")
	}
}

// A tenant that goes quiet at 2am must not receive a webhook every five seconds
// until morning.
func TestSilenceAlertsOncePerEpisode(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	busyThenSilent(t, s, tenantA, now, 10*time.Minute)

	d, err := service.NewDetector(s, service.DetectorOptions{})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}

	total := 0
	for i := 0; i < 20; i++ {
		res, err := d.Sweep(ctx)
		if err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
		total += res.SilenceAlerts
	}
	if total != 1 {
		t.Fatalf("20 sweeps produced %d alerts, want 1", total)
	}

	deliveries, err := s.ListDeliveries(ctx, tenantA, "", 100)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	if len(deliveries) != 1 {
		t.Errorf("queued %d deliveries for one episode, want 1", len(deliveries))
	}
}

// An alert that never clears trains people to ignore alerts.
func TestRecoveryClosesTheEpisodeAndRearmsIt(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	busyThenSilent(t, s, tenantA, now, 10*time.Minute)

	clock := now
	d, err := service.NewDetector(s, service.DetectorOptions{Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	if res, err := d.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	} else if res.SilenceAlerts != 1 {
		t.Fatalf("silence alerts = %d, want 1", res.SilenceAlerts)
	}

	// They come back.
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantA, Bucket: clock, Received: 400},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep after recovery: %v", err)
	}
	if res.RecoveryAlerts != 1 {
		t.Fatalf("recovery alerts = %d, want 1", res.RecoveryAlerts)
	}

	// And the all-clear is sent once, not on every subsequent sweep.
	if res2, err := d.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	} else if res2.RecoveryAlerts != 0 {
		t.Errorf("recovery alerted again on the next sweep: %d", res2.RecoveryAlerts)
	}

	// The episode is re-armed: breaking again must alert again, rather than stay
	// quiet because we already told them once.
	clock = now.Add(2 * time.Hour)
	busyThenSilent(t, s, tenantA, clock, 10*time.Minute)
	if res3, err := d.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	} else if res3.SilenceAlerts != 1 {
		t.Errorf("second episode produced %d alerts, want 1", res3.SilenceAlerts)
	}
}

// With the check disabled every tenant looks recovered, and closing the episodes
// would fire an all-clear nobody earned.
func TestDisabledSilenceCheckSendsNothing(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	busyThenSilent(t, s, tenantA, time.Now().UTC(), 10*time.Minute)

	d, err := service.NewDetector(s, service.DetectorOptions{Silence: &store.SilenceParams{}})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.SilenceAlerts != 0 || res.RecoveryAlerts != 0 {
		t.Errorf("disabled check still alerted: %+v", res)
	}
}

// A tenant with no history at all must not be treated as silent, or a brand new
// deployment would never detect anything.
func TestTenantWithNoHistoryIsNotSilent(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 5*time.Minute, time.Minute))

	d, err := service.NewDetector(s, service.DetectorOptions{})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SilentTenants) != 0 {
		t.Errorf("a tenant with no ingest history was reported silent: %v", res.SilentTenants)
	}
	if res.Claimed != 1 {
		t.Errorf("claimed %d, want 1", res.Claimed)
	}
}
