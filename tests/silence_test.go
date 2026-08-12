package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
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
	if len(deliveries) != 0 {
		t.Fatalf("queued %d reversals into a tenant that is already down, want 0", len(deliveries))
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
