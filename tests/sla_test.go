package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// atRiskDetector warns 4h before a 24h deadline, on a clock the test controls.
func atRiskDetector(t *testing.T, s store.Store, now func() time.Time) *service.Detector {
	t.Helper()
	d, err := service.NewDetector(s, service.DetectorOptions{
		Now:              now,
		ReversalDeadline: 24 * time.Hour,
		SLAWarnBefore:    4 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	return d
}

// atRiskDeliveries returns the sla.at_risk deliveries queued for a tenant.
func atRiskDeliveries(t *testing.T, s store.Store, tenantID string) []*store.DeliveryRecord {
	t.Helper()
	all, err := s.ListDeliveries(context.Background(), tenantID, "", 100)
	if err != nil {
		t.Fatalf("ListDeliveries: %v", err)
	}
	var out []*store.DeliveryRecord
	for _, d := range all {
		if d.EventType == string(webhook.EventSLAAtRisk) {
			out = append(out, d)
		}
	}
	return out
}

// The compliance report scores a breach after the fact. This is the same
// information while there is still time to act.
func TestSLAWarningFiresBeforeTheDeadline(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	// Debited 21 hours ago: 3 hours left of a 24-hour deadline, inside the
	// 4-hour warning window.
	txn := newExpiredTxn(tenantA, "TX-1", 21*time.Hour, time.Minute)
	mustUpsert(t, s, txn)

	now := time.Now().UTC()
	d := atRiskDetector(t, s, func() time.Time { return now })
	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.AtRisk != 1 {
		t.Fatalf("at risk = %d, want 1", res.AtRisk)
	}

	queued := atRiskDeliveries(t, s, tenantA)
	if len(queued) != 1 {
		t.Fatalf("queued %d sla.at_risk deliveries, want 1", len(queued))
	}

	// Claimed slightly ahead: the sweep enqueued with the wall clock, which is a
	// moment after the frozen one the detector was given.
	due, err := s.ClaimDueDeliveries(ctx, time.Now().UTC().Add(time.Minute), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	var env webhook.Envelope
	for _, d := range due {
		if d.EventType != string(webhook.EventSLAAtRisk) {
			continue
		}
		if err := json.Unmarshal(d.Payload, &env); err != nil {
			t.Fatalf("payload: %v", err)
		}
	}
	if env.Data.DeadlineSeconds == nil {
		t.Fatal("no seconds_until_breach on the warning")
	}
	// Three hours left, which is the number that makes it actionable.
	if *env.Data.DeadlineSeconds < 2*3600 || *env.Data.DeadlineSeconds > 4*3600 {
		t.Errorf("seconds_until_breach = %d, want about 3 hours", *env.Data.DeadlineSeconds)
	}
	// Not a verdict about whether the transfer failed — a statement about the
	// clock. A confidence here would invite a receiver to treat it as one.
	if env.Data.Confidence != 0 {
		t.Errorf("confidence = %v, want 0 on a deadline warning", env.Data.Confidence)
	}
}

// A transaction with plenty of time left is not at risk, and warning early
// would train people to ignore the warning.
func TestSLAWarningStaysQuietWhileThereIsTime(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	// Two hours old: 22 hours left, well outside the warning window.
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 2*time.Hour, time.Minute))

	now := time.Now().UTC()
	res, err := atRiskDetector(t, s, func() time.Time { return now }).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.AtRisk != 0 {
		t.Errorf("at risk = %d, want 0 with 22 hours left", res.AtRisk)
	}
}

// Once per transaction. A sweep runs every five seconds; warning on each one
// would be seventeen thousand webhooks before the deadline arrived.
func TestSLAWarningFiresOncePerTransaction(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 21*time.Hour, time.Minute))

	now := time.Now().UTC()
	d := atRiskDetector(t, s, func() time.Time { return now })

	total := 0
	for i := 0; i < 10; i++ {
		res, err := d.Sweep(context.Background())
		if err != nil {
			t.Fatalf("sweep %d: %v", i, err)
		}
		total += res.AtRisk
	}
	if total != 1 {
		t.Fatalf("10 sweeps produced %d warnings, want 1", total)
	}
	if queued := atRiskDeliveries(t, s, tenantA); len(queued) != 1 {
		t.Errorf("queued %d deliveries for one transaction, want 1", len(queued))
	}
}

// Money that came back is not at risk. Warning about it would be noise at the
// exact moment the customer did the right thing.
func TestSLAWarningIgnoresResolvedTransactions(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	mustUpsert(t, s,
		newExpiredTxn(tenantA, "TX-SETTLED", 21*time.Hour, time.Minute),
		newExpiredTxn(tenantA, "TX-REVERSED", 21*time.Hour, time.Minute))

	now := time.Now().UTC()
	if _, err := s.ApplyCredit(ctx, tenantA, "TX-SETTLED", domain.StatusCompleted, now); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}
	// Drive the other one all the way to a confirmed reversal.
	if _, err := s.ApplyCredit(ctx, tenantA, "TX-REVERSED", domain.StatusOrphaned, now); err != nil {
		t.Fatalf("ApplyCredit: %v", err)
	}
	if _, err := s.MarkReversalPending(ctx, tenantA, "TX-REVERSED", now); err != nil {
		t.Fatalf("MarkReversalPending: %v", err)
	}
	if _, err := s.MarkReversalCompleted(ctx, tenantA, "TX-REVERSED", now); err != nil {
		t.Fatalf("MarkReversalCompleted: %v", err)
	}

	res, err := atRiskDetector(t, s, func() time.Time { return now }).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.AtRisk != 0 {
		t.Errorf("at risk = %d, want 0 — both are resolved", res.AtRisk)
	}
}

// Replayed history must not warn: shadow mode would otherwise fire a webhook
// for every failure in the last 90 days.
func TestSLAWarningIgnoresBackfill(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")

	txn := newExpiredTxn(tenantA, "TX-OLD", 21*time.Hour, time.Minute)
	txn.IsBackfill = true
	mustUpsert(t, s, txn)

	now := time.Now().UTC()
	res, err := atRiskDetector(t, s, func() time.Time { return now }).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.AtRisk != 0 {
		t.Errorf("at risk = %d, want 0 for replayed history", res.AtRisk)
	}
}

// The warning is opt-out, because a deployment that does not want it should not
// have to filter it at the receiver.
func TestSLAWarningCanBeDisabled(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 21*time.Hour, time.Minute))

	now := time.Now().UTC()
	d, err := service.NewDetector(s, service.DetectorOptions{
		Now:              func() time.Time { return now },
		ReversalDeadline: 24 * time.Hour,
		SLAWarnBefore:    -1, // negative disables
	})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	res, err := d.Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.AtRisk != 0 {
		t.Errorf("at risk = %d, want 0 when disabled", res.AtRisk)
	}
}

// --- store conformance ---

func testClaimSLAAtRisk(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	soon := newExpiredTxn(tenantA, "TX-SOON", 21*time.Hour, time.Minute)
	later := newExpiredTxn(tenantA, "TX-LATER", 2*time.Hour, time.Minute)
	mustUpsert(t, s, soon, later)
	if _, err := s.ClaimExpired(ctx, now, 100); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	got, err := s.ClaimSLAAtRisk(ctx, now, 24*time.Hour, 4*time.Hour, 100)
	if err != nil {
		t.Fatalf("ClaimSLAAtRisk: %v", err)
	}
	if len(got) != 1 || got[0].TransactionID != "TX-SOON" {
		t.Fatalf("claimed %v, want [TX-SOON]", txnIDs(got))
	}
	if got[0].SLAWarnedAt == nil {
		t.Error("claimed transaction was not marked as warned")
	}

	// The claim is what makes it exactly-once: a second sweep gets nothing.
	again, err := s.ClaimSLAAtRisk(ctx, now, 24*time.Hour, 4*time.Hour, 100)
	if err != nil {
		t.Fatalf("ClaimSLAAtRisk: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("claimed %v twice", txnIDs(again))
	}

	// And the mark survives a round trip, so a restart does not re-warn.
	stored, err := s.Get(ctx, tenantA, "TX-SOON")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.SLAWarnedAt == nil {
		t.Error("sla_warned_at was not persisted")
	}
}

// B5, expressed as a threshold rather than a hard-coded rule. Silence alone
// reaches 0.70, so a floor above that means nothing is advised as a reversal
// without a second, independent signal.
func TestConfidenceFloorDowngradesAThinVerdict(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 5*time.Minute, time.Minute))

	d, err := service.NewDetector(s, service.DetectorOptions{
		// Above what silence alone can reach.
		MinReversalConfidence: 0.8,
	})
	if err != nil {
		t.Fatalf("NewDetector: %v", err)
	}
	res, err := d.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.BelowConfidenceFloor != 1 {
		t.Fatalf("below floor = %d, want 1", res.BelowConfidenceFloor)
	}

	// It becomes an investigation, not a reversal.
	txn, err := s.Get(ctx, tenantA, "TX-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if txn.Status != domain.StatusSuspect {
		t.Errorf("status = %s, want suspect", txn.Status)
	}

	due, err := s.ClaimDueDeliveries(ctx, time.Now().UTC().Add(time.Minute), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	for _, d := range due {
		if d.EventType == string(webhook.EventReversalTriggered) {
			t.Error("advised a reversal on evidence below the floor")
		}
	}
}

// A floor of zero is what every deployment had before this existed, and must
// keep behaving identically.
func TestNoConfidenceFloorKeepsEveryVerdict(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-1", 5*time.Minute, time.Minute))

	res, err := newDetector(t, s).Sweep(context.Background())
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.BelowConfidenceFloor != 0 {
		t.Errorf("downgraded a verdict with no floor configured: %d", res.BelowConfidenceFloor)
	}
	if res.Queued != 1 {
		t.Errorf("queued %d, want the reversal", res.Queued)
	}
}
