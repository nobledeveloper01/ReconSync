package tests

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/evidence"
	"github.com/nobledeveloper01/ReconSync/internal/provider"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

func TestEvidenceConfidenceSumsWeights(t *testing.T) {
	ev := evidence.New()
	if got := ev.Confidence(); got != 0 {
		t.Errorf("empty set confidence = %v, want 0", got)
	}

	ev.Add(evidence.SignalWindowExpired, "no credit within 300s", evidence.WeightWindowExpired)
	if got := ev.Confidence(); got != 0.55 {
		t.Errorf("confidence = %v, want 0.55", got)
	}

	ev.Add(evidence.SignalIngestIntact, "no events lost", evidence.WeightIngestIntact)
	if got := ev.Confidence(); got != 0.70 {
		t.Errorf("confidence = %v, want 0.70", got)
	}

	ev.Add(evidence.SignalProviderFailed, "rail confirms failure", evidence.WeightProviderFailed)
	if got := ev.Confidence(); got != 1.0 {
		t.Errorf("confidence = %v, want 1.0", got)
	}
}

// Silence alone must never reach the range where auto-reversal looks safe. That
// ordering is the entire point of publishing a number.
func TestEvidenceSilenceAloneStaysBelowCorroborated(t *testing.T) {
	inferred := evidence.New()
	inferred.Add(evidence.SignalWindowExpired, "no credit", evidence.WeightWindowExpired)
	inferred.Add(evidence.SignalIngestIntact, "intact", evidence.WeightIngestIntact)

	checked := evidence.New()
	checked.Add(evidence.SignalWindowExpired, "no credit", evidence.WeightWindowExpired)
	checked.Add(evidence.SignalIngestIntact, "intact", evidence.WeightIngestIntact)
	checked.Add(evidence.SignalProviderNotFound, "no record", evidence.WeightProviderNotFound)

	if inferred.Confidence() >= checked.Confidence() {
		t.Fatalf("inferred %v is not below corroborated %v", inferred.Confidence(), checked.Confidence())
	}
	if inferred.Confidence() > 0.75 {
		t.Errorf("inference alone reached %v — too close to certainty", inferred.Confidence())
	}
}

func TestEvidenceIsClampedAndRounded(t *testing.T) {
	ev := evidence.New()
	for i := 0; i < 10; i++ {
		ev.Add("piled_on", "x", 0.5)
	}
	if got := ev.Confidence(); got != 1.0 {
		t.Errorf("confidence = %v, want it clamped to 1.0", got)
	}

	neg := evidence.New()
	neg.Add("negative", "x", -5)
	if got := neg.Confidence(); got != 0 {
		t.Errorf("confidence = %v, want it clamped to 0", got)
	}
}

func TestEvidenceSignalsAreHeaviestFirst(t *testing.T) {
	ev := evidence.New()
	ev.Add(evidence.SignalIngestIntact, "intact", evidence.WeightIngestIntact)
	ev.Add(evidence.SignalWindowExpired, "no credit", evidence.WeightWindowExpired)
	ev.Add(evidence.SignalProviderUnreachable, "timed out", evidence.WeightProviderUnreached)

	got := ev.Signals()
	if len(got) != 3 {
		t.Fatalf("got %d signals, want 3", len(got))
	}
	if got[0].Name != evidence.SignalWindowExpired {
		t.Errorf("first = %s, want the heaviest signal", got[0].Name)
	}
	if got[2].Name != evidence.SignalProviderUnreachable {
		t.Errorf("last = %s, want the zero-weight signal", got[2].Name)
	}
}

// A nil set is the honest reading of "we recorded nothing", and must not panic.
func TestEvidenceNilSetIsSafe(t *testing.T) {
	var ev *evidence.Set
	if got := ev.Confidence(); got != 0 {
		t.Errorf("nil confidence = %v, want 0", got)
	}
	if got := ev.Signals(); got != nil {
		t.Errorf("nil signals = %v, want nil", got)
	}
	if ev.Has(evidence.SignalWindowExpired) {
		t.Error("nil set reported having a signal")
	}
	ev.Add("x", "y", 1) // must not panic
}

// --- what actually reaches the customer ---

func decodeDelivered(t *testing.T, s store.Store, txnID string) webhook.Envelope {
	t.Helper()
	ctx := context.Background()

	due, err := s.ClaimDueDeliveries(ctx, time.Now().UTC(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	for _, d := range due {
		if d.TransactionID != txnID {
			continue
		}
		var env webhook.Envelope
		if err := json.Unmarshal(d.Payload, &env); err != nil {
			t.Fatalf("payload is not a valid envelope: %v", err)
		}
		return env
	}
	t.Fatalf("no delivery queued for %s", txnID)
	return webhook.Envelope{}
}

// Without corroboration a reversal is an inference, and the payload must say so.
func TestReversalWithoutCorroborationCarriesModerateConfidence(t *testing.T) {
	s, _ := corroboratingSetup(t)

	if res := sweepWith(t, s, nil); res.Queued != 1 {
		t.Fatalf("queued = %d, want 1", res.Queued)
	}

	env := decodeDelivered(t, s, "TX-1")
	if env.Data.Confidence != 0.70 {
		t.Errorf("confidence = %v, want 0.70 for an uncorroborated reversal", env.Data.Confidence)
	}
	if len(env.Data.Evidence) != 2 {
		t.Fatalf("evidence = %v, want window_expired and ingest_intact", env.Data.Evidence)
	}
	for _, sig := range env.Data.Evidence {
		if sig.Name == evidence.SignalProviderFailed || sig.Name == evidence.SignalProviderNotFound {
			t.Error("claimed provider corroboration without asking anyone")
		}
	}
}

// With the rail confirming failure, the same verdict earns near certainty.
func TestCorroboratedReversalCarriesHighConfidence(t *testing.T) {
	s, _ := corroboratingSetup(t)

	if res := sweepWith(t, s, registryAnswering(t, provider.Failed)); res.Queued != 1 {
		t.Fatalf("queued = %d, want 1", res.Queued)
	}

	env := decodeDelivered(t, s, "TX-1")
	if env.Data.Confidence < 0.95 {
		t.Errorf("confidence = %v, want >= 0.95 when the rail confirmed failure", env.Data.Confidence)
	}

	var sawProvider bool
	for _, sig := range env.Data.Evidence {
		if sig.Name == evidence.SignalProviderFailed {
			sawProvider = true
		}
	}
	if !sawProvider {
		t.Error("the provider signal is missing from the evidence trail")
	}
}

// An ingest gap suppresses the reversal, and the suspect payload must record why.
func TestSuspectFromIngestGapRecordsTheGap(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	newEndpoint(t, s, tenantA, "we_1", "https://customer.example.com/hook")
	ctx := context.Background()

	txn := newExpiredTxn(tenantA, "TX-GAP", 5*time.Minute, time.Minute)
	mustUpsert(t, s, txn)
	if err := s.RecordIngestHealth(ctx, []store.IngestSample{
		{TenantID: tenantA, Bucket: txn.DebitAt, Dropped: 4},
	}); err != nil {
		t.Fatalf("RecordIngestHealth: %v", err)
	}

	if res := sweepWith(t, s, nil); res.Suspect != 1 {
		t.Fatalf("suspect = %d, want 1", res.Suspect)
	}

	env := decodeDelivered(t, s, "TX-GAP")
	if env.Event != webhook.EventTransactionSuspect {
		t.Errorf("event = %s, want transaction.suspect", env.Event)
	}
	// A gap carries no weight, so the number must stay low.
	if env.Data.Confidence > 0.6 {
		t.Errorf("confidence = %v, too high for a transaction we cannot vouch for", env.Data.Confidence)
	}

	var sawGap bool
	for _, sig := range env.Data.Evidence {
		if sig.Name == evidence.SignalIngestGap {
			sawGap = true
		}
	}
	if !sawGap {
		t.Error("the ingest gap is missing from the evidence trail")
	}
}

// When the rail cannot be reached we record that we tried, so a receiver can
// tell it apart from never having asked.
func TestUnreachableRailIsRecordedAsEvidence(t *testing.T) {
	s, _ := corroboratingSetup(t)

	if res := sweepWith(t, s, registryAnswering(t, provider.Unknown)); res.Uncertain != 1 {
		t.Fatalf("uncertain = %d, want 1", res.Uncertain)
	}

	env := decodeDelivered(t, s, "TX-1")
	if env.Event != webhook.EventTransactionSuspect {
		t.Errorf("event = %s, want transaction.suspect", env.Event)
	}

	var sawUnreachable bool
	for _, sig := range env.Data.Evidence {
		if sig.Name == evidence.SignalProviderUnreachable {
			sawUnreachable = true
			if sig.Weight != 0 {
				t.Errorf("an unreachable rail carried weight %v, want 0", sig.Weight)
			}
		}
	}
	if !sawUnreachable {
		t.Error("no record that we tried to reach the rail and failed")
	}
}

// The evidence trail must never carry customer data.
func TestEvidenceDoesNotLeakCustomerData(t *testing.T) {
	s, _ := corroboratingSetup(t)
	if res := sweepWith(t, s, registryAnswering(t, provider.Failed)); res.Queued != 1 {
		t.Fatalf("queued = %d, want 1", res.Queued)
	}

	due, err := s.ClaimDueDeliveries(context.Background(), time.Now().UTC(), time.Minute, 10)
	if err != nil {
		t.Fatalf("ClaimDueDeliveries: %v", err)
	}
	raw := string(due[0].Payload)
	for _, forbidden := range []string{"customer_ref", "hash_9931", "usr_"} {
		if strings.Contains(raw, forbidden) {
			t.Errorf("payload leaked %q: %s", forbidden, raw)
		}
	}
}
