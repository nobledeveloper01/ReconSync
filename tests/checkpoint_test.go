package tests

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/service"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

func testSigner(t *testing.T) *audit.Signer {
	t.Helper()
	seed, _, err := audit.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	signer, err := audit.NewSigner(seed)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	return signer
}

func appendRecords(t *testing.T, s store.Store, tenantID string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, err := s.AppendAudit(ctx, tenantID, &audit.Record{
			EventType:  audit.EventDetected,
			OccurredAt: time.Now().UTC(),
			Subject:    map[string]any{"type": "transaction", "id": "TX"},
			Payload:    map[string]any{"verdict": "orphaned"},
		}); err != nil {
			t.Fatalf("AppendAudit: %v", err)
		}
	}
}

// The attack a hash chain alone cannot catch: someone with write access deletes
// the history, writes their own, and recomputes every hash. VerifyChain passes.
// The signature is the part they cannot produce.
func TestCheckpointCatchesAWholesaleRewrite(t *testing.T) {
	s := store.NewMemory()
	ctx := context.Background()
	seedTenants(t, s)
	appendRecords(t, s, tenantA, 5)

	signer := testSigner(t)
	cp, err := service.NewCheckpointer(s, service.CheckpointerOptions{Signer: signer})
	if err != nil {
		t.Fatalf("NewCheckpointer: %v", err)
	}
	if res, err := cp.Checkpoint(ctx); err != nil || res.Signed != 1 {
		t.Fatalf("Checkpoint: signed=%d err=%v", res.Signed, err)
	}

	signed, err := s.LatestCheckpoint(ctx, tenantA)
	if err != nil {
		t.Fatalf("LatestCheckpoint: %v", err)
	}

	// The genuine chain matches its signature.
	records, err := s.ListAudit(ctx, tenantA, 0)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if check := audit.VerifyAgainstCheckpoint(records, *signed, signer.PublicKey()); !check.Matches {
		t.Fatalf("a genuine chain failed its own checkpoint: %s", check.Reason)
	}

	// Now rewrite history the way an attacker with database access would:
	// a completely different, internally consistent chain of the same length.
	forged := store.NewMemory()
	seedTenants(t, forged)
	for i := 0; i < 5; i++ {
		if _, err := forged.AppendAudit(ctx, tenantA, &audit.Record{
			EventType:  audit.EventDetected,
			OccurredAt: time.Now().UTC(),
			Subject:    map[string]any{"type": "transaction", "id": "TX"},
			Payload:    map[string]any{"verdict": "completed"}, // the lie
		}); err != nil {
			t.Fatalf("AppendAudit: %v", err)
		}
	}
	forgedRecords, err := forged.ListAudit(ctx, tenantA, 0)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}

	// The forged chain verifies against itself. This is the whole problem.
	if v := audit.VerifyChain(tenantA, forgedRecords); !v.Verified {
		t.Fatalf("the forged chain did not even verify against itself: %s", v.Reason)
	}
	// And fails against the signature.
	check := audit.VerifyAgainstCheckpoint(forgedRecords, *signed, signer.PublicKey())
	if check.Matches {
		t.Fatal("a rewritten history passed the checkpoint")
	}
	if !strings.Contains(check.Reason, "rewritten") {
		t.Errorf("reason = %q, want it to say history was rewritten", check.Reason)
	}
}

// Truncating the chain is the other half of the same attack.
func TestCheckpointCatchesRemovedRecords(t *testing.T) {
	s := store.NewMemory()
	ctx := context.Background()
	seedTenants(t, s)
	appendRecords(t, s, tenantA, 6)

	signer := testSigner(t)
	cp, _ := service.NewCheckpointer(s, service.CheckpointerOptions{Signer: signer})
	if _, err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	signed, err := s.LatestCheckpoint(ctx, tenantA)
	if err != nil {
		t.Fatalf("LatestCheckpoint: %v", err)
	}

	records, err := s.ListAudit(ctx, tenantA, 0)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	truncated := records[:3]

	check := audit.VerifyAgainstCheckpoint(truncated, *signed, signer.PublicKey())
	if check.Matches {
		t.Fatal("a truncated chain passed the checkpoint")
	}
	if !strings.Contains(check.Reason, "removed") {
		t.Errorf("reason = %q, want it to say records were removed", check.Reason)
	}
}

// A checkpoint anyone could forge is decoration.
func TestCheckpointSignatureCannotBeForged(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemory()
	seedTenants(t, s)
	appendRecords(t, s, tenantA, 3)

	real, attacker := testSigner(t), testSigner(t)
	cp, _ := service.NewCheckpointer(s, service.CheckpointerOptions{Signer: real})
	if _, err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	signed, err := s.LatestCheckpoint(ctx, tenantA)
	if err != nil {
		t.Fatalf("LatestCheckpoint: %v", err)
	}

	// The attacker's key does not verify our signature.
	if err := audit.VerifyCheckpoint(*signed, attacker.PublicKey()); err == nil {
		t.Error("a checkpoint verified against the wrong public key")
	}
	// And editing what was signed breaks it.
	tampered := *signed
	tampered.Hash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if err := audit.VerifyCheckpoint(tampered, real.PublicKey()); err == nil {
		t.Error("an edited checkpoint still verified")
	}
	// Verified by anyone holding the public key, with no secret involved —
	// which is the only reason publishing one is worth anything.
	if err := audit.VerifyCheckpoint(*signed, real.PublicKey()); err != nil {
		t.Errorf("a genuine checkpoint did not verify: %v", err)
	}
}

// Signing a chain that is already broken would launder a forgery.
func TestCheckpointerRefusesToSignABrokenChain(t *testing.T) {
	s := &brokenChainStore{Memory: store.NewMemory()}
	ctx := context.Background()
	seedTenants(t, s.Memory)
	appendRecords(t, s.Memory, tenantA, 3)

	cp, err := service.NewCheckpointer(s, service.CheckpointerOptions{Signer: testSigner(t)})
	if err != nil {
		t.Fatalf("NewCheckpointer: %v", err)
	}

	res, err := cp.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if res.Signed != 0 {
		t.Error("signed a chain that does not verify")
	}
	if _, err := s.LatestCheckpoint(ctx, tenantA); err == nil {
		t.Error("a checkpoint was stored for a broken chain")
	}
}

// brokenChainStore serves a chain with an altered record, as a tampered database
// would.
type brokenChainStore struct {
	*store.Memory
}

func (b *brokenChainStore) ListAudit(ctx context.Context, tenantID string, limit int) ([]audit.Record, error) {
	records, err := b.Memory.ListAudit(ctx, tenantID, limit)
	if err != nil || len(records) < 2 {
		return records, err
	}
	records[1].Payload = map[string]any{"verdict": "completed"}
	return records, nil
}

// Re-signing an unchanged head should not pile up identical rows.
func TestCheckpointerIsIdempotentOnAnUnchangedChain(t *testing.T) {
	s := store.NewMemory()
	ctx := context.Background()
	seedTenants(t, s)
	appendRecords(t, s, tenantA, 2)

	cp, _ := service.NewCheckpointer(s, service.CheckpointerOptions{Signer: testSigner(t)})
	for i := 0; i < 3; i++ {
		if _, err := cp.Checkpoint(ctx); err != nil {
			t.Fatalf("Checkpoint %d: %v", i, err)
		}
	}
	list, err := s.ListCheckpoints(ctx, tenantA, 100)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("checkpoints = %d, want 1 for an unchanged chain", len(list))
	}

	// New records produce a new checkpoint.
	appendRecords(t, s, tenantA, 1)
	if _, err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if list, _ := s.ListCheckpoints(ctx, tenantA, 100); len(list) != 2 {
		t.Errorf("checkpoints = %d, want 2 after the chain grew", len(list))
	}
}

func TestSignerRejectsBadKeys(t *testing.T) {
	for _, key := range []string{"", "not-a-key!", "abcd"} {
		if _, err := audit.NewSigner(key); err == nil {
			t.Errorf("accepted %q as a signing key", key)
		}
	}
	// A 32-byte seed is what most generators hand you, and must work.
	seed, _, err := audit.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := audit.NewSigner(seed); err != nil {
		t.Errorf("rejected a generated seed: %v", err)
	}
}

// --- HTTP ---

func TestIngestAuditCheckpoints(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	ctx := context.Background()
	appendRecords(t, f.store, tenantA, 3)

	// Before any checkpoint, verify must say the guarantee is absent rather
	// than implying the chain is fully protected.
	w := f.do(t, http.MethodGet, "/v1/audit/verify", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("verify = %d", w.Code)
	}
	body := decodeBody(t, w)
	check, _ := body["checkpoint"].(map[string]any)
	if check["checked"] != false {
		t.Errorf("checked = %v, want false with no checkpoint", check["checked"])
	}
	if check["reason"] == nil || check["reason"] == "" {
		t.Error("no explanation that the chain is unsigned")
	}

	// Take one.
	signer := testSigner(t)
	cp, _ := service.NewCheckpointer(f.store, service.CheckpointerOptions{Signer: signer})
	if _, err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	w = f.do(t, http.MethodGet, "/v1/audit/verify", f.keyA, nil)
	body = decodeBody(t, w)
	if body["verified"] != true {
		t.Errorf("verified = %v: %s", body["verified"], w.Body.String())
	}
	check, _ = body["checkpoint"].(map[string]any)
	if check["matches"] != true {
		t.Errorf("checkpoint did not match a genuine chain: %v", check)
	}

	// The list is what a customer archives.
	w = f.do(t, http.MethodGet, "/v1/audit/checkpoints", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("checkpoints = %d", w.Code)
	}
	body = decodeBody(t, w)
	list, _ := body["checkpoints"].([]any)
	if len(list) != 1 {
		t.Fatalf("checkpoints = %v, want 1", body["checkpoints"])
	}
	first, _ := list[0].(map[string]any)
	for _, field := range []string{"signature", "public_key", "hash", "seq"} {
		if first[field] == nil || first[field] == "" {
			t.Errorf("checkpoint has no %s", field)
		}
	}
	// It must say to keep these somewhere else.
	if body["notice"] == nil {
		t.Error("no notice about archiving checkpoints externally")
	}

	// Tenant B has its own, empty, list.
	w = f.do(t, http.MethodGet, "/v1/audit/checkpoints", f.keyB, nil)
	if cps, _ := decodeBody(t, w)["checkpoints"].([]any); len(cps) != 0 {
		t.Errorf("tenant B saw tenant A's checkpoints: %v", cps)
	}
}

// --- store conformance ---

func testCheckpointRoundTrip(t *testing.T, s store.Store) {
	ctx := context.Background()
	tenantID := uniqueTenant(t, s)

	if _, err := s.LatestCheckpoint(ctx, tenantID); err == nil {
		t.Error("found a checkpoint before any was written")
	}

	signer := testSigner(t)
	for _, seq := range []int64{1, 2} {
		c, err := signer.Sign(audit.Checkpoint{
			TenantID: tenantID,
			Seq:      seq,
			Hash:     "sha256:deadbeef",
			TakenAt:  time.Now().UTC().Truncate(time.Microsecond),
		})
		if err != nil {
			t.Fatalf("Sign: %v", err)
		}
		if err := s.SaveCheckpoint(ctx, c); err != nil {
			t.Fatalf("SaveCheckpoint: %v", err)
		}
		// Saving the same head again is a no-op, not a duplicate row.
		if err := s.SaveCheckpoint(ctx, c); err != nil {
			t.Fatalf("SaveCheckpoint (repeat): %v", err)
		}
	}

	latest, err := s.LatestCheckpoint(ctx, tenantID)
	if err != nil {
		t.Fatalf("LatestCheckpoint: %v", err)
	}
	if latest.Seq != 2 {
		t.Errorf("latest seq = %d, want 2", latest.Seq)
	}
	// The signature must survive storage, or the whole thing is worthless.
	if err := audit.VerifyCheckpoint(*latest, signer.PublicKey()); err != nil {
		t.Errorf("a stored checkpoint no longer verifies: %v", err)
	}

	list, err := s.ListCheckpoints(ctx, tenantID, 100)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(list) != 2 || list[0].Seq != 2 {
		t.Errorf("list = %+v, want 2 newest-first", list)
	}

	tenants, err := s.TenantsWithAudit(ctx)
	if err != nil {
		t.Fatalf("TenantsWithAudit: %v", err)
	}
	_ = tenants
}
