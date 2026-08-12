package tests

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// claimable drives a transaction to reversal_pending — the state a customer is
// in when a reversal webhook has actually been sent to them.
func claimable(t *testing.T, s store.Store, txnID string) {
	t.Helper()
	ctx := context.Background()

	newEndpoint(t, s, tenantA, "we_"+txnID, "https://customer.example.com/hook")
	mustUpsert(t, s, newExpiredTxn(tenantA, txnID, 5*time.Minute, time.Minute))
	if _, err := newDetector(t, s).Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	txn, err := s.Get(ctx, tenantA, txnID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if txn.Status != domain.StatusReversalPending {
		t.Fatalf("%s is %s, want reversal_pending", txnID, txn.Status)
	}
}

func claim(t *testing.T, f *ingestFixture, txnID, worker string) (int, map[string]any) {
	t.Helper()
	w := f.do(t, http.MethodPost, "/v1/reversals/"+txnID+"/claim", f.keyA,
		map[string]string{"claimed_by": worker})
	return w.Code, decodeBody(t, w)
}

// The whole point: the same reversal delivered twice must not be actioned twice.
func TestOnlyOneWorkerCanClaimAReversal(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	claimable(t, f.store, "TX-1")

	firstCode, body := claim(t, f, "TX-1", "worker-1")
	if firstCode != http.StatusOK {
		t.Fatalf("first claim = %d, want 200: %v", firstCode, body)
	}
	if body["granted"] != true {
		t.Errorf("granted = %v, want true", body["granted"])
	}
	if body["claim_token"] == nil || body["claim_token"] == "" {
		t.Error("no claim token issued")
	}

	secondCode, body2 := claim(t, f, "TX-1", "worker-2")
	// 409, not 200-with-a-flag: a client that checks only the status code then
	// does the safe thing by default. This is the whole safety argument.
	if secondCode != http.StatusConflict {
		t.Fatalf("second claim = %d, want 409", secondCode)
	}
	if body2["granted"] != false {
		t.Errorf("granted = %v, want false", body2["granted"])
	}
	// Who holds it is answerable during an incident.
	if body2["claimed_by"] != "worker-1" {
		t.Errorf("claimed_by = %v, want worker-1", body2["claimed_by"])
	}
	if body2["claim_token"] != nil {
		t.Error("the loser was handed a claim token")
	}
}

// The claim is the last checkpoint before money moves, so it re-reads the
// current verdict rather than trusting a webhook that may be minutes old. A
// duplicate delivery arriving after the reversal is done must find the door
// shut, even though no claim was ever taken.
func TestClaimIsRefusedOnceTheAdviceNoLongerStands(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	ctx := context.Background()
	claimable(t, f.store, "TX-DONE-ALREADY")

	// Between the webhook leaving and their worker acting, the customer's own
	// system confirms the credit landed after all.
	if _, err := f.store.MarkReversalCompleted(ctx, tenantA, "TX-DONE-ALREADY", time.Now().UTC()); err != nil {
		t.Fatalf("MarkReversalCompleted: %v", err)
	}

	code, body := claim(t, f, "TX-DONE-ALREADY", "worker-1")
	if code != http.StatusConflict {
		t.Fatalf("claim = %d, want 409 — reversing now would take back money the destination has", code)
	}
	errBody, _ := body["error"].(map[string]any)
	if errBody["code"] != "not_reversible" {
		t.Errorf("code = %v, want not_reversible", errBody["code"])
	}
}

// Concurrent workers are the case this exists for, so it is tested concurrently.
func TestClaimIsExactlyOnceUnderRace(t *testing.T) {
	s := store.NewMemory()
	seedTenants(t, s)
	ctx := context.Background()

	const workers = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		granted int
		tokens  = map[string]bool{}
	)
	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			c, ok, err := s.ClaimReversal(ctx, tenantA, "TX-RACE", "worker", "rcl_test", time.Now().UTC())
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				t.Errorf("ClaimReversal: %v", err)
				return
			}
			if ok {
				granted++
			}
			tokens[c.ClaimToken] = true
		}()
	}
	close(start)
	wg.Wait()

	if granted != 1 {
		t.Fatalf("%d of %d workers were granted the claim, want exactly 1", granted, workers)
	}
	// Everyone sees the same claim, so the losers can log which one won.
	if len(tokens) != 1 {
		t.Errorf("workers saw %d different claim tokens, want 1", len(tokens))
	}
}

// A worker that dies between claiming and reversing would otherwise hold the
// claim forever, and the reversal would never happen.
func TestAnUnconfirmedClaimCanBeReleased(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	claimable(t, f.store, "TX-STUCK")

	if code, _ := claim(t, f, "TX-STUCK", "worker-dead"); code != http.StatusOK {
		t.Fatalf("first claim = %d", code)
	}
	if w := f.do(t, http.MethodPost, "/v1/reversals/TX-STUCK/claim/release", f.keyA, nil); w.Code != http.StatusOK {
		t.Fatalf("release = %d, want 200: %s", w.Code, w.Body.String())
	}

	// And it can be taken again, by a worker that is alive.
	code, body := claim(t, f, "TX-STUCK", "worker-alive")
	if code != http.StatusOK || body["granted"] != true {
		t.Errorf("re-claim = %d granted=%v, want 200 true", code, body["granted"])
	}
}

// Releasing a confirmed claim would let a second worker reverse money that has
// already moved. This is the one thing release must never do.
func TestAConfirmedClaimIsNeverReleased(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	claimable(t, f.store, "TX-DONE")

	if code, _ := claim(t, f, "TX-DONE", "worker-1"); code != http.StatusOK {
		t.Fatalf("claim = %d", code)
	}
	// They reverse it and tell us.
	if w := f.do(t, http.MethodPost, "/v1/events/reversal-completed", f.keyA,
		map[string]any{"transaction_id": "TX-DONE"}); w.Code != http.StatusOK {
		t.Fatalf("reversal-completed = %d: %s", w.Code, w.Body.String())
	}

	if w := f.do(t, http.MethodPost, "/v1/reversals/TX-DONE/claim/release", f.keyA, nil); w.Code != http.StatusConflict {
		t.Fatalf("release of a confirmed claim = %d, want 409", w.Code)
	}

	// The claim is closed, with the time the money actually moved.
	got, err := f.store.GetReversalClaim(context.Background(), tenantA, "TX-DONE")
	if err != nil {
		t.Fatalf("GetReversalClaim: %v", err)
	}
	if got.ConfirmedAt == nil {
		t.Error("confirming the reversal did not close the claim")
	}
}

func TestClaimRequiresAWorkerName(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	claimable(t, f.store, "TX-1")

	// Without it, an outstanding claim cannot be traced back to anything.
	if w := f.do(t, http.MethodPost, "/v1/reversals/TX-1/claim", f.keyA, map[string]string{}); w.Code != http.StatusBadRequest {
		t.Errorf("missing claimed_by = %d, want 400", w.Code)
	}
	// An unknown transaction is a 404, not a silent grant.
	if w := f.do(t, http.MethodPost, "/v1/reversals/NOPE/claim", f.keyA,
		map[string]string{"claimed_by": "w"}); w.Code != http.StatusNotFound {
		t.Errorf("unknown transaction = %d, want 404", w.Code)
	}
	// And another tenant cannot claim ours.
	if w := f.do(t, http.MethodPost, "/v1/reversals/TX-1/claim", f.keyB,
		map[string]string{"claimed_by": "w"}); w.Code != http.StatusNotFound {
		t.Errorf("cross-tenant claim = %d, want 404", w.Code)
	}
}

// --- store conformance ---

func testReversalClaims(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)

	first, granted, err := s.ClaimReversal(ctx, tenantA, "TX1", "worker-1", "rcl_a", now)
	if err != nil || !granted {
		t.Fatalf("first claim: granted=%v err=%v", granted, err)
	}
	if first.ClaimToken != "rcl_a" {
		t.Errorf("token = %q, want rcl_a", first.ClaimToken)
	}

	second, granted, err := s.ClaimReversal(ctx, tenantA, "TX1", "worker-2", "rcl_b", now.Add(time.Second))
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if granted {
		t.Fatal("the claim was granted twice")
	}
	// The loser is told about the holder, not about itself.
	if second.ClaimedBy != "worker-1" || second.ClaimToken != "rcl_a" {
		t.Errorf("loser saw %+v, want worker-1 holding rcl_a", second)
	}

	// The same transaction id under another tenant is a different claim.
	if _, granted, err := s.ClaimReversal(ctx, tenantB, "TX1", "worker-b", "rcl_c", now); err != nil || !granted {
		t.Errorf("tenant B was blocked by tenant A's claim: granted=%v err=%v", granted, err)
	}

	if err := s.ConfirmReversalClaim(ctx, tenantA, "TX1", now.Add(time.Minute)); err != nil {
		t.Fatalf("ConfirmReversalClaim: %v", err)
	}
	got, err := s.GetReversalClaim(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("GetReversalClaim: %v", err)
	}
	if got.ConfirmedAt == nil || !got.ConfirmedAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("confirmed_at = %v, want %v", got.ConfirmedAt, now.Add(time.Minute))
	}

	// A replayed confirmation must not rewrite when the money actually moved.
	if err := s.ConfirmReversalClaim(ctx, tenantA, "TX1", now.Add(time.Hour)); err != nil {
		t.Fatalf("second confirm: %v", err)
	}
	again, err := s.GetReversalClaim(ctx, tenantA, "TX1")
	if err != nil {
		t.Fatalf("GetReversalClaim: %v", err)
	}
	if !again.ConfirmedAt.Equal(*got.ConfirmedAt) {
		t.Errorf("a replayed confirmation moved confirmed_at from %v to %v", got.ConfirmedAt, again.ConfirmedAt)
	}

	// Confirmed claims are never released.
	if err := s.ReleaseReversalClaim(ctx, tenantA, "TX1"); err == nil {
		t.Error("released a confirmed claim, which would let a second worker reverse again")
	}
}

func testReleaseAndReclaim(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	now := time.Now().UTC()

	if _, granted, err := s.ClaimReversal(ctx, tenantA, "TX1", "dead-worker", "rcl_a", now); err != nil || !granted {
		t.Fatalf("claim: granted=%v err=%v", granted, err)
	}
	if err := s.ReleaseReversalClaim(ctx, tenantA, "TX1"); err != nil {
		t.Fatalf("ReleaseReversalClaim: %v", err)
	}
	if _, err := s.GetReversalClaim(ctx, tenantA, "TX1"); err == nil {
		t.Error("the released claim is still there")
	}
	if _, granted, err := s.ClaimReversal(ctx, tenantA, "TX1", "live-worker", "rcl_b", now); err != nil || !granted {
		t.Errorf("re-claim after release: granted=%v err=%v", granted, err)
	}

	// Releasing nothing is a not-found, not a silent success.
	if err := s.ReleaseReversalClaim(ctx, tenantA, "NOPE"); err == nil {
		t.Error("releasing a claim that does not exist reported success")
	}
}
