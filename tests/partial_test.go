package tests

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// amountCredit builds a credit that states how much arrived.
func amountCredit(txnID, idemKey string, amount int64) *domain.CreditEvent {
	return &domain.CreditEvent{
		TenantID: tenantA, TransactionID: txnID, IdempotencyKey: idemKey,
		CreditAt: time.Now().UTC(), Status: domain.CreditSuccess, AmountMinor: amount,
	}
}

// Accumulation is not naturally idempotent the way the old path was. The
// pipeline can deliver the same credit twice — parked then drained, or a client
// retry — and a running total would count it twice, settling a ₦20,000 transfer
// with a ₦10,000 credit. Found by a flaky test showing a doubled total.
func testPartialCreditIsCountedOnce(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "TX-DUP", time.Hour))

	credit := amountCredit("TX-DUP", "same-key", 1_000_000)
	for i := 0; i < 3; i++ {
		if _, err := s.ApplyPartialCredit(ctx, tenantA, credit); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}

	got, err := s.Get(ctx, tenantA, "TX-DUP")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.CreditedMinor != 1_000_000 {
		t.Fatalf("credited = %d after three deliveries of one credit, want 1000000",
			got.CreditedMinor)
	}
	if got.Status == domain.StatusCompleted {
		t.Error("a replayed credit settled a transaction that is still short")
	}

	// A genuinely different credit still counts.
	if _, err := s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-DUP", "other-key", 4_000_000)); err != nil {
		t.Fatalf("second credit: %v", err)
	}
	if got, _ = s.Get(ctx, tenantA, "TX-DUP"); got.CreditedMinor != 5_000_000 {
		t.Errorf("credited = %d, want the two credits summed", got.CreditedMinor)
	}
}

// The correctness limit this closes: a credit carried no amount, so a ₦50,000
// debit settled by a ₦10,000 credit was marked completed and the customer was
// quietly short ₦40,000. The product exists to notice a customer is out of
// pocket; it could not notice being partly out of pocket.
func testPartialCreditDoesNotSettle(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	txn := newDebitTxn(tenantA, "TX-1", time.Hour) // 5,000,000 debited
	mustUpsert(t, s, txn)

	got, err := s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-1", "c1", 1_000_000))
	if err != nil {
		t.Fatalf("ApplyPartialCredit: %v", err)
	}
	if got.Status == domain.StatusCompleted {
		t.Fatal("a fifth of the money arriving marked the transaction complete")
	}
	if got.CreditedMinor != 1_000_000 {
		t.Errorf("credited = %d, want 1000000", got.CreditedMinor)
	}
	if got.ShortfallMinor() != 4_000_000 {
		t.Errorf("shortfall = %d, want 4000000", got.ShortfallMinor())
	}

	// The rest arrives, and only now does it settle. A split settlement adds
	// up rather than the last one winning.
	got, err = s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-1", "c2", 4_000_000))
	if err != nil {
		t.Fatalf("ApplyPartialCredit: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s after the full amount arrived, want completed", got.Status)
	}
	if got.ShortfallMinor() != 0 {
		t.Errorf("shortfall = %d, want 0", got.ShortfallMinor())
	}
}

// A transfer with a fee credits less than it debits, and that is correct rather
// than short.
func testExpectedCreditAllowsAFee(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	txn := newDebitTxn(tenantA, "TX-FEE", time.Hour)
	txn.ExpectedCreditMinor = 4_975_000 // ₦25,000 fee on a ₦50,000 transfer
	mustUpsert(t, s, txn)

	got, err := s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-FEE", "cf", 4_975_000))
	if err != nil {
		t.Fatalf("ApplyPartialCredit: %v", err)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %s, want completed — the fee is not a shortfall", got.Status)
	}
	if got.ShortfallMinor() != 0 {
		t.Errorf("shortfall = %d on a fully settled transfer with a fee", got.ShortfallMinor())
	}
}

// More arriving than was ever expected is not a settlement and not a failure.
func testOverpaymentGoesToAHuman(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "TX-OVER", time.Hour))

	got, err := s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-OVER", "co", 9_999_999))
	if err != nil {
		t.Fatalf("ApplyPartialCredit: %v", err)
	}
	if got.Status != domain.StatusSuspect {
		t.Errorf("status = %s on an overpayment, want suspect", got.Status)
	}
}

// A partly settled transaction is money still outstanding, so its window must
// still expire and reverse.
func testPartialSettlementStillOrphans(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	mustUpsert(t, s, newExpiredTxn(tenantA, "TX-SHORT", 10*time.Minute, time.Minute))
	if _, err := s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-SHORT", "cs", 1_000_000)); err != nil {
		t.Fatalf("ApplyPartialCredit: %v", err)
	}

	claimed, err := s.ClaimExpired(ctx, time.Now().UTC(), 10)
	if err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if len(claimed) != 1 || claimed[0].TransactionID != "TX-SHORT" {
		t.Fatalf("claimed %v, want the partly settled transaction", txnIDs(claimed))
	}
	if claimed[0].CreditedMinor != 1_000_000 {
		t.Errorf("the partial credit was lost on detection: %+v", claimed[0])
	}
}

// A settled transaction is closed to further credits, the same as before.
func testPartialCreditRejectsAReplay(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()
	mustUpsert(t, s, newDebitTxn(tenantA, "TX-DONE", time.Hour))

	if _, err := s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-DONE", "cd1", 5_000_000)); err != nil {
		t.Fatalf("ApplyPartialCredit: %v", err)
	}
	if _, err := s.ApplyPartialCredit(ctx, tenantA, amountCredit("TX-DONE", "cd2", 5_000_000)); err == nil {
		t.Error("a settled transaction accepted another credit")
	}
}

// A credit with no amount behaves exactly as it always has: the whole thing.
func TestCreditWithoutAnAmountStillSettles(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	now := time.Now().UTC()

	post := func(body map[string]any) {
		t.Helper()
		if w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, body); w.Code != http.StatusAccepted {
			t.Fatalf("debit = %d: %s", w.Code, w.Body.String())
		}
	}
	post(map[string]any{
		"transaction_id": "TX-1", "transaction_type": "transfer", "provider": "paystack",
		"amount_minor": 5000000, "currency": "NGN", "debit_at": now.Format(time.RFC3339),
		"customer_ref": "usr", "idempotency_key": "d1",
	})
	if w := f.do(t, http.MethodPost, "/v1/events/credit", f.keyA, map[string]any{
		"transaction_id": "TX-1", "status": "success", "idempotency_key": "c1",
		"credit_at": now.Format(time.RFC3339),
	}); w.Code != http.StatusAccepted {
		t.Fatalf("credit = %d", w.Code)
	}

	waitFor(t, func() bool {
		got, err := f.store.Get(context.Background(), tenantA, "TX-1")
		return err == nil && got.Status == domain.StatusCompleted
	}, "a credit with no amount did not settle the transaction")
}

// And one that states a short amount does not.
func TestCreditStatingAShortAmountDoesNotSettle(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	now := time.Now().UTC()

	if w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, map[string]any{
		"transaction_id": "TX-2", "transaction_type": "transfer", "provider": "paystack",
		"amount_minor": 5000000, "currency": "NGN", "debit_at": now.Format(time.RFC3339),
		"customer_ref": "usr", "idempotency_key": "d2",
	}); w.Code != http.StatusAccepted {
		t.Fatalf("debit = %d", w.Code)
	}
	if w := f.do(t, http.MethodPost, "/v1/events/credit", f.keyA, map[string]any{
		"transaction_id": "TX-2", "status": "success", "idempotency_key": "c2",
		"amount_minor": 1000000, "credit_at": now.Format(time.RFC3339),
	}); w.Code != http.StatusAccepted {
		t.Fatalf("credit = %d", w.Code)
	}

	// The pipeline is asynchronous and a credit that overtakes its debit is
	// parked and drained on the next batch, so this is eventual by design.
	// Waits long enough for the slow path, and says what it actually saw
	// rather than only that it timed out.
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	var got *domain.Transaction
	for time.Now().Before(deadline) {
		var err error
		if got, err = f.store.Get(ctx, tenantA, "TX-2"); err == nil && got.CreditedMinor == 1_000_000 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got == nil || got.CreditedMinor != 1_000_000 {
		parked, _ := f.store.PeekParkedCredits(ctx, tenantA, []string{"TX-2"})
		t.Fatalf("credited = %+v, still parked = %d — the partial credit never landed", got, len(parked))
	}
	if got.Status == domain.StatusCompleted {
		t.Error("a fifth of the money arriving marked the transaction complete")
	}
	if got.ShortfallMinor() != 4_000_000 {
		t.Errorf("shortfall = %d, want 4000000", got.ShortfallMinor())
	}
}

// A credit that arrives before its debit is parked, and the amount has to
// survive that round trip. It did not: the credit came back with no amount and
// settled the transaction in full. Found by running it, not by reading it.
func testParkedCreditKeepsItsAmount(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	credit := newCreditEvent(tenantA, "TX-EARLY", domain.CreditSuccess)
	credit.AmountMinor = 1_000_000
	if err := s.ParkCredit(ctx, tenantA, credit); err != nil {
		t.Fatalf("ParkCredit: %v", err)
	}

	parked, err := s.PeekParkedCredits(ctx, tenantA, []string{"TX-EARLY"})
	if err != nil {
		t.Fatalf("PeekParkedCredits: %v", err)
	}
	if len(parked) != 1 {
		t.Fatalf("parked = %d, want 1", len(parked))
	}
	if parked[0].AmountMinor != 1_000_000 {
		t.Errorf("amount = %d after parking, want 1000000 — a partial credit would settle in full",
			parked[0].AmountMinor)
	}
}
