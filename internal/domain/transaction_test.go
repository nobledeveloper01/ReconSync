package domain

import (
	"strings"
	"testing"
	"time"
)

func validDebit() DebitEvent {
	return DebitEvent{
		TenantID:        "tnt_1",
		TransactionID:   "TXN-2026-08-11-8842",
		IdempotencyKey:  "550e8400-e29b-41d4-a716-446655440000",
		TransactionType: "transfer",
		Provider:        "paystack",
		AmountMinor:     5_000_000,
		Currency:        "NGN",
		DebitAt:         time.Now().UTC(),
		CustomerRef:     "usr_9931",
		Metadata:        map[string]any{"channel": "mobile"},
	}
}

func TestDebitEventValidateAcceptsValidEvent(t *testing.T) {
	e := validDebit()
	if err := e.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
}

func TestDebitEventValidateRejectsBadInput(t *testing.T) {
	long := strings.Repeat("x", maxIdentifierLen+1)

	cases := []struct {
		name   string
		mutate func(*DebitEvent)
		field  string
	}{
		{"missing tenant", func(e *DebitEvent) { e.TenantID = "" }, "tenant_id"},
		{"missing transaction id", func(e *DebitEvent) { e.TransactionID = "" }, "transaction_id"},
		{"missing idempotency key", func(e *DebitEvent) { e.IdempotencyKey = "" }, "idempotency_key"},
		{"missing type", func(e *DebitEvent) { e.TransactionType = "" }, "transaction_type"},
		{"oversized transaction id", func(e *DebitEvent) { e.TransactionID = long }, "transaction_id"},
		{"oversized provider", func(e *DebitEvent) { e.Provider = long }, "provider"},
		{"zero amount", func(e *DebitEvent) { e.AmountMinor = 0 }, "amount_minor"},
		{"negative amount", func(e *DebitEvent) { e.AmountMinor = -1 }, "amount_minor"},
		{"short currency", func(e *DebitEvent) { e.Currency = "NG" }, "currency"},
		{"lowercase currency", func(e *DebitEvent) { e.Currency = "ngn" }, "currency"},
		{"missing debit_at", func(e *DebitEvent) { e.DebitAt = time.Time{} }, "debit_at"},
		{"too much metadata", func(e *DebitEvent) {
			e.Metadata = make(map[string]any, maxMetadataKeys+1)
			for i := 0; i <= maxMetadataKeys; i++ {
				e.Metadata[string(rune('a'+i%26))+strings.Repeat("k", i)] = 1
			}
		}, "metadata"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validDebit()
			tc.mutate(&e)

			err := e.Validate()
			if err == nil {
				t.Fatalf("accepted invalid event")
			}
			ve, ok := err.(ValidationError)
			if !ok {
				t.Fatalf("got %T, want ValidationError", err)
			}
			if ve.Field != tc.field {
				t.Errorf("field = %q, want %q", ve.Field, tc.field)
			}
		})
	}
}

func TestCreditEventValidate(t *testing.T) {
	valid := CreditEvent{
		TenantID:       "tnt_1",
		TransactionID:  "TXN-1",
		IdempotencyKey: "key-1",
		CreditAt:       time.Now().UTC(),
		Status:         CreditSuccess,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid credit rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*CreditEvent)
		field  string
	}{
		{"missing tenant", func(e *CreditEvent) { e.TenantID = "" }, "tenant_id"},
		{"missing transaction id", func(e *CreditEvent) { e.TransactionID = "" }, "transaction_id"},
		{"missing idempotency key", func(e *CreditEvent) { e.IdempotencyKey = "" }, "idempotency_key"},
		{"missing credit_at", func(e *CreditEvent) { e.CreditAt = time.Time{} }, "credit_at"},
		{"unknown status", func(e *CreditEvent) { e.Status = "maybe" }, "status"},
		{"empty status", func(e *CreditEvent) { e.Status = "" }, "status"},
		{"oversized provider ref", func(e *CreditEvent) {
			e.ProviderReference = strings.Repeat("x", maxIdentifierLen+1)
		}, "provider_reference"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := valid
			tc.mutate(&e)
			err := e.Validate()
			if err == nil {
				t.Fatal("accepted invalid credit event")
			}
			ve, ok := err.(ValidationError)
			if !ok {
				t.Fatalf("got %T, want ValidationError", err)
			}
			if ve.Field != tc.field {
				t.Errorf("field = %q, want %q", ve.Field, tc.field)
			}
		})
	}
}

func TestReversalCompletedEventValidate(t *testing.T) {
	valid := ReversalCompletedEvent{
		TenantID:      "tnt_1",
		TransactionID: "TXN-1",
		CompletedAt:   time.Now().UTC(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*ReversalCompletedEvent)
		field  string
	}{
		{"missing tenant", func(e *ReversalCompletedEvent) { e.TenantID = "" }, "tenant_id"},
		{"missing transaction id", func(e *ReversalCompletedEvent) { e.TransactionID = "" }, "transaction_id"},
		{"missing completed_at", func(e *ReversalCompletedEvent) { e.CompletedAt = time.Time{} }, "completed_at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := valid
			tc.mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatalf("accepted invalid event")
			} else if ve, ok := err.(ValidationError); !ok || ve.Field != tc.field {
				t.Errorf("got %v, want ValidationError on %s", err, tc.field)
			}
		})
	}
}

func TestCreditStatusTargetStatus(t *testing.T) {
	cases := map[CreditStatus]Status{
		CreditSuccess: StatusCompleted,
		CreditFailed:  StatusOrphaned, // ADR-0001
		CreditUnknown: StatusPendingUnknown,
	}
	for in, want := range cases {
		got, err := in.TargetStatus()
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("%s -> %s, want %s", in, got, want)
		}
		// Every mapping must be a legal edge out of pending_debit, or the store
		// will reject writes the API layer thinks it accepted.
		if !CanTransition(StatusPendingDebit, got) {
			t.Errorf("%s maps to %s, which is not reachable from pending_debit", in, got)
		}
	}

	if _, err := CreditStatus("maybe").TargetStatus(); err == nil {
		t.Error("unknown credit status did not error")
	}
}

func TestCreditStatusValid(t *testing.T) {
	for _, s := range []CreditStatus{CreditSuccess, CreditFailed, CreditUnknown} {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	for _, s := range []CreditStatus{"", "SUCCESS", "pending", "maybe"} {
		if s.Valid() {
			t.Errorf("%q should not be valid", s)
		}
	}
}

func TestTransactionWindow(t *testing.T) {
	debitAt := time.Date(2026, 8, 11, 9, 14, 22, 0, time.UTC)
	txn := &Transaction{
		Status:               StatusPendingDebit,
		DebitAt:              debitAt,
		ExpectedCompletionAt: debitAt.Add(300 * time.Second),
	}
	if got := txn.Window(); got != 300*time.Second {
		t.Errorf("window = %s, want 5m", got)
	}
}

func TestTransactionIsExpiredAt(t *testing.T) {
	debitAt := time.Date(2026, 8, 11, 9, 14, 22, 0, time.UTC)
	deadline := debitAt.Add(300 * time.Second)
	txn := &Transaction{
		Status:               StatusPendingDebit,
		DebitAt:              debitAt,
		ExpectedCompletionAt: deadline,
	}

	if txn.IsExpiredAt(deadline.Add(-time.Second)) {
		t.Error("expired one second before the deadline")
	}
	// The boundary counts as expired: at the deadline the window has closed.
	if !txn.IsExpiredAt(deadline) {
		t.Error("not expired exactly at the deadline")
	}
	if !txn.IsExpiredAt(deadline.Add(time.Second)) {
		t.Error("not expired after the deadline")
	}

	// A settled transaction is never re-examined, however old it is.
	txn.Status = StatusCompleted
	if txn.IsExpiredAt(deadline.Add(time.Hour)) {
		t.Error("a completed transaction must never be reported as expired")
	}
}
