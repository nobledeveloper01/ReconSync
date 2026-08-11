package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

func validDebit() domain.DebitEvent {
	return *newDebitEvent("tnt_1", "TXN-2026-08-11-8842")
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
		mutate func(*domain.DebitEvent)
		field  string
	}{
		{"missing tenant", func(e *domain.DebitEvent) { e.TenantID = "" }, "tenant_id"},
		{"missing transaction id", func(e *domain.DebitEvent) { e.TransactionID = "" }, "transaction_id"},
		{"missing idempotency key", func(e *domain.DebitEvent) { e.IdempotencyKey = "" }, "idempotency_key"},
		{"missing type", func(e *domain.DebitEvent) { e.TransactionType = "" }, "transaction_type"},
		{"oversized transaction id", func(e *domain.DebitEvent) { e.TransactionID = long }, "transaction_id"},
		{"oversized provider", func(e *domain.DebitEvent) { e.Provider = long }, "provider"},
		{"zero amount", func(e *domain.DebitEvent) { e.AmountMinor = 0 }, "amount_minor"},
		{"negative amount", func(e *domain.DebitEvent) { e.AmountMinor = -1 }, "amount_minor"},
		{"short currency", func(e *domain.DebitEvent) { e.Currency = "NG" }, "currency"},
		{"lowercase currency", func(e *domain.DebitEvent) { e.Currency = "ngn" }, "currency"},
		{"missing debit_at", func(e *domain.DebitEvent) { e.DebitAt = time.Time{} }, "debit_at"},
		{"too much metadata", func(e *domain.DebitEvent) {
			e.Metadata = make(map[string]any, maxMetadataKeys+1)
			for i := 0; i <= maxMetadataKeys; i++ {
				e.Metadata[strings.Repeat("k", i+1)] = 1
			}
		}, "metadata"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validDebit()
			tc.mutate(&e)

			err := e.Validate()
			if err == nil {
				t.Fatal("accepted invalid event")
			}
			ve, ok := err.(domain.ValidationError)
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
	valid := *newCreditEvent("tnt_1", "TXN-1", domain.CreditSuccess)
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid credit rejected: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*domain.CreditEvent)
		field  string
	}{
		{"missing tenant", func(e *domain.CreditEvent) { e.TenantID = "" }, "tenant_id"},
		{"missing transaction id", func(e *domain.CreditEvent) { e.TransactionID = "" }, "transaction_id"},
		{"missing idempotency key", func(e *domain.CreditEvent) { e.IdempotencyKey = "" }, "idempotency_key"},
		{"missing credit_at", func(e *domain.CreditEvent) { e.CreditAt = time.Time{} }, "credit_at"},
		{"unknown status", func(e *domain.CreditEvent) { e.Status = "maybe" }, "status"},
		{"empty status", func(e *domain.CreditEvent) { e.Status = "" }, "status"},
		{"oversized provider ref", func(e *domain.CreditEvent) {
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
			ve, ok := err.(domain.ValidationError)
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
	valid := domain.ReversalCompletedEvent{
		TenantID:      "tnt_1",
		TransactionID: "TXN-1",
		CompletedAt:   time.Now().UTC(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*domain.ReversalCompletedEvent)
		field  string
	}{
		{"missing tenant", func(e *domain.ReversalCompletedEvent) { e.TenantID = "" }, "tenant_id"},
		{"missing transaction id", func(e *domain.ReversalCompletedEvent) { e.TransactionID = "" }, "transaction_id"},
		{"missing completed_at", func(e *domain.ReversalCompletedEvent) { e.CompletedAt = time.Time{} }, "completed_at"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := valid
			tc.mutate(&e)
			if err := e.Validate(); err == nil {
				t.Fatal("accepted invalid event")
			} else if ve, ok := err.(domain.ValidationError); !ok || ve.Field != tc.field {
				t.Errorf("got %v, want ValidationError on %s", err, tc.field)
			}
		})
	}
}

func TestEventEnvelope(t *testing.T) {
	d := domain.NewDebitEvent(newDebitEvent("tnt_1", "TX1"))
	if d.TenantID() != "tnt_1" || d.TransactionID() != "TX1" {
		t.Errorf("debit envelope = %s/%s, want tnt_1/TX1", d.TenantID(), d.TransactionID())
	}
	if err := d.Validate(); err != nil {
		t.Errorf("valid debit envelope rejected: %v", err)
	}

	c := domain.NewCreditEvent(newCreditEvent("tnt_1", "TX1", domain.CreditSuccess))
	if c.TenantID() != "tnt_1" || c.TransactionID() != "TX1" {
		t.Errorf("credit envelope = %s/%s, want tnt_1/TX1", c.TenantID(), c.TransactionID())
	}

	// An empty envelope, and one carrying both legs, are both malformed.
	var empty domain.Event
	if empty.TenantID() != "" || empty.TransactionID() != "" {
		t.Error("empty envelope reported identifiers")
	}
	if err := empty.Validate(); err == nil {
		t.Error("empty envelope validated")
	}
	both := domain.Event{Debit: newDebitEvent("tnt_1", "TX1"), Credit: newCreditEvent("tnt_1", "TX1", domain.CreditSuccess)}
	if err := both.Validate(); err == nil {
		t.Error("envelope carrying both legs validated")
	}
}

func TestCreditStatusTargetStatus(t *testing.T) {
	cases := map[domain.CreditStatus]domain.Status{
		domain.CreditSuccess: domain.StatusCompleted,
		domain.CreditFailed:  domain.StatusOrphaned, // ADR-0001
		domain.CreditUnknown: domain.StatusPendingUnknown,
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
		// Each mapping must be a legal edge out of pending_debit, or the store
		// would reject writes the API layer thinks it accepted.
		if !domain.CanTransition(domain.StatusPendingDebit, got) {
			t.Errorf("%s maps to %s, which is not reachable from pending_debit", in, got)
		}
	}

	if _, err := domain.CreditStatus("maybe").TargetStatus(); err == nil {
		t.Error("unknown credit status did not error")
	}
}

func TestCreditStatusValid(t *testing.T) {
	for _, s := range []domain.CreditStatus{domain.CreditSuccess, domain.CreditFailed, domain.CreditUnknown} {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	for _, s := range []domain.CreditStatus{"", "SUCCESS", "pending", "maybe"} {
		if s.Valid() {
			t.Errorf("%q should not be valid", s)
		}
	}
}

func TestTransactionWindow(t *testing.T) {
	debitAt := time.Date(2026, 8, 11, 9, 14, 22, 0, time.UTC)
	txn := &domain.Transaction{
		Status:               domain.StatusPendingDebit,
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
	txn := &domain.Transaction{
		Status:               domain.StatusPendingDebit,
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
	txn.Status = domain.StatusCompleted
	if txn.IsExpiredAt(deadline.Add(time.Hour)) {
		t.Error("a completed transaction was reported as expired")
	}
}
