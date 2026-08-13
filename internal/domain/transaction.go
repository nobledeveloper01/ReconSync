package domain

import (
	"fmt"
	"time"
)

// CreditStatus is the provider's verdict on the credit leg (§7.1).
type CreditStatus string

const (
	CreditSuccess CreditStatus = "success"
	CreditFailed  CreditStatus = "failed"
	CreditUnknown CreditStatus = "unknown" // ambiguous; we refuse to guess
)

func (c CreditStatus) Valid() bool {
	return c == CreditSuccess || c == CreditFailed || c == CreditUnknown
}

// TargetStatus maps a provider verdict onto the next state.
//
// success and unknown are per §7.1. failed is an interpretation: an explicit
// provider failure means the credit leg definitively didn't happen, so waiting
// out the window only spends regulatory clock to learn what we already know.
func (c CreditStatus) TargetStatus() (Status, error) {
	switch c {
	case CreditSuccess:
		return StatusCompleted, nil
	case CreditFailed:
		return StatusOrphaned, nil
	case CreditUnknown:
		return StatusPendingUnknown, nil
	default:
		return "", fmt.Errorf("unknown credit status %q", c)
	}
}

// Transaction mirrors the transactions table in migrations/0001.
type Transaction struct {
	ID       int64
	TenantID string

	TransactionID  string // the customer's own identifier, unique per tenant
	IdempotencyKey string

	TransactionType string
	Provider        string
	AmountMinor     int64 // integer minor units, never a float
	Currency        string

	Status Status

	DebitAt              time.Time
	CreditAt             *time.Time
	ExpectedCompletionAt time.Time
	DetectedAt           *time.Time
	ReversalTriggeredAt  *time.Time
	ReversalCompletedAt  *time.Time

	// CustomerRefHash is pseudonymised (§8.4); there is no field here that can
	// hold the plaintext.
	CustomerRefHash string

	Metadata   map[string]any
	IsBackfill bool

	// SLAWarnedAt is when we warned that this transaction was approaching its
	// regulatory deadline. Nil means never warned, which is what stops the
	// warning repeating on every sweep.
	SLAWarnedAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Window returns the reconciliation window this transaction was admitted under.
func (t *Transaction) Window() time.Duration {
	return t.ExpectedCompletionAt.Sub(t.DebitAt)
}

// IsExpiredAt reports whether the window has closed. Only meaningful while open.
func (t *Transaction) IsExpiredAt(now time.Time) bool {
	return t.Status.IsOpen() && !now.Before(t.ExpectedCompletionAt)
}

// DebitEvent reports money leaving the wallet. Starts the regulatory clock.
type DebitEvent struct {
	TenantID        string
	TransactionID   string
	IdempotencyKey  string
	TransactionType string
	Provider        string
	AmountMinor     int64
	Currency        string
	DebitAt         time.Time

	// CustomerRef is raw; hashed on the way into storage, never persisted as-is.
	CustomerRef string

	Metadata map[string]any

	// IsBackfill marks a historical replay. Never triggers webhooks (§3.2 A3).
	IsBackfill bool
}

// CreditEvent reports the provider's verdict on the credit leg.
type CreditEvent struct {
	TenantID          string
	TransactionID     string
	IdempotencyKey    string
	CreditAt          time.Time
	ProviderReference string
	Status            CreditStatus
}

// ReversalCompletedEvent reports the customer finished reversing (§3.2 C2).
type ReversalCompletedEvent struct {
	TenantID      string
	TransactionID string
	CompletedAt   time.Time
}

// ValidationError names the failing field so ingest and the SDKs can report it.
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("invalid field %q: %s", e.Field, e.Reason)
}

const (
	maxIdentifierLen = 255 // these are references, not content
	maxMetadataKeys  = 50  // routing hints, not a document store
)

// Validate checks shape only. The card-data screen is a separate pass
// (sensitive.go) so a refactor here can't disable it.
func (e *DebitEvent) Validate() error {
	if err := requireIdentifier("tenant_id", e.TenantID); err != nil {
		return err
	}
	if err := requireIdentifier("transaction_id", e.TransactionID); err != nil {
		return err
	}
	if err := requireIdentifier("idempotency_key", e.IdempotencyKey); err != nil {
		return err
	}
	if err := requireIdentifier("transaction_type", e.TransactionType); err != nil {
		return err
	}
	if len(e.Provider) > maxIdentifierLen {
		return ValidationError{Field: "provider", Reason: "exceeds 255 characters"}
	}
	if e.AmountMinor <= 0 {
		return ValidationError{Field: "amount_minor", Reason: "must be greater than zero"}
	}
	if err := validateCurrency(e.Currency); err != nil {
		return err
	}
	if e.DebitAt.IsZero() {
		return ValidationError{Field: "debit_at", Reason: "is required"}
	}
	if len(e.Metadata) > maxMetadataKeys {
		return ValidationError{Field: "metadata", Reason: "exceeds 50 keys"}
	}
	return nil
}

func (e *CreditEvent) Validate() error {
	if err := requireIdentifier("tenant_id", e.TenantID); err != nil {
		return err
	}
	if err := requireIdentifier("transaction_id", e.TransactionID); err != nil {
		return err
	}
	if err := requireIdentifier("idempotency_key", e.IdempotencyKey); err != nil {
		return err
	}
	if e.CreditAt.IsZero() {
		return ValidationError{Field: "credit_at", Reason: "is required"}
	}
	if !e.Status.Valid() {
		return ValidationError{Field: "status", Reason: "must be one of success, failed, unknown"}
	}
	if len(e.ProviderReference) > maxIdentifierLen {
		return ValidationError{Field: "provider_reference", Reason: "exceeds 255 characters"}
	}
	return nil
}

func (e *ReversalCompletedEvent) Validate() error {
	if err := requireIdentifier("tenant_id", e.TenantID); err != nil {
		return err
	}
	if err := requireIdentifier("transaction_id", e.TransactionID); err != nil {
		return err
	}
	if e.CompletedAt.IsZero() {
		return ValidationError{Field: "completed_at", Reason: "is required"}
	}
	return nil
}

func requireIdentifier(field, v string) error {
	if v == "" {
		return ValidationError{Field: field, Reason: "is required"}
	}
	if len(v) > maxIdentifierLen {
		return ValidationError{Field: field, Reason: "exceeds 255 characters"}
	}
	return nil
}

// validateCurrency is structural, not an ISO lookup — a stale list shouldn't
// silently stop reconciliation for a real currency.
func validateCurrency(c string) error {
	if len(c) != 3 {
		return ValidationError{Field: "currency", Reason: "must be a 3-letter ISO 4217 code"}
	}
	for _, r := range c {
		if r < 'A' || r > 'Z' {
			return ValidationError{Field: "currency", Reason: "must be uppercase A-Z"}
		}
	}
	return nil
}
