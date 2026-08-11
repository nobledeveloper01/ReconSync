package webhook

import (
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// EventType is the webhook event name (§7.2).
type EventType string

const (
	EventReversalTriggered  EventType = "reversal.triggered"
	EventReversalCompleted  EventType = "reversal.completed"
	EventReversalFailed     EventType = "reversal.failed"
	EventTransactionSuspect EventType = "transaction.suspect"
	EventTransactionSettled EventType = "transaction.reconciled"
	EventSLAAtRisk          EventType = "sla.at_risk"
)

// Envelope is the body delivered to a customer endpoint.
type Envelope struct {
	Event      EventType `json:"event"`
	OccurredAt time.Time `json:"occurred_at"`
	Data       Data      `json:"data"`
}

// Data describes the transaction the event concerns.
type Data struct {
	TransactionID string `json:"transaction_id"`
	AmountMinor   int64  `json:"amount_minor"`
	Currency      string `json:"currency"`
	Reason        string `json:"reason,omitempty"`

	DebitAt       time.Time  `json:"debit_at"`
	WindowSeconds int        `json:"window_seconds"`
	DetectedAt    *time.Time `json:"detected_at,omitempty"`

	// RegulatoryDeadline lets the receiver prioritise by urgency without
	// knowing our rules — it removes a whole class of integration question.
	RegulatoryDeadline time.Time `json:"regulatory_deadline"`

	// Advisory is always true and is stated in the payload deliberately: the
	// receiver must verify against its own ledger before moving money (§10.1).
	// A compromise of ReconSync must not be able to cause a payment.
	Advisory bool `json:"advisory"`
}

// EnvelopeFor builds the payload for a transaction event.
func EnvelopeFor(event EventType, t *domain.Transaction, occurredAt time.Time) Envelope {
	return Envelope{
		Event:      event,
		OccurredAt: occurredAt.UTC(),
		Data: Data{
			TransactionID:      t.TransactionID,
			AmountMinor:        t.AmountMinor,
			Currency:           t.Currency,
			Reason:             reasonFor(event),
			DebitAt:            t.DebitAt,
			WindowSeconds:      int(t.Window() / time.Second),
			DetectedAt:         t.DetectedAt,
			RegulatoryDeadline: t.ExpectedCompletionAt,
			Advisory:           true,
		},
	}
}

func reasonFor(event EventType) string {
	switch event {
	case EventReversalTriggered:
		return "no_credit_confirmation_within_window"
	case EventTransactionSuspect:
		return "ambiguous_provider_response_within_window"
	case EventReversalFailed:
		return "reversal_webhook_delivery_exhausted"
	default:
		return ""
	}
}
