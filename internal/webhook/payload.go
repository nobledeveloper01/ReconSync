package webhook

import (
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/evidence"
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

	// These two concern the integration, not a transaction. A tenant that
	// normally sends steadily and has gone quiet is not having a slow day —
	// their pipe to us is broken, and nothing else in their stack is positioned
	// to notice an absence.
	EventIntegrationSilent    EventType = "integration.silent"
	EventIntegrationRecovered EventType = "integration.recovered"
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

	// Confidence is how sure we are that reversing is correct, 0 to 1. It lets a
	// receiver set its own bar — auto-reverse above a threshold, queue for a
	// human below — instead of treating every verdict as equally certain.
	Confidence float64 `json:"confidence"`

	// Evidence is what that number rests on, heaviest signal first.
	Evidence []evidence.Signal `json:"evidence,omitempty"`

	// DeadlineSeconds is how long is left before this becomes a breach. Only
	// set on sla.at_risk, where it is the whole point of the message.
	DeadlineSeconds *int `json:"seconds_until_breach,omitempty"`

	// Drill marks a synthetic transaction sent by a fire drill. It is absent
	// from every real event, so a handler can treat its presence as an
	// instruction to acknowledge and do nothing else.
	//
	// A drill that could be mistaken for a real reversal would be worse than no
	// drill at all, which is why this is in the payload as well as a header.
	Drill bool `json:"drill,omitempty"`
}

// EnvelopeFor builds the payload for a transaction event. A nil evidence set
// yields zero confidence and no signals, which is the honest reading of "we
// recorded nothing".
func EnvelopeFor(event EventType, t *domain.Transaction, occurredAt time.Time, ev *evidence.Set) Envelope {
	// Every timestamp normalised to UTC, including the ones that came back from
	// the database in the server's local zone. They were correct instants
	// either way, but a payload carrying "…+01:00" beside "…Z" is a
	// side-by-side comparison a reader gets wrong, and a receiver comparing
	// strings rather than parsing gets wrong too.
	return Envelope{
		Event:      event,
		OccurredAt: occurredAt.UTC(),
		Data: Data{
			TransactionID:      t.TransactionID,
			AmountMinor:        t.AmountMinor,
			Currency:           t.Currency,
			Reason:             reasonFor(event),
			DebitAt:            t.DebitAt.UTC(),
			WindowSeconds:      int(t.Window() / time.Second),
			DetectedAt:         utcOrNil(t.DetectedAt),
			RegulatoryDeadline: t.ExpectedCompletionAt.UTC(),
			Advisory:           true,
			Confidence:         ev.Confidence(),
			Evidence:           ev.Signals(),
		},
	}
}

// IntegrationEnvelope is the body for an event about the stream itself. It has
// no transaction, and inventing one to fit the transaction shape would put a
// transaction that does not exist into the customer's delivery log.
type IntegrationEnvelope struct {
	Event      EventType       `json:"event"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       IntegrationData `json:"data"`
}

// IntegrationData describes a silence episode.
type IntegrationData struct {
	TenantID    string    `json:"tenant_id"`
	Reason      string    `json:"reason"`
	SilentSince time.Time `json:"silent_since"`
	SilentFor   int       `json:"silent_for_seconds"`
	Advisory    bool      `json:"advisory"`
	Actionable  string    `json:"actionable"`

	// DetectionSuspended is the part that costs money. While a tenant is
	// silent we stop judging their transactions, because an absent credit
	// proves nothing when every event is absent — so nothing is being watched
	// until they fix it.
	DetectionSuspended bool `json:"detection_suspended"`
}

// SilenceEnvelope builds the alert for a tenant that has stopped sending.
func SilenceEnvelope(tenantID string, silentSince, occurredAt time.Time) IntegrationEnvelope {
	return IntegrationEnvelope{
		Event:      EventIntegrationSilent,
		OccurredAt: occurredAt.UTC(),
		Data: IntegrationData{
			TenantID:           tenantID,
			Reason:             "no_events_received",
			SilentSince:        silentSince.UTC(),
			SilentFor:          int(occurredAt.Sub(silentSince) / time.Second),
			Advisory:           true,
			DetectionSuspended: true,
			Actionable:         "check that your transaction service can still reach ReconSync; reconciliation is paused until events resume",
		},
	}
}

// RecoveryEnvelope closes the episode, so a customer is never left wondering
// whether an alert is still live.
func RecoveryEnvelope(tenantID string, silentSince, occurredAt time.Time) IntegrationEnvelope {
	return IntegrationEnvelope{
		Event:      EventIntegrationRecovered,
		OccurredAt: occurredAt.UTC(),
		Data: IntegrationData{
			TenantID:           tenantID,
			Reason:             "events_resumed",
			SilentSince:        silentSince.UTC(),
			SilentFor:          int(occurredAt.Sub(silentSince) / time.Second),
			Advisory:           true,
			DetectionSuspended: false,
			Actionable:         "reconciliation has resumed; transactions opened during the gap are being judged again",
		},
	}
}

// AtRiskEnvelope warns that a transaction will breach its deadline.
//
// The compliance report scores a breach after the fact; this is the same
// information while there is still time to act, over exactly the same
// population — a warning that fired for a different set than the report scores
// would train people to ignore it.
func AtRiskEnvelope(t *domain.Transaction, deadlineAt, occurredAt time.Time) Envelope {
	env := EnvelopeFor(EventSLAAtRisk, t, occurredAt, nil)
	remaining := int(deadlineAt.Sub(occurredAt) / time.Second)
	env.Data.DeadlineSeconds = &remaining
	env.Data.RegulatoryDeadline = deadlineAt.UTC()
	env.Data.Reason = "approaching_reversal_deadline"
	// No confidence: this is not a verdict about whether the transfer failed,
	// it is a statement about the clock. Carrying a number here would invite a
	// receiver to treat it as one.
	env.Data.Confidence = 0
	return env
}

// utcOrNil normalises an optional timestamp without inventing one.
func utcOrNil(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	utc := t.UTC()
	return &utc
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
