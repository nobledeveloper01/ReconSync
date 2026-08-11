package domain

import "errors"

// Event is one ingested item. Exactly one leg is set. It is the shared unit
// between transport, pipeline and correlation so none of them need to know
// about the others.
type Event struct {
	Debit  *DebitEvent
	Credit *CreditEvent
}

// NewDebitEvent wraps a debit leg.
func NewDebitEvent(e *DebitEvent) Event { return Event{Debit: e} }

// NewCreditEvent wraps a credit leg.
func NewCreditEvent(e *CreditEvent) Event { return Event{Credit: e} }

// ErrEmptyEvent means neither or both legs were set.
var ErrEmptyEvent = errors.New("event must carry exactly one of debit or credit")

// TenantID returns the owning tenant, or "" for a malformed event.
func (e Event) TenantID() string {
	switch {
	case e.Debit != nil:
		return e.Debit.TenantID
	case e.Credit != nil:
		return e.Credit.TenantID
	default:
		return ""
	}
}

// TransactionID returns the customer's identifier, or "" for a malformed event.
func (e Event) TransactionID() string {
	switch {
	case e.Debit != nil:
		return e.Debit.TransactionID
	case e.Credit != nil:
		return e.Credit.TransactionID
	default:
		return ""
	}
}

// Validate checks the envelope and then the leg it carries.
func (e Event) Validate() error {
	switch {
	case e.Debit != nil && e.Credit != nil:
		return ErrEmptyEvent
	case e.Debit != nil:
		return e.Debit.Validate()
	case e.Credit != nil:
		return e.Credit.Validate()
	default:
		return ErrEmptyEvent
	}
}
