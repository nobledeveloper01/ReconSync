// Package domain holds the transaction types and the state machine (§4.2).
// No storage or transport dependencies, so the rules that decide whether money
// gets reversed can be tested on their own.
package domain

import (
	"fmt"
	"sort"
)

// Status is the lifecycle state of a tracked transaction.
type Status string

const (
	StatusPendingDebit      Status = "pending_debit"      // debited, awaiting credit confirmation
	StatusPendingUnknown    Status = "pending_unknown"    // provider answered ambiguously
	StatusCompleted         Status = "completed"          // credit confirmed; terminal and absorbing
	StatusSuspect           Status = "suspect"            // ambiguous and expired; needs investigation
	StatusOrphaned          Status = "orphaned"           // debit with no credit; the condition we exist to find
	StatusReversalPending   Status = "reversal_pending"   // reversal webhook dispatched
	StatusReversalCompleted Status = "reversal_completed" // customer confirmed; terminal
	StatusReversalFailed    Status = "reversal_failed"    // retries exhausted, dead-lettered
)

// Valid reports whether s is a recognised status. Values read from storage go
// through here so a row written by a newer binary fails loudly.
func (s Status) Valid() bool {
	_, ok := allowedTransitions[s]
	return ok
}

// IsTerminal reports whether the transaction is finished.
func (s Status) IsTerminal() bool {
	return len(allowedTransitions[s]) == 0
}

// IsOpen reports whether the scheduler should still watch for window expiry.
// Must stay identical to the partial index predicate in migrations/0001.
func (s Status) IsOpen() bool {
	return s == StatusPendingDebit || s == StatusPendingUnknown
}

// NeedsReversal reports whether the customer is currently out of pocket.
func (s Status) NeedsReversal() bool {
	return s == StatusOrphaned || s == StatusReversalPending || s == StatusReversalFailed
}

func (s Status) String() string { return string(s) }

// allowedTransitions is the whole state machine as data, so tests can enumerate
// it. A status with no destinations is terminal, which means a state added
// without edges fails closed.
var allowedTransitions = map[Status]map[Status]struct{}{
	StatusPendingDebit: {
		StatusCompleted:      {},
		StatusPendingUnknown: {},
		StatusOrphaned:       {},
		// Not in the §4.2 diagram. Reached when our own ingest had a gap over
		// this transaction's window, so the absence of a credit proves nothing.
		// See ADR-0004.
		StatusSuspect: {},
	},
	StatusPendingUnknown: {
		StatusCompleted: {},
		StatusSuspect:   {},
	},
	StatusSuspect: {
		StatusOrphaned:  {},
		StatusCompleted: {},
	},
	StatusOrphaned: {
		StatusReversalPending: {},
		// Both added by ADR-0005, reachable only before a reversal is dispatched.
		// The rail confirmed it settled after all, or we could not get an answer
		// and must not guess.
		StatusCompleted: {},
		StatusSuspect:   {},
	},
	StatusReversalPending: {
		StatusReversalCompleted: {},
		StatusReversalFailed:    {},
	},
	// Not in the §4.2 diagram; required by dead-letter replay (§11.5 step 5).
	StatusReversalFailed: {
		StatusReversalPending: {},
	},

	StatusCompleted:         {},
	StatusReversalCompleted: {},
}

// InvalidTransitionError is distinct so ingest can map it to 409 — a replayed or
// out-of-order event is normal client behaviour, not our bug.
type InvalidTransitionError struct {
	From Status
	To   Status
}

func (e InvalidTransitionError) Error() string {
	return fmt.Sprintf("invalid transaction state transition: %s -> %s", e.From, e.To)
}

// CanTransition reports whether from -> to is a legal edge.
//
// Self-transitions are illegal: accepting a no-op would make a duplicate event
// look applied. Idempotent replay is handled by idempotency keys at ingest.
func CanTransition(from, to Status) bool {
	dests, ok := allowedTransitions[from]
	if !ok {
		return false
	}
	_, ok = dests[to]
	return ok
}

// Transition validates from -> to.
func Transition(from, to Status) error {
	if !CanTransition(from, to) {
		return InvalidTransitionError{From: from, To: to}
	}
	return nil
}

// SourcesFor returns the states that may legally reach target, sorted.
// The store uses this to build its UPDATE guard, so the SQL cannot drift from
// the state machine.
func SourcesFor(target Status) []Status {
	var out []Status
	for from, dests := range allowedTransitions {
		if _, ok := dests[target]; ok {
			out = append(out, from)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
