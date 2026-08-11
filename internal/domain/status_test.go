package domain

import "testing"

// allStatuses is written out by hand rather than derived from the transition
// table. If it were derived, a status accidentally dropped from the table would
// also vanish from every test that ranges over it, and the suite would go green
// while losing coverage.
var allStatuses = []Status{
	StatusPendingDebit,
	StatusPendingUnknown,
	StatusCompleted,
	StatusSuspect,
	StatusOrphaned,
	StatusReversalPending,
	StatusReversalCompleted,
	StatusReversalFailed,
}

func TestAllStatusesAreValid(t *testing.T) {
	for _, s := range allStatuses {
		if !s.Valid() {
			t.Errorf("status %q is not present in the transition table", s)
		}
	}
	if len(allowedTransitions) != len(allStatuses) {
		t.Errorf("transition table has %d states, the known set has %d — a state was added to one and not the other",
			len(allowedTransitions), len(allStatuses))
	}
}

func TestUnknownStatusIsInvalid(t *testing.T) {
	for _, s := range []Status{"", "PENDING_DEBIT", "refunded", "pending"} {
		if Status(s).Valid() {
			t.Errorf("status %q should not be valid", s)
		}
	}
}

func TestTransitionTableExactlyMatchesSpec(t *testing.T) {
	// The complete edge set from §4.2 of the specification, plus the documented
	// reversal_failed -> reversal_pending replay edge. Asserting the table
	// exactly — rather than spot-checking a few edges — is what makes an
	// accidentally-added edge fail the build. An extra edge in a state machine
	// that decides whether to reverse money is a defect, not an enhancement.
	want := map[Status][]Status{
		StatusPendingDebit:      {StatusCompleted, StatusPendingUnknown, StatusOrphaned},
		StatusPendingUnknown:    {StatusCompleted, StatusSuspect},
		StatusSuspect:           {StatusOrphaned, StatusCompleted},
		StatusOrphaned:          {StatusReversalPending},
		StatusReversalPending:   {StatusReversalCompleted, StatusReversalFailed},
		StatusReversalFailed:    {StatusReversalPending},
		StatusCompleted:         {},
		StatusReversalCompleted: {},
	}

	for from, dests := range want {
		got := allowedTransitions[from]
		if len(got) != len(dests) {
			t.Errorf("state %s: has %d outgoing edges, spec defines %d", from, len(got), len(dests))
		}
		for _, to := range dests {
			if !CanTransition(from, to) {
				t.Errorf("state %s: missing required edge -> %s", from, to)
			}
		}
	}

	// And the negative half: every pair not in want must be rejected.
	for _, from := range allStatuses {
		for _, to := range allStatuses {
			allowed := false
			for _, d := range want[from] {
				if d == to {
					allowed = true
					break
				}
			}
			if got := CanTransition(from, to); got != allowed {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, allowed)
			}
		}
	}
}

func TestNoSelfTransitions(t *testing.T) {
	for _, s := range allStatuses {
		if CanTransition(s, s) {
			t.Errorf("state %s allows a transition to itself; duplicate events would look like real changes", s)
		}
	}
}

func TestCompletedIsAbsorbing(t *testing.T) {
	// §10: a replayed credit event must never be able to move a settled
	// transaction. Completed has no exits, so no later event can reach any other
	// state from it — including orphaned.
	if !StatusCompleted.IsTerminal() {
		t.Fatal("completed must be terminal")
	}
	for _, to := range allStatuses {
		if CanTransition(StatusCompleted, to) {
			t.Errorf("completed must not transition to %s", to)
		}
	}
}

func TestTerminalStates(t *testing.T) {
	terminal := map[Status]bool{
		StatusCompleted:         true,
		StatusReversalCompleted: true,
	}
	for _, s := range allStatuses {
		if got := s.IsTerminal(); got != terminal[s] {
			t.Errorf("%s.IsTerminal() = %v, want %v", s, got, terminal[s])
		}
	}
}

func TestIsOpenMatchesSchedulerPredicate(t *testing.T) {
	// IsOpen must stay identical to the partial index predicate in
	// migrations/0001: WHERE status IN ('pending_debit','pending_unknown').
	// If these drift, the scheduler either misses transactions that should be
	// detected or scans rows it can never act on.
	open := map[Status]bool{
		StatusPendingDebit:   true,
		StatusPendingUnknown: true,
	}
	for _, s := range allStatuses {
		if got := s.IsOpen(); got != open[s] {
			t.Errorf("%s.IsOpen() = %v, want %v", s, got, open[s])
		}
	}
}

func TestNeedsReversal(t *testing.T) {
	want := map[Status]bool{
		StatusOrphaned:        true,
		StatusReversalPending: true,
		StatusReversalFailed:  true,
	}
	for _, s := range allStatuses {
		if got := s.NeedsReversal(); got != want[s] {
			t.Errorf("%s.NeedsReversal() = %v, want %v", s, got, want[s])
		}
	}
}

func TestEveryStateIsReachableFromPendingDebit(t *testing.T) {
	// A state nobody can reach is dead code in a state machine. Breadth-first
	// walk from the one entry state the machine has.
	seen := map[Status]bool{StatusPendingDebit: true}
	queue := []Status{StatusPendingDebit}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for next := range allowedTransitions[cur] {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	for _, s := range allStatuses {
		if !seen[s] {
			t.Errorf("state %s is unreachable from pending_debit", s)
		}
	}
}

func TestTransitionReturnsTypedError(t *testing.T) {
	if err := Transition(StatusPendingDebit, StatusCompleted); err != nil {
		t.Fatalf("legal transition returned error: %v", err)
	}

	err := Transition(StatusCompleted, StatusOrphaned)
	if err == nil {
		t.Fatal("illegal transition returned nil error")
	}
	var ite InvalidTransitionError
	if !asInvalidTransition(err, &ite) {
		t.Fatalf("error is %T, want InvalidTransitionError so ingest can map it to 409", err)
	}
	if ite.From != StatusCompleted || ite.To != StatusOrphaned {
		t.Errorf("error carries %s -> %s, want completed -> orphaned", ite.From, ite.To)
	}
}

func asInvalidTransition(err error, target *InvalidTransitionError) bool {
	ite, ok := err.(InvalidTransitionError)
	if ok {
		*target = ite
	}
	return ok
}
