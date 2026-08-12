package tests

import (
	"errors"
	"testing"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// Written out by hand rather than derived from the package, so a status dropped
// from the implementation fails a test instead of vanishing from it.
var allStatuses = []domain.Status{
	domain.StatusPendingDebit,
	domain.StatusPendingUnknown,
	domain.StatusCompleted,
	domain.StatusSuspect,
	domain.StatusOrphaned,
	domain.StatusReversalPending,
	domain.StatusReversalCompleted,
	domain.StatusReversalFailed,
}

// The complete edge set from §4.2, plus the documented reversal_failed replay
// edge. Asserting it exactly means an added transition fails the build — an
// extra edge in a machine that decides whether to move money is a defect.
var specEdges = map[domain.Status][]domain.Status{
	// StatusSuspect from pending_debit is ADR-0004: an ingest gap means the
	// absence of a credit proves nothing, so it must not auto-reverse.
	domain.StatusPendingDebit:   {domain.StatusCompleted, domain.StatusPendingUnknown, domain.StatusOrphaned, domain.StatusSuspect},
	domain.StatusPendingUnknown: {domain.StatusCompleted, domain.StatusSuspect},
	domain.StatusSuspect:        {domain.StatusOrphaned, domain.StatusCompleted},
	// Completed and Suspect from orphaned are ADR-0005: provider corroboration
	// settles it, or fails to answer and must not guess.
	domain.StatusOrphaned:          {domain.StatusReversalPending, domain.StatusCompleted, domain.StatusSuspect},
	domain.StatusReversalPending:   {domain.StatusReversalCompleted, domain.StatusReversalFailed},
	domain.StatusReversalFailed:    {domain.StatusReversalPending},
	domain.StatusCompleted:         {},
	domain.StatusReversalCompleted: {},
}

func TestAllStatusesAreValid(t *testing.T) {
	for _, s := range allStatuses {
		if !s.Valid() {
			t.Errorf("status %q is not recognised", s)
		}
	}
}

func TestUnknownStatusIsInvalid(t *testing.T) {
	for _, s := range []domain.Status{"", "PENDING_DEBIT", "refunded", "pending"} {
		if s.Valid() {
			t.Errorf("status %q should not be valid", s)
		}
	}
}

func TestTransitionTableExactlyMatchesSpec(t *testing.T) {
	for _, from := range allStatuses {
		for _, to := range allStatuses {
			allowed := false
			for _, d := range specEdges[from] {
				if d == to {
					allowed = true
					break
				}
			}
			if got := domain.CanTransition(from, to); got != allowed {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, allowed)
			}
		}
	}
}

func TestNoSelfTransitions(t *testing.T) {
	for _, s := range allStatuses {
		if domain.CanTransition(s, s) {
			t.Errorf("state %s allows a transition to itself; duplicates would look like real changes", s)
		}
	}
}

func TestCompletedIsAbsorbing(t *testing.T) {
	// §10: a replayed credit must never move a settled transaction.
	if !domain.StatusCompleted.IsTerminal() {
		t.Fatal("completed must be terminal")
	}
	for _, to := range allStatuses {
		if domain.CanTransition(domain.StatusCompleted, to) {
			t.Errorf("completed must not transition to %s", to)
		}
	}
}

func TestTerminalStates(t *testing.T) {
	terminal := map[domain.Status]bool{
		domain.StatusCompleted:         true,
		domain.StatusReversalCompleted: true,
	}
	for _, s := range allStatuses {
		if got := s.IsTerminal(); got != terminal[s] {
			t.Errorf("%s.IsTerminal() = %v, want %v", s, got, terminal[s])
		}
	}
}

func TestIsOpenMatchesSchedulerPredicate(t *testing.T) {
	// Must stay identical to the partial index predicate in migrations/0001.
	open := map[domain.Status]bool{
		domain.StatusPendingDebit:   true,
		domain.StatusPendingUnknown: true,
	}
	for _, s := range allStatuses {
		if got := s.IsOpen(); got != open[s] {
			t.Errorf("%s.IsOpen() = %v, want %v", s, got, open[s])
		}
	}
}

func TestNeedsReversal(t *testing.T) {
	want := map[domain.Status]bool{
		domain.StatusOrphaned:        true,
		domain.StatusReversalPending: true,
		domain.StatusReversalFailed:  true,
	}
	for _, s := range allStatuses {
		if got := s.NeedsReversal(); got != want[s] {
			t.Errorf("%s.NeedsReversal() = %v, want %v", s, got, want[s])
		}
	}
}

func TestEveryStateIsReachableFromPendingDebit(t *testing.T) {
	seen := map[domain.Status]bool{domain.StatusPendingDebit: true}
	queue := []domain.Status{domain.StatusPendingDebit}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range allStatuses {
			if domain.CanTransition(cur, next) && !seen[next] {
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

func TestSourcesForMatchesEdges(t *testing.T) {
	// The store builds its SQL guard from SourcesFor, so it must agree with
	// CanTransition or the database would accept or reject the wrong writes.
	for _, target := range allStatuses {
		got := map[domain.Status]bool{}
		for _, s := range domain.SourcesFor(target) {
			got[s] = true
		}
		for _, from := range allStatuses {
			want := domain.CanTransition(from, target)
			if got[from] != want {
				t.Errorf("SourcesFor(%s) contains %s = %v, want %v", target, from, got[from], want)
			}
		}
	}
}

func TestTransitionReturnsTypedError(t *testing.T) {
	if err := domain.Transition(domain.StatusPendingDebit, domain.StatusCompleted); err != nil {
		t.Fatalf("legal transition returned error: %v", err)
	}

	err := domain.Transition(domain.StatusCompleted, domain.StatusOrphaned)
	if err == nil {
		t.Fatal("illegal transition returned nil error")
	}
	var ite domain.InvalidTransitionError
	if !errors.As(err, &ite) {
		t.Fatalf("error is %T, want InvalidTransitionError so ingest can map it to 409", err)
	}
	if ite.From != domain.StatusCompleted || ite.To != domain.StatusOrphaned {
		t.Errorf("error carries %s -> %s, want completed -> orphaned", ite.From, ite.To)
	}
}
