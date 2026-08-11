// Package rules resolves the reconciliation window for a transaction (§3.2 B2).
package rules

import (
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// DefaultWindow applies when no rule matches (§3.2 B1).
const DefaultWindow = 300 * time.Second

// Action is what to do when a transaction breaches its window.
type Action string

const (
	ActionAutoReverse Action = "auto_reverse"
	ActionAlertOnly   Action = "alert_only"
	ActionInvestigate Action = "investigate"
)

// Rule matches transactions on any combination of criteria. An empty or nil
// field matches anything. Mirrors the reconciliation_rules table.
type Rule struct {
	ID              int64
	TransactionType string
	Provider        string
	Currency        string
	MinAmountMinor  *int64
	MaxAmountMinor  *int64

	WindowSeconds int
	Action        Action
	Priority      int
	Enabled       bool
}

// specificity counts the constraints a rule sets, used to break priority ties.
func (r Rule) specificity() int {
	n := 0
	for _, s := range []string{r.TransactionType, r.Provider, r.Currency} {
		if s != "" {
			n++
		}
	}
	if r.MinAmountMinor != nil {
		n++
	}
	if r.MaxAmountMinor != nil {
		n++
	}
	return n
}

// matches reports whether every criterion the rule sets holds.
func (r Rule) matches(t *domain.Transaction) bool {
	if !r.Enabled {
		return false
	}
	if r.TransactionType != "" && r.TransactionType != t.TransactionType {
		return false
	}
	if r.Provider != "" && r.Provider != t.Provider {
		return false
	}
	if r.Currency != "" && r.Currency != t.Currency {
		return false
	}
	if r.MinAmountMinor != nil && t.AmountMinor < *r.MinAmountMinor {
		return false
	}
	if r.MaxAmountMinor != nil && t.AmountMinor > *r.MaxAmountMinor {
		return false
	}
	return true
}

// Resolution is the outcome of matching a transaction against a rule set.
type Resolution struct {
	Window time.Duration
	Action Action
	RuleID int64 // 0 when the default applied
}

// Set is an immutable, pre-sorted collection of rules for one tenant.
type Set struct {
	rules []Rule
}

// NewSet builds a rule set. Disabled rules are dropped up front.
func NewSet(rules []Rule) *Set {
	kept := make([]Rule, 0, len(rules))
	for _, r := range rules {
		if r.Enabled {
			kept = append(kept, r)
		}
	}
	return &Set{rules: kept}
}

// Resolve returns the window and action for a transaction.
//
// Highest priority wins; ties break toward the more specific rule, then toward
// the lower rule ID so the outcome never depends on map or scan ordering.
func (s *Set) Resolve(t *domain.Transaction) Resolution {
	var best *Rule
	for i := range s.rules {
		r := &s.rules[i]
		if !r.matches(t) {
			continue
		}
		if best == nil || better(r, best) {
			best = r
		}
	}

	if best == nil {
		return Resolution{Window: DefaultWindow, Action: ActionAutoReverse}
	}
	return Resolution{
		Window: time.Duration(best.WindowSeconds) * time.Second,
		Action: best.Action,
		RuleID: best.ID,
	}
}

func better(candidate, current *Rule) bool {
	if candidate.Priority != current.Priority {
		return candidate.Priority > current.Priority
	}
	if cs, bs := candidate.specificity(), current.specificity(); cs != bs {
		return cs > bs
	}
	return candidate.ID < current.ID
}

// Amount is a helper for building amount-bounded rules in tests and config.
func Amount(v int64) *int64 { return &v }
