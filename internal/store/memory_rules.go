package store

import (
	"context"
	"fmt"
	"sort"

	"github.com/nobledeveloper01/ReconSync/internal/rules"
)

func (m *Memory) ListRules(_ context.Context, tenantID string) ([]rules.Rule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]rules.Rule, 0, len(m.rules[tenantID]))
	out = append(out, m.rules[tenantID]...)

	// Same ordering as Postgres, so callers cannot depend on one and break on
	// the other.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func (m *Memory) CreateRule(_ context.Context, tenantID string, r *rules.Rule) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if r.WindowSeconds <= 0 {
		return 0, fmt.Errorf("store: window_seconds must be positive, got %d", r.WindowSeconds)
	}

	m.nextRuleID++
	stored := *r
	stored.ID = m.nextRuleID
	if stored.Action == "" {
		stored.Action = rules.ActionAutoReverse
	}
	m.rules[tenantID] = append(m.rules[tenantID], stored)
	return stored.ID, nil
}

func (m *Memory) DeleteRule(_ context.Context, tenantID string, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, r := range m.rules[tenantID] {
		if r.ID == id {
			m.rules[tenantID] = append(m.rules[tenantID][:i], m.rules[tenantID][i+1:]...)
			return nil
		}
	}
	return ErrNotFound
}
