package store

import (
	"context"
	"sort"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

func (m *Memory) CountByStatus(_ context.Context, tenantID string, from, to time.Time) (map[domain.Status]int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[domain.Status]int)
	for _, t := range m.byTenant[tenantID] {
		if !inPeriod(t.DebitAt, from, to) {
			continue
		}
		out[t.Status]++
	}
	return out, nil
}

func (m *Memory) ListReversalCandidates(_ context.Context, tenantID string, from, to time.Time, limit int) ([]*domain.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		limit = 10000
	}

	var out []*domain.Transaction
	for _, t := range m.byTenant[tenantID] {
		if !inPeriod(t.DebitAt, from, to) || t.DetectedAt == nil {
			continue
		}
		cp := *t
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DebitAt.Before(out[j].DebitAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// inPeriod is half-open: [from, to). Adjacent reports then neither double-count
// a transaction nor drop one on the boundary.
func inPeriod(at, from, to time.Time) bool {
	return !at.Before(from) && at.Before(to)
}
