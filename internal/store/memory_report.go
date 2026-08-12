package store

import (
	"context"
	"sort"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/report"
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

func (m *Memory) ProviderStats(_ context.Context, tenantID string, from, to time.Time) ([]report.ProviderStat, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	type acc struct {
		stat      report.ProviderStat
		latencies []float64
	}
	byRail := map[string]*acc{}

	for _, t := range m.byTenant[tenantID] {
		if !inPeriod(t.DebitAt, from, to) {
			continue
		}
		rail := t.Provider
		if rail == "" {
			rail = "unspecified"
		}
		a, ok := byRail[rail]
		if !ok {
			a = &acc{stat: report.ProviderStat{Provider: rail}}
			byRail[rail] = a
		}
		a.stat.Total++

		switch t.Status {
		case domain.StatusCompleted:
			a.stat.Settled++
			if t.CreditAt != nil {
				a.latencies = append(a.latencies, t.CreditAt.Sub(t.DebitAt).Seconds())
			}
		case domain.StatusOrphaned, domain.StatusReversalPending,
			domain.StatusReversalCompleted, domain.StatusReversalFailed:
			a.stat.Failed++
		case domain.StatusSuspect:
			a.stat.Suspect++
		case domain.StatusPendingDebit, domain.StatusPendingUnknown:
			a.stat.StillOpen++
		}
	}

	out := make([]report.ProviderStat, 0, len(byRail))
	for _, a := range byRail {
		a.stat.P50, a.stat.P95, a.stat.Max = percentiles(a.latencies)
		out = append(out, a.stat)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Provider < out[j].Provider })
	return out, nil
}

// percentiles picks observed values by nearest rank, matching what
// percentile_disc does in Postgres so the two stores cannot disagree.
func percentiles(values []float64) (p50, p95, max float64) {
	if len(values) == 0 {
		return 0, 0, 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	at := func(p float64) float64 {
		idx := int(p*float64(len(sorted))+0.5) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	return at(0.50), at(0.95), sorted[len(sorted)-1]
}

// inPeriod is half-open: [from, to). Adjacent reports then neither double-count
// a transaction nor drop one on the boundary.
func inPeriod(at, from, to time.Time) bool {
	return !at.Before(from) && at.Before(to)
}
