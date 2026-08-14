package store

import (
	"context"
	"sort"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/report"
)

func (m *Memory) Exposure(_ context.Context, tenantID string, scope report.Scope, now time.Time) ([]report.ExposureTotal, []report.AgeBand, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	type acc struct {
		total     report.ExposureTotal
		customers map[string]struct{}
	}
	byCurrency := map[string]*acc{}
	bands := map[[2]string]*report.AgeBand{}

	for _, t := range m.byTenant[tenantID] {
		if !isExposed(t.Status) || !inScope(t.IsBackfill, scope) {
			continue
		}

		a, ok := byCurrency[t.Currency]
		if !ok {
			a = &acc{
				total:     report.ExposureTotal{Currency: t.Currency, OldestDebitAt: t.DebitAt},
				customers: map[string]struct{}{},
			}
			byCurrency[t.Currency] = a
		}
		// What left and has not arrived, not what was debited: counting the
		// full amount of a partly settled transaction would report money that
		// did reach the destination as still outstanding.
		outstanding := t.AmountMinor - t.CreditedMinor
		if outstanding < 0 {
			outstanding = 0
		}

		a.total.Transactions++
		a.total.AmountMinor += outstanding
		a.customers[t.CustomerRefHash] = struct{}{}
		if t.DebitAt.Before(a.total.OldestDebitAt) {
			a.total.OldestDebitAt = t.DebitAt
		}
		if t.Status == domain.StatusSuspect {
			a.total.UnresolvedTransactions++
			a.total.UnresolvedAmountMinor += outstanding
		}
		accumulateBand(bands, t.Currency, now.Sub(t.DebitAt), outstanding)
	}

	totals := make([]report.ExposureTotal, 0, len(byCurrency))
	for _, a := range byCurrency {
		a.total.Customers = len(a.customers)
		a.total.OldestDebitAt = a.total.OldestDebitAt.UTC()
		totals = append(totals, a.total)
	}
	sort.Slice(totals, func(i, j int) bool { return totals[i].Currency < totals[j].Currency })
	return totals, flattenBands(bands), nil
}

// isExposed reports whether the customer's money left and has not come back.
func isExposed(s domain.Status) bool {
	switch s {
	case domain.StatusOrphaned, domain.StatusReversalPending,
		domain.StatusReversalFailed, domain.StatusSuspect:
		return true
	default:
		return false
	}
}

func inScope(isBackfill bool, scope report.Scope) bool {
	switch scope {
	case report.ScopeBackfill:
		return isBackfill
	case report.ScopeLive:
		return !isBackfill
	default:
		return true
	}
}
