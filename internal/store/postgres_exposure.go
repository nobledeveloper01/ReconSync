package store

import (
	"context"
	"fmt"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/report"
)

// exposedStatuses are the states in which the customer's money left and has not
// come back: no confirmed credit, no confirmed reversal.
const exposedStatuses = `('orphaned','reversal_pending','reversal_failed','suspect')`

// scopeClause narrows to replayed history or live traffic.
//
// Written as a fixed clause per scope rather than interpolated input: the value
// reaches here from a query parameter, and the only safe way to put a caller's
// value into SQL is to not put it there at all.
func scopeClause(scope report.Scope) string {
	switch scope {
	case report.ScopeBackfill:
		return " AND is_backfill"
	case report.ScopeLive:
		return " AND NOT is_backfill"
	default:
		return ""
	}
}

func (p *Postgres) Exposure(ctx context.Context, tenantID string, scope report.Scope, now time.Time) ([]report.ExposureTotal, []report.AgeBand, error) {
	where := `WHERE tenant_id = $1 AND status IN ` + exposedStatuses + scopeClause(scope)

	rows, err := p.pool.Query(ctx, `
		SELECT currency,
		       COUNT(*),
		       COUNT(DISTINCT customer_ref_hash),
		       COALESCE(SUM(amount_minor), 0),
		       MIN(debit_at),
		       COUNT(*) FILTER (WHERE status = 'suspect'),
		       COALESCE(SUM(amount_minor) FILTER (WHERE status = 'suspect'), 0)
		FROM transactions `+where+`
		GROUP BY currency`, tenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("exposure totals: %w", err)
	}
	defer rows.Close()

	var totals []report.ExposureTotal
	for rows.Next() {
		var t report.ExposureTotal
		if err := rows.Scan(&t.Currency, &t.Transactions, &t.Customers, &t.AmountMinor,
			&t.OldestDebitAt, &t.UnresolvedTransactions, &t.UnresolvedAmountMinor); err != nil {
			return nil, nil, fmt.Errorf("scan exposure total: %w", err)
		}
		t.OldestDebitAt = t.OldestDebitAt.UTC()
		totals = append(totals, t)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate exposure totals: %w", err)
	}

	// The bands are bucketed in Go rather than in SQL so the two stores cannot
	// disagree about a boundary, and so changing them is a one-line change in
	// one place.
	bandRows, err := p.pool.Query(ctx, `
		SELECT currency, debit_at, amount_minor
		FROM transactions `+where, tenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("exposure ages: %w", err)
	}
	defer bandRows.Close()

	agg := map[[2]string]*report.AgeBand{}
	for bandRows.Next() {
		var (
			currency string
			debitAt  time.Time
			amount   int64
		)
		if err := bandRows.Scan(&currency, &debitAt, &amount); err != nil {
			return nil, nil, fmt.Errorf("scan exposure age: %w", err)
		}
		accumulateBand(agg, currency, now.Sub(debitAt), amount)
	}
	if err := bandRows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate exposure ages: %w", err)
	}
	return totals, flattenBands(agg), nil
}

func accumulateBand(agg map[[2]string]*report.AgeBand, currency string, age time.Duration, amount int64) {
	key := [2]string{currency, report.BandFor(age)}
	b, ok := agg[key]
	if !ok {
		b = &report.AgeBand{Currency: currency, Band: key[1]}
		agg[key] = b
	}
	b.Transactions++
	b.AmountMinor += amount
}

func flattenBands(agg map[[2]string]*report.AgeBand) []report.AgeBand {
	out := make([]report.AgeBand, 0, len(agg))
	for _, b := range agg {
		out = append(out, *b)
	}
	return out
}
