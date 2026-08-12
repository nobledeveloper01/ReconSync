package store

import (
	"context"
	"fmt"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/report"
)

// ProviderStats aggregates a tenant's transactions by rail.
//
// Percentiles are computed in the database with percentile_disc, which picks an
// actual observed value rather than interpolating between two — the number is
// then one a real transaction took, and an auditor can find it in the table.
func (p *Postgres) ProviderStats(ctx context.Context, tenantID string, from, to time.Time) ([]report.ProviderStat, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT
			COALESCE(NULLIF(provider, ''), 'unspecified') AS rail,
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status IN ('orphaned','reversal_pending','reversal_completed','reversal_failed')),
			COUNT(*) FILTER (WHERE status = 'suspect'),
			COUNT(*) FILTER (WHERE status IN ('pending_debit','pending_unknown')),
			COALESCE(percentile_disc(0.50) WITHIN GROUP (
				ORDER BY EXTRACT(EPOCH FROM credit_at - debit_at))
				FILTER (WHERE status = 'completed' AND credit_at IS NOT NULL), 0),
			COALESCE(percentile_disc(0.95) WITHIN GROUP (
				ORDER BY EXTRACT(EPOCH FROM credit_at - debit_at))
				FILTER (WHERE status = 'completed' AND credit_at IS NOT NULL), 0),
			COALESCE(MAX(EXTRACT(EPOCH FROM credit_at - debit_at))
				FILTER (WHERE status = 'completed' AND credit_at IS NOT NULL), 0)
		FROM transactions
		WHERE tenant_id = $1 AND debit_at >= $2 AND debit_at < $3
		GROUP BY rail
		ORDER BY rail`, tenantID, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("provider stats: %w", err)
	}
	defer rows.Close()

	var out []report.ProviderStat
	for rows.Next() {
		var s report.ProviderStat
		if err := rows.Scan(&s.Provider, &s.Total, &s.Settled, &s.Failed, &s.Suspect,
			&s.StillOpen, &s.P50, &s.P95, &s.Max); err != nil {
			return nil, fmt.Errorf("scan provider stat: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate provider stats: %w", err)
	}
	return out, nil
}

// CountByStatus counts a tenant's transactions per state over a period.
//
// Aggregated in the database rather than by fetching rows: a healthy tenant has
// millions of settled transactions and a compliance report must not drag them
// all across the wire to count them.
func (p *Postgres) CountByStatus(ctx context.Context, tenantID string, from, to time.Time) (map[domain.Status]int, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT status, COUNT(*)
		FROM transactions
		WHERE tenant_id = $1 AND debit_at >= $2 AND debit_at < $3
		GROUP BY status`, tenantID, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}
	defer rows.Close()

	out := make(map[domain.Status]int)
	for rows.Next() {
		var (
			status string
			n      int
		)
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("scan status count: %w", err)
		}
		out[domain.Status(status)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate status counts: %w", err)
	}
	return out, nil
}

// ListReversalCandidates returns, in full, every transaction that reached
// orphaned or beyond — the only ones a reversal SLA can be measured against.
func (p *Postgres) ListReversalCandidates(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]*domain.Transaction, error) {
	if limit <= 0 {
		limit = 10000
	}
	rows, err := p.pool.Query(ctx,
		`SELECT `+txnColumns+` FROM transactions
		 WHERE tenant_id = $1
		   AND debit_at >= $2 AND debit_at < $3
		   AND detected_at IS NOT NULL
		 ORDER BY debit_at
		 LIMIT $4`, tenantID, from.UTC(), to.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list reversal candidates: %w", err)
	}
	defer rows.Close()
	return collectTransactions(rows)
}
