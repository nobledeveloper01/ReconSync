package store

import (
	"context"
	"fmt"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

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
