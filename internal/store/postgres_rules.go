package store

import (
	"context"
	"fmt"

	"github.com/nobledeveloper01/ReconSync/internal/rules"
)

// A NULL criterion in the table means "matches anything"; the Go type spells the
// same thing as an empty string. COALESCE and NULLIF translate between them, so
// neither side has to carry the other's convention.
//
// currency in particular must be NULL rather than '' — the column is CHAR(3) and
// an empty string would be stored as three spaces, matching nothing ever.

func (p *Postgres) ListRules(ctx context.Context, tenantID string) ([]rules.Rule, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT id,
		       COALESCE(transaction_type, ''),
		       COALESCE(provider, ''),
		       COALESCE(currency, ''),
		       min_amount_minor,
		       max_amount_minor,
		       window_seconds,
		       action,
		       priority,
		       enabled
		FROM reconciliation_rules
		WHERE tenant_id = $1
		ORDER BY priority DESC, id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list rules: %w", err)
	}
	defer rows.Close()

	var out []rules.Rule
	for rows.Next() {
		var (
			r      rules.Rule
			action string
		)
		if err := rows.Scan(&r.ID, &r.TransactionType, &r.Provider, &r.Currency,
			&r.MinAmountMinor, &r.MaxAmountMinor, &r.WindowSeconds, &action,
			&r.Priority, &r.Enabled); err != nil {
			return nil, fmt.Errorf("scan rule: %w", err)
		}
		r.Action = rules.Action(action)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rules: %w", err)
	}
	return out, nil
}

func (p *Postgres) CreateRule(ctx context.Context, tenantID string, r *rules.Rule) (int64, error) {
	if r.WindowSeconds <= 0 {
		return 0, fmt.Errorf("store: window_seconds must be positive, got %d", r.WindowSeconds)
	}
	action := r.Action
	if action == "" {
		action = rules.ActionAutoReverse
	}

	var id int64
	err := p.pool.QueryRow(ctx, `
		INSERT INTO reconciliation_rules (
			tenant_id, transaction_type, provider, currency,
			min_amount_minor, max_amount_minor, window_seconds, action, priority, enabled)
		VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), $5, $6, $7, $8, $9, $10)
		RETURNING id`,
		tenantID, r.TransactionType, r.Provider, r.Currency,
		r.MinAmountMinor, r.MaxAmountMinor, r.WindowSeconds, string(action),
		r.Priority, r.Enabled).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("create rule: %w", err)
	}
	return id, nil
}

func (p *Postgres) DeleteRule(ctx context.Context, tenantID string, id int64) error {
	tag, err := p.pool.Exec(ctx,
		`DELETE FROM reconciliation_rules WHERE tenant_id = $1 AND id = $2`, tenantID, id)
	if err != nil {
		return fmt.Errorf("delete rule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
