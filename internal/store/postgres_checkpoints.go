package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
)

func (p *Postgres) SaveCheckpoint(ctx context.Context, c audit.Checkpoint) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO audit_checkpoints (tenant_id, seq, hash, taken_at, signature, public_key)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, seq) DO NOTHING`,
		c.TenantID, c.Seq, c.Hash, c.TakenAt.UTC(), c.Signature, c.PublicKey)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

func (p *Postgres) LatestCheckpoint(ctx context.Context, tenantID string) (*audit.Checkpoint, error) {
	var c audit.Checkpoint
	err := p.pool.QueryRow(ctx, `
		SELECT tenant_id, seq, hash, taken_at, signature, public_key
		FROM audit_checkpoints
		WHERE tenant_id = $1
		ORDER BY seq DESC
		LIMIT 1`, tenantID).
		Scan(&c.TenantID, &c.Seq, &c.Hash, &c.TakenAt, &c.Signature, &c.PublicKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("latest checkpoint: %w", err)
	}
	c.TakenAt = c.TakenAt.UTC()
	return &c, nil
}

func (p *Postgres) ListCheckpoints(ctx context.Context, tenantID string, limit int) ([]audit.Checkpoint, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := p.pool.Query(ctx, `
		SELECT tenant_id, seq, hash, taken_at, signature, public_key
		FROM audit_checkpoints
		WHERE tenant_id = $1
		ORDER BY seq DESC
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list checkpoints: %w", err)
	}
	defer rows.Close()

	var out []audit.Checkpoint
	for rows.Next() {
		var c audit.Checkpoint
		if err := rows.Scan(&c.TenantID, &c.Seq, &c.Hash, &c.TakenAt, &c.Signature, &c.PublicKey); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		c.TakenAt = c.TakenAt.UTC()
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}
	return out, nil
}

func (p *Postgres) TenantsWithAudit(ctx context.Context) ([]string, error) {
	rows, err := p.pool.Query(ctx, `SELECT DISTINCT tenant_id FROM audit_records ORDER BY tenant_id`)
	if err != nil {
		return nil, fmt.Errorf("tenants with audit: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan audit tenant: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit tenants: %w", err)
	}
	return out, nil
}
