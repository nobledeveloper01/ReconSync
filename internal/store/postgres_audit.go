package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
)

// uniqueViolation is Postgres' SQLSTATE for a duplicate key.
const uniqueViolation = "23505"

// AppendAudit adds a record to the tenant's chain.
//
// A chain is serial by construction — the new record's hash depends on the
// previous record's — so appends are serialised per tenant with an advisory
// lock rather than raced optimistically. Optimistic retry looks cheaper but
// scales the wrong way: contention rises with writers, and a burst of concurrent
// verdicts is exactly when losing an audit record matters most.
//
// The lock is transaction-scoped, so it is released on commit or rollback with
// nothing to clean up, and it is per tenant, so busy tenants never block each
// other. The unique constraint on (tenant_id, seq) remains as a backstop against
// any path that forgets to take the lock.
func (p *Postgres) AppendAudit(ctx context.Context, tenantID string, r *audit.Record) (*audit.Record, error) {
	if tenantID == "" {
		return nil, errors.New("store: audit record needs a tenant")
	}
	r.TenantID = tenantID
	if r.OccurredAt.IsZero() {
		r.OccurredAt = time.Now().UTC()
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin audit append: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, tenantID); err != nil {
		return nil, fmt.Errorf("lock audit chain for %s: %w", tenantID, err)
	}

	var (
		lastSeq  int64
		lastHash string
	)
	err = tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq), 0),
		        COALESCE((SELECT hash FROM audit_records
		                  WHERE tenant_id = $1 ORDER BY seq DESC LIMIT 1), '')
		 FROM audit_records WHERE tenant_id = $1`, tenantID).Scan(&lastSeq, &lastHash)
	if err != nil {
		return nil, fmt.Errorf("read audit chain tip: %w", err)
	}

	r.Seq = lastSeq + 1
	r.PrevHash = lastHash
	hash, err := audit.ComputeHash(*r, lastHash)
	if err != nil {
		return nil, err
	}
	r.Hash = hash

	actor, subject, payload, err := encodeAuditJSON(r)
	if err != nil {
		return nil, err
	}

	var (
		id         int64
		recordedAt time.Time
	)
	err = tx.QueryRow(ctx, `
		INSERT INTO audit_records (
			tenant_id, seq, event_type, occurred_at, actor, subject, payload, prev_hash, hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8,''), $9)
		RETURNING id, recorded_at`,
		tenantID, r.Seq, r.EventType, r.OccurredAt, actor, subject, payload, r.PrevHash, r.Hash).
		Scan(&id, &recordedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			// Only reachable if something appended without taking the lock.
			return nil, fmt.Errorf("append audit record for %s: seq %d already taken; "+
				"an unlocked writer is racing the chain", tenantID, r.Seq)
		}
		return nil, fmt.Errorf("append audit record: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit audit append: %w", err)
	}

	r.ID = id
	r.RecordedAt = recordedAt
	return r, nil
}

func encodeAuditJSON(r *audit.Record) (actor, subject, payload string, err error) {
	for _, f := range []struct {
		name string
		src  map[string]any
		dst  *string
	}{
		{"actor", r.Actor, &actor},
		{"subject", r.Subject, &subject},
		{"payload", r.Payload, &payload},
	} {
		m := f.src
		if m == nil {
			m = map[string]any{}
		}
		raw, marshalErr := json.Marshal(m)
		if marshalErr != nil {
			return "", "", "", fmt.Errorf("encode audit %s: %w", f.name, marshalErr)
		}
		*f.dst = string(raw)
	}
	return actor, subject, payload, nil
}

// ListAudit returns a tenant's chain in sequence order.
func (p *Postgres) ListAudit(ctx context.Context, tenantID string, limit int) ([]audit.Record, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := p.pool.Query(ctx, `
		SELECT id, seq, tenant_id, event_type, occurred_at, recorded_at,
		       actor, subject, payload, COALESCE(prev_hash, ''), hash
		FROM audit_records
		WHERE tenant_id = $1
		ORDER BY seq
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list audit records: %w", err)
	}
	defer rows.Close()
	return collectAudit(rows)
}

func collectAudit(rows pgx.Rows) ([]audit.Record, error) {
	var out []audit.Record
	for rows.Next() {
		var (
			r                             audit.Record
			rawActor, rawSubject, rawLoad []byte
		)
		if err := rows.Scan(&r.ID, &r.Seq, &r.TenantID, &r.EventType, &r.OccurredAt,
			&r.RecordedAt, &rawActor, &rawSubject, &rawLoad, &r.PrevHash, &r.Hash); err != nil {
			return nil, fmt.Errorf("scan audit record: %w", err)
		}
		for _, f := range []struct {
			raw []byte
			dst *map[string]any
		}{
			{rawActor, &r.Actor}, {rawSubject, &r.Subject}, {rawLoad, &r.Payload},
		} {
			if len(f.raw) == 0 {
				continue
			}
			if err := json.Unmarshal(f.raw, f.dst); err != nil {
				return nil, fmt.Errorf("decode audit record %d: %w", r.Seq, err)
			}
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit records: %w", err)
	}
	return out, nil
}
