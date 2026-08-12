package store

import (
	"context"
	"fmt"
	"time"
)

func (p *Postgres) RecordIngestHealth(ctx context.Context, samples []IngestSample) error {
	if len(samples) == 0 {
		return nil
	}

	n := len(samples)
	var (
		tenants = make([]string, 0, n)
		buckets = make([]time.Time, 0, n)
		recv    = make([]int64, 0, n)
		dropped = make([]int64, 0, n)
		errs    = make([]int64, 0, n)
	)
	for _, s := range samples {
		tenants = append(tenants, s.TenantID)
		buckets = append(buckets, s.Bucket.UTC().Truncate(time.Minute))
		recv = append(recv, s.Received)
		dropped = append(dropped, s.Dropped)
		errs = append(errs, s.HandlerErrors)
	}

	// Counts accumulate: a flush carries the delta since the last one, and two
	// replicas writing the same minute must sum rather than overwrite.
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ingest_health (tenant_id, bucket, received, dropped, handler_errors)
		SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::bigint[], $4::bigint[], $5::bigint[])
		ON CONFLICT (tenant_id, bucket) DO UPDATE SET
			received       = ingest_health.received + EXCLUDED.received,
			dropped        = ingest_health.dropped + EXCLUDED.dropped,
			handler_errors = ingest_health.handler_errors + EXCLUDED.handler_errors,
			updated_at     = now()`,
		tenants, buckets, recv, dropped, errs)
	if err != nil {
		return fmt.Errorf("record ingest health: %w", err)
	}
	return nil
}

// HasIngestGap reports whether anything was lost for this tenant between two
// instants. Bucket granularity is a minute, so the range is widened to whole
// minutes — a gap that overlaps the window at all makes it unreliable.
func (p *Postgres) HasIngestGap(ctx context.Context, tenantID string, from, to time.Time) (bool, error) {
	var exists bool
	err := p.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM ingest_health
			WHERE tenant_id = $1
			  AND bucket >= date_trunc('minute', $2::timestamptz)
			  AND bucket <= date_trunc('minute', $3::timestamptz)
			  AND (dropped > 0 OR handler_errors > 0)
		)`, tenantID, from.UTC(), to.UTC()).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check ingest gap: %w", err)
	}
	return exists, nil
}

// SilentTenants finds tenants that were sending steadily and have stopped.
//
// The baseline requirement is what stops a low-volume tenant being mistaken for
// a broken one: a tenant that sends four events a day is not "silent" between
// them, and suppressing their detection would be a bug, not a safeguard.
func (p *Postgres) SilentTenants(ctx context.Context, now time.Time, params SilenceParams) ([]string, error) {
	if params.Quiet <= 0 || params.Baseline <= 0 || params.MinActiveBuckets <= 0 {
		return nil, nil
	}

	quietFrom := now.UTC().Add(-params.Quiet)
	baselineFrom := quietFrom.Add(-params.Baseline)

	rows, err := p.pool.Query(ctx, `
		SELECT b.tenant_id
		FROM ingest_health b
		WHERE b.bucket >= date_trunc('minute', $1::timestamptz)
		  AND b.bucket <  date_trunc('minute', $2::timestamptz)
		GROUP BY b.tenant_id
		HAVING COUNT(*) FILTER (WHERE b.received > 0) >= $3
		   AND NOT EXISTS (
		       SELECT 1 FROM ingest_health q
		       WHERE q.tenant_id = b.tenant_id
		         AND q.bucket >= date_trunc('minute', $2::timestamptz)
		         AND q.received > 0
		   )`,
		baselineFrom, quietFrom, params.MinActiveBuckets)
	if err != nil {
		return nil, fmt.Errorf("find silent tenants: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan silent tenant: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate silent tenants: %w", err)
	}
	return out, nil
}

// IngestActivity summarises what a tenant sent over a period, for the silence
// check.
func (p *Postgres) IngestActivity(ctx context.Context, tenantID string, from, to time.Time) (IngestActivitySummary, error) {
	var s IngestActivitySummary
	err := p.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(received), 0), COUNT(*) FILTER (WHERE received > 0)
		FROM ingest_health
		WHERE tenant_id = $1
		  AND bucket >= date_trunc('minute', $2::timestamptz)
		  AND bucket <= date_trunc('minute', $3::timestamptz)`,
		tenantID, from.UTC(), to.UTC()).Scan(&s.Received, &s.ActiveBuckets)
	if err != nil {
		return s, fmt.Errorf("read ingest activity: %w", err)
	}
	return s, nil
}
