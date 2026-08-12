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
