package store

import (
	"context"
	"fmt"
	"time"
)

func (p *Postgres) CreateEndpoint(ctx context.Context, tenantID string, ep *WebhookEndpoint) error {
	if ep.TenantID != tenantID {
		return ErrTenantMismatch
	}
	events := ep.Events
	if events == nil {
		events = []string{}
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO webhook_endpoints (id, tenant_id, url, secret_ref, events, enabled)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		ep.ID, tenantID, ep.URL, ep.SecretRef, events, ep.Enabled)
	if err != nil {
		return fmt.Errorf("create endpoint: %w", err)
	}
	return nil
}

func (p *Postgres) ListEndpoints(ctx context.Context, tenantID string) ([]*WebhookEndpoint, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT id, tenant_id, url, secret_ref, events, enabled
		 FROM webhook_endpoints WHERE tenant_id = $1 ORDER BY created_at`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	defer rows.Close()

	var out []*WebhookEndpoint
	for rows.Next() {
		var ep WebhookEndpoint
		if err := rows.Scan(&ep.ID, &ep.TenantID, &ep.URL, &ep.SecretRef, &ep.Events, &ep.Enabled); err != nil {
			return nil, fmt.Errorf("scan endpoint: %w", err)
		}
		out = append(out, &ep)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate endpoints: %w", err)
	}
	return out, nil
}

func (p *Postgres) EnqueueDelivery(ctx context.Context, tenantID string, d *PendingDelivery) (int64, error) {
	if d.TenantID != tenantID {
		return 0, ErrTenantMismatch
	}

	var id int64
	err := p.pool.QueryRow(ctx, `
		INSERT INTO webhook_deliveries (
			tenant_id, endpoint_id, transaction_id, event_type, payload,
			attempt, status, next_retry_at)
		VALUES ($1, $2, $3, $4, $5, 0, 'pending', now())
		RETURNING id`,
		tenantID, d.EndpointID, d.TransactionID, d.EventType, d.Payload).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("enqueue delivery: %w", err)
	}
	return id, nil
}

// ClaimDueDeliveries leases due deliveries across all tenants.
//
// The lease is written as a future next_retry_at rather than a separate
// "sending" state: a worker that dies mid-attempt simply becomes claimable again
// when the lease lapses, with no stuck rows to reap.
func (p *Postgres) ClaimDueDeliveries(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]*DueDelivery, error) {
	if limit <= 0 {
		limit = 100
	}
	if lease <= 0 {
		lease = time.Minute
	}

	rows, err := p.pool.Query(ctx, `
		UPDATE webhook_deliveries d
		SET next_retry_at = $1::timestamptz + $2::interval
		FROM webhook_endpoints e
		WHERE d.endpoint_id = e.id
		  AND e.enabled
		  AND d.id IN (
			SELECT id FROM webhook_deliveries
			WHERE status = 'pending' AND next_retry_at <= $1
			ORDER BY next_retry_at
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		  )
		RETURNING d.id, d.tenant_id, d.endpoint_id, d.transaction_id, d.event_type,
		          d.payload, d.attempt, e.url, e.secret_ref`,
		now, lease, limit)
	if err != nil {
		return nil, fmt.Errorf("claim deliveries: %w", err)
	}
	defer rows.Close()

	var out []*DueDelivery
	for rows.Next() {
		var d DueDelivery
		if err := rows.Scan(&d.ID, &d.TenantID, &d.EndpointID, &d.TransactionID,
			&d.EventType, &d.Payload, &d.Attempt, &d.URL, &d.SecretRef); err != nil {
			return nil, fmt.Errorf("scan due delivery: %w", err)
		}
		out = append(out, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due deliveries: %w", err)
	}
	return out, nil
}

func (p *Postgres) RecordDeliveryOutcome(ctx context.Context, id int64, out DeliveryOutcome) error {
	body := out.ResponseBody
	if len(body) > maxStoredResponseBody {
		body = body[:maxStoredResponseBody]
	}

	tag, err := p.pool.Exec(ctx, `
		UPDATE webhook_deliveries
		SET attempt = attempt + 1,
		    status = $2,
		    response_code = $3,
		    response_body = $4,
		    duration_ms = $5,
		    next_retry_at = $6
		WHERE id = $1`,
		id, out.Status, out.ResponseCode, body, out.DurationMS, out.NextRetryAt)
	if err != nil {
		return fmt.Errorf("record delivery outcome: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// maxStoredResponseBody bounds what a customer's error page can put in our
// database (§5: response_body truncated to 1KB).
const maxStoredResponseBody = 1 << 10

func (p *Postgres) ListDeliveries(ctx context.Context, tenantID, status string, limit int) ([]*DeliveryRecord, error) {
	if limit <= 0 {
		limit = 100
	}

	// An empty status means "any", so the dashboard can show the whole log.
	rows, err := p.pool.Query(ctx, `
		SELECT id, tenant_id, endpoint_id, transaction_id, event_type, attempt,
		       status, response_code, response_body, duration_ms, next_retry_at, created_at
		FROM webhook_deliveries
		WHERE tenant_id = $1 AND ($2 = '' OR status = $2)
		ORDER BY created_at DESC, id DESC
		LIMIT $3`, tenantID, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()

	var out []*DeliveryRecord
	for rows.Next() {
		var d DeliveryRecord
		if err := rows.Scan(&d.ID, &d.TenantID, &d.EndpointID, &d.TransactionID, &d.EventType,
			&d.Attempt, &d.Status, &d.ResponseCode, &d.ResponseBody, &d.DurationMS,
			&d.NextRetryAt, &d.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan delivery: %w", err)
		}
		out = append(out, &d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate deliveries: %w", err)
	}
	return out, nil
}

// ReplayDelivery puts a dead-lettered delivery back on the queue with its
// attempt count reset (§11.5 step 5). Replay is idempotent by design, so
// over-replaying is safe.
func (p *Postgres) ReplayDelivery(ctx context.Context, tenantID string, id int64) error {
	tag, err := p.pool.Exec(ctx, `
		UPDATE webhook_deliveries
		SET status = 'pending', attempt = 0, next_retry_at = now()
		WHERE tenant_id = $1 AND id = $2 AND status IN ('dead_letter', 'failed')`,
		tenantID, id)
	if err != nil {
		return fmt.Errorf("replay delivery: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
