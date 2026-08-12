package store

import (
	"context"
	"fmt"
	"sort"
	"time"
)

func (m *Memory) CreateEndpoint(_ context.Context, tenantID string, ep *WebhookEndpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ep.TenantID != tenantID {
		return ErrTenantMismatch
	}
	if _, exists := m.endpoints[ep.ID]; exists {
		return fmt.Errorf("store: endpoint %q already exists", ep.ID)
	}
	cp := *ep
	m.endpoints[ep.ID] = &cp
	return nil
}

func (m *Memory) SetEndpointEnabled(_ context.Context, tenantID, id string, enabled bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ep, ok := m.endpoints[id]
	if !ok || ep.TenantID != tenantID {
		return ErrNotFound
	}
	ep.Enabled = enabled
	return nil
}

func (m *Memory) DeleteEndpoint(_ context.Context, tenantID, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ep, ok := m.endpoints[id]
	if !ok || ep.TenantID != tenantID {
		return ErrNotFound
	}
	delete(m.endpoints, id)
	return nil
}

func (m *Memory) ListEndpoints(_ context.Context, tenantID string) ([]*WebhookEndpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*WebhookEndpoint
	for _, ep := range m.endpoints {
		if ep.TenantID == tenantID {
			cp := *ep
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *Memory) EnqueueDelivery(_ context.Context, tenantID string, d *PendingDelivery) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if d.TenantID != tenantID {
		return 0, ErrTenantMismatch
	}

	m.nextDeliveryID++
	now := time.Now().UTC()
	m.deliveries[m.nextDeliveryID] = &DeliveryRecord{
		ID:            m.nextDeliveryID,
		TenantID:      tenantID,
		EndpointID:    d.EndpointID,
		TransactionID: d.TransactionID,
		EventType:     d.EventType,
		Attempt:       0,
		Status:        "pending",
		NextRetryAt:   &now,
		CreatedAt:     now,
	}
	m.payloads[m.nextDeliveryID] = append([]byte(nil), d.Payload...)
	return m.nextDeliveryID, nil
}

func (m *Memory) ClaimDueDeliveries(_ context.Context, now time.Time, lease time.Duration, limit int) ([]*DueDelivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if limit <= 0 {
		limit = 100
	}
	if lease <= 0 {
		lease = time.Minute
	}

	var due []*DeliveryRecord
	for _, d := range m.deliveries {
		if d.Status != "pending" || d.NextRetryAt == nil || d.NextRetryAt.After(now) {
			continue
		}
		ep, ok := m.endpoints[d.EndpointID]
		if !ok || !ep.Enabled {
			continue
		}
		due = append(due, d)
	}
	sort.Slice(due, func(i, j int) bool { return due[i].NextRetryAt.Before(*due[j].NextRetryAt) })
	if len(due) > limit {
		due = due[:limit]
	}

	out := make([]*DueDelivery, 0, len(due))
	for _, d := range due {
		// Lease it forward so a concurrent claim skips it.
		leased := now.Add(lease)
		d.NextRetryAt = &leased

		ep := m.endpoints[d.EndpointID]
		out = append(out, &DueDelivery{
			ID:            d.ID,
			TenantID:      d.TenantID,
			EndpointID:    d.EndpointID,
			TransactionID: d.TransactionID,
			EventType:     d.EventType,
			Payload:       append([]byte(nil), m.payloads[d.ID]...),
			Attempt:       d.Attempt,
			URL:           ep.URL,
			SecretRef:     ep.SecretRef,
		})
	}
	return out, nil
}

func (m *Memory) RecordDeliveryOutcome(_ context.Context, id int64, out DeliveryOutcome) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	d, ok := m.deliveries[id]
	if !ok {
		return ErrNotFound
	}

	body := out.ResponseBody
	if len(body) > maxStoredResponseBody {
		body = body[:maxStoredResponseBody]
	}

	d.Attempt++
	d.Status = out.Status
	d.ResponseCode = out.ResponseCode
	d.ResponseBody = body
	d.DurationMS = &out.DurationMS
	d.NextRetryAt = out.NextRetryAt
	return nil
}

func (m *Memory) ListDeliveries(_ context.Context, tenantID, status string, limit int) ([]*DeliveryRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		limit = 100
	}

	var out []*DeliveryRecord
	for _, d := range m.deliveries {
		if d.TenantID != tenantID {
			continue
		}
		if status != "" && d.Status != status {
			continue
		}
		cp := *d
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Memory) ReplayDelivery(_ context.Context, tenantID string, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	d, ok := m.deliveries[id]
	if !ok || d.TenantID != tenantID {
		return ErrNotFound
	}
	if d.Status != "dead_letter" && d.Status != "failed" {
		return ErrNotFound
	}

	now := time.Now().UTC()
	d.Status = "pending"
	d.Attempt = 0
	d.NextRetryAt = &now
	return nil
}
