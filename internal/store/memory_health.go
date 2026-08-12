package store

import (
	"context"
	"time"
)

type healthKey struct {
	tenantID string
	bucket   time.Time
}

func (m *Memory) RecordIngestHealth(_ context.Context, samples []IngestSample) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range samples {
		key := healthKey{tenantID: s.TenantID, bucket: s.Bucket.UTC().Truncate(time.Minute)}
		cur := m.health[key]
		cur.Received += s.Received
		cur.Dropped += s.Dropped
		cur.HandlerErrors += s.HandlerErrors
		m.health[key] = cur
	}
	return nil
}

func (m *Memory) HasIngestGap(_ context.Context, tenantID string, from, to time.Time) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hasGapLocked(tenantID, from, to), nil
}

// hasGapLocked is the lock-free body, callable from ClaimExpired which already
// holds the write lock.
func (m *Memory) hasGapLocked(tenantID string, from, to time.Time) bool {
	lo := from.UTC().Truncate(time.Minute)
	hi := to.UTC().Truncate(time.Minute)

	for key, v := range m.health {
		if key.tenantID != tenantID {
			continue
		}
		if key.bucket.Before(lo) || key.bucket.After(hi) {
			continue
		}
		if v.Dropped > 0 || v.HandlerErrors > 0 {
			return true
		}
	}
	return false
}

func (m *Memory) IngestActivity(_ context.Context, tenantID string, from, to time.Time) (IngestActivitySummary, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	lo := from.UTC().Truncate(time.Minute)
	hi := to.UTC().Truncate(time.Minute)

	var s IngestActivitySummary
	for key, v := range m.health {
		if key.tenantID != tenantID {
			continue
		}
		if key.bucket.Before(lo) || key.bucket.After(hi) {
			continue
		}
		s.Received += v.Received
		if v.Received > 0 {
			s.ActiveBuckets++
		}
	}
	return s, nil
}
