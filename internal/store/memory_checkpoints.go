package store

import (
	"context"
	"sort"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
)

func (m *Memory) SaveCheckpoint(_ context.Context, c audit.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, existing := range m.checkpoints[c.TenantID] {
		if existing.Seq == c.Seq {
			return nil // re-signing the same head changes nothing
		}
	}
	c.TakenAt = c.TakenAt.UTC()
	m.checkpoints[c.TenantID] = append(m.checkpoints[c.TenantID], c)
	return nil
}

func (m *Memory) LatestCheckpoint(_ context.Context, tenantID string) (*audit.Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var latest *audit.Checkpoint
	for i, c := range m.checkpoints[tenantID] {
		if latest == nil || c.Seq > latest.Seq {
			latest = &m.checkpoints[tenantID][i]
		}
	}
	if latest == nil {
		return nil, ErrNotFound
	}
	cp := *latest
	return &cp, nil
}

func (m *Memory) ListCheckpoints(_ context.Context, tenantID string, limit int) ([]audit.Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	out := append([]audit.Checkpoint(nil), m.checkpoints[tenantID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Seq > out[j].Seq })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Memory) TenantsWithAudit(_ context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]string, 0, len(m.audit))
	for tenantID, records := range m.audit {
		if len(records) > 0 {
			out = append(out, tenantID)
		}
	}
	sort.Strings(out)
	return out, nil
}
