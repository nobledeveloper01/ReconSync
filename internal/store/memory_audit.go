package store

import (
	"context"
	"errors"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
)

func (m *Memory) AppendAudit(_ context.Context, tenantID string, r *audit.Record) (*audit.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tenantID == "" {
		return nil, errors.New("store: audit record needs a tenant")
	}

	r.TenantID = tenantID
	if r.OccurredAt.IsZero() {
		r.OccurredAt = time.Now().UTC()
	}

	chain := m.audit[tenantID]
	r.Seq = int64(len(chain)) + 1
	r.PrevHash = ""
	if len(chain) > 0 {
		r.PrevHash = chain[len(chain)-1].Hash
	}

	hash, err := audit.ComputeHash(*r, r.PrevHash)
	if err != nil {
		return nil, err
	}
	r.Hash = hash
	r.ID = r.Seq
	r.RecordedAt = time.Now().UTC()

	m.audit[tenantID] = append(chain, *r)
	return r, nil
}

func (m *Memory) ListAudit(_ context.Context, tenantID string, limit int) ([]audit.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		limit = 1000
	}
	chain := m.audit[tenantID]
	if len(chain) > limit {
		chain = chain[:limit]
	}

	out := make([]audit.Record, len(chain))
	copy(out, chain)
	return out, nil
}
