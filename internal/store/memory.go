package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
)

// Memory is an in-memory Store for unit tests and local runs. It is held to the
// same conformance suite as Postgres so the two cannot drift.
type Memory struct {
	mu      sync.RWMutex
	tenants map[string]struct{}

	// byTenant[tenantID][transactionID]
	byTenant map[string]map[string]*domain.Transaction
	// idem[tenantID][idempotencyKey]
	idem map[string]map[string]struct{}

	// parked[tenantID][transactionID] — credits awaiting their debit
	parked map[string]map[string]*domain.CreditEvent

	// apiKeys by prefix
	apiKeys map[string]*auth.Record

	// webhook endpoints by id, and the delivery queue by id
	endpoints  map[string]*WebhookEndpoint
	deliveries map[int64]*DeliveryRecord
	payloads   map[int64][]byte

	// reconciliation rules by tenant
	rules map[string][]rules.Rule

	// ingest counters by tenant and minute
	health map[healthKey]IngestSample

	// per-tenant audit chain, in sequence order
	audit map[string][]audit.Record

	nextID         int64
	nextDeliveryID int64
	nextRuleID     int64
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{
		tenants:  make(map[string]struct{}),
		byTenant: make(map[string]map[string]*domain.Transaction),
		idem:     make(map[string]map[string]struct{}),
		parked:   make(map[string]map[string]*domain.CreditEvent),
		apiKeys:  make(map[string]*auth.Record),

		endpoints:  make(map[string]*WebhookEndpoint),
		deliveries: make(map[int64]*DeliveryRecord),
		payloads:   make(map[int64][]byte),
		rules:      make(map[string][]rules.Rule),
		health:     make(map[healthKey]IngestSample),
		audit:      make(map[string][]audit.Record),
	}
}

var _ Store = (*Memory)(nil)

func (m *Memory) EnsureTenant(_ context.Context, id, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tenants[id] = struct{}{}
	return nil
}

func (m *Memory) UpsertDebits(_ context.Context, tenantID string, txns []*domain.Transaction) (UpsertResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var res UpsertResult
	for _, t := range txns {
		if t.TenantID != tenantID {
			return UpsertResult{}, ErrTenantMismatch
		}
	}

	if m.byTenant[tenantID] == nil {
		m.byTenant[tenantID] = make(map[string]*domain.Transaction)
		m.idem[tenantID] = make(map[string]struct{})
	}

	for _, t := range txns {
		_, dupKey := m.idem[tenantID][t.IdempotencyKey]
		_, dupTxn := m.byTenant[tenantID][t.TransactionID]
		if dupKey || dupTxn {
			res.Duplicates = append(res.Duplicates, t.TransactionID)
			continue
		}

		m.nextID++
		stored := *t // copy: callers must not be able to mutate stored state
		stored.ID = m.nextID
		stored.Status = domain.StatusPendingDebit
		now := time.Now().UTC()
		stored.CreatedAt, stored.UpdatedAt = now, now

		m.byTenant[tenantID][t.TransactionID] = &stored
		m.idem[tenantID][t.IdempotencyKey] = struct{}{}
		res.Inserted = append(res.Inserted, t.TransactionID)
	}
	return res, nil
}

func (m *Memory) ApplyCredit(_ context.Context, tenantID, transactionID string, target domain.Status, creditAt time.Time) (*domain.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stored, ok := m.byTenant[tenantID][transactionID]
	if !ok {
		return nil, ErrNotFound
	}
	if err := domain.Transition(stored.Status, target); err != nil {
		return nil, err
	}

	stored.Status = target
	at := creditAt.UTC()
	stored.CreditAt = &at
	stored.UpdatedAt = time.Now().UTC()
	if target == domain.StatusOrphaned {
		detected := stored.UpdatedAt
		stored.DetectedAt = &detected
	}

	out := *stored
	return &out, nil
}

func (m *Memory) MarkSettled(_ context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	return m.markReversal(tenantID, transactionID, domain.StatusCompleted, at)
}

func (m *Memory) MarkUncertain(_ context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	return m.markReversal(tenantID, transactionID, domain.StatusSuspect, at)
}

func (m *Memory) MarkReversalPending(_ context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	return m.markReversal(tenantID, transactionID, domain.StatusReversalPending, at)
}

func (m *Memory) MarkReversalCompleted(_ context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	return m.markReversal(tenantID, transactionID, domain.StatusReversalCompleted, at)
}

func (m *Memory) MarkReversalFailed(_ context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	return m.markReversal(tenantID, transactionID, domain.StatusReversalFailed, at)
}

func (m *Memory) markReversal(tenantID, transactionID string, target domain.Status, at time.Time) (*domain.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stored, ok := m.byTenant[tenantID][transactionID]
	if !ok {
		return nil, ErrNotFound
	}
	if err := domain.Transition(stored.Status, target); err != nil {
		return nil, err
	}

	stored.Status = target
	stamp := at.UTC()
	switch target {
	case domain.StatusReversalPending:
		stored.ReversalTriggeredAt = &stamp
	case domain.StatusReversalCompleted:
		stored.ReversalCompletedAt = &stamp
	case domain.StatusCompleted:
		// The rail confirmed arrival; record when, without overwriting a real
		// credit timestamp if one already landed.
		if stored.CreditAt == nil {
			stored.CreditAt = &stamp
		}
	}
	stored.UpdatedAt = time.Now().UTC()

	out := *stored
	return &out, nil
}

func (m *Memory) CreateAPIKey(_ context.Context, tenantID, keyID string, key auth.Key, scopes []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.apiKeys[key.Prefix]; exists {
		return fmt.Errorf("store: api key prefix %q already exists", key.Prefix)
	}
	env, _ := auth.EnvironmentOf(key.Secret)
	m.apiKeys[key.Prefix] = &auth.Record{
		ID:          keyID,
		TenantID:    tenantID,
		Prefix:      key.Prefix,
		Hash:        key.Hash,
		Scopes:      scopes,
		Environment: env,
	}
	return nil
}

func (m *Memory) APIKeyByPrefix(_ context.Context, prefix string) (*auth.Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rec, ok := m.apiKeys[prefix]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *rec
	return &cp, nil
}

func (m *Memory) TouchAPIKey(_ context.Context, _ string) error { return nil }

func (m *Memory) RevokeAPIKey(_ context.Context, tenantID, keyID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, rec := range m.apiKeys {
		// Already-revoked keys are skipped, so a second revoke reports not-found
		// rather than looking like a fresh one.
		if rec.ID == keyID && rec.TenantID == tenantID && rec.RevokedAt == nil {
			now := time.Now().UTC()
			rec.RevokedAt = &now
			return nil
		}
	}
	return ErrNotFound
}

func (m *Memory) ParkCredit(_ context.Context, tenantID string, ev *domain.CreditEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ev.TenantID != tenantID {
		return ErrTenantMismatch
	}
	if m.parked[tenantID] == nil {
		m.parked[tenantID] = make(map[string]*domain.CreditEvent)
	}
	if _, exists := m.parked[tenantID][ev.TransactionID]; exists {
		return nil // first verdict wins
	}
	cp := *ev
	m.parked[tenantID][ev.TransactionID] = &cp
	return nil
}

func (m *Memory) PeekParkedCredits(_ context.Context, tenantID string, transactionIDs []string) ([]*domain.CreditEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*domain.CreditEvent
	for _, id := range transactionIDs {
		ev, ok := m.parked[tenantID][id]
		if !ok {
			continue
		}
		cp := *ev
		out = append(out, &cp)
	}
	return out, nil
}

func (m *Memory) DeleteParkedCredit(_ context.Context, tenantID, transactionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.parked[tenantID], transactionID)
	return nil
}

func (m *Memory) Get(_ context.Context, tenantID, transactionID string) (*domain.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stored, ok := m.byTenant[tenantID][transactionID]
	if !ok {
		return nil, ErrNotFound
	}
	out := *stored
	return &out, nil
}

func (m *Memory) ListByStatus(_ context.Context, tenantID string, status domain.Status, limit int) ([]*domain.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []*domain.Transaction
	for _, t := range m.byTenant[tenantID] {
		if t.Status == status {
			cp := *t
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DebitAt.After(out[j].DebitAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Memory) ClaimExpired(_ context.Context, now time.Time, limit int, opts ...ClaimOption) ([]*domain.Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	skip := make(map[string]struct{})
	for _, id := range ResolveClaimOptions(opts).SkipTenants {
		skip[id] = struct{}{}
	}

	var due []*domain.Transaction
	for tenantID, txns := range m.byTenant {
		if _, skipped := skip[tenantID]; skipped {
			continue
		}
		for _, t := range txns {
			if t.IsExpiredAt(now) {
				due = append(due, t)
			}
		}
	}
	// Oldest deadline first, matching the scheduler's ORDER BY.
	sort.Slice(due, func(i, j int) bool {
		return due[i].ExpectedCompletionAt.Before(due[j].ExpectedCompletionAt)
	})
	if limit > 0 && len(due) > limit {
		due = due[:limit]
	}

	out := make([]*domain.Transaction, 0, len(due))
	for _, t := range due {
		target := domain.StatusOrphaned
		if t.Status == domain.StatusPendingUnknown {
			target = domain.StatusSuspect
		}
		// An ingest gap over this window means the missing credit proves
		// nothing, so it must not auto-reverse (ADR-0004).
		if m.hasGapLocked(t.TenantID, t.DebitAt, t.ExpectedCompletionAt) {
			target = domain.StatusSuspect
		}
		t.Status = target
		detected := now.UTC()
		t.DetectedAt = &detected
		t.UpdatedAt = detected
		cp := *t
		out = append(out, &cp)
	}
	return out, nil
}
