package store

import (
	"context"
	"time"
)

type claimKey struct {
	tenantID      string
	transactionID string
}

func (m *Memory) ClaimReversal(_ context.Context, tenantID, transactionID, claimedBy, token string, now time.Time) (*ReversalClaim, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := claimKey{tenantID, transactionID}
	if existing, ok := m.claims[key]; ok {
		cp := *existing
		return &cp, false, nil
	}

	claim := &ReversalClaim{
		TenantID:      tenantID,
		TransactionID: transactionID,
		ClaimToken:    token,
		ClaimedBy:     claimedBy,
		ClaimedAt:     now.UTC(),
	}
	m.claims[key] = claim
	cp := *claim
	return &cp, true, nil
}

func (m *Memory) GetReversalClaim(_ context.Context, tenantID, transactionID string) (*ReversalClaim, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	claim, ok := m.claims[claimKey{tenantID, transactionID}]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *claim
	return &cp, nil
}

func (m *Memory) ReleaseReversalClaim(_ context.Context, tenantID, transactionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := claimKey{tenantID, transactionID}
	claim, ok := m.claims[key]
	// A confirmed claim is never released: the money has already moved, and
	// letting a second worker take it would move it again.
	if !ok || claim.ConfirmedAt != nil {
		return ErrNotFound
	}
	delete(m.claims, key)
	return nil
}

func (m *Memory) ConfirmReversalClaim(_ context.Context, tenantID, transactionID string, at time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	claim, ok := m.claims[claimKey{tenantID, transactionID}]
	if !ok {
		return ErrNotFound
	}
	// The first confirmation stands: a replay must not rewrite when the money
	// actually moved.
	if claim.ConfirmedAt == nil {
		t := at.UTC()
		claim.ConfirmedAt = &t
	}
	return nil
}
