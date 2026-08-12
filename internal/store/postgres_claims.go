package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ClaimReversal grants the claim to whoever gets there first.
//
// The interlock is the primary key, not application logic: INSERT ... ON
// CONFLICT DO NOTHING means exactly one caller can win, across any number of
// their workers and any number of our replicas. A check-then-insert would race.
func (p *Postgres) ClaimReversal(ctx context.Context, tenantID, transactionID, claimedBy, token string, now time.Time) (*ReversalClaim, bool, error) {
	claim := &ReversalClaim{
		TenantID:      tenantID,
		TransactionID: transactionID,
		ClaimToken:    token,
		ClaimedBy:     claimedBy,
		ClaimedAt:     now.UTC(),
	}

	err := p.pool.QueryRow(ctx, `
		INSERT INTO reversal_claims (tenant_id, transaction_id, claim_token, claimed_by, claimed_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, transaction_id) DO NOTHING
		RETURNING claim_token, claimed_by, claimed_at, confirmed_at`,
		tenantID, transactionID, token, claimedBy, now.UTC()).
		Scan(&claim.ClaimToken, &claim.ClaimedBy, &claim.ClaimedAt, &claim.ConfirmedAt)

	switch {
	case err == nil:
		// Postgres returns timestamptz in the connection's zone; every timestamp
		// this API emits is UTC.
		claim.ClaimedAt = claim.ClaimedAt.UTC()
		return claim, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// Someone holds it. Report who, so "which worker already has this" is
		// answerable during an incident.
		existing, err := p.GetReversalClaim(ctx, tenantID, transactionID)
		if err != nil {
			return nil, false, err
		}
		return existing, false, nil
	default:
		return nil, false, fmt.Errorf("claim reversal: %w", err)
	}
}

func (p *Postgres) GetReversalClaim(ctx context.Context, tenantID, transactionID string) (*ReversalClaim, error) {
	claim := &ReversalClaim{TenantID: tenantID, TransactionID: transactionID}
	err := p.pool.QueryRow(ctx, `
		SELECT claim_token, claimed_by, claimed_at, confirmed_at
		FROM reversal_claims
		WHERE tenant_id = $1 AND transaction_id = $2`, tenantID, transactionID).
		Scan(&claim.ClaimToken, &claim.ClaimedBy, &claim.ClaimedAt, &claim.ConfirmedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get reversal claim: %w", err)
	}
	claim.ClaimedAt = claim.ClaimedAt.UTC()
	if claim.ConfirmedAt != nil {
		utc := claim.ConfirmedAt.UTC()
		claim.ConfirmedAt = &utc
	}
	return claim, nil
}

// ReleaseReversalClaim drops an unconfirmed claim. The confirmed_at guard is the
// whole safety property: releasing a claim whose money already moved would let a
// second worker move it again.
func (p *Postgres) ReleaseReversalClaim(ctx context.Context, tenantID, transactionID string) error {
	tag, err := p.pool.Exec(ctx, `
		DELETE FROM reversal_claims
		WHERE tenant_id = $1 AND transaction_id = $2 AND confirmed_at IS NULL`,
		tenantID, transactionID)
	if err != nil {
		return fmt.Errorf("release reversal claim: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) ConfirmReversalClaim(ctx context.Context, tenantID, transactionID string, at time.Time) error {
	// COALESCE keeps the first confirmation: a replayed confirmation must not
	// rewrite when the money actually moved.
	tag, err := p.pool.Exec(ctx, `
		UPDATE reversal_claims
		SET confirmed_at = COALESCE(confirmed_at, $3)
		WHERE tenant_id = $1 AND transaction_id = $2`,
		tenantID, transactionID, at.UTC())
	if err != nil {
		return fmt.Errorf("confirm reversal claim: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
