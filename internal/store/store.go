// Package store is the persistence port and its implementations.
//
// Every tenant-scoped method takes tenantID as its first argument. That is
// layer 2 of the §8.1 tenancy model: the interface makes a forgotten scope a
// compile error rather than a data leak.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

var (
	// ErrNotFound means no transaction matched within the tenant. Callers must
	// map this to 404, never 403 — a 403 confirms the row exists (§8.1).
	ErrNotFound = errors.New("store: transaction not found")

	// ErrTenantMismatch means a record's tenant disagreed with the scope it was
	// written under. Always a bug, never client input.
	ErrTenantMismatch = errors.New("store: record tenant does not match requested tenant")
)

// UpsertResult reports which debits were stored and which were already present.
type UpsertResult struct {
	Inserted   []string // transaction_ids newly stored
	Duplicates []string // suppressed by idempotency key
}

// TransactionStore persists tracked transactions.
type TransactionStore interface {
	// UpsertDebits stores debit legs idempotently on (tenant_id, idempotency_key).
	// Re-sending an event is normal client behaviour, not an error.
	UpsertDebits(ctx context.Context, tenantID string, txns []*domain.Transaction) (UpsertResult, error)

	// ApplyCredit moves a transaction to target, refusing the write if the state
	// machine forbids it. The guard is applied in the same statement as the
	// update so a credit racing detection cannot overwrite it.
	// Returns ErrNotFound, or domain.InvalidTransitionError if the edge is illegal.
	ApplyCredit(ctx context.Context, tenantID, transactionID string, target domain.Status, creditAt time.Time) (*domain.Transaction, error)

	// Get returns one transaction scoped to the tenant.
	Get(ctx context.Context, tenantID, transactionID string) (*domain.Transaction, error)

	// ListByStatus returns a tenant's transactions in a given state, newest first.
	ListByStatus(ctx context.Context, tenantID string, status domain.Status, limit int) ([]*domain.Transaction, error)

	// ClaimExpired atomically marks open transactions whose window has closed as
	// orphaned and returns them.
	//
	// Deliberately not tenant-scoped: the scheduler sweeps all tenants, and
	// per-tenant polling would not scale. It is the single exception to the
	// tenantID-first rule and never runs on a request path.
	ClaimExpired(ctx context.Context, now time.Time, limit int) ([]*domain.Transaction, error)
}

// TenantStore manages tenant records. Admin-plane only.
type TenantStore interface {
	EnsureTenant(ctx context.Context, id, name, environment string) error
}

// Store is the full persistence surface.
type Store interface {
	TransactionStore
	TenantStore
}
