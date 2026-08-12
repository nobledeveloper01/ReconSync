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

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
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

	// MarkReversalPending records that the reversal webhook has been dispatched.
	MarkReversalPending(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error)

	// MarkReversalCompleted records the customer confirming a reversal (§3.2 C2).
	MarkReversalCompleted(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error)

	// MarkReversalFailed records that delivery exhausted its retries and a human
	// is now on the hook.
	MarkReversalFailed(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error)

	// Get returns one transaction scoped to the tenant.
	Get(ctx context.Context, tenantID, transactionID string) (*domain.Transaction, error)

	// ListByStatus returns a tenant's transactions in a given state, newest first.
	ListByStatus(ctx context.Context, tenantID string, status domain.Status, limit int) ([]*domain.Transaction, error)

	// ParkCredit stores a credit whose debit has not arrived yet (§3.2 A2).
	// Idempotent per transaction: the first verdict wins, so a duplicate cannot
	// overwrite it.
	ParkCredit(ctx context.Context, tenantID string, ev *domain.CreditEvent) error

	// PeekParkedCredits returns parked credits without removing them.
	//
	// Reading and removing are deliberately separate. If a read removed the
	// credit, a failed apply would drop it permanently — the debit's own sweep
	// may already have run, so nobody would ever apply it and the transaction
	// would be reversed despite its credit having succeeded.
	PeekParkedCredits(ctx context.Context, tenantID string, transactionIDs []string) ([]*domain.CreditEvent, error)

	// DeleteParkedCredit removes a parked credit once it has been resolved.
	// Idempotent: deleting an absent credit is not an error, so two workers
	// racing the same credit both succeed.
	DeleteParkedCredit(ctx context.Context, tenantID, transactionID string) error

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

// APIKeyStore persists API keys and satisfies auth.Lookup.
//
// APIKeyByPrefix is the second documented exception to the tenantID-first rule:
// it is what resolves the tenant, so it cannot already be scoped to one.
type APIKeyStore interface {
	CreateAPIKey(ctx context.Context, tenantID, keyID string, key auth.Key, scopes []string) error
	APIKeyByPrefix(ctx context.Context, prefix string) (*auth.Record, error)
	TouchAPIKey(ctx context.Context, keyID string) error
	RevokeAPIKey(ctx context.Context, tenantID, keyID string) error
}

// WebhookEndpoint is a registered destination for a tenant's events.
type WebhookEndpoint struct {
	ID        string
	TenantID  string
	URL       string
	SecretRef string // KMS reference, never the secret itself
	Events    []string
	Enabled   bool
}

// PendingDelivery is a webhook queued for its first attempt.
type PendingDelivery struct {
	TenantID      string
	EndpointID    string
	TransactionID string
	EventType     string
	Payload       []byte
}

// DueDelivery is a claimed delivery, joined to the endpoint that receives it.
// The secret is referenced, not carried: resolving it is the dispatcher's job.
type DueDelivery struct {
	ID            int64
	TenantID      string
	EndpointID    string
	TransactionID string
	EventType     string
	Payload       []byte
	Attempt       int
	URL           string
	SecretRef     string
}

// DeliveryRecord is the delivery log entry shown in the dashboard.
type DeliveryRecord struct {
	ID            int64
	TenantID      string
	EndpointID    string
	TransactionID string
	EventType     string
	Attempt       int
	Status        string
	ResponseCode  *int
	ResponseBody  string
	DurationMS    *int
	NextRetryAt   *time.Time
	CreatedAt     time.Time
}

// DeliveryOutcome is what an attempt decided, written back to the queue.
type DeliveryOutcome struct {
	Status       string
	ResponseCode *int
	ResponseBody string
	DurationMS   int
	NextRetryAt  *time.Time
}

// WebhookStore persists endpoints and the delivery queue.
type WebhookStore interface {
	CreateEndpoint(ctx context.Context, tenantID string, ep *WebhookEndpoint) error
	ListEndpoints(ctx context.Context, tenantID string) ([]*WebhookEndpoint, error)

	// EnqueueDelivery queues a webhook for immediate delivery.
	EnqueueDelivery(ctx context.Context, tenantID string, d *PendingDelivery) (int64, error)

	// ClaimDueDeliveries leases deliveries whose retry time has arrived.
	//
	// Not tenant-scoped, like ClaimExpired: the dispatcher sweeps every tenant.
	// The lease pushes next_retry_at forward so a worker that dies mid-attempt
	// releases its claim instead of stranding the delivery.
	ClaimDueDeliveries(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]*DueDelivery, error)

	// RecordDeliveryOutcome writes back the result of one attempt.
	RecordDeliveryOutcome(ctx context.Context, id int64, out DeliveryOutcome) error

	// ListDeliveries returns a tenant's delivery log, newest first.
	ListDeliveries(ctx context.Context, tenantID, status string, limit int) ([]*DeliveryRecord, error)

	// ReplayDelivery returns a dead-lettered delivery to the queue (§11.5).
	ReplayDelivery(ctx context.Context, tenantID string, id int64) error
}

// RuleStore persists the reconciliation windows a tenant has configured (§3.2 B2).
type RuleStore interface {
	ListRules(ctx context.Context, tenantID string) ([]rules.Rule, error)
	CreateRule(ctx context.Context, tenantID string, r *rules.Rule) (int64, error)
	DeleteRule(ctx context.Context, tenantID string, id int64) error
}

// Store is the full persistence surface.
type Store interface {
	TransactionStore
	TenantStore
	APIKeyStore
	WebhookStore
	RuleStore
}
