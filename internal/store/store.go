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

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/report"
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

	// ApplyPartialCredit accumulates a credit that states its amount, and
	// settles the transaction only once the whole expected amount has arrived.
	//
	// Separate from ApplyCredit because the decision moves: ApplyCredit is told
	// which status to move to, while here the running total decides. Doing the
	// accumulation and the decision in one guarded statement is what stops a
	// second credit racing the first and both concluding "still short".
	// The idempotency key is what makes accumulation safe: the pipeline can
	// legitimately deliver the same credit twice — parked then drained, or a
	// client retry — and a running total would add it twice.
	ApplyPartialCredit(ctx context.Context, tenantID string, c *domain.CreditEvent) (*domain.Transaction, error)

	// MarkSettled closes an orphan the rail has since confirmed arrived, so no
	// reversal is sent (ADR-0005).
	MarkSettled(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error)

	// MarkUncertain moves an orphan to suspect because we could not establish
	// what happened. It raises an investigation and never a reversal.
	MarkUncertain(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error)

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
	// orphaned and returns them. SkipTenants excludes tenants we cannot
	// currently vouch for.
	//
	// Deliberately not tenant-scoped: the scheduler sweeps all tenants, and
	// per-tenant polling would not scale. It is the single exception to the
	// tenantID-first rule and never runs on a request path.
	ClaimExpired(ctx context.Context, now time.Time, limit int, opts ...ClaimOption) ([]*domain.Transaction, error)

	// ClaimSLAAtRisk marks and returns transactions whose regulatory deadline
	// is approaching while the customer's money is still out.
	//
	// Marking and returning in one statement is what makes the warning
	// exactly-once across replicas: a read-then-write would let two sweeps
	// warn about the same transaction.
	ClaimSLAAtRisk(ctx context.Context, now time.Time, deadline, warnBefore time.Duration, limit int) ([]*domain.Transaction, error)
}

// ClaimOption adjusts a detection sweep.
type ClaimOption func(*ClaimConfig)

// ClaimConfig is the resolved set of sweep options.
type ClaimConfig struct {
	SkipTenants []string
}

// SkipTenants excludes tenants from a sweep. Used when a tenant has gone silent
// and nothing can be concluded about their transactions.
func SkipTenants(ids ...string) ClaimOption {
	return func(c *ClaimConfig) { c.SkipTenants = append(c.SkipTenants, ids...) }
}

// ResolveClaimOptions applies options, for implementations.
func ResolveClaimOptions(opts []ClaimOption) ClaimConfig {
	var c ClaimConfig
	for _, opt := range opts {
		opt(&c)
	}
	return c
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

// DefaultSecretRef is where the signing secret is found by default.
//
// A reference, not a secret: it names the environment variable to read. The
// distinction is the point of the field — an endpoint row can be dumped,
// reviewed or backed up without carrying a signing key.
const DefaultSecretRef = "env://RECONSYNC_WEBHOOK_SECRET" //nolint:gosec // a variable name, not a credential

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

	// SetEndpointEnabled turns delivery to an endpoint on or off.
	//
	// Disabling rather than deleting is the safe way to stop deliveries: the
	// delivery log keeps its foreign key, so the history of what was sent
	// where survives the decision to stop sending.
	SetEndpointEnabled(ctx context.Context, tenantID, id string, enabled bool) error

	// DeleteEndpoint removes an endpoint and, by cascade, its delivery history.
	DeleteEndpoint(ctx context.Context, tenantID, id string) error

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

// IngestSample is one tenant's ingest counters for one minute.
type IngestSample struct {
	TenantID      string
	Bucket        time.Time
	Received      int64
	Dropped       int64
	HandlerErrors int64
}

// IngestActivitySummary describes what a tenant sent over a period.
type IngestActivitySummary struct {
	Received      int64
	ActiveBuckets int // minutes in which at least one event arrived
}

// HealthStore records how intact our own view of each tenant's stream was.
//
// Detection infers failure from the absence of a credit event, which is only
// sound if we received everything. This is what makes that assumption checkable.
type HealthStore interface {
	// RecordIngestHealth accumulates counters. Samples carry deltas, so repeated
	// writes to the same minute sum rather than overwrite.
	RecordIngestHealth(ctx context.Context, samples []IngestSample) error

	// HasIngestGap reports whether anything was lost for this tenant across a
	// time range.
	HasIngestGap(ctx context.Context, tenantID string, from, to time.Time) (bool, error)

	// IngestActivity summarises what a tenant sent, for the silence check.
	IngestActivity(ctx context.Context, tenantID string, from, to time.Time) (IngestActivitySummary, error)

	// SilentTenants returns tenants that were sending steadily and have stopped.
	//
	// Zero events from a tenant that normally sends thousands is a broken
	// integration, not a quiet period — and nothing can be concluded about their
	// individual transactions while it lasts.
	SilentTenants(ctx context.Context, now time.Time, p SilenceParams) ([]string, error)

	// SyncSilenceEpisodes reconciles the set of tenants currently silent against
	// the open episodes, and reports only what changed.
	//
	// The alert has to fire once per episode, not once per sweep and not once
	// per replica — a tenant that goes quiet at 2am must not produce a webhook
	// every five seconds until morning. Ownership is settled in the database,
	// so concurrent sweeps cannot both claim the same episode.
	SyncSilenceEpisodes(ctx context.Context, silent []string, now time.Time) (SilenceChange, error)
}

// SilenceEpisode is one stretch of a tenant sending nothing.
type SilenceEpisode struct {
	TenantID    string
	SilentSince time.Time
}

// SilenceChange is what one reconciliation moved.
type SilenceChange struct {
	// Opened are episodes this caller now owns and must alert on.
	Opened []SilenceEpisode

	// Recovered are episodes that ended because events resumed.
	Recovered []SilenceEpisode
}

// SilenceParams defines what counts as anomalous silence.
type SilenceParams struct {
	// Quiet is how long a tenant must have sent nothing.
	Quiet time.Duration

	// Baseline is the period before that in which they must have been active,
	// so a genuinely low-volume tenant is never mistaken for a broken one.
	Baseline time.Duration

	// MinActiveBuckets is how many minutes of the baseline must carry events.
	MinActiveBuckets int
}

// AuditStore appends to and reads the per-tenant hash chain.
type AuditStore interface {
	// AppendAudit links a record to the tenant's chain and stores it.
	AppendAudit(ctx context.Context, tenantID string, r *audit.Record) (*audit.Record, error)

	// ListAudit returns the chain in sequence order.
	ListAudit(ctx context.Context, tenantID string, limit int) ([]audit.Record, error)

	// SaveCheckpoint stores a signed chain head. Re-signing the same head is a
	// no-op rather than another identical row.
	SaveCheckpoint(ctx context.Context, c audit.Checkpoint) error

	// LatestCheckpoint returns the newest checkpoint for a tenant, or
	// ErrNotFound when none has been taken.
	LatestCheckpoint(ctx context.Context, tenantID string) (*audit.Checkpoint, error)

	// ListCheckpoints returns a tenant's checkpoints, newest first. This is the
	// list a customer publishes or archives; without it the signatures only
	// exist where an attacker can reach them.
	ListCheckpoints(ctx context.Context, tenantID string, limit int) ([]audit.Checkpoint, error)

	// TenantsWithAudit lists tenants that have a chain, so the checkpointer can
	// sweep all of them. Not tenant-scoped, like ClaimExpired, and never on a
	// request path.
	TenantsWithAudit(ctx context.Context) ([]string, error)
}

// ReversalClaim is one customer worker's authorisation to reverse.
type ReversalClaim struct {
	TenantID      string
	TransactionID string
	ClaimToken    string
	ClaimedBy     string
	ClaimedAt     time.Time
	ConfirmedAt   *time.Time
}

// ClaimStore is the exactly-once interlock between our advice and their money
// movement.
//
// The same reversal webhook can arrive more than once — a retry after a timeout
// they actually processed, a dead-letter replay, two of their workers on the
// same job. Today we say "reverse this" and hope they deduplicate; this makes it
// our guarantee.
type ClaimStore interface {
	// ClaimReversal grants the claim to the first caller and reports the
	// existing holder to everyone after. Not granting is a normal outcome, not
	// an error: the caller's correct response is simply to stop.
	ClaimReversal(ctx context.Context, tenantID, transactionID, claimedBy, token string, now time.Time) (claim *ReversalClaim, granted bool, err error)

	// GetReversalClaim returns the claim on a transaction, if any.
	GetReversalClaim(ctx context.Context, tenantID, transactionID string) (*ReversalClaim, error)

	// ReleaseReversalClaim drops an unconfirmed claim so it can be taken again,
	// for the case where the holder died between claiming and reversing.
	// Confirmed claims are never released — the money has already moved.
	ReleaseReversalClaim(ctx context.Context, tenantID, transactionID string) error

	// ConfirmReversalClaim marks the claim as carried through.
	ConfirmReversalClaim(ctx context.Context, tenantID, transactionID string, at time.Time) error
}

// ReportStore backs the compliance report.
type ReportStore interface {
	// CountByStatus aggregates in the database, so a healthy tenant's millions
	// of settled transactions are never dragged across the wire to be counted.
	CountByStatus(ctx context.Context, tenantID string, from, to time.Time) (map[domain.Status]int, error)

	// ListReversalCandidates returns, in full, only the transactions that
	// reached orphaned or beyond — the ones an SLA can be measured against.
	ListReversalCandidates(ctx context.Context, tenantID string, from, to time.Time, limit int) ([]*domain.Transaction, error)

	// ProviderStats aggregates per rail: how much each carried, how much it
	// failed to deliver, and how long the successes took.
	//
	// The return type lives in report because it is a report input, and
	// duplicating it here would only add a conversion with nothing to say.
	// report imports nothing but domain, so this cannot cycle.
	ProviderStats(ctx context.Context, tenantID string, from, to time.Time) ([]report.ProviderStat, error)

	// Exposure totals a tenant's outstanding position per currency, and the
	// same broken down by age. Two results rather than one because a distinct
	// customer count cannot be summed across age brackets.
	Exposure(ctx context.Context, tenantID string, scope report.Scope, now time.Time) ([]report.ExposureTotal, []report.AgeBand, error)
}

// Store is the full persistence surface.
type Store interface {
	TransactionStore
	TenantStore
	APIKeyStore
	WebhookStore
	RuleStore
	HealthStore
	AuditStore
	ReportStore
	ClaimStore
}
