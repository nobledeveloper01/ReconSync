package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// Postgres is the production Store.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ Store = (*Postgres)(nil)

// NewPostgres wraps an existing pool. The caller owns the pool's lifecycle.
func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

// transitionAttempts bounds the retry when the update and the follow-up
// classification read see different snapshots.
const transitionAttempts = 3

// txnColumns is the read projection, in the order scanTransaction expects.
const txnColumns = `id, tenant_id, transaction_id, idempotency_key, transaction_type,
	provider, amount_minor, currency, status, debit_at, credit_at,
	expected_completion_at, detected_at, reversal_triggered_at, reversal_completed_at,
	customer_ref_hash, metadata, is_backfill, sla_warned_at,
	expected_credit_minor, credited_minor, created_at, updated_at`

func (p *Postgres) EnsureTenant(ctx context.Context, id, name, environment string) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO tenants (id, name, environment) VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO NOTHING`, id, name, environment)
	if err != nil {
		return fmt.Errorf("ensure tenant: %w", err)
	}
	return nil
}

func (p *Postgres) UpsertDebits(ctx context.Context, tenantID string, txns []*domain.Transaction) (UpsertResult, error) {
	if len(txns) == 0 {
		return UpsertResult{}, nil
	}

	n := len(txns)
	var (
		txnIDs    = make([]string, 0, n)
		idemKeys  = make([]string, 0, n)
		types     = make([]string, 0, n)
		providers = make([]string, 0, n)
		amounts   = make([]int64, 0, n)
		curr      = make([]string, 0, n)
		debitAt   = make([]time.Time, 0, n)
		expectAt  = make([]time.Time, 0, n)
		refHashes = make([]string, 0, n)
		// JSON as strings, not [][]byte — pgx encodes [][]byte as bytea[], which
		// will not cast to jsonb[].
		metas    = make([]string, 0, n)
		backfill = make([]bool, 0, n)

		// Pointers so "not stated" survives as NULL rather than collapsing into
		// a declared expectation of zero.
		expectedCredit = make([]*int64, 0, n)
	)

	var res UpsertResult

	// Dedupe within the batch first so the outcome does not depend on statement
	// ordering, then let the unique constraints handle cross-batch duplicates.
	seenTxn := make(map[string]struct{}, n)
	seenKey := make(map[string]struct{}, n)

	for _, t := range txns {
		if t.TenantID != tenantID {
			return UpsertResult{}, ErrTenantMismatch
		}
		if _, dup := seenTxn[t.TransactionID]; dup {
			res.Duplicates = append(res.Duplicates, t.TransactionID)
			continue
		}
		if _, dup := seenKey[t.IdempotencyKey]; dup {
			res.Duplicates = append(res.Duplicates, t.TransactionID)
			continue
		}
		seenTxn[t.TransactionID] = struct{}{}
		seenKey[t.IdempotencyKey] = struct{}{}

		meta := t.Metadata
		if meta == nil {
			meta = map[string]any{}
		}
		raw, err := json.Marshal(meta)
		if err != nil {
			return UpsertResult{}, fmt.Errorf("marshal metadata for %s: %w", t.TransactionID, err)
		}

		txnIDs = append(txnIDs, t.TransactionID)
		idemKeys = append(idemKeys, t.IdempotencyKey)
		types = append(types, t.TransactionType)
		providers = append(providers, t.Provider)
		amounts = append(amounts, t.AmountMinor)
		curr = append(curr, t.Currency)
		debitAt = append(debitAt, t.DebitAt)
		expectAt = append(expectAt, t.ExpectedCompletionAt)
		refHashes = append(refHashes, t.CustomerRefHash)
		metas = append(metas, string(raw))
		backfill = append(backfill, t.IsBackfill)
		// Nil rather than zero, so "not stated" stays distinguishable from a
		// declared expectation of nothing.
		if t.ExpectedCreditMinor > 0 {
			v := t.ExpectedCreditMinor
			expectedCredit = append(expectedCredit, &v)
			continue
		}
		expectedCredit = append(expectedCredit, nil)
	}

	if len(txnIDs) == 0 {
		return res, nil
	}

	rows, err := p.pool.Query(ctx, `
		INSERT INTO transactions (
			tenant_id, transaction_id, idempotency_key, transaction_type, provider,
			amount_minor, currency, status, debit_at, expected_completion_at,
			customer_ref_hash, metadata, is_backfill, expected_credit_minor)
		SELECT $1, u.transaction_id, u.idempotency_key, u.transaction_type, u.provider,
		       u.amount_minor, u.currency, $2, u.debit_at, u.expected_completion_at,
		       u.customer_ref_hash, u.metadata, u.is_backfill, u.expected_credit_minor
		FROM unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::bigint[],
		            $8::text[], $9::timestamptz[], $10::timestamptz[], $11::text[],
		            $12::jsonb[], $13::boolean[], $14::bigint[])
		     AS u(transaction_id, idempotency_key, transaction_type, provider,
		          amount_minor, currency, debit_at, expected_completion_at,
		          customer_ref_hash, metadata, is_backfill, expected_credit_minor)
		ON CONFLICT DO NOTHING
		RETURNING transaction_id`,
		tenantID, string(domain.StatusPendingDebit),
		txnIDs, idemKeys, types, providers, amounts, curr,
		debitAt, expectAt, refHashes, metas, backfill, expectedCredit)
	if err != nil {
		return UpsertResult{}, fmt.Errorf("upsert debits: %w", err)
	}
	defer rows.Close()

	inserted := make(map[string]struct{}, len(txnIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return UpsertResult{}, fmt.Errorf("scan inserted id: %w", err)
		}
		inserted[id] = struct{}{}
		res.Inserted = append(res.Inserted, id)
	}
	if err := rows.Err(); err != nil {
		return UpsertResult{}, fmt.Errorf("upsert debits: %w", err)
	}

	for _, id := range txnIDs {
		if _, ok := inserted[id]; !ok {
			res.Duplicates = append(res.Duplicates, id)
		}
	}
	return res, nil
}

func (p *Postgres) ApplyCredit(ctx context.Context, tenantID, transactionID string, target domain.Status, creditAt time.Time) (*domain.Transaction, error) {
	allowed := allowedSources(target)
	if len(allowed) == 0 {
		return nil, domain.InvalidTransitionError{From: "", To: target}
	}

	// The status guard rides in the same statement as the write, so a credit
	// racing the detection sweep either wins cleanly or is rejected — it can
	// never overwrite a state the machine forbids.
	return p.applyTransition(ctx, tenantID, transactionID, target, `
		UPDATE transactions
		SET status = $3,
		    credit_at = $4,
		    detected_at = CASE WHEN $3 = 'orphaned' THEN now() ELSE detected_at END,
		    updated_at = now()
		WHERE tenant_id = $1 AND transaction_id = $2 AND status = ANY($5)
		RETURNING `+txnColumns,
		tenantID, transactionID, string(target), creditAt, allowed)
}

// MarkSettled closes an orphan the rail confirmed arrived (ADR-0005).
func (p *Postgres) MarkSettled(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	target := domain.StatusCompleted
	return p.applyTransition(ctx, tenantID, transactionID, target, `
		UPDATE transactions
		SET status = $3,
		    credit_at = COALESCE(credit_at, $4),
		    updated_at = $4
		WHERE tenant_id = $1 AND transaction_id = $2 AND status = ANY($5)
		RETURNING `+txnColumns,
		tenantID, transactionID, string(target), at, allowedSources(target))
}

// MarkUncertain moves a transaction to suspect for a human to investigate.
func (p *Postgres) MarkUncertain(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	target := domain.StatusSuspect
	return p.applyTransition(ctx, tenantID, transactionID, target, `
		UPDATE transactions
		SET status = $3,
		    updated_at = $4
		WHERE tenant_id = $1 AND transaction_id = $2 AND status = ANY($5)
		RETURNING `+txnColumns,
		tenantID, transactionID, string(target), at, allowedSources(target))
}

// MarkReversalPending records that the reversal webhook has been dispatched.
// Legal from orphaned, and from reversal_failed on a dead-letter replay.
func (p *Postgres) MarkReversalPending(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	target := domain.StatusReversalPending
	return p.applyTransition(ctx, tenantID, transactionID, target, `
		UPDATE transactions
		SET status = $3,
		    reversal_triggered_at = $4,
		    updated_at = now()
		WHERE tenant_id = $1 AND transaction_id = $2 AND status = ANY($5)
		RETURNING `+txnColumns,
		tenantID, transactionID, string(target), at, allowedSources(target))
}

// MarkReversalCompleted records that the customer's system finished reversing
// (§3.2 C2). Only legal from reversal_pending.
func (p *Postgres) MarkReversalCompleted(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	target := domain.StatusReversalCompleted
	return p.applyTransition(ctx, tenantID, transactionID, target, `
		UPDATE transactions
		SET status = $3,
		    reversal_completed_at = $4,
		    updated_at = now()
		WHERE tenant_id = $1 AND transaction_id = $2 AND status = ANY($5)
		RETURNING `+txnColumns,
		tenantID, transactionID, string(target), at, allowedSources(target))
}

// MarkReversalFailed records that delivery exhausted its retries.
func (p *Postgres) MarkReversalFailed(ctx context.Context, tenantID, transactionID string, at time.Time) (*domain.Transaction, error) {
	target := domain.StatusReversalFailed
	return p.applyTransition(ctx, tenantID, transactionID, target, `
		UPDATE transactions
		SET status = $3,
		    updated_at = $4
		WHERE tenant_id = $1 AND transaction_id = $2 AND status = ANY($5)
		RETURNING `+txnColumns,
		tenantID, transactionID, string(target), at, allowedSources(target))
}

// allowedSources renders the legal predecessors of a state for a SQL guard.
func allowedSources(target domain.Status) []string {
	sources := domain.SourcesFor(target)
	out := make([]string, len(sources))
	for i, s := range sources {
		out[i] = string(s)
	}
	return out
}

// applyTransition runs a guarded state change and classifies a no-match result.
func (p *Postgres) applyTransition(ctx context.Context, tenantID, transactionID string, target domain.Status, query string, args ...any) (*domain.Transaction, error) {
	for attempt := 0; attempt < transitionAttempts; attempt++ {
		row := p.pool.QueryRow(ctx, query, args...)

		t, err := scanTransaction(row)
		if err == nil {
			return t, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("transition %s: %w", transactionID, err)
		}

		// No row matched: either it does not exist, or the guard rejected it.
		current, getErr := p.Get(ctx, tenantID, transactionID)
		if getErr != nil {
			return nil, getErr // ErrNotFound
		}

		// The row is now in a state the target is reachable from, so the UPDATE
		// and this read saw different snapshots — the row landed, or the sweep
		// moved it, in between. Reporting an invalid transition here would
		// discard a write that is perfectly legal, so retry instead.
		if domain.CanTransition(current.Status, target) {
			continue
		}
		return nil, domain.InvalidTransitionError{From: current.Status, To: target}
	}

	return nil, fmt.Errorf("transition %s -> %s: gave up after %d contended attempts",
		transactionID, target, transitionAttempts)
}

func (p *Postgres) CreateAPIKey(ctx context.Context, tenantID, keyID string, key auth.Key, scopes []string) error {
	if scopes == nil {
		scopes = []string{}
	}
	_, err := p.pool.Exec(ctx,
		`INSERT INTO api_keys (id, tenant_id, prefix, hash, scopes) VALUES ($1, $2, $3, $4, $5)`,
		keyID, tenantID, key.Prefix, key.Hash, scopes)
	if err != nil {
		return fmt.Errorf("create api key: %w", err)
	}
	return nil
}

// APIKeyByPrefix resolves a key prefix. Not tenant-scoped: it is what determines
// the tenant. Revoked keys are returned so the caller can reject them uniformly
// rather than learning anything from a different error.
func (p *Postgres) APIKeyByPrefix(ctx context.Context, prefix string) (*auth.Record, error) {
	var (
		rec    auth.Record
		scopes []string
	)
	err := p.pool.QueryRow(ctx,
		`SELECT id, tenant_id, prefix, hash, scopes, revoked_at FROM api_keys WHERE prefix = $1`,
		prefix).Scan(&rec.ID, &rec.TenantID, &rec.Prefix, &rec.Hash, &scopes, &rec.RevokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lookup api key: %w", err)
	}

	rec.Scopes = scopes
	rec.Environment, _ = auth.EnvironmentOf(rec.Prefix)
	return &rec, nil
}

func (p *Postgres) TouchAPIKey(ctx context.Context, keyID string) error {
	_, err := p.pool.Exec(ctx, `UPDATE api_keys SET last_used_at = now() WHERE id = $1`, keyID)
	if err != nil {
		return fmt.Errorf("touch api key: %w", err)
	}
	return nil
}

func (p *Postgres) RevokeAPIKey(ctx context.Context, tenantID, keyID string) error {
	tag, err := p.pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = now()
		 WHERE tenant_id = $1 AND id = $2 AND revoked_at IS NULL`,
		tenantID, keyID)
	if err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (p *Postgres) ParkCredit(ctx context.Context, tenantID string, ev *domain.CreditEvent) error {
	if ev.TenantID != tenantID {
		return ErrTenantMismatch
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO pending_credits (
			tenant_id, transaction_id, idempotency_key, credit_at, provider_reference,
			status, amount_minor)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, transaction_id) DO NOTHING`,
		tenantID, ev.TransactionID, ev.IdempotencyKey, ev.CreditAt, ev.ProviderReference,
		string(ev.Status), ev.AmountMinor)
	if err != nil {
		return fmt.Errorf("park credit: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteParkedCredit(ctx context.Context, tenantID, transactionID string) error {
	_, err := p.pool.Exec(ctx,
		`DELETE FROM pending_credits WHERE tenant_id = $1 AND transaction_id = $2`,
		tenantID, transactionID)
	if err != nil {
		return fmt.Errorf("delete parked credit: %w", err)
	}
	return nil
}

func (p *Postgres) PeekParkedCredits(ctx context.Context, tenantID string, transactionIDs []string) ([]*domain.CreditEvent, error) {
	if len(transactionIDs) == 0 {
		return nil, nil
	}
	rows, err := p.pool.Query(ctx, `
		SELECT transaction_id, idempotency_key, credit_at, provider_reference, status, amount_minor
		FROM pending_credits
		WHERE tenant_id = $1 AND transaction_id = ANY($2)`,
		tenantID, transactionIDs)
	if err != nil {
		return nil, fmt.Errorf("peek parked credits: %w", err)
	}
	defer rows.Close()

	var out []*domain.CreditEvent
	for rows.Next() {
		ev := &domain.CreditEvent{TenantID: tenantID}
		var status string
		if err := rows.Scan(&ev.TransactionID, &ev.IdempotencyKey, &ev.CreditAt,
			&ev.ProviderReference, &status, &ev.AmountMinor); err != nil {
			return nil, fmt.Errorf("scan parked credit: %w", err)
		}
		ev.Status = domain.CreditStatus(status)
		if !ev.Status.Valid() {
			return nil, fmt.Errorf("parked credit %s: unrecognised status %q", ev.TransactionID, status)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate parked credits: %w", err)
	}
	return out, nil
}

func (p *Postgres) Get(ctx context.Context, tenantID, transactionID string) (*domain.Transaction, error) {
	row := p.pool.QueryRow(ctx,
		`SELECT `+txnColumns+` FROM transactions WHERE tenant_id = $1 AND transaction_id = $2`,
		tenantID, transactionID)

	t, err := scanTransaction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get transaction: %w", err)
	}
	return t, nil
}

func (p *Postgres) ListByStatus(ctx context.Context, tenantID string, status domain.Status, limit int) ([]*domain.Transaction, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := p.pool.Query(ctx,
		`SELECT `+txnColumns+` FROM transactions
		 WHERE tenant_id = $1 AND status = $2
		 ORDER BY debit_at DESC LIMIT $3`,
		tenantID, string(status), limit)
	if err != nil {
		return nil, fmt.Errorf("list by status: %w", err)
	}
	defer rows.Close()
	return collectTransactions(rows)
}

// ClaimExpired implements the §4.4 sweep. SKIP LOCKED makes it safe across
// replicas with no leader election.
// ApplyPartialCredit accumulates a credit and settles only when the whole
// expected amount has arrived.
//
// The accumulation and the decision happen in one statement. Splitting them
// would let two credits for the same transaction each read the old total,
// each conclude the transaction is still short, and leave it open with the
// money fully arrived — which the sweep would then reverse.
func (p *Postgres) ApplyPartialCredit(ctx context.Context, tenantID string, c *domain.CreditEvent) (*domain.Transaction, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin partial credit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Claim the credit first. A second delivery of the same one inserts
	// nothing, and must not move the total.
	tag, err := tx.Exec(ctx, `
		INSERT INTO credit_applications (tenant_id, idempotency_key, transaction_id, amount_minor)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
		tenantID, c.IdempotencyKey, c.TransactionID, c.AmountMinor)
	if err != nil {
		return nil, fmt.Errorf("record credit application: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Already counted. Report the transaction as it stands rather than
		// erroring: a replay is ordinary client behaviour.
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit partial credit: %w", err)
		}
		return p.Get(ctx, tenantID, c.TransactionID)
	}

	transactionID, amountMinor, creditAt := c.TransactionID, c.AmountMinor, c.CreditAt
	rows, err := tx.Query(ctx, `
		UPDATE transactions t
		SET credited_minor = t.credited_minor + $3,
		    credit_at = $4,
		    updated_at = now(),
		    status = CASE
		        -- More arrived than was ever expected. Not a settlement and not
		        -- a failure: a human decides what an overpayment means.
		        WHEN t.credited_minor + $3 > COALESCE(t.expected_credit_minor, t.amount_minor) THEN 'suspect'
		        WHEN t.credited_minor + $3 = COALESCE(t.expected_credit_minor, t.amount_minor) THEN 'completed'
		        -- Still short, so the transaction stays open and its window can
		        -- still expire. A partial settlement is money outstanding.
		        ELSE t.status
		    END
		WHERE t.tenant_id = $1 AND t.transaction_id = $2
		  AND t.status IN ('pending_debit', 'pending_unknown')
		RETURNING `+txnColumns, tenantID, transactionID, amountMinor, creditAt.UTC())
	if err != nil {
		return nil, fmt.Errorf("apply partial credit: %w", err)
	}
	defer rows.Close()

	out, err := collectTransactions(rows)
	if err != nil {
		return nil, err
	}
	rows.Close()

	if len(out) == 0 {
		// Either it does not exist, or it is already closed. Rolling back
		// releases the credit claim so a genuine later delivery still counts.
		_ = tx.Rollback(ctx)
		existing, err := p.Get(ctx, tenantID, transactionID)
		if err != nil {
			return nil, err
		}
		return nil, domain.InvalidTransitionError{From: existing.Status, To: domain.StatusCompleted}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit partial credit: %w", err)
	}
	return out[0], nil
}

// slaAtRiskStatuses are the states in which the customer's money is still out.
//
// Exactly the set the exposure report counts and the compliance report will
// score as a breach if nothing changes. Tying the warning to the same set is the
// point: an alert that fires for a different population than the report scores
// would be worse than no alert, because it would train people to ignore it.
const slaAtRiskStatuses = `('orphaned','reversal_pending','reversal_failed','suspect')`

// ClaimSLAAtRisk marks transactions approaching their deadline and returns them.
func (p *Postgres) ClaimSLAAtRisk(ctx context.Context, now time.Time, deadline, warnBefore time.Duration, limit int) ([]*domain.Transaction, error) {
	if limit <= 0 {
		limit = 500
	}
	// The deadline runs from the debit, because that is when the customer's
	// money left — not from when we noticed, which would let a late detection
	// quietly extend a regulatory clock.
	warnFrom := now.Add(warnBefore).Add(-deadline)

	rows, err := p.pool.Query(ctx, `
		UPDATE transactions t
		SET sla_warned_at = $1, updated_at = $1
		WHERE t.id IN (
			SELECT id FROM transactions
			WHERE sla_warned_at IS NULL
			  AND NOT is_backfill
			  AND status IN `+slaAtRiskStatuses+`
			  AND debit_at <= $2
			ORDER BY debit_at
			FOR UPDATE SKIP LOCKED
			LIMIT $3
		)
		RETURNING `+txnColumns, now.UTC(), warnFrom.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("claim sla at risk: %w", err)
	}
	defer rows.Close()
	return collectTransactions(rows)
}

func (p *Postgres) ClaimExpired(ctx context.Context, now time.Time, limit int, opts ...ClaimOption) ([]*domain.Transaction, error) {
	if limit <= 0 {
		limit = 500
	}
	cfg := ResolveClaimOptions(opts)
	skip := cfg.SkipTenants
	if skip == nil {
		skip = []string{}
	}
	// The gap check rides in the same statement as the claim, so a transaction
	// cannot be claimed under one verdict and reclassified under another.
	//
	// A transaction only becomes orphaned if our own view of that tenant was
	// intact across its whole window. If we dropped events or failed a batch in
	// any minute it spans, the absence of a credit proves nothing and it goes to
	// suspect for a human instead (ADR-0004).
	rows, err := p.pool.Query(ctx, `
		UPDATE transactions t
		SET status = CASE
		                WHEN EXISTS (
		                    SELECT 1 FROM ingest_health h
		                    WHERE h.tenant_id = t.tenant_id
		                      AND h.bucket >= date_trunc('minute', t.debit_at)
		                      AND h.bucket <= date_trunc('minute', t.expected_completion_at)
		                      AND (h.dropped > 0 OR h.handler_errors > 0)
		                ) THEN 'suspect'
		                WHEN t.status = 'pending_debit' THEN 'orphaned'
		                ELSE 'suspect'
		             END,
		    detected_at = $1,
		    updated_at = $1
		WHERE t.id IN (
			SELECT id FROM transactions
			WHERE status IN ('pending_debit','pending_unknown')
			  AND expected_completion_at <= $1
			  AND NOT (tenant_id = ANY($3))
			ORDER BY expected_completion_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		RETURNING `+txnColumns, now, limit, skip)
	if err != nil {
		return nil, fmt.Errorf("claim expired: %w", err)
	}
	defer rows.Close()
	return collectTransactions(rows)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTransaction(row scanner) (*domain.Transaction, error) {
	var (
		t              domain.Transaction
		status         string
		rawMeta        []byte
		expectedCredit *int64
	)
	err := row.Scan(
		&t.ID, &t.TenantID, &t.TransactionID, &t.IdempotencyKey, &t.TransactionType,
		&t.Provider, &t.AmountMinor, &t.Currency, &status, &t.DebitAt, &t.CreditAt,
		&t.ExpectedCompletionAt, &t.DetectedAt, &t.ReversalTriggeredAt, &t.ReversalCompletedAt,
		&t.CustomerRefHash, &rawMeta, &t.IsBackfill, &t.SLAWarnedAt,
		&expectedCredit, &t.CreditedMinor, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if expectedCredit != nil {
		t.ExpectedCreditMinor = *expectedCredit
	}

	t.Status = domain.Status(status)
	if !t.Status.Valid() {
		return nil, fmt.Errorf("transaction %s: unrecognised status %q from storage", t.TransactionID, status)
	}
	if len(rawMeta) > 0 {
		if err := json.Unmarshal(rawMeta, &t.Metadata); err != nil {
			return nil, fmt.Errorf("transaction %s: decode metadata: %w", t.TransactionID, err)
		}
	}
	return &t, nil
}

func collectTransactions(rows pgx.Rows) ([]*domain.Transaction, error) {
	var out []*domain.Transaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transactions: %w", err)
	}
	return out, nil
}
