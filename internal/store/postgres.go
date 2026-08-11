package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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

// applyCreditAttempts bounds the retry when the update and the follow-up
// classification read see different snapshots.
const applyCreditAttempts = 3

// txnColumns is the read projection, in the order scanTransaction expects.
const txnColumns = `id, tenant_id, transaction_id, idempotency_key, transaction_type,
	provider, amount_minor, currency, status, debit_at, credit_at,
	expected_completion_at, detected_at, reversal_triggered_at, reversal_completed_at,
	customer_ref_hash, metadata, is_backfill, created_at, updated_at`

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
	}

	if len(txnIDs) == 0 {
		return res, nil
	}

	rows, err := p.pool.Query(ctx, `
		INSERT INTO transactions (
			tenant_id, transaction_id, idempotency_key, transaction_type, provider,
			amount_minor, currency, status, debit_at, expected_completion_at,
			customer_ref_hash, metadata, is_backfill)
		SELECT $1, u.transaction_id, u.idempotency_key, u.transaction_type, u.provider,
		       u.amount_minor, u.currency, $2, u.debit_at, u.expected_completion_at,
		       u.customer_ref_hash, u.metadata, u.is_backfill
		FROM unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::bigint[],
		            $8::text[], $9::timestamptz[], $10::timestamptz[], $11::text[],
		            $12::jsonb[], $13::boolean[])
		     AS u(transaction_id, idempotency_key, transaction_type, provider,
		          amount_minor, currency, debit_at, expected_completion_at,
		          customer_ref_hash, metadata, is_backfill)
		ON CONFLICT DO NOTHING
		RETURNING transaction_id`,
		tenantID, string(domain.StatusPendingDebit),
		txnIDs, idemKeys, types, providers, amounts, curr,
		debitAt, expectAt, refHashes, metas, backfill)
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
	sources := domain.SourcesFor(target)
	if len(sources) == 0 {
		return nil, domain.InvalidTransitionError{From: "", To: target}
	}
	allowed := make([]string, len(sources))
	for i, s := range sources {
		allowed[i] = string(s)
	}

	// The status guard rides in the same statement as the write, so a credit
	// racing the detection sweep either wins cleanly or is rejected — it can
	// never overwrite a state the machine forbids.
	for attempt := 0; attempt < applyCreditAttempts; attempt++ {
		row := p.pool.QueryRow(ctx, `
			UPDATE transactions
			SET status = $3,
			    credit_at = $4,
			    detected_at = CASE WHEN $3 = 'orphaned' THEN now() ELSE detected_at END,
			    updated_at = now()
			WHERE tenant_id = $1 AND transaction_id = $2 AND status = ANY($5)
			RETURNING `+txnColumns,
			tenantID, transactionID, string(target), creditAt, allowed)

		t, err := scanTransaction(row)
		if err == nil {
			return t, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("apply credit: %w", err)
		}

		// No row matched: either it does not exist, or the guard rejected it.
		current, getErr := p.Get(ctx, tenantID, transactionID)
		if getErr != nil {
			return nil, getErr // ErrNotFound: the debit has not arrived
		}

		// The row is now in a state the target is reachable from, so the UPDATE
		// and this read saw different snapshots — the debit landed, or the sweep
		// moved it, in between. Reporting an invalid transition here would
		// discard a credit that is perfectly legal to apply, so retry instead.
		if domain.CanTransition(current.Status, target) {
			continue
		}
		return nil, domain.InvalidTransitionError{From: current.Status, To: target}
	}

	return nil, fmt.Errorf("apply credit %s: gave up after %d contended attempts",
		transactionID, applyCreditAttempts)
}

func (p *Postgres) ParkCredit(ctx context.Context, tenantID string, ev *domain.CreditEvent) error {
	if ev.TenantID != tenantID {
		return ErrTenantMismatch
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO pending_credits (
			tenant_id, transaction_id, idempotency_key, credit_at, provider_reference, status)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, transaction_id) DO NOTHING`,
		tenantID, ev.TransactionID, ev.IdempotencyKey, ev.CreditAt, ev.ProviderReference, string(ev.Status))
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
		SELECT transaction_id, idempotency_key, credit_at, provider_reference, status
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
		if err := rows.Scan(&ev.TransactionID, &ev.IdempotencyKey, &ev.CreditAt, &ev.ProviderReference, &status); err != nil {
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
func (p *Postgres) ClaimExpired(ctx context.Context, now time.Time, limit int) ([]*domain.Transaction, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := p.pool.Query(ctx, `
		UPDATE transactions
		SET status = CASE status
		                WHEN 'pending_debit' THEN 'orphaned'
		                ELSE 'suspect'
		             END,
		    detected_at = $1,
		    updated_at = $1
		WHERE id IN (
			SELECT id FROM transactions
			WHERE status IN ('pending_debit','pending_unknown')
			  AND expected_completion_at <= $1
			ORDER BY expected_completion_at
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		RETURNING `+txnColumns, now, limit)
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
		t       domain.Transaction
		status  string
		rawMeta []byte
	)
	err := row.Scan(
		&t.ID, &t.TenantID, &t.TransactionID, &t.IdempotencyKey, &t.TransactionType,
		&t.Provider, &t.AmountMinor, &t.Currency, &status, &t.DebitAt, &t.CreditAt,
		&t.ExpectedCompletionAt, &t.DetectedAt, &t.ReversalTriggeredAt, &t.ReversalCompletedAt,
		&t.CustomerRefHash, &rawMeta, &t.IsBackfill, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
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
