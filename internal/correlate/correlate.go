// Package correlate matches credit legs to their debits and decides what each
// batch of events does to stored state.
package correlate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// RuleProvider returns the reconciliation rules in force for a tenant.
type RuleProvider func(ctx context.Context, tenantID string) (*rules.Set, error)

// SaltProvider returns a tenant's pseudonymisation salt (§8.4). In production
// this is backed by KMS; the salt never leaves the process.
type SaltProvider func(ctx context.Context, tenantID string) (string, error)

// Engine applies batches of events to the store.
type Engine struct {
	store store.TransactionStore
	rules RuleProvider
	salt  SaltProvider
	now   func() time.Time
}

// Options configures an Engine. Rules and Salt are required.
type Options struct {
	Rules RuleProvider
	Salt  SaltProvider
	Now   func() time.Time // defaults to time.Now
}

// New builds an Engine.
func New(s store.TransactionStore, opts Options) (*Engine, error) {
	if s == nil {
		return nil, errors.New("correlate: store is required")
	}
	if opts.Rules == nil {
		return nil, errors.New("correlate: rule provider is required")
	}
	if opts.Salt == nil {
		return nil, errors.New("correlate: salt provider is required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Engine{store: s, rules: opts.Rules, salt: opts.Salt, now: now}, nil
}

// Rejection records an event that could not be accepted. Rejections are
// per-event and never fail the batch — one malformed event must not discard the
// other ninety-nine.
type Rejection struct {
	TransactionID string
	Err           error
}

// Result summarises what a batch did.
type Result struct {
	DebitsStored    int
	DebitsDuplicate int
	CreditsApplied  int
	CreditsParked   int
	CreditsIgnored  int // legal-but-refused, e.g. a replay against a settled transaction
	Rejections      []Rejection
}

// Apply processes one tenant's batch. Returns an error only for infrastructure
// failures; per-event problems land in Result.Rejections.
func (e *Engine) Apply(ctx context.Context, tenantID string, events []domain.Event) (Result, error) {
	var res Result

	ruleSet, err := e.rules(ctx, tenantID)
	if err != nil {
		return res, fmt.Errorf("load rules for %s: %w", tenantID, err)
	}
	salt, err := e.salt(ctx, tenantID)
	if err != nil {
		return res, fmt.Errorf("load salt for %s: %w", tenantID, err)
	}

	var (
		debits  []*domain.Transaction
		credits []*domain.CreditEvent
	)

	for _, ev := range events {
		if ev.TenantID() != tenantID {
			res.Rejections = append(res.Rejections, Rejection{
				TransactionID: ev.TransactionID(),
				Err:           store.ErrTenantMismatch,
			})
			continue
		}
		if err := ev.Validate(); err != nil {
			res.Rejections = append(res.Rejections, Rejection{TransactionID: ev.TransactionID(), Err: err})
			continue
		}

		switch {
		case ev.Debit != nil:
			txn, err := e.toTransaction(ev.Debit, ruleSet, salt)
			if err != nil {
				res.Rejections = append(res.Rejections, Rejection{TransactionID: ev.Debit.TransactionID, Err: err})
				continue
			}
			debits = append(debits, txn)
		case ev.Credit != nil:
			if err := screenCredit(ev.Credit); err != nil {
				res.Rejections = append(res.Rejections, Rejection{TransactionID: ev.Credit.TransactionID, Err: err})
				continue
			}
			credits = append(credits, ev.Credit)
		}
	}

	// Debits first: a batch often carries a fast transaction's debit and credit
	// together, and the credit can only land once its debit exists.
	var storedIDs []string
	if len(debits) > 0 {
		up, err := e.store.UpsertDebits(ctx, tenantID, debits)
		if err != nil {
			return res, fmt.Errorf("store debits for %s: %w", tenantID, err)
		}
		res.DebitsStored = len(up.Inserted)
		res.DebitsDuplicate = len(up.Duplicates)
		storedIDs = up.Inserted
	}

	// Credits that arrived before their debit can be applied now that it exists.
	if len(storedIDs) > 0 {
		applied, err := e.drainParked(ctx, tenantID, storedIDs)
		if err != nil {
			return res, err
		}
		res.CreditsApplied += applied
	}

	for _, c := range credits {
		applied, err := e.applyCredit(ctx, tenantID, c)
		if err != nil {
			return res, err
		}
		switch applied {
		case creditApplied:
			res.CreditsApplied++
		case creditParked:
			res.CreditsParked++
		case creditIgnored:
			res.CreditsIgnored++
		}
	}

	return res, nil
}

type creditOutcome int

const (
	creditApplied creditOutcome = iota
	creditParked
	creditIgnored
	creditNotFound // the debit has not arrived yet
)

func (e *Engine) applyCredit(ctx context.Context, tenantID string, c *domain.CreditEvent) (creditOutcome, error) {
	outcome, err := e.tryApply(ctx, tenantID, c)
	if err != nil {
		return creditIgnored, err
	}
	if outcome != creditNotFound {
		return outcome, nil
	}

	// The debit has not arrived. Park rather than drop (§3.2 A2).
	if err := e.store.ParkCredit(ctx, tenantID, c); err != nil {
		return creditIgnored, fmt.Errorf("park credit %s: %w", c.TransactionID, err)
	}

	// The debit can land between the failed apply above and the park, so its
	// sweep would have found nothing. Draining our own park closes that window:
	// the credit is durable before we look, so it cannot be lost by either side.
	applied, err := e.drainParked(ctx, tenantID, []string{c.TransactionID})
	if err != nil {
		return creditIgnored, err
	}
	if applied > 0 {
		return creditApplied, nil
	}
	return creditParked, nil
}

// drainParked applies any parked credits for the given transactions and removes
// only the ones that resolved.
func (e *Engine) drainParked(ctx context.Context, tenantID string, transactionIDs []string) (int, error) {
	parked, err := e.store.PeekParkedCredits(ctx, tenantID, transactionIDs)
	if err != nil {
		return 0, fmt.Errorf("peek parked credits for %s: %w", tenantID, err)
	}

	applied := 0
	for _, pc := range parked {
		outcome, err := e.tryApply(ctx, tenantID, pc)
		if err != nil {
			return applied, err
		}
		if outcome == creditNotFound {
			continue // debit still absent; leave it parked
		}
		// Applied, or legally refused because another worker already applied it.
		// Either way it is resolved and can be removed.
		if err := e.store.DeleteParkedCredit(ctx, tenantID, pc.TransactionID); err != nil {
			return applied, fmt.Errorf("delete parked credit %s: %w", pc.TransactionID, err)
		}
		if outcome == creditApplied {
			applied++
		}
	}
	return applied, nil
}

// tryApply attempts a single credit against stored state without parking.
func (e *Engine) tryApply(ctx context.Context, tenantID string, c *domain.CreditEvent) (creditOutcome, error) {
	// Validate rejects unknown verdicts on the way in and the database
	// CHECK-constrains parked ones, so reaching here means stored state is
	// corrupt. Surface it rather than silently dropping the credit.
	target, err := c.Status.TargetStatus()
	if err != nil {
		return creditIgnored, fmt.Errorf("credit %s: %w", c.TransactionID, err)
	}

	_, err = e.store.ApplyCredit(ctx, tenantID, c.TransactionID, target, c.CreditAt)
	switch {
	case err == nil:
		return creditApplied, nil

	case errors.Is(err, store.ErrNotFound):
		return creditNotFound, nil

	default:
		var ite domain.InvalidTransitionError
		if errors.As(err, &ite) {
			// A replay against a settled transaction, or a verdict the machine
			// forbids. Expected client behaviour, not an infrastructure failure.
			return creditIgnored, nil
		}
		return creditIgnored, fmt.Errorf("apply credit %s: %w", c.TransactionID, err)
	}
}

// toTransaction turns a validated debit into a storable transaction, resolving
// its window and pseudonymising the customer reference.
func (e *Engine) toTransaction(d *domain.DebitEvent, ruleSet *rules.Set, salt string) (*domain.Transaction, error) {
	if err := domain.ScreenMetadata(d.Metadata); err != nil {
		return nil, err
	}
	if err := domain.ScreenString("transaction_id", d.TransactionID); err != nil {
		return nil, err
	}

	txn := &domain.Transaction{
		TenantID:        d.TenantID,
		TransactionID:   d.TransactionID,
		IdempotencyKey:  d.IdempotencyKey,
		TransactionType: d.TransactionType,
		Provider:        d.Provider,
		AmountMinor:     d.AmountMinor,
		Currency:        d.Currency,
		Status:          domain.StatusPendingDebit,
		DebitAt:         d.DebitAt,
		Metadata:        d.Metadata,
		IsBackfill:      d.IsBackfill,
		CustomerRefHash: HashCustomerRef(salt, d.CustomerRef),
	}

	res := ruleSet.Resolve(txn)
	txn.ExpectedCompletionAt = d.DebitAt.Add(res.Window)
	return txn, nil
}

func screenCredit(c *domain.CreditEvent) error {
	if err := domain.ScreenString("transaction_id", c.TransactionID); err != nil {
		return err
	}
	return domain.ScreenString("provider_reference", c.ProviderReference)
}

// HashCustomerRef pseudonymises a customer reference with a per-tenant salt.
// An empty reference hashes to empty so absence stays distinguishable.
func HashCustomerRef(salt, ref string) string {
	if ref == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(salt + "|" + ref))
	return hex.EncodeToString(sum[:])
}
