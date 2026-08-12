package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// DefaultCheckpointInterval is how often each tenant's chain head is signed.
//
// The interval is the size of the window an attacker can rewrite undetected:
// anything appended since the last checkpoint has no signature behind it. Hourly
// is a deliberate trade — signing every record would make the checkpoint table
// as large as the chain and buy little, since a rewrite of the last few minutes
// is the least interesting kind.
const DefaultCheckpointInterval = time.Hour

// Checkpointer signs each tenant's chain head on a schedule.
type Checkpointer struct {
	store    store.AuditStore
	signer   *audit.Signer
	log      *slog.Logger
	now      func() time.Time
	interval time.Duration
}

// CheckpointerOptions configures a Checkpointer. Signer is required.
type CheckpointerOptions struct {
	Signer   *audit.Signer
	Logger   *slog.Logger
	Now      func() time.Time
	Interval time.Duration
}

// NewCheckpointer builds a Checkpointer.
func NewCheckpointer(s store.AuditStore, opts CheckpointerOptions) (*Checkpointer, error) {
	if s == nil {
		return nil, errors.New("service: store is required")
	}
	if opts.Signer == nil {
		return nil, audit.ErrNoSigningKey
	}

	c := &Checkpointer{store: s, signer: opts.Signer, log: opts.Logger,
		now: opts.Now, interval: opts.Interval}
	if c.log == nil {
		c.log = slog.Default()
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.interval <= 0 {
		c.interval = DefaultCheckpointInterval
	}
	return c, nil
}

// Run signs until the context is cancelled.
func (c *Checkpointer) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Sign once at start: a process that restarts more often than the interval
	// would otherwise never checkpoint at all.
	if _, err := c.Checkpoint(ctx); err != nil && !errors.Is(err, context.Canceled) {
		c.log.ErrorContext(ctx, "initial checkpoint failed", slog.String("error", err.Error()))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := c.Checkpoint(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.log.ErrorContext(ctx, "checkpoint failed", slog.String("error", err.Error()))
			}
		}
	}
}

// CheckpointResult reports what one round signed.
type CheckpointResult struct {
	Tenants int
	Signed  int
}

// Checkpoint signs every tenant's current chain head.
func (c *Checkpointer) Checkpoint(ctx context.Context) (CheckpointResult, error) {
	var res CheckpointResult

	tenants, err := c.store.TenantsWithAudit(ctx)
	if err != nil {
		return res, fmt.Errorf("list tenants with a chain: %w", err)
	}
	res.Tenants = len(tenants)

	for _, tenantID := range tenants {
		signed, err := c.checkpointTenant(ctx, tenantID)
		if err != nil {
			// One tenant's failure must not stop the rest: the tenant most
			// likely to fail here is the one whose chain is already damaged,
			// and that is no reason to leave everyone else unsigned.
			c.log.ErrorContext(ctx, "could not checkpoint tenant",
				slog.String("tenant_id", tenantID), slog.String("error", err.Error()))
			continue
		}
		if signed {
			res.Signed++
		}
	}
	return res, nil
}

func (c *Checkpointer) checkpointTenant(ctx context.Context, tenantID string) (bool, error) {
	records, err := c.store.ListAudit(ctx, tenantID, 0)
	if err != nil {
		return false, err
	}
	if len(records) == 0 {
		return false, nil
	}

	// Sign what the chain actually verifies to, not what the last row claims.
	// Signing a stored hash without recomputing it would mean happily signing a
	// forgery — the exact outcome this whole mechanism exists to prevent.
	v := audit.VerifyChain(tenantID, records)
	if !v.Verified {
		return false, fmt.Errorf("chain is broken at seq %d, refusing to sign it: %s", v.BrokenAt, v.Reason)
	}

	head := records[len(records)-1]
	if latest, err := c.store.LatestCheckpoint(ctx, tenantID); err == nil && latest.Seq >= head.Seq {
		return false, nil // nothing new to sign
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, err
	}

	signed, err := c.signer.Sign(audit.Checkpoint{
		TenantID: tenantID,
		Seq:      head.Seq,
		Hash:     v.LastHash,
		TakenAt:  c.now().UTC(),
	})
	if err != nil {
		return false, err
	}
	if err := c.store.SaveCheckpoint(ctx, signed); err != nil {
		return false, err
	}

	c.log.InfoContext(ctx, "signed the audit chain head",
		slog.String("tenant_id", tenantID),
		slog.Int64("seq", signed.Seq),
		slog.String("public_key", signed.PublicKey))
	return true, nil
}
