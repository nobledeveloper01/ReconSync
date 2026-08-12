// Package service holds the background loops: detection and webhook dispatch.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// DefaultDetectInterval is the §4.4 poll period. Detection granularity equals
// this interval, which is the cost of a poll-based scheduler and the reason the
// SLO is 60s rather than 5s.
const DefaultDetectInterval = 5 * time.Second

// DefaultDetectBatch bounds one sweep.
const DefaultDetectBatch = 500

// Defaults for the silence check.
//
// A tenant that normally sends events steadily and has sent nothing for
// DefaultQuietPeriod has a broken integration, not a quiet spell. Nothing can be
// concluded about their individual transactions until it is fixed.
//
// The baseline requirement is what keeps a genuinely low-volume tenant from
// being mistaken for a broken one — a tenant sending four events a day is not
// silent between them, and suppressing their detection would be a bug.
const (
	DefaultQuietPeriod      = 5 * time.Minute
	DefaultBaselinePeriod   = time.Hour
	DefaultMinActiveBuckets = 10
)

// Detector sweeps for transactions whose reconciliation window has closed and
// queues the reversal webhook.
type Detector struct {
	store    store.Store
	log      *slog.Logger
	now      func() time.Time
	interval time.Duration
	batch    int
	silence  store.SilenceParams
}

// DetectorOptions configures a Detector.
type DetectorOptions struct {
	Logger   *slog.Logger
	Now      func() time.Time
	Interval time.Duration
	Batch    int

	// Silence tunes when a tenant is considered too quiet to judge. A zero
	// Quiet, Baseline or MinActiveBuckets disables the check entirely.
	Silence *store.SilenceParams
}

// NewDetector builds a Detector.
func NewDetector(s store.Store, opts DetectorOptions) (*Detector, error) {
	if s == nil {
		return nil, errors.New("service: store is required")
	}
	d := &Detector{store: s, log: opts.Logger, now: opts.Now, interval: opts.Interval, batch: opts.Batch}
	if d.log == nil {
		d.log = slog.Default()
	}
	if d.now == nil {
		d.now = time.Now
	}
	if d.interval <= 0 {
		d.interval = DefaultDetectInterval
	}
	if d.batch <= 0 {
		d.batch = DefaultDetectBatch
	}
	if opts.Silence != nil {
		d.silence = *opts.Silence
	} else {
		d.silence = store.SilenceParams{
			Quiet:            DefaultQuietPeriod,
			Baseline:         DefaultBaselinePeriod,
			MinActiveBuckets: DefaultMinActiveBuckets,
		}
	}
	return d, nil
}

// Run sweeps until the context is cancelled.
func (d *Detector) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := d.Sweep(ctx); err != nil && !errors.Is(err, context.Canceled) {
				// A failed sweep is retried on the next tick rather than
				// stopping the loop: the scheduler stalling is what the
				// detection-lag alert pages on.
				d.log.ErrorContext(ctx, "detection sweep failed", slog.String("error", err.Error()))
			}
		}
	}
}

// SweepResult reports what one sweep did.
type SweepResult struct {
	Claimed  int
	Queued   int
	Suspect  int
	NoTarget int // orphaned but the tenant has no enabled endpoint

	// SilentTenants were skipped entirely because they have stopped sending.
	SilentTenants []string
}

// Sweep claims expired transactions and queues their notifications.
func (d *Detector) Sweep(ctx context.Context) (SweepResult, error) {
	var res SweepResult

	now := d.now().UTC()

	// A tenant that has stopped sending tells us nothing about any individual
	// transaction. Sweeping them anyway would fire a reversal for every debit
	// in flight — thousands of them, into a system that is already broken, at
	// the worst possible moment. Skip them and raise one alert instead.
	silent, err := d.store.SilentTenants(ctx, now, d.silence)
	if err != nil {
		return res, fmt.Errorf("check for silent tenants: %w", err)
	}
	res.SilentTenants = silent

	for _, tenantID := range silent {
		d.log.WarnContext(ctx, "tenant has stopped sending events; detection suppressed",
			slog.String("tenant_id", tenantID),
			slog.Duration("quiet_for_at_least", d.silence.Quiet))
	}

	var opts []store.ClaimOption
	if len(silent) > 0 {
		opts = append(opts, store.SkipTenants(silent...))
	}

	claimed, err := d.store.ClaimExpired(ctx, now, d.batch, opts...)
	if err != nil {
		return res, err
	}
	res.Claimed = len(claimed)

	// Endpoints are looked up once per tenant per sweep, not once per
	// transaction: a large sweep is usually one tenant having a bad minute.
	endpoints := map[string][]*store.WebhookEndpoint{}

	for _, txn := range claimed {
		event := webhook.EventReversalTriggered
		if txn.Status == domain.StatusSuspect {
			// Ambiguous transactions raise an investigation, never a reversal —
			// reversing a credit that actually succeeded pays the customer twice.
			event = webhook.EventTransactionSuspect
			res.Suspect++
		}

		// Backfilled events are correlated and stored but never notify (§3.2 A3).
		if txn.IsBackfill {
			continue
		}

		eps, ok := endpoints[txn.TenantID]
		if !ok {
			eps, err = d.store.ListEndpoints(ctx, txn.TenantID)
			if err != nil {
				return res, err
			}
			endpoints[txn.TenantID] = eps
		}

		queued, err := d.queue(ctx, txn, event, eps)
		if err != nil {
			return res, err
		}
		if queued == 0 {
			res.NoTarget++
			continue
		}
		res.Queued += queued
	}

	return res, nil
}

func (d *Detector) queue(ctx context.Context, txn *domain.Transaction, event webhook.EventType, eps []*store.WebhookEndpoint) (int, error) {
	payload, err := webhook.Marshal(webhook.EnvelopeFor(event, txn, d.now().UTC()))
	if err != nil {
		return 0, err
	}

	queued := 0
	for _, ep := range eps {
		if !ep.Enabled || !subscribes(ep, event) {
			continue
		}
		if _, err := d.store.EnqueueDelivery(ctx, txn.TenantID, &store.PendingDelivery{
			TenantID:      txn.TenantID,
			EndpointID:    ep.ID,
			TransactionID: txn.TransactionID,
			EventType:     string(event),
			Payload:       payload,
		}); err != nil {
			return queued, err
		}
		queued++
	}

	// Only an orphan moves to reversal_pending, and only once something is
	// actually queued to deliver. A suspect stays suspect until investigated.
	if queued > 0 && event == webhook.EventReversalTriggered {
		if _, err := d.store.MarkReversalPending(ctx, txn.TenantID, txn.TransactionID, d.now().UTC()); err != nil {
			var ite domain.InvalidTransitionError
			if !errors.As(err, &ite) {
				return queued, err
			}
			// Another replica got there first; the delivery is queued either way.
		}
	}
	return queued, nil
}

// subscribes reports whether an endpoint wants this event. An empty list means
// all events, which is what a first-run endpoint registration gets.
func subscribes(ep *store.WebhookEndpoint, event webhook.EventType) bool {
	if len(ep.Events) == 0 {
		return true
	}
	for _, e := range ep.Events {
		if e == string(event) {
			return true
		}
	}
	return false
}
