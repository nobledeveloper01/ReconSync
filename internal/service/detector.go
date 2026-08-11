// Package service holds the background loops: detection and webhook dispatch.
package service

import (
	"context"
	"errors"
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

// Detector sweeps for transactions whose reconciliation window has closed and
// queues the reversal webhook.
type Detector struct {
	store    store.Store
	log      *slog.Logger
	now      func() time.Time
	interval time.Duration
	batch    int
}

// DetectorOptions configures a Detector.
type DetectorOptions struct {
	Logger   *slog.Logger
	Now      func() time.Time
	Interval time.Duration
	Batch    int
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
}

// Sweep claims expired transactions and queues their notifications.
func (d *Detector) Sweep(ctx context.Context) (SweepResult, error) {
	var res SweepResult

	now := d.now().UTC()
	claimed, err := d.store.ClaimExpired(ctx, now, d.batch)
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
