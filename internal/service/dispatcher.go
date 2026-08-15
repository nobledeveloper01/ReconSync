package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/metrics"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

const (
	// DefaultDispatchInterval is how often the queue is polled.
	DefaultDispatchInterval = time.Second

	// DefaultDispatchBatch bounds one dispatch round.
	DefaultDispatchBatch = 100

	// DefaultLease is how long a claimed delivery is held before another worker
	// may retry it. Longer than the request timeout, so a slow endpoint is not
	// delivered to twice.
	DefaultLease = 60 * time.Second

	// DefaultConcurrency bounds simultaneous outbound requests. Deliveries are
	// I/O bound and one slow endpoint must not stall the rest.
	DefaultConcurrency = 8
)

// SecretResolver turns an endpoint's secret reference into its signing secrets.
// Production resolves through KMS; the reference is what is stored (§5).
type SecretResolver func(ctx context.Context, ref string) ([]string, error)

// Dispatcher delivers queued webhooks with retries and dead-lettering.
type Dispatcher struct {
	store   store.Store
	sender  *webhook.Sender
	secrets SecretResolver
	log     *slog.Logger
	now     func() time.Time
	metrics *metrics.Registry

	interval    time.Duration
	batch       int
	lease       time.Duration
	concurrency int
}

// DispatcherOptions configures a Dispatcher. Secrets is required.
type DispatcherOptions struct {
	Sender      *webhook.Sender
	Secrets     SecretResolver
	Logger      *slog.Logger
	Now         func() time.Time
	Metrics     *metrics.Registry
	Interval    time.Duration
	Batch       int
	Lease       time.Duration
	Concurrency int
}

// NewDispatcher builds a Dispatcher.
func NewDispatcher(s store.Store, opts DispatcherOptions) (*Dispatcher, error) {
	if s == nil {
		return nil, errors.New("service: store is required")
	}
	if opts.Secrets == nil {
		return nil, errors.New("service: secret resolver is required")
	}

	d := &Dispatcher{
		store:       s,
		sender:      opts.Sender,
		secrets:     opts.Secrets,
		log:         opts.Logger,
		now:         opts.Now,
		metrics:     opts.Metrics,
		interval:    opts.Interval,
		batch:       opts.Batch,
		lease:       opts.Lease,
		concurrency: opts.Concurrency,
	}
	if d.sender == nil {
		d.sender = webhook.NewSender(webhook.SenderOptions{})
	}
	if d.log == nil {
		d.log = slog.Default()
	}
	if d.now == nil {
		d.now = time.Now
	}
	if d.interval <= 0 {
		d.interval = DefaultDispatchInterval
	}
	if d.batch <= 0 {
		d.batch = DefaultDispatchBatch
	}
	if d.lease <= 0 {
		d.lease = DefaultLease
	}
	if d.concurrency <= 0 {
		d.concurrency = DefaultConcurrency
	}
	return d, nil
}

// Run dispatches until the context is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := d.Dispatch(ctx); err != nil && !errors.Is(err, context.Canceled) {
				d.log.ErrorContext(ctx, "dispatch round failed", slog.String("error", err.Error()))
			}
		}
	}
}

// DispatchResult reports what one round did.
type DispatchResult struct {
	Claimed     int
	Delivered   int
	Retrying    int
	DeadLetters int
}

// Dispatch claims due deliveries and attempts each one.
func (d *Dispatcher) Dispatch(ctx context.Context) (DispatchResult, error) {
	var res DispatchResult

	due, err := d.store.ClaimDueDeliveries(ctx, d.now().UTC(), d.lease, d.batch)
	if err != nil {
		return res, err
	}
	defer func() {
		d.metrics.RecordDispatch(d.now().UTC(), res.Delivered, res.Retrying, res.DeadLetters)
	}()
	res.Claimed = len(due)
	if len(due) == 0 {
		return res, nil
	}

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, d.concurrency)
	)

	for _, item := range due {
		wg.Add(1)
		go func(item *store.DueDelivery) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			outcome, err := d.attempt(ctx, item)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				d.log.ErrorContext(ctx, "delivery attempt failed",
					slog.Int64("delivery_id", item.ID),
					slog.String("tenant_id", item.TenantID),
					slog.String("error", err.Error()))
				return
			}
			switch outcome {
			case webhook.StatusDelivered:
				res.Delivered++
			case webhook.StatusPending:
				res.Retrying++
			case webhook.StatusDeadLetter:
				res.DeadLetters++
			}
		}(item)
	}
	wg.Wait()

	return res, nil
}

// attempt performs one delivery and writes back what it decided.
func (d *Dispatcher) attempt(ctx context.Context, item *store.DueDelivery) (webhook.DeliveryStatus, error) {
	secrets, err := d.secrets(ctx, item.SecretRef)
	if err != nil {
		return "", err
	}

	delivery := webhook.Delivery{
		ID:            item.ID,
		TenantID:      item.TenantID,
		EndpointID:    item.EndpointID,
		TransactionID: item.TransactionID,
		URL:           item.URL,
		Secrets:       secrets,
		Event:         webhook.EventType(item.EventType),
		Payload:       item.Payload,
		Attempt:       item.Attempt,
	}

	result := d.sender.Send(ctx, delivery)
	outcome := webhook.Decide(delivery, result, d.now().UTC())

	var code *int
	if result.StatusCode != 0 {
		code = &result.StatusCode
	}
	if err := d.store.RecordDeliveryOutcome(ctx, item.ID, store.DeliveryOutcome{
		Status:       string(outcome.Status),
		ResponseCode: code,
		ResponseBody: result.ResponseBody,
		DurationMS:   int(result.Duration.Milliseconds()),
		NextRetryAt:  outcome.NextRetryAt,
	}); err != nil {
		return "", err
	}

	// A dead-lettered reversal means nobody is going to act on it, so the
	// transaction has to say so — it is the state the DLQ alert pages on.
	if outcome.Status == webhook.StatusDeadLetter && delivery.Event == webhook.EventReversalTriggered {
		if _, err := d.store.MarkReversalFailed(ctx, item.TenantID, item.TransactionID, d.now().UTC()); err != nil {
			var ite domain.InvalidTransitionError
			if !errors.Is(err, store.ErrNotFound) && !errors.As(err, &ite) {
				return outcome.Status, err
			}
		}
	}

	return outcome.Status, nil
}
