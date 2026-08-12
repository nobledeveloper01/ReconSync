// Package service holds the background loops: detection and webhook dispatch.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/evidence"
	"github.com/nobledeveloper01/ReconSync/internal/provider"
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
	store     store.Store
	log       *slog.Logger
	now       func() time.Time
	interval  time.Duration
	batch     int
	silence   store.SilenceParams
	providers *provider.Registry
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

	// Providers corroborates an orphan against the rail before any reversal is
	// queued. Optional, and opt-in for a reason: with no adapter registered
	// every answer is Unknown, which would send every orphan to suspect and
	// stop reversals altogether. Nil means detection behaves as it always has.
	Providers *provider.Registry
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
	d.providers = opts.Providers
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

	// SilenceAlerts and RecoveryAlerts are webhooks queued about the stream
	// itself, one per episode rather than one per sweep.
	SilenceAlerts  int
	RecoveryAlerts int

	// Corroboration outcomes, when a provider registry is configured.
	Settled      int // the rail confirmed arrival, so no reversal was sent
	Corroborated int // the rail confirmed failure, so the orphan has evidence
	Uncertain    int // no answer, so it went to a human instead of a reversal
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

	// Suppressing detection silently would be the worst of both worlds: nothing
	// is being watched and nobody has been told. Tell them.
	if err := d.alertOnSilence(ctx, silent, now, &res); err != nil {
		return res, err
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
		// Every verdict carries what it rests on, so a receiver can tell
		// "we checked with the rail" apart from "we heard nothing".
		ev := d.baseEvidence(ctx, txn)

		// Ask the rail before anything leaves the building. This runs after the
		// claim, so two replicas cannot both corroborate, and before the queue,
		// so a wrong verdict never reaches the customer.
		settled, err := d.corroborate(ctx, txn, now, &res, ev)
		if err != nil {
			return res, err
		}
		if settled {
			continue // the rail says it arrived, so there is nothing to send
		}

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

		d.recordVerdict(ctx, txn, event, ev)

		queued, err := d.queue(ctx, txn, event, eps, ev)
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

// alertOnSilence tells a tenant their stream has stopped, once per episode.
//
// The alert goes to the endpoint they registered, which may well be part of the
// same system that has gone quiet. That is worth doing anyway: the failure is
// usually one direction of one integration, not the whole stack, and the log
// line plus the metric cover the case where it is not.
//
// A failure here is logged, never fatal. Losing an alert is bad; abandoning the
// sweep — and with it every real orphan waiting to be detected — is worse.
func (d *Detector) alertOnSilence(ctx context.Context, silent []string, now time.Time, res *SweepResult) error {
	// With the check disabled every tenant looks recovered, and closing the
	// episodes would fire an all-clear nobody earned.
	if d.silence.Quiet <= 0 || d.silence.Baseline <= 0 || d.silence.MinActiveBuckets <= 0 {
		return nil
	}

	change, err := d.store.SyncSilenceEpisodes(ctx, silent, now)
	if err != nil {
		return fmt.Errorf("sync silence episodes: %w", err)
	}

	for _, ep := range change.Opened {
		env := webhook.SilenceEnvelope(ep.TenantID, ep.SilentSince, now)
		if d.queueIntegrationAlert(ctx, ep.TenantID, env) {
			res.SilenceAlerts++
		}
		d.recordSilence(ctx, audit.EventSilenceDetected, ep, now)
	}

	for _, ep := range change.Recovered {
		env := webhook.RecoveryEnvelope(ep.TenantID, ep.SilentSince, now)
		if d.queueIntegrationAlert(ctx, ep.TenantID, env) {
			res.RecoveryAlerts++
		}
		d.log.InfoContext(ctx, "tenant is sending events again; detection resumed",
			slog.String("tenant_id", ep.TenantID),
			slog.Duration("silent_for", now.Sub(ep.SilentSince)))
		d.recordSilence(ctx, audit.EventSilenceResolved, ep, now)
	}
	return nil
}

// queueIntegrationAlert delivers to every subscribed endpoint and reports
// whether anything was queued.
func (d *Detector) queueIntegrationAlert(ctx context.Context, tenantID string, env webhook.IntegrationEnvelope) bool {
	payload, err := webhook.Marshal(env)
	if err != nil {
		d.log.ErrorContext(ctx, "could not build the integration alert",
			slog.String("tenant_id", tenantID), slog.String("error", err.Error()))
		return false
	}

	eps, err := d.store.ListEndpoints(ctx, tenantID)
	if err != nil {
		d.log.ErrorContext(ctx, "could not load endpoints for the integration alert",
			slog.String("tenant_id", tenantID), slog.String("error", err.Error()))
		return false
	}

	queued := false
	for _, ep := range eps {
		if !ep.Enabled || !subscribes(ep, env.Event) {
			continue
		}
		// TransactionID is deliberately empty: this event is about the stream,
		// not about any one transaction.
		if _, err := d.store.EnqueueDelivery(ctx, tenantID, &store.PendingDelivery{
			TenantID:   tenantID,
			EndpointID: ep.ID,
			EventType:  string(env.Event),
			Payload:    payload,
		}); err != nil {
			d.log.ErrorContext(ctx, "could not queue the integration alert",
				slog.String("tenant_id", tenantID),
				slog.String("endpoint_id", ep.ID),
				slog.String("error", err.Error()))
			continue
		}
		queued = true
	}
	return queued
}

// recordSilence puts the episode on the audit chain. "Why did you stop watching
// our transactions for two hours" is a question that must be answerable.
func (d *Detector) recordSilence(ctx context.Context, event string, ep store.SilenceEpisode, now time.Time) {
	if _, err := d.store.AppendAudit(ctx, ep.TenantID, &audit.Record{
		EventType:  event,
		OccurredAt: now.UTC(),
		Actor:      map[string]any{"type": "system", "id": "detection-sweep"},
		Subject:    map[string]any{"type": "tenant", "id": ep.TenantID},
		Payload: map[string]any{
			"silent_since":       ep.SilentSince.UTC(),
			"silent_for_seconds": int(now.Sub(ep.SilentSince) / time.Second),
			"quiet_threshold_s":  int(d.silence.Quiet / time.Second),
		},
	}); err != nil {
		d.log.ErrorContext(ctx, "could not record the silence episode on the audit chain",
			slog.String("tenant_id", ep.TenantID), slog.String("error", err.Error()))
	}
}

// corroborate asks the rail what actually happened to a freshly claimed orphan
// and applies the answer. It reports whether the transaction settled, in which
// case there is nothing to notify anyone about.
//
// Only orphans are corroborated. A suspect transaction is already destined for a
// human, and asking again would not change that.
// baseEvidence records what we knew before asking anyone: that the window
// closed with no credit, and whether our own view of the stream was intact.
//
// Silence carries well under half the available confidence on purpose. Without
// corroboration a reversal tops out around 0.70, which is the honest number for
// "we inferred this".
func (d *Detector) baseEvidence(ctx context.Context, txn *domain.Transaction) *evidence.Set {
	ev := evidence.New()
	ev.Add(evidence.SignalWindowExpired,
		fmt.Sprintf("no credit within %ds", int(txn.Window()/time.Second)),
		evidence.WeightWindowExpired)

	gap, err := d.store.HasIngestGap(ctx, txn.TenantID, txn.DebitAt, txn.ExpectedCompletionAt)
	switch {
	case err != nil:
		// Unable to check, so we cannot claim the view was intact. Record the
		// uncertainty rather than silently crediting ourselves the weight.
		ev.Add(evidence.SignalIngestGap, "could not verify ingest health", evidence.WeightIngestGap)
	case gap:
		ev.Add(evidence.SignalIngestGap, "events were dropped over this window", evidence.WeightIngestGap)
	default:
		ev.Add(evidence.SignalIngestIntact, "no events lost over this window", evidence.WeightIngestIntact)
	}
	return ev
}

func (d *Detector) corroborate(ctx context.Context, txn *domain.Transaction, now time.Time, res *SweepResult, ev *evidence.Set) (bool, error) {
	if d.providers == nil || txn.Status != domain.StatusOrphaned {
		return false, nil
	}

	status := d.providers.Query(ctx, provider.Ref{
		TenantID:      txn.TenantID,
		TransactionID: txn.TransactionID,
		Provider:      txn.Provider,
		AmountMinor:   txn.AmountMinor,
		Currency:      txn.Currency,
	})

	switch status.Outcome {
	case provider.Settled:
		// It arrived after all. Reversing now would take money back that the
		// destination already has.
		if _, err := d.store.MarkSettled(ctx, txn.TenantID, txn.TransactionID, now); err != nil {
			return false, d.tolerateRace(ctx, txn, "settle", err)
		}
		res.Settled++
		d.log.InfoContext(ctx, "rail confirmed the credit arrived; no reversal sent",
			slog.String("tenant_id", txn.TenantID),
			slog.String("transaction_id", txn.TransactionID),
			slog.String("provider", status.Provider))
		return true, nil

	case provider.Unknown:
		// We could not establish anything. A guess here moves real money, so it
		// goes to a human instead. Recorded with zero weight so the receiver can
		// see we tried and failed, rather than that we never asked.
		ev.Add(evidence.SignalProviderUnreachable, status.Detail, evidence.WeightProviderUnreached)

		if _, err := d.store.MarkUncertain(ctx, txn.TenantID, txn.TransactionID, now); err != nil {
			return false, d.tolerateRace(ctx, txn, "mark uncertain", err)
		}
		txn.Status = domain.StatusSuspect
		res.Uncertain++
		d.log.WarnContext(ctx, "could not corroborate with the rail; raising an investigation",
			slog.String("tenant_id", txn.TenantID),
			slog.String("transaction_id", txn.TransactionID),
			slog.String("provider", status.Provider),
			slog.String("detail", status.Detail))
		return false, nil

	case provider.Failed:
		// The only signal in the system that is evidence rather than inference.
		ev.Add(evidence.SignalProviderFailed, status.Provider+" reports the credit leg failed",
			evidence.WeightProviderFailed)
		res.Corroborated++
		return false, nil

	default: // NotFound
		ev.Add(evidence.SignalProviderNotFound, status.Provider+" has no record of this transaction",
			evidence.WeightProviderNotFound)
		res.Corroborated++
		return false, nil
	}
}

// recordVerdict writes the decision and its evidence to the tenant's audit
// chain, so "why did you reverse this" is answerable months later.
//
// A failed append is logged, never fatal. Losing an audit record is bad; refusing
// to reverse a transaction because we could not write one would leave a customer
// out of pocket, which is worse. The alert is the mitigation.
func (d *Detector) recordVerdict(ctx context.Context, txn *domain.Transaction, event webhook.EventType, ev *evidence.Set) {
	signals := make([]map[string]any, 0, len(ev.Signals()))
	for _, s := range ev.Signals() {
		signals = append(signals, map[string]any{
			"signal": s.Name,
			"value":  s.Value,
			"weight": s.Weight,
		})
	}

	_, err := d.store.AppendAudit(ctx, txn.TenantID, &audit.Record{
		EventType:  audit.EventDetected,
		OccurredAt: d.now().UTC(),
		Actor:      map[string]any{"type": "system", "id": "detection-sweep"},
		Subject:    map[string]any{"type": "transaction", "id": txn.TransactionID},
		Payload: map[string]any{
			"verdict":        string(txn.Status),
			"event":          string(event),
			"confidence":     ev.Confidence(),
			"evidence":       signals,
			"amount_minor":   txn.AmountMinor,
			"currency":       txn.Currency,
			"window_seconds": int(txn.Window() / time.Second),
		},
	})
	if err != nil {
		d.log.ErrorContext(ctx, "could not record the verdict on the audit chain",
			slog.String("tenant_id", txn.TenantID),
			slog.String("transaction_id", txn.TransactionID),
			slog.String("error", err.Error()))
	}
}

// tolerateRace swallows an illegal-transition error, which here means another
// replica moved the transaction first. Anything else is a real failure.
func (d *Detector) tolerateRace(ctx context.Context, txn *domain.Transaction, action string, err error) error {
	var ite domain.InvalidTransitionError
	if errors.As(err, &ite) || errors.Is(err, store.ErrNotFound) {
		d.log.DebugContext(ctx, "another replica moved this transaction first",
			slog.String("transaction_id", txn.TransactionID),
			slog.String("action", action))
		return nil
	}
	return fmt.Errorf("%s %s: %w", action, txn.TransactionID, err)
}

func (d *Detector) queue(ctx context.Context, txn *domain.Transaction, event webhook.EventType, eps []*store.WebhookEndpoint, ev *evidence.Set) (int, error) {
	payload, err := webhook.Marshal(webhook.EnvelopeFor(event, txn, d.now().UTC(), ev))
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
