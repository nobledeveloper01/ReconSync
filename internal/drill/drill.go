// Package drill exercises the reversal path on demand.
//
// The webhook a customer receives is the one code path that only ever runs
// during an incident. Six quiet months later the first real reversal arrives at
// a handler that was refactored in month two, and nobody finds out until it
// matters. A fire drill is how they find out on a Tuesday afternoon instead.
//
// It is synchronous and reports what their endpoint actually did, because an
// integration test whose result you have to go looking for does not get run.
package drill

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// TransactionPrefix marks a drill's transaction id. It is deliberately obvious
// in a log line: anyone reading one of these should be able to tell at a glance
// that no real money is involved.
const TransactionPrefix = "drill_"

// DefaultTimeout bounds one endpoint. Short on purpose — a handler that takes
// longer than this during a real incident is already a problem worth reporting.
const DefaultTimeout = 10 * time.Second

// Result is what one endpoint did.
type Result struct {
	EndpointID string `json:"endpoint_id"`
	URL        string `json:"url"`

	// Passed means the endpoint accepted the webhook. It is not a claim that
	// they handled it correctly — only they can verify that.
	Passed bool `json:"passed"`

	StatusCode   int    `json:"status_code,omitempty"`
	LatencyMS    int64  `json:"latency_ms"`
	Error        string `json:"error,omitempty"`
	ResponseBody string `json:"response_body,omitempty"`

	// Diagnosis translates the outcome into what to do about it.
	Diagnosis string `json:"diagnosis"`
}

// Report is the whole drill.
type Report struct {
	TenantID      string    `json:"tenant_id"`
	TransactionID string    `json:"transaction_id"`
	RanAt         time.Time `json:"ran_at"`

	Endpoints int `json:"endpoints_tested"`
	Passed    int `json:"passed"`
	Failed    int `json:"failed"`

	Results []Result `json:"results"`

	// Notice restates the safety property in the response itself, because the
	// person reading it is deciding whether it was safe to run.
	Notice string `json:"notice"`
}

// Sender performs one delivery attempt. Satisfied by webhook.Sender, so a drill
// travels the same signing, transport and SSRF-guarded path as a real reversal
// — testing a different path would prove nothing about the real one.
type Sender interface {
	Send(ctx context.Context, d webhook.Delivery) webhook.Result
}

// Runner executes drills.
type Runner struct {
	store   store.WebhookStore
	sender  Sender
	secrets func(ctx context.Context, ref string) (string, error)
	now     func() time.Time
	timeout time.Duration
}

// Options configures a Runner. Store, Sender and Secrets are required.
type Options struct {
	Store   store.WebhookStore
	Sender  Sender
	Secrets func(ctx context.Context, ref string) (string, error)
	Now     func() time.Time
	Timeout time.Duration
}

// New builds a Runner.
func New(opts Options) (*Runner, error) {
	if opts.Store == nil {
		return nil, errors.New("drill: store is required")
	}
	if opts.Sender == nil {
		return nil, errors.New("drill: sender is required")
	}
	if opts.Secrets == nil {
		return nil, errors.New("drill: secret resolver is required")
	}

	r := &Runner{store: opts.Store, sender: opts.Sender, secrets: opts.Secrets,
		now: opts.Now, timeout: opts.Timeout}
	if r.now == nil {
		r.now = time.Now
	}
	if r.timeout <= 0 {
		r.timeout = DefaultTimeout
	}
	return r, nil
}

// ErrNoEndpoint means there is nothing to drill.
var ErrNoEndpoint = errors.New("drill: no enabled endpoint subscribes to reversal.triggered")

// Run delivers a synthetic reversal to every endpoint that would receive a real
// one, and reports what each did.
//
// Nothing is written: no transaction, no delivery row, no state change. A drill
// that left rows behind would contaminate the compliance report it exists to
// support, and the whole point is that it is safe to run in production.
func (r *Runner) Run(ctx context.Context, tenantID string) (Report, error) {
	now := r.now().UTC()

	txnID, err := syntheticID()
	if err != nil {
		return Report{}, err
	}

	rep := Report{
		TenantID:      tenantID,
		TransactionID: txnID,
		RanAt:         now,
		Results:       []Result{},
		Notice: "synthetic transaction; no money is involved and nothing was recorded. " +
			"Your handler must refuse any payload carrying \"drill\": true.",
	}

	endpoints, err := r.store.ListEndpoints(ctx, tenantID)
	if err != nil {
		return Report{}, fmt.Errorf("drill: list endpoints: %w", err)
	}

	payload, err := webhook.Marshal(Envelope(tenantID, txnID, now))
	if err != nil {
		return Report{}, err
	}

	var targets []*store.WebhookEndpoint
	for _, ep := range endpoints {
		if ep.Enabled && subscribes(ep) {
			targets = append(targets, ep)
		}
	}
	if len(targets) == 0 {
		return Report{}, ErrNoEndpoint
	}

	// Concurrently, so the whole drill is bounded by the slowest endpoint rather
	// than the sum of them. This is a synchronous request: a tenant with five
	// slow endpoints must not time out the caller.
	results := make([]Result, len(targets))
	var wg sync.WaitGroup
	for i, ep := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = r.deliver(ctx, tenantID, txnID, ep, payload)
		}()
	}
	wg.Wait()

	rep.Results = results
	rep.Endpoints = len(results)
	for _, res := range rep.Results {
		if res.Passed {
			rep.Passed++
			continue
		}
		rep.Failed++
	}
	return rep, nil
}

func (r *Runner) deliver(ctx context.Context, tenantID, txnID string, ep *store.WebhookEndpoint, payload []byte) Result {
	res := Result{EndpointID: ep.ID, URL: ep.URL}

	secret, err := r.secrets(ctx, ep.SecretRef)
	if err != nil {
		res.Error = "could not resolve the signing secret"
		res.Diagnosis = "the drill never left ReconSync; this is our fault, not yours"
		return res
	}

	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	out := r.sender.Send(ctx, webhook.Delivery{
		TenantID:      tenantID,
		EndpointID:    ep.ID,
		TransactionID: txnID,
		URL:           ep.URL,
		Secret:        secret,
		Event:         webhook.EventReversalTriggered,
		Payload:       payload,
		Drill:         true,
	})

	res.LatencyMS = out.Duration.Milliseconds()
	res.StatusCode = out.StatusCode
	res.ResponseBody = out.ResponseBody
	res.Passed = out.Delivered
	if out.Err != nil {
		res.Error = out.Err.Error()
	}
	res.Diagnosis = diagnose(out)
	return res
}

// diagnose says what the result means for the next real incident, which is the
// only reason anyone runs this.
func diagnose(out webhook.Result) string {
	switch {
	case out.Err != nil:
		return "we could not reach your endpoint at all. A real reversal would retry and then dead-letter"
	case out.Delivered:
		return "your endpoint accepted the reversal. Check your own logs that it was recorded, not just acknowledged"
	case out.StatusCode == 401 || out.StatusCode == 403:
		return "your endpoint rejected the signature. Verify your handler is using the current signing secret"
	case out.StatusCode == 404:
		return "your endpoint is not there. A real reversal would never arrive"
	case out.StatusCode >= 500:
		return "your handler errored on a valid reversal. This is the failure the drill exists to find"
	default:
		return fmt.Sprintf("your endpoint refused the reversal with %d", out.StatusCode)
	}
}

// Envelope builds the synthetic payload. Exported so tests can assert on the
// exact bytes a customer receives.
//
// It is a real reversal.triggered in every respect but one: the drill flag. A
// payload that differed in shape would test a shape nobody ever receives.
func Envelope(tenantID, txnID string, now time.Time) webhook.Envelope {
	debitAt := now.Add(-5 * time.Minute)

	env := webhook.EnvelopeFor(webhook.EventReversalTriggered, &domain.Transaction{
		TenantID:      tenantID,
		TransactionID: txnID,
		// A realistic amount on purpose: a handler that takes a shortcut on
		// zero would pass a drill it should have failed.
		AmountMinor:          5_000_000,
		Currency:             "NGN",
		Status:               domain.StatusOrphaned,
		DebitAt:              debitAt,
		ExpectedCompletionAt: now,
		DetectedAt:           &now,
	}, now, nil)

	env.Data.Drill = true
	env.Data.Reason = "fire_drill"
	env.Data.Confidence = 0
	return env
}

func subscribes(ep *store.WebhookEndpoint) bool {
	if len(ep.Events) == 0 {
		return true
	}
	for _, e := range ep.Events {
		if e == string(webhook.EventReversalTriggered) {
			return true
		}
	}
	return false
}

// syntheticID is random rather than sequential so a drill can never collide with
// a real transaction id, in our store or in the customer's.
func syntheticID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("drill: generate id: %w", err)
	}
	return TransactionPrefix + hex.EncodeToString(b[:]), nil
}
