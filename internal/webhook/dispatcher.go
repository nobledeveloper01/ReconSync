package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxResponseBody is how much of a customer's response we keep for the delivery
// log. Enough to debug, bounded so a large error page cannot fill the database.
const maxResponseBody = 1 << 10 // 1 KiB

// Delivery is one attempt's worth of work.
type Delivery struct {
	ID            int64
	TenantID      string
	EndpointID    string
	TransactionID string
	URL           string
	Secret        string
	Event         EventType
	Payload       []byte
	Attempt       int
}

// Result records what happened, for the delivery log and the retry decision.
type Result struct {
	Delivered    bool
	StatusCode   int
	ResponseBody string
	Duration     time.Duration
	Err          error

	// Retryable reports whether another attempt is worth making at all,
	// independent of whether attempts remain.
	Retryable bool
}

// Sender performs a single delivery attempt.
type Sender struct {
	client    *http.Client
	now       func() time.Time
	userAgent string
}

// SenderOptions configures a Sender.
type SenderOptions struct {
	Client    *http.Client
	Now       func() time.Time
	UserAgent string
}

// NewSender builds a Sender. With no client, one with the SSRF guard is used.
func NewSender(opts SenderOptions) *Sender {
	s := &Sender{client: opts.Client, now: opts.Now, userAgent: opts.UserAgent}
	if s.client == nil {
		s.client = NewClient(TransportOptions{})
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.userAgent == "" {
		s.userAgent = "ReconSync-Webhook/1.0"
	}
	return s
}

// Send performs one attempt. A transport failure is a Result with Err set, not
// an error return: every attempt is a record to be logged either way.
func (s *Sender) Send(ctx context.Context, d Delivery) Result {
	started := s.now()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Payload))
	if err != nil {
		return Result{Err: fmt.Errorf("build request: %w", err), Retryable: false}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set(SignatureHeader, Sign(d.Secret, started, d.Payload))
	req.Header.Set(EventHeader, string(d.Event))
	req.Header.Set(DeliveryHeader, fmt.Sprintf("dlv_%d", d.ID))

	resp, err := s.client.Do(req)
	if err != nil {
		// A blocked private address is a configuration fault, not a transient
		// one: retrying cannot make the endpoint public.
		retryable := !errors.Is(err, ErrPrivateAddress)
		return Result{
			Duration:  s.now().Sub(started),
			Err:       err,
			Retryable: retryable,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))

	return Result{
		Delivered:    resp.StatusCode >= 200 && resp.StatusCode < 300,
		StatusCode:   resp.StatusCode,
		ResponseBody: string(body),
		Duration:     s.now().Sub(started),
		Retryable:    RetryableStatus(resp.StatusCode),
	}
}

// Marshal renders an envelope for delivery.
func Marshal[T Envelope | IntegrationEnvelope](e T) ([]byte, error) {
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("webhook: marshal envelope: %w", err)
	}
	return raw, nil
}

// Outcome is what the dispatcher decided to do with an attempt.
type Outcome struct {
	Status      DeliveryStatus
	NextRetryAt *time.Time
	Result      Result
}

// DeliveryStatus mirrors the webhook_deliveries.status column.
type DeliveryStatus string

const (
	StatusPending    DeliveryStatus = "pending"
	StatusDelivered  DeliveryStatus = "delivered"
	StatusFailed     DeliveryStatus = "failed"
	StatusDeadLetter DeliveryStatus = "dead_letter"
)

// Decide turns an attempt's result into the next state.
//
// Kept separate from Send so the policy — what retries, what dead-letters — is
// testable without a network.
func Decide(d Delivery, res Result, now time.Time) Outcome {
	if res.Delivered {
		return Outcome{Status: StatusDelivered, Result: res}
	}

	// Out of attempts, or the failure is one no retry can fix.
	if !res.Retryable || !ShouldRetry(d.Attempt) {
		return Outcome{Status: StatusDeadLetter, Result: res}
	}

	next := NextRetryAt(now, d.Attempt+1)
	return Outcome{Status: StatusPending, NextRetryAt: &next, Result: res}
}
