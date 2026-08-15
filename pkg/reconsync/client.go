package reconsync

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

// Client reports transaction legs to ReconSync.
//
// The whole surface is four calls, because the integration is deliberately
// small: tell us about the debit, tell us the verdict, and verify the webhook
// that may come back.

// Client is safe for concurrent use.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client

	maxAttempts int
	backoff     time.Duration
	userAgent   string
	now         func() time.Time
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient supplies your own, for proxies, tracing or a custom transport.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) {
		if h != nil {
			c.http = h
		}
	}
}

// WithMaxAttempts bounds retries. One means no retry.
func WithMaxAttempts(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.maxAttempts = n
		}
	}
}

// WithUserAgent identifies your service in ReconSync's logs, which is what makes
// a support conversation about a misbehaving integration short.
func WithUserAgent(ua string) Option {
	return func(c *Client) {
		if ua != "" {
			c.userAgent = ua
		}
	}
}

// New builds a client.
//
// The default timeout is deliberately short. This call sits beside a money
// movement, and a reconciliation service having a slow day must never become
// the reason a customer's transfer hangs.
func New(baseURL, apiKey string, opts ...Option) (*Client, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	switch {
	case baseURL == "":
		return nil, errors.New("reconsync: base URL is required")
	case !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://"):
		return nil, fmt.Errorf("reconsync: base URL must start with http:// or https://, got %q", baseURL)
	case apiKey == "":
		return nil, errors.New("reconsync: api key is required")
	}

	c := &Client{
		baseURL:     baseURL,
		apiKey:      apiKey,
		http:        &http.Client{Timeout: 5 * time.Second},
		maxAttempts: 3,
		backoff:     100 * time.Millisecond,
		userAgent:   "reconsync-go",
		now:         time.Now,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Debit is one outgoing leg: money has left the customer.
type Debit struct {
	// TransactionID is yours. It is what every later call and every webhook
	// refers to, so it must be the id your own ledger uses.
	TransactionID string `json:"transaction_id"`

	// IdempotencyKey makes a retry safe. Left empty, the client derives one
	// from the transaction id, so a network timeout followed by a retry cannot
	// register the same debit twice.
	IdempotencyKey string `json:"idempotency_key"`

	TransactionType string         `json:"transaction_type"`
	Provider        string         `json:"provider,omitempty"`
	AmountMinor     int64          `json:"amount_minor"`
	Currency        string         `json:"currency"`
	DebitAt         time.Time      `json:"debit_at"`
	CustomerRef     string         `json:"customer_ref"`
	Metadata        map[string]any `json:"metadata,omitempty"`

	// ExpectedCreditMinor is what should arrive when fees are deducted in
	// flight. Zero means the full amount is expected.
	ExpectedCreditMinor int64 `json:"expected_credit_minor,omitempty"`

	// Backfill marks historical data being replayed. Backfilled transactions
	// are reported on but never trigger a reversal — replaying last quarter
	// must not fire ten thousand webhooks.
	Backfill bool `json:"backfill,omitempty"`
}

// Accepted is the answer to a debit: the window it was granted.
type Accepted struct {
	Status               string    `json:"status"`
	TransactionID        string    `json:"transaction_id"`
	ExpectedCompletionAt time.Time `json:"expected_completion_at"`
	WindowSeconds        int       `json:"window_seconds"`
}

// CreditStatus is the verdict on a transaction.
type CreditStatus string

const (
	// CreditSuccess means the money arrived. The transaction is closed.
	CreditSuccess CreditStatus = "success"

	// CreditFailed means the rail said it definitively did not arrive. This
	// orphans immediately rather than waiting out the window: waiting would
	// spend regulatory clock to learn what is already known.
	CreditFailed CreditStatus = "failed"

	// CreditUnknown is the honest answer to a timeout, and the case this
	// product exists for. Do not guess.
	CreditUnknown CreditStatus = "unknown"
)

// Credit is the verdict on a transaction.
type Credit struct {
	TransactionID     string       `json:"transaction_id"`
	IdempotencyKey    string       `json:"idempotency_key"`
	CreditAt          time.Time    `json:"credit_at"`
	ProviderReference string       `json:"provider_reference,omitempty"`
	Status            CreditStatus `json:"status"`

	// AmountMinor reports a partial settlement. Zero means the whole expected
	// amount arrived, which is the ordinary case.
	AmountMinor int64  `json:"amount_minor,omitempty"`
	Currency    string `json:"currency,omitempty"`
}

// ReportDebit registers an outgoing leg and returns the window it was granted.
func (c *Client) ReportDebit(ctx context.Context, d Debit) (*Accepted, error) {
	if d.TransactionID == "" {
		return nil, errors.New("reconsync: transaction id is required")
	}
	if d.IdempotencyKey == "" {
		d.IdempotencyKey = "debit-" + d.TransactionID
	}
	if d.DebitAt.IsZero() {
		d.DebitAt = c.now().UTC()
	}

	var out Accepted
	if err := c.post(ctx, "/v1/events/debit", d, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReportCredit records the verdict.
func (c *Client) ReportCredit(ctx context.Context, cr Credit) error {
	if cr.TransactionID == "" {
		return errors.New("reconsync: transaction id is required")
	}
	if cr.IdempotencyKey == "" {
		// Keyed on the verdict as well as the id: a transaction can legitimately
		// go unknown and then succeed, and those are two distinct events.
		cr.IdempotencyKey = fmt.Sprintf("credit-%s-%s", cr.TransactionID, cr.Status)
	}
	if cr.CreditAt.IsZero() {
		cr.CreditAt = c.now().UTC()
	}
	if cr.Status == "" {
		return errors.New("reconsync: credit status is required: success, failed or unknown")
	}
	return c.post(ctx, "/v1/events/credit", cr, nil)
}

// ReportReversalCompleted closes the loop after you have reversed.
//
// Without this the transaction stays outstanding on the compliance report
// forever: ReconSync advises the reversal but never sees it happen, so nothing
// else can tell it the money came back.
func (c *Client) ReportReversalCompleted(ctx context.Context, transactionID string, at time.Time) error {
	if transactionID == "" {
		return errors.New("reconsync: transaction id is required")
	}
	if at.IsZero() {
		at = c.now().UTC()
	}
	return c.post(ctx, "/v1/events/reversal-completed", map[string]any{
		"transaction_id": transactionID,
		"completed_at":   at,
	}, nil)
}

// BulkEvent is one entry in a bulk submission.
type BulkEvent struct {
	Type string `json:"type"` // "debit" or "credit"

	TransactionID   string         `json:"transaction_id"`
	IdempotencyKey  string         `json:"idempotency_key"`
	TransactionType string         `json:"transaction_type,omitempty"`
	Provider        string         `json:"provider,omitempty"`
	AmountMinor     int64          `json:"amount_minor,omitempty"`
	Currency        string         `json:"currency,omitempty"`
	DebitAt         time.Time      `json:"debit_at,omitempty"`
	CustomerRef     string         `json:"customer_ref,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	Backfill        bool           `json:"backfill,omitempty"`

	CreditAt          time.Time    `json:"credit_at,omitempty"`
	ProviderReference string       `json:"provider_reference,omitempty"`
	CreditStatus      CreditStatus `json:"status,omitempty"`
}

// BulkResult reports what was taken and what was refused.
type BulkResult struct {
	Accepted int `json:"accepted"`
	Rejected []struct {
		Index int    `json:"index"`
		Code  string `json:"code"`
		Error string `json:"error"`
		Field string `json:"field,omitempty"`
	} `json:"rejected,omitempty"`
}

// MaxBulkEvents is the server's ceiling on one bulk call.
const MaxBulkEvents = 1000

// Bulk submits up to MaxBulkEvents at once, for backfill.
//
// Partial acceptance is the norm: valid events are taken and invalid ones
// listed by index, so one malformed row in a historical export does not reject
// the other nine hundred.
func (c *Client) Bulk(ctx context.Context, events []BulkEvent) (*BulkResult, error) {
	if len(events) == 0 {
		return &BulkResult{}, nil
	}
	if len(events) > MaxBulkEvents {
		return nil, fmt.Errorf("reconsync: %d events exceeds the maximum of %d per call",
			len(events), MaxBulkEvents)
	}

	var out BulkResult
	if err := c.post(ctx, "/v1/events/bulk", map[string]any{"events": events}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// APIError is a refusal from the server, with the code and field it named.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Field      string
	RequestID  string
}

func (e *APIError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("reconsync: %s (%s, field %s)", e.Message, e.Code, e.Field)
	}
	return fmt.Sprintf("reconsync: %s (%s)", e.Message, e.Code)
}

// Retryable reports whether sending the same request again could succeed.
func (e *APIError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

func (c *Client) post(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("reconsync: encode request: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		if attempt > 1 {
			// Exponential with jitter. Without jitter, a fleet that all timed
			// out on the same server retries in lockstep and keeps it down.
			wait := time.Duration(float64(c.backoff) * math.Pow(2, float64(attempt-2)))
			if err := sleep(ctx, wait+jitter(wait)); err != nil {
				return err
			}
		}

		err := c.attempt(ctx, path, raw, out)
		if err == nil {
			return nil
		}
		lastErr = err

		var apiErr *APIError
		if errors.As(err, &apiErr) && !apiErr.Retryable() {
			// A 400 says the request is wrong. Sending it again more slowly
			// will not make it right.
			return err
		}
		if ctx.Err() != nil {
			return err
		}
	}
	return lastErr
}

func (c *Client) attempt(ctx context.Context, path string, raw []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("reconsync: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("reconsync: %s: %w", path, err)
	}
	defer func() {
		// Drained before closing so the connection returns to the pool rather
		// than being thrown away, which matters on a path called per payment.
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
		_ = res.Body.Close()
	}()

	if res.StatusCode >= 300 {
		return parseAPIError(res)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(out); err != nil {
		return fmt.Errorf("reconsync: decode response: %w", err)
	}
	return nil
}

func parseAPIError(res *http.Response) error {
	apiErr := &APIError{
		StatusCode: res.StatusCode,
		Code:       "http_" + fmt.Sprint(res.StatusCode),
		Message:    res.Status,
		RequestID:  res.Header.Get("X-Request-Id"),
	}

	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			Field     string `json:"field"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	// A proxy returning HTML is a real case, so a body that will not parse
	// leaves the status-derived message rather than becoming a decode error
	// that hides what actually happened.
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&envelope); err == nil && envelope.Error.Code != "" {
		apiErr.Code = envelope.Error.Code
		apiErr.Message = envelope.Error.Message
		apiErr.Field = envelope.Error.Field
		if envelope.Error.RequestID != "" {
			apiErr.RequestID = envelope.Error.RequestID
		}
	}
	return apiErr
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// jitter returns a random fraction of d, up to a quarter.
func jitter(d time.Duration) time.Duration {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	n := int64(0)
	for _, v := range b[:4] {
		n = n<<8 | int64(v)
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(n % int64(d/4+1))
}

// NewIdempotencyKey returns a random key, for callers with no natural one.
func NewIdempotencyKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("reconsync: generate idempotency key: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
