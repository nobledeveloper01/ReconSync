package tests

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/evidence"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// independentSignature recomputes the signature from the §7.2 description
// alone, without touching the signing code. Verifying with the signer would
// only prove self-consistency (§11.9).
func independentSignature(t *testing.T, secret string, unixTime int64, body []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(unixTime, 10) + "." + string(body)))
	return hex.EncodeToString(mac.Sum(nil))
}

func TestSignMatchesAnIndependentImplementation(t *testing.T) {
	const secret = "whsec_test"
	body := []byte(`{"event":"reversal.triggered"}`)
	at := time.Unix(1754903662, 0)

	header := webhook.Sign(secret, at, body)

	// t=<unix>,v1=<hex>
	parts := strings.Split(header, ",")
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "t=") || !strings.HasPrefix(parts[1], "v1=") {
		t.Fatalf("header %q is not t=...,v1=...", header)
	}
	if parts[0] != "t=1754903662" {
		t.Errorf("timestamp = %q, want t=1754903662", parts[0])
	}

	want := independentSignature(t, secret, at.Unix(), body)
	if got := strings.TrimPrefix(parts[1], "v1="); got != want {
		t.Errorf("signature = %s, want %s", got, want)
	}
}

func TestSignatureCoversTimestampAndBody(t *testing.T) {
	const secret = "whsec_test"
	body := []byte(`{"a":1}`)
	at := time.Unix(1754903662, 0)
	base := webhook.Sign(secret, at, body)

	// Changing any input must change the signature, or the scheme protects
	// nothing it claims to.
	if webhook.Sign(secret, at.Add(time.Second), body) == base {
		t.Error("signature unchanged when the timestamp changed")
	}
	if webhook.Sign(secret, at, []byte(`{"a":2}`)) == base {
		t.Error("signature unchanged when the body changed")
	}
	if webhook.Sign("whsec_other", at, body) == base {
		t.Error("signature unchanged when the secret changed")
	}
}

func TestVerify(t *testing.T) {
	const secret = "whsec_test"
	body := []byte(`{"event":"reversal.triggered"}`)
	now := time.Unix(1754903662, 0)
	header := webhook.Sign(secret, now, body)

	if err := webhook.Verify(secret, header, body, now, webhook.DefaultTolerance); err != nil {
		t.Fatalf("a freshly signed payload did not verify: %v", err)
	}

	// A tampered body must not verify — this is the whole point.
	if err := webhook.Verify(secret, header, []byte(`{"event":"nope"}`), now, webhook.DefaultTolerance); !errors.Is(err, webhook.ErrSignatureMismatch) {
		t.Errorf("tampered body: %v, want ErrSignatureMismatch", err)
	}
	if err := webhook.Verify("whsec_wrong", header, body, now, webhook.DefaultTolerance); !errors.Is(err, webhook.ErrSignatureMismatch) {
		t.Errorf("wrong secret: %v, want ErrSignatureMismatch", err)
	}

	// Replay of a captured request outside the tolerance window.
	late := now.Add(webhook.DefaultTolerance + time.Minute)
	if err := webhook.Verify(secret, header, body, late, webhook.DefaultTolerance); !errors.Is(err, webhook.ErrSignatureExpired) {
		t.Errorf("stale signature: %v, want ErrSignatureExpired", err)
	}
	// A clock skewed the other way is equally suspect.
	early := now.Add(-(webhook.DefaultTolerance + time.Minute))
	if err := webhook.Verify(secret, header, body, early, webhook.DefaultTolerance); !errors.Is(err, webhook.ErrSignatureExpired) {
		t.Errorf("future signature: %v, want ErrSignatureExpired", err)
	}
	// Tolerance of zero disables the age check.
	if err := webhook.Verify(secret, header, body, late, 0); err != nil {
		t.Errorf("zero tolerance should skip the age check: %v", err)
	}
}

func TestVerifyRejectsMalformedHeaders(t *testing.T) {
	for _, header := range []string{
		"",
		"garbage",
		"t=abc,v1=deadbeef",
		"t=1754903662",    // no signature
		"v1=deadbeef",     // no timestamp
		"t=1754903662,v1", // no value
	} {
		err := webhook.Verify("s", header, []byte("{}"), time.Unix(1754903662, 0), 0)
		if !errors.Is(err, webhook.ErrMalformedSignature) {
			t.Errorf("header %q: %v, want ErrMalformedSignature", header, err)
		}
	}
}

func TestRetrySchedule(t *testing.T) {
	// The §7.2 C1 schedule: immediate, 30s, 2m, 10m, 1h, 6h.
	want := []time.Duration{0, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, time.Hour, 6 * time.Hour}
	if webhook.MaxAttempts != len(want) {
		t.Fatalf("MaxAttempts = %d, want %d", webhook.MaxAttempts, len(want))
	}
	for i, w := range want {
		if got := webhook.RetryDelay(i); got != w {
			t.Errorf("RetryDelay(%d) = %s, want %s", i, got, w)
		}
	}

	// Out of range must clamp rather than panic.
	if got := webhook.RetryDelay(-1); got != 0 {
		t.Errorf("RetryDelay(-1) = %s, want 0", got)
	}
	if got := webhook.RetryDelay(99); got != 6*time.Hour {
		t.Errorf("RetryDelay(99) = %s, want 6h", got)
	}

	for i := 0; i < webhook.MaxAttempts-1; i++ {
		if !webhook.ShouldRetry(i) {
			t.Errorf("ShouldRetry(%d) = false, want true", i)
		}
	}
	if webhook.ShouldRetry(webhook.MaxAttempts - 1) {
		t.Error("ShouldRetry on the final attempt should be false")
	}
}

func TestNextRetryAtAppliesBoundedJitter(t *testing.T) {
	now := time.Unix(1754903662, 0)
	base := webhook.RetryDelay(2) // 2 minutes

	var sawSpread bool
	first := webhook.NextRetryAt(now, 2)
	for i := 0; i < 200; i++ {
		got := webhook.NextRetryAt(now, 2).Sub(now)

		// Jitter must stay within ±20% or the schedule stops meaning anything.
		if got < time.Duration(float64(base)*0.8) || got > time.Duration(float64(base)*1.2) {
			t.Fatalf("delay %s is outside ±20%% of %s", got, base)
		}
		if webhook.NextRetryAt(now, 2) != first {
			sawSpread = true
		}
	}
	if !sawSpread {
		t.Error("no jitter observed; every retry would fire at the same instant")
	}
}

func TestRetryableStatus(t *testing.T) {
	// A 4xx means the request is wrong; retrying repeats the same rejection.
	for _, code := range []int{400, 401, 403, 404, 422} {
		if webhook.RetryableStatus(code) {
			t.Errorf("status %d should not be retried", code)
		}
	}
	for _, code := range []int{408, 429, 500, 502, 503, 504} {
		if !webhook.RetryableStatus(code) {
			t.Errorf("status %d should be retried", code)
		}
	}
}

// --- SSRF guard (§10) ---

func TestValidateEndpointURL(t *testing.T) {
	for _, ok := range []string{
		"https://customer.example.com/hooks/reconsync",
		"https://8.8.8.8/hook",
	} {
		if err := webhook.ValidateEndpointURL(ok, false); err != nil {
			t.Errorf("rejected a valid endpoint %q: %v", ok, err)
		}
	}

	// HTTPS only: a plaintext webhook leaks the payload and the signature.
	for _, bad := range []string{
		"http://customer.example.com/hook",
		"ftp://customer.example.com/hook",
		"https://",
		"://nonsense",
	} {
		if err := webhook.ValidateEndpointURL(bad, false); err == nil {
			t.Errorf("accepted an invalid endpoint %q", bad)
		}
	}

	// Literal private and metadata addresses, rejected at registration.
	for _, private := range []string{
		"https://127.0.0.1/hook",
		"https://10.0.0.5/hook",
		"https://192.168.1.1/hook",
		"https://172.16.0.1/hook",
		"https://169.254.169.254/latest/meta-data",
		"https://100.64.0.1/hook",
		"https://[::1]/hook",
		"https://0.0.0.0/hook",
	} {
		if err := webhook.ValidateEndpointURL(private, false); !errors.Is(err, webhook.ErrPrivateAddress) {
			t.Errorf("endpoint %q: %v, want ErrPrivateAddress", private, err)
		}
	}
}

// The two relaxations are separate flags because they relax different things:
// granting one must not grant the other.
func TestInsecureSchemeIsItsOwnRelaxation(t *testing.T) {
	const httpEndpoint = "http://echo.internal:8411/hook"

	if err := webhook.ValidateEndpointURL(httpEndpoint, false); !errors.Is(err, webhook.ErrInsecureScheme) {
		t.Errorf("plaintext = %v, want ErrInsecureScheme", err)
	}
	// Allowing a private address must not quietly allow plaintext too.
	if err := webhook.ValidateEndpointURL(httpEndpoint, true); !errors.Is(err, webhook.ErrInsecureScheme) {
		t.Errorf("--allow-private also allowed http: %v", err)
	}
	// And allowing plaintext must not allow a private address.
	if err := webhook.ValidateEndpointURL("http://127.0.0.1/hook", false,
		webhook.AllowInsecureScheme()); !errors.Is(err, webhook.ErrPrivateAddress) {
		t.Errorf("--allow-insecure also allowed a private address: %v", err)
	}
	// Both, which is what the Compose quickstart needs.
	if err := webhook.ValidateEndpointURL(httpEndpoint, true, webhook.AllowInsecureScheme()); err != nil {
		t.Errorf("both relaxations still rejected the endpoint: %v", err)
	}
	// A scheme that is neither is refused whatever the flags say.
	if err := webhook.ValidateEndpointURL("ftp://example.com/hook", true,
		webhook.AllowInsecureScheme()); !errors.Is(err, webhook.ErrInsecureScheme) {
		t.Errorf("ftp:// was accepted: %v", err)
	}
}

// Registration-time validation cannot stop a hostname being re-pointed at an
// internal address later, so the guard has to run at dial time too.
func TestClientBlocksPrivateAddressesAtDialTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// srv listens on loopback, which is exactly what the guard must refuse.
	client := webhook.NewClient(webhook.TransportOptions{})
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	blocked, err := client.Do(req)
	if err == nil {
		_ = blocked.Body.Close()
		t.Fatal("client reached a loopback address")
	}
	if !strings.Contains(err.Error(), "non-public") {
		t.Errorf("error = %v, want the private-address guard", err)
	}

	// And the escape hatch works, otherwise nothing below could be tested.
	relaxed := webhook.NewClient(webhook.TransportOptions{AllowPrivateAddresses: true})
	req2, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := relaxed.Do(req2)
	if err != nil {
		t.Fatalf("relaxed client failed: %v", err)
	}
	_ = resp.Body.Close()
}

// --- sending ---

func testSender() *webhook.Sender {
	return webhook.NewSender(webhook.SenderOptions{
		Client: webhook.NewClient(webhook.TransportOptions{AllowPrivateAddresses: true}),
	})
}

func testDelivery(url string, attempt int) webhook.Delivery {
	return webhook.Delivery{
		ID:            1,
		TenantID:      tenantA,
		EndpointID:    "we_1",
		TransactionID: "TX1",
		URL:           url,
		Secret:        "whsec_test",
		Event:         webhook.EventReversalTriggered,
		Payload:       []byte(`{"event":"reversal.triggered"}`),
		Attempt:       attempt,
	}
}

func TestSendSignsAndDelivers(t *testing.T) {
	var (
		gotSignature string
		gotEvent     string
		gotDelivery  string
		gotBody      []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSignature = r.Header.Get("X-ReconSync-Signature")
		gotEvent = r.Header.Get("X-ReconSync-Event")
		gotDelivery = r.Header.Get("X-ReconSync-Delivery")
		gotBody, _ = readAll(r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res := testSender().Send(context.Background(), testDelivery(srv.URL, 0))
	if !res.Delivered {
		t.Fatalf("not delivered: status=%d err=%v", res.StatusCode, res.Err)
	}
	if res.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", res.StatusCode)
	}
	if gotEvent != string(webhook.EventReversalTriggered) {
		t.Errorf("event header = %q", gotEvent)
	}
	if gotDelivery == "" {
		t.Error("no delivery header")
	}

	// The receiver must be able to verify what it was sent.
	if err := webhook.Verify("whsec_test", gotSignature, gotBody, time.Now(), webhook.DefaultTolerance); err != nil {
		t.Errorf("receiver could not verify the signature: %v", err)
	}
}

func TestSendClassifiesFailures(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		delivered bool
		retryable bool
	}{
		{"200", http.StatusOK, true, false},
		{"204", http.StatusNoContent, true, false},
		{"400", http.StatusBadRequest, false, false},
		{"401", http.StatusUnauthorized, false, false},
		{"429", http.StatusTooManyRequests, false, true},
		{"500", http.StatusInternalServerError, false, true},
		{"503", http.StatusServiceUnavailable, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			res := testSender().Send(context.Background(), testDelivery(srv.URL, 0))
			if res.Delivered != tc.delivered {
				t.Errorf("delivered = %v, want %v", res.Delivered, tc.delivered)
			}
			if !tc.delivered && res.Retryable != tc.retryable {
				t.Errorf("retryable = %v, want %v", res.Retryable, tc.retryable)
			}
		})
	}
}

func TestSendTruncatesResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(strings.Repeat("x", 100_000)))
	}))
	defer srv.Close()

	res := testSender().Send(context.Background(), testDelivery(srv.URL, 0))
	// A large error page must not be stored whole.
	if len(res.ResponseBody) > 4096 {
		t.Errorf("stored %d bytes of response body, want it truncated", len(res.ResponseBody))
	}
}

func TestSendDoesNotFollowRedirects(t *testing.T) {
	var reached bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()

	res := testSender().Send(context.Background(), testDelivery(redirector.URL, 0))
	// Following a redirect would let an endpoint bounce us to an internal host.
	if reached {
		t.Error("the sender followed a redirect")
	}
	if res.Delivered {
		t.Error("a 302 was treated as delivered")
	}
}

func TestSendReportsTransportFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing is listening now

	res := testSender().Send(context.Background(), testDelivery(url, 0))
	if res.Delivered || res.Err == nil {
		t.Fatalf("expected a transport failure, got delivered=%v err=%v", res.Delivered, res.Err)
	}
	if !res.Retryable {
		t.Error("a connection failure should be retryable")
	}
}

// --- retry policy ---

func TestDecide(t *testing.T) {
	now := time.Unix(1754903662, 0)

	delivered := webhook.Decide(testDelivery("https://x/", 0), webhook.Result{Delivered: true}, now)
	if delivered.Status != webhook.StatusDelivered {
		t.Errorf("status = %s, want delivered", delivered.Status)
	}

	// Retryable with attempts left: scheduled, not dead-lettered.
	retry := webhook.Decide(testDelivery("https://x/", 0), webhook.Result{Retryable: true}, now)
	if retry.Status != webhook.StatusPending {
		t.Errorf("status = %s, want pending", retry.Status)
	}
	if retry.NextRetryAt == nil || !retry.NextRetryAt.After(now) {
		t.Error("no future retry time scheduled")
	}

	// Out of attempts.
	exhausted := webhook.Decide(testDelivery("https://x/", webhook.MaxAttempts-1), webhook.Result{Retryable: true}, now)
	if exhausted.Status != webhook.StatusDeadLetter {
		t.Errorf("status = %s, want dead_letter", exhausted.Status)
	}

	// A permanent failure dead-letters immediately rather than burning six hours
	// of retries on a request that will never be accepted.
	permanent := webhook.Decide(testDelivery("https://x/", 0), webhook.Result{Retryable: false}, now)
	if permanent.Status != webhook.StatusDeadLetter {
		t.Errorf("status = %s, want dead_letter", permanent.Status)
	}
	if permanent.NextRetryAt != nil {
		t.Error("a dead-lettered delivery must not be scheduled again")
	}
}

// --- payload ---

func TestEnvelopeForReversal(t *testing.T) {
	detected := time.Date(2026, 8, 11, 9, 19, 25, 0, time.UTC)
	debitAt := time.Date(2026, 8, 11, 9, 14, 22, 0, time.UTC)
	txn := &domain.Transaction{
		TransactionID:        "TXN-2026-08-11-8842",
		AmountMinor:          5_000_000,
		Currency:             "NGN",
		Status:               domain.StatusOrphaned,
		DebitAt:              debitAt,
		ExpectedCompletionAt: debitAt.Add(300 * time.Second),
		DetectedAt:           &detected,
	}

	ev := evidence.New()
	ev.Add(evidence.SignalWindowExpired, "no credit within 300s", evidence.WeightWindowExpired)
	ev.Add(evidence.SignalIngestIntact, "no events lost over this window", evidence.WeightIngestIntact)
	ev.Add(evidence.SignalProviderFailed, "paystack reports the credit leg failed", evidence.WeightProviderFailed)

	env := webhook.EnvelopeFor(webhook.EventReversalTriggered, txn, detected, ev)
	if env.Event != webhook.EventReversalTriggered {
		t.Errorf("event = %s", env.Event)
	}
	if env.Data.Reason != "no_credit_confirmation_within_window" {
		t.Errorf("reason = %q", env.Data.Reason)
	}
	if env.Data.WindowSeconds != 300 {
		t.Errorf("window_seconds = %d, want 300", env.Data.WindowSeconds)
	}
	if !env.Data.RegulatoryDeadline.Equal(txn.ExpectedCompletionAt) {
		t.Errorf("regulatory_deadline = %s, want %s", env.Data.RegulatoryDeadline, txn.ExpectedCompletionAt)
	}
	// §10.1: the payload states that the receiver must check its own ledger.
	if !env.Data.Advisory {
		t.Error("payload must be marked advisory")
	}

	// Corroborated by the rail, so this should be near certainty.
	if env.Data.Confidence < 0.9 {
		t.Errorf("confidence = %v, want >= 0.9 when the rail confirmed failure", env.Data.Confidence)
	}
	if len(env.Data.Evidence) != 3 {
		t.Fatalf("evidence has %d signals, want 3", len(env.Data.Evidence))
	}
	// Heaviest first, so the reason that mattered most reads first.
	if env.Data.Evidence[0].Name != evidence.SignalWindowExpired {
		t.Errorf("first signal is %s, want the heaviest", env.Data.Evidence[0].Name)
	}

	raw, err := webhook.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var back map[string]any
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	for _, field := range []string{"event", "occurred_at", "data"} {
		if _, ok := back[field]; !ok {
			t.Errorf("payload missing %q", field)
		}
	}
	// Nothing customer-identifying may appear in an outbound payload.
	if strings.Contains(string(raw), "customer_ref") {
		t.Error("payload leaked a customer reference")
	}
}

func readAll(r *http.Request) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()
	return io.ReadAll(r.Body)
}
