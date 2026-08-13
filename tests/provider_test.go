package tests

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/provider"
)

type stubProvider struct {
	name   string
	status provider.Status
	err    error
	calls  int
}

func (s *stubProvider) Name() string { return s.name }

func (s *stubProvider) Query(context.Context, provider.Ref) (provider.Status, error) {
	s.calls++
	return s.status, s.err
}

func testRef() provider.Ref {
	return provider.Ref{
		TenantID:      tenantA,
		TransactionID: "TXN-1",
		Provider:      "paystack",
		AmountMinor:   5_000_000,
		Currency:      "NGN",
	}
}

func TestOutcomeConclusive(t *testing.T) {
	for _, o := range []provider.Outcome{provider.Settled, provider.Failed, provider.NotFound} {
		if !o.Conclusive() {
			t.Errorf("%s should be conclusive", o)
		}
	}
	// Unknown is the whole point: it must never be acted on.
	if provider.Unknown.Conclusive() {
		t.Error("unknown must not be conclusive")
	}
}

func TestRegistryRegisterAndQuery(t *testing.T) {
	r := provider.NewRegistry()
	stub := &stubProvider{name: "paystack", status: provider.Status{Outcome: provider.Settled}}

	if err := r.Register(stub); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if names := r.Names(); len(names) != 1 || names[0] != "paystack" {
		t.Errorf("names = %v, want [paystack]", names)
	}

	got := r.Query(context.Background(), testRef())
	if got.Outcome != provider.Settled {
		t.Errorf("outcome = %s, want settled", got.Outcome)
	}
	// The registry fills in provenance the adapter left blank.
	if got.Provider != "paystack" {
		t.Errorf("provider = %q, want paystack", got.Provider)
	}
	if got.ObservedAt.IsZero() {
		t.Error("observed_at not set")
	}
}

func TestRegistryRejectsBadRegistrations(t *testing.T) {
	r := provider.NewRegistry()
	if err := r.Register(nil); err == nil {
		t.Error("registered a nil provider")
	}
	if err := r.Register(&stubProvider{name: ""}); err == nil {
		t.Error("registered a provider with no name")
	}
}

// A rail we cannot query is a normal state, not a failure. It must degrade to
// "we do not know" rather than break the sweep.
func TestRegistryUnregisteredRailIsUnknown(t *testing.T) {
	r := provider.NewRegistry()

	got := r.Query(context.Background(), testRef())
	if got.Outcome != provider.Unknown {
		t.Errorf("outcome = %s, want unknown", got.Outcome)
	}
	if got.Detail == "" {
		t.Error("no detail explaining why the answer is unknown")
	}
}

// An adapter that errors has told us nothing. That must never become a verdict.
func TestRegistryAdapterErrorBecomesUnknown(t *testing.T) {
	r := provider.NewRegistry()
	if err := r.Register(&stubProvider{
		name:   "paystack",
		status: provider.Status{Outcome: provider.Failed},
		err:    errors.New("connection reset"),
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got := r.Query(context.Background(), testRef())
	if got.Outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want unknown — an errored query is not a verdict", got.Outcome)
	}
}

// An adapter returning something unrecognised is a bug, and the safe reading of
// a bug is that we do not know.
func TestRegistryUnrecognisedOutcomeBecomesUnknown(t *testing.T) {
	r := provider.NewRegistry()
	if err := r.Register(&stubProvider{name: "paystack", status: provider.Status{Outcome: "probably-fine"}}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if got := r.Query(context.Background(), testRef()); got.Outcome != provider.Unknown {
		t.Errorf("outcome = %s, want unknown", got.Outcome)
	}
}

func TestRegistryReRegisterReplaces(t *testing.T) {
	r := provider.NewRegistry()
	first := &stubProvider{name: "paystack", status: provider.Status{Outcome: provider.Settled}}
	second := &stubProvider{name: "paystack", status: provider.Status{Outcome: provider.Failed}}

	if err := r.Register(first); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Register(second); err != nil {
		t.Fatalf("re-register: %v", err)
	}

	if got := r.Query(context.Background(), testRef()); got.Outcome != provider.Failed {
		t.Errorf("outcome = %s, want the replacement's answer", got.Outcome)
	}
	if first.calls != 0 {
		t.Error("the replaced adapter was still queried")
	}
}

// --- the HTTP adapter ---

func paystackish(t *testing.T, url string) *provider.HTTPProvider {
	t.Helper()
	p, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "paystack",
		URLTemplate:   url + "/transfer/verify/{reference}",
		AuthHeader:    "Authorization",
		AuthValue:     "Bearer sk_test",
		StatusPath:    "data.status",
		SettledValues: []string{"success"},
		FailedValues:  []string{"failed", "reversed"},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	return p
}

func TestNewHTTPValidatesConfig(t *testing.T) {
	base := provider.HTTPConfig{
		ProviderName:  "p",
		URLTemplate:   "https://x/{reference}",
		StatusPath:    "status",
		SettledValues: []string{"ok"},
	}

	cases := map[string]func(*provider.HTTPConfig){
		"no name":            func(c *provider.HTTPConfig) { c.ProviderName = "" },
		"no url":             func(c *provider.HTTPConfig) { c.URLTemplate = "" },
		"url without ref":    func(c *provider.HTTPConfig) { c.URLTemplate = "https://x/fixed" },
		"no status path":     func(c *provider.HTTPConfig) { c.StatusPath = "" },
		"no outcome mapping": func(c *provider.HTTPConfig) { c.SettledValues = nil; c.FailedValues = nil },
	}
	for name, mutate := range cases {
		cfg := base
		mutate(&cfg)
		if _, err := provider.NewHTTP(cfg); err == nil {
			t.Errorf("%s: accepted an invalid config", name)
		}
	}

	if _, err := provider.NewHTTP(base); err != nil {
		t.Errorf("rejected a valid config: %v", err)
	}
}

func TestHTTPProviderMapsStatuses(t *testing.T) {
	cases := []struct {
		name   string
		status string
		want   provider.Outcome
	}{
		{"success", `{"data":{"status":"success"}}`, provider.Settled},
		{"uppercase success", `{"data":{"status":"SUCCESS"}}`, provider.Settled},
		{"failed", `{"data":{"status":"failed"}}`, provider.Failed},
		{"reversed", `{"data":{"status":"reversed"}}`, provider.Failed},
		// Anything in flight is not a verdict.
		{"pending", `{"data":{"status":"pending"}}`, provider.Unknown},
		{"unrecognised", `{"data":{"status":"something-new"}}`, provider.Unknown},
		{"field missing", `{"data":{}}`, provider.Unknown},
		{"field not a string", `{"data":{"status":42}}`, provider.Unknown},
		{"not json", `<html>502</html>`, provider.Unknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.status))
			}))
			defer srv.Close()

			got, err := paystackish(t, srv.URL).Query(context.Background(), testRef())
			if err != nil {
				t.Fatalf("Query returned an error instead of Unknown: %v", err)
			}
			if got.Outcome != tc.want {
				t.Errorf("outcome = %s, want %s", got.Outcome, tc.want)
			}
		})
	}
}

func TestHTTPProviderSendsAuthAndReference(t *testing.T) {
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"data":{"status":"success"}}`))
	}))
	defer srv.Close()

	ref := testRef()
	ref.ProviderRef = "ps_ref_88213"
	if _, err := paystackish(t, srv.URL).Query(context.Background(), ref); err != nil {
		t.Fatalf("Query: %v", err)
	}

	if gotPath != "/transfer/verify/ps_ref_88213" {
		t.Errorf("path = %q, want the provider reference substituted", gotPath)
	}
	if gotAuth != "Bearer sk_test" {
		t.Errorf("auth = %q", gotAuth)
	}
}

// Without a provider reference the transaction id is the next best identifier.
func TestHTTPProviderFallsBackToTransactionID(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"data":{"status":"success"}}`))
	}))
	defer srv.Close()

	if _, err := paystackish(t, srv.URL).Query(context.Background(), testRef()); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if gotPath != "/transfer/verify/TXN-1" {
		t.Errorf("path = %q, want the transaction id substituted", gotPath)
	}
}

// A provider with no record of a transfer we believe we initiated is itself
// evidence that it never happened.
func TestHTTPProvider404IsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	got, err := paystackish(t, srv.URL).Query(context.Background(), testRef())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.NotFound {
		t.Errorf("outcome = %s, want not_found", got.Outcome)
	}
}

// Every transport failure must be Unknown. A 500 is not a failed transfer.
func TestHTTPProviderFailuresAreUnknownNeverFailed(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusUnauthorized, http.StatusTooManyRequests} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))

		got, err := paystackish(t, srv.URL).Query(context.Background(), testRef())
		srv.Close()

		if err != nil {
			t.Errorf("HTTP %d returned an error instead of Unknown: %v", code, err)
		}
		if got.Outcome != provider.Unknown {
			t.Errorf("HTTP %d gave %s, want unknown — a provider fault is not a failed transfer", code, got.Outcome)
		}
	}
}

func TestHTTPProviderUnreachableIsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nothing listening

	p, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "paystack",
		URLTemplate:   url + "/verify/{reference}",
		StatusPath:    "status",
		SettledValues: []string{"success"},
		Timeout:       500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}

	got, err := p.Query(context.Background(), testRef())
	if err != nil {
		t.Fatalf("unreachable provider returned an error instead of Unknown: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Errorf("outcome = %s, want unknown", got.Outcome)
	}
}

// A rail that hangs must not stall the detection sweep for every other tenant.
func TestHTTPProviderTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer func() { close(release); srv.Close() }()

	p, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "paystack",
		URLTemplate:   srv.URL + "/verify/{reference}",
		StatusPath:    "status",
		SettledValues: []string{"success"},
		Timeout:       200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}

	started := time.Now()
	got, err := p.Query(context.Background(), testRef())
	elapsed := time.Since(started)

	if err != nil {
		t.Fatalf("timeout returned an error instead of Unknown: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Errorf("outcome = %s, want unknown", got.Outcome)
	}
	if elapsed > 3*time.Second {
		t.Errorf("took %s — the timeout did not bound the call", elapsed)
	}
}

// The evidence trail must never carry the provider's raw response, which can
// hold customer data.
func TestHTTPProviderDetailDoesNotLeakResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"weird","customer_email":"ada@example.com"}}`))
	}))
	defer srv.Close()

	got, err := paystackish(t, srv.URL).Query(context.Background(), testRef())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Detail == "" {
		t.Fatal("no detail recorded")
	}
	for _, leaked := range []string{"ada@example.com", "customer_email"} {
		if strings.Contains(got.Detail, leaked) {
			t.Errorf("detail leaked %q: %s", leaked, got.Detail)
		}
	}
}

// Paystack's transfer-verify response, as their public documentation gives it.
//
// This is not a claim that we have tested against Paystack — that needs a
// sandbox account. It is a claim that the generic adapter handles the shape
// their docs describe, which is what "the abstraction is proven" can honestly
// mean without credentials.
const paystackVerifyBody = `{
  "status": true,
  "message": "Transfer retrieved",
  "data": {
    "amount": 5000000,
    "currency": "NGN",
    "reference": "TXN-1",
    "status": "success",
    "transfer_code": "TRF_abc123",
    "recipient": {"type": "nuban", "name": "Ada Lovelace"}
  }
}`

func paystackConfig(t *testing.T, url string) *provider.HTTPProvider {
	t.Helper()
	p, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:   "paystack",
		URLTemplate:    url + "/transfer/verify/{reference}",
		AuthHeader:     "Authorization",
		AuthValue:      "Bearer {value}",
		AuthCredential: "sk_test_notreal",
		StatusPath:     "data.status",
		AmountPath:     "data.amount",
		SettledValues:  []string{"success"},
		FailedValues:   []string{"failed", "reversed", "abandoned"},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	return p
}

func TestHTTPAdapterHandlesPaystackShape(t *testing.T) {
	var gotAuth, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(paystackVerifyBody))
	}))
	defer srv.Close()

	got, err := paystackConfig(t, srv.URL).Query(context.Background(), provider.Ref{
		TransactionID: "TXN-1", AmountMinor: 5_000_000, Currency: "NGN",
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Settled {
		t.Errorf("outcome = %s, want settled: %s", got.Outcome, got.Detail)
	}

	// The credential is rendered into the scheme the rail expects. An operator
	// storing a bare key would otherwise get 401 on every query — which is
	// Unknown, which silently stops every reversal.
	if gotAuth != "Bearer sk_test_notreal" {
		t.Errorf("Authorization = %q, want the templated bearer token", gotAuth)
	}
	if gotPath != "/transfer/verify/TXN-1" {
		t.Errorf("path = %q, want the reference substituted", gotPath)
	}
}

// A settled response for a different amount is not this transaction settling,
// and believing it would cancel a reversal that should have happened.
func TestHTTPAdapterChecksTheAmount(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(paystackVerifyBody))
	}))
	defer srv.Close()

	got, err := paystackConfig(t, srv.URL).Query(context.Background(), provider.Ref{
		TransactionID: "TXN-1", AmountMinor: 9_999_999,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want unknown on an amount mismatch", got.Outcome)
	}
	if !strings.Contains(got.Detail, "5000000") {
		t.Errorf("detail = %q, want both amounts named", got.Detail)
	}
}

// Paystack's in-flight statuses must not be read as a verdict either way.
func TestHTTPAdapterTreatsPendingAsUnknown(t *testing.T) {
	for _, status := range []string{"pending", "otp", "processing", "something_new"} {
		t.Run(status, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, `{"data":{"status":%q,"amount":5000000}}`, status)
			}))
			defer srv.Close()

			got, err := paystackConfig(t, srv.URL).Query(context.Background(), provider.Ref{
				TransactionID: "TXN-1", AmountMinor: 5_000_000,
			})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if got.Outcome != provider.Unknown {
				t.Errorf("%s = %s, want unknown", status, got.Outcome)
			}
		})
	}
}

// An auth value with no template keeps working exactly as before.
func TestHTTPAdapterAuthWithoutATemplate(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("X-Api-Key")
		_, _ = w.Write([]byte(`{"data":{"status":"success"}}`))
	}))
	defer srv.Close()

	p, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "bank",
		URLTemplate:   srv.URL + "/status/{reference}",
		AuthHeader:    "X-Api-Key",
		AuthValue:     "raw-key-verbatim",
		StatusPath:    "data.status",
		SettledValues: []string{"success"},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	if _, err := p.Query(context.Background(), provider.Ref{TransactionID: "TXN-1"}); err != nil {
		t.Fatalf("Query: %v", err)
	}
	if gotAuth != "raw-key-verbatim" {
		t.Errorf("X-Api-Key = %q, want the value verbatim", gotAuth)
	}
}

// Flutterwave answers a reference lookup with a list rather than an object.
func TestHTTPAdapterHandlesAnArrayResponse(t *testing.T) {
	const oneMatch = `{"status":"success","data":[
		{"reference":"TXN-1","amount":5000000,"status":"SUCCESSFUL"}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(oneMatch))
	}))
	defer srv.Close()

	build := func(statusPath, amountPath string) *provider.HTTPProvider {
		t.Helper()
		p, err := provider.NewHTTP(provider.HTTPConfig{
			ProviderName:  "flutterwave",
			URLTemplate:   srv.URL + "/transfers?reference={reference}",
			StatusPath:    statusPath,
			AmountPath:    amountPath,
			SettledValues: []string{"successful"},
			FailedValues:  []string{"failed"},
		})
		if err != nil {
			t.Fatalf("NewHTTP: %v", err)
		}
		return p
	}

	// Both the explicit index and the bare field name work against a single
	// match, so an operator does not have to know which shape they will get.
	for _, path := range []string{"data.0.status", "data.status"} {
		t.Run(path, func(t *testing.T) {
			got, err := build(path, "data.0.amount").Query(context.Background(),
				provider.Ref{TransactionID: "TXN-1", AmountMinor: 5_000_000})
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if got.Outcome != provider.Settled {
				t.Errorf("outcome = %s, want settled: %s", got.Outcome, got.Detail)
			}
		})
	}
}

// Several transfers for one reference is ambiguous. Picking the first would be
// a guess about which one is ours, and a guess here moves real money.
func TestHTTPAdapterRefusesAnAmbiguousArray(t *testing.T) {
	const twoMatches = `{"data":[
		{"reference":"TXN-1","amount":5000000,"status":"SUCCESSFUL"},
		{"reference":"TXN-1","amount":5000000,"status":"FAILED"}]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(twoMatches))
	}))
	defer srv.Close()

	p, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "flutterwave",
		URLTemplate:   srv.URL + "/transfers?reference={reference}",
		StatusPath:    "data.status",
		SettledValues: []string{"successful"},
		FailedValues:  []string{"failed"},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}

	got, err := p.Query(context.Background(), provider.Ref{TransactionID: "TXN-1"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want unknown — two transfers share the reference", got.Outcome)
	}

	// An explicit index is the operator saying which one they mean, and is
	// honoured. The first here reports success, so it settles.
	indexed, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "flutterwave",
		URLTemplate:   srv.URL + "/transfers?reference={reference}",
		StatusPath:    "data.0.status",
		SettledValues: []string{"successful"},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	if got, _ := indexed.Query(context.Background(), provider.Ref{TransactionID: "TXN-1"}); got.Outcome != provider.Settled {
		t.Errorf("explicit index = %s, want settled", got.Outcome)
	}
}

// An empty array means the rail has no record of it, and must not be read as
// a settlement.
func TestHTTPAdapterHandlesAnEmptyArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	p, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "flutterwave",
		URLTemplate:   srv.URL + "/transfers?reference={reference}",
		StatusPath:    "data.status",
		SettledValues: []string{"successful"},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	if got, _ := p.Query(context.Background(), provider.Ref{TransactionID: "TXN-1"}); got.Outcome != provider.Unknown {
		t.Errorf("outcome = %s, want unknown for an empty result", got.Outcome)
	}
}
