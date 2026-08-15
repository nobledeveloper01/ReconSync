package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/pkg/reconsync"
)

// The Go client library, exercised against a stub of the real API.
//
// What is worth testing here is not that it can send JSON. It is the behaviour
// an integrating team relies on without reading the source: that a retry cannot
// double-report a debit, that a 400 is not retried forever, and above all that
// nothing in here can fail or delay the payment it sits beside.

func stubServer(t *testing.T, handler http.HandlerFunc) (*reconsync.Client, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c, err := reconsync.New(srv.URL, "rs_test_key")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, srv
}

func TestClientRejectsAnUnusableConfiguration(t *testing.T) {
	for _, tc := range []struct{ base, key, why string }{
		{"", "k", "no base URL"},
		{"reconsync.internal", "k", "no scheme"},
		{"https://reconsync.internal", "", "no api key"},
	} {
		if _, err := reconsync.New(tc.base, tc.key); err == nil {
			t.Errorf("accepted %s", tc.why)
		}
	}
}

// A retry that changed the idempotency key would register the same debit twice,
// and two debits for one transfer is the double-count this product exists to
// prevent.
func TestARetriedDebitCarriesTheSameIdempotencyKey(t *testing.T) {
	var keys []string
	var mu sync.Mutex
	attempts := 0

	c, _ := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var got struct {
			IdempotencyKey string `json:"idempotency_key"`
		}
		_ = json.Unmarshal(body, &got)

		mu.Lock()
		keys = append(keys, got.IdempotencyKey)
		attempts++
		n := attempts
		mu.Unlock()

		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "accepted", "transaction_id": "TX1", "window_seconds": 300,
		})
	})

	if _, err := c.ReportDebit(context.Background(), reconsync.Debit{
		TransactionID: "TX1", AmountMinor: 5000, Currency: "NGN",
		TransactionType: "transfer", CustomerRef: "cust_1",
	}); err != nil {
		t.Fatalf("ReportDebit: %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("attempts = %d, want 3", len(keys))
	}
	for i, k := range keys {
		if k == "" {
			t.Fatalf("attempt %d sent no idempotency key", i+1)
		}
		if k != keys[0] {
			t.Errorf("attempt %d key = %q, want %q — a retry would double-report", i+1, k, keys[0])
		}
	}
}

// A 400 means the request is wrong. Sending it again more slowly does not make
// it right, and retrying it wastes the caller's timeout budget.
func TestAClientErrorIsNotRetried(t *testing.T) {
	var attempts atomic.Int64
	c, _ := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code": "invalid_request", "message": "is required", "field": "currency",
			},
		})
	})

	_, err := c.ReportDebit(context.Background(), reconsync.Debit{TransactionID: "TX1"})
	if err == nil {
		t.Fatal("a 400 was reported as success")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("attempts = %d, want 1", n)
	}

	// The field the server named survives to the caller, so the integrating
	// team is told which field rather than being left to diff payloads.
	var apiErr *reconsync.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is %T, want *reconsync.APIError", err)
	}
	if apiErr.Field != "currency" || apiErr.Code != "invalid_request" {
		t.Errorf("got code %q field %q", apiErr.Code, apiErr.Field)
	}
	if apiErr.Retryable() {
		t.Error("a 400 reports itself as retryable")
	}
}

func TestServerErrorsAreRetriedAndThenReported(t *testing.T) {
	var attempts atomic.Int64
	c, _ := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	})

	err := c.ReportCredit(context.Background(), reconsync.Credit{
		TransactionID: "TX1", Status: reconsync.CreditSuccess,
	})
	if err == nil {
		t.Fatal("a persistent 502 was reported as success")
	}
	if n := attempts.Load(); n != 3 {
		t.Errorf("attempts = %d, want 3", n)
	}
}

// The whole reason the Reporter exists: reporting must not be able to slow down
// or fail the money movement it sits beside.
func TestTheReporterNeverBlocksThePaymentPath(t *testing.T) {
	release := make(chan struct{})
	var inFlight atomic.Int64

	c, _ := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		inFlight.Add(1)
		<-release // the server is wedged, as it would be in an outage
		w.WriteHeader(http.StatusAccepted)
	})

	var dropped atomic.Int64
	r := reconsync.NewReporter(c,
		reconsync.WithBuffer(4),
		reconsync.WithWorkers(1),
		reconsync.OnDrop(func(string, string) { dropped.Add(1) }))

	// Far more than the buffer holds, against a server that never answers.
	start := time.Now()
	for i := 0; i < 500; i++ {
		r.ReportDebit(reconsync.Debit{
			TransactionID: fmt.Sprintf("TX%d", i), AmountMinor: 1000, Currency: "NGN",
		})
	}
	elapsed := time.Since(start)

	// The bar is deliberately generous; the point is that it is not the many
	// seconds a blocking implementation would take against a wedged server.
	if elapsed > 2*time.Second {
		t.Errorf("500 reports took %s against a wedged server; the payment path was blocked", elapsed)
	}
	if dropped.Load() == 0 {
		t.Error("nothing was dropped, so something must have waited")
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Close(ctx)

	// And the drops are counted rather than silent: a hole in ReconSync's view
	// of your traffic is indistinguishable from a quiet day unless something
	// says otherwise.
	if s := r.Stats(); s.Dropped == 0 {
		t.Error("Stats reports no drops")
	}
}

// Reporting concurrently with shutdown must not panic. A library that panics
// inside a payment service takes the service with it.
func TestClosingWhileReportingIsSafe(t *testing.T) {
	c, _ := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	r := reconsync.NewReporter(c, reconsync.WithBuffer(8), reconsync.WithWorkers(2))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.ReportDebit(reconsync.Debit{
					TransactionID: fmt.Sprintf("TX%d-%d", i, j), AmountMinor: 100, Currency: "NGN",
				})
			}
		}(i)
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = r.Close(ctx)
	}()

	wg.Wait() // a panic in any goroutine fails the test by crashing it

	// Reporting after Close is refused rather than accepted and lost.
	if r.ReportDebit(reconsync.Debit{TransactionID: "TX-after"}) {
		t.Error("a report was accepted after Close")
	}
}

func TestCloseReportsWhatItCouldNotDrain(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	c, _ := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		<-block
	})
	r := reconsync.NewReporter(c, reconsync.WithBuffer(64), reconsync.WithWorkers(1))
	for i := 0; i < 32; i++ {
		r.ReportDebit(reconsync.Debit{TransactionID: fmt.Sprintf("TX%d", i)})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	// Silence on a lost queue would mean a gap in the record at every rolling
	// restart, which nobody would ever notice.
	if err := r.Close(ctx); !errors.Is(err, reconsync.ErrDrainTimeout) {
		t.Errorf("Close = %v, want ErrDrainTimeout", err)
	}
}

func TestBulkRefusesMoreThanTheServerAccepts(t *testing.T) {
	c, _ := stubServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("the request reached the server; it should have been refused locally")
	})

	events := make([]reconsync.BulkEvent, reconsync.MaxBulkEvents+1)
	// Caught here rather than as a 413 after uploading megabytes.
	if _, err := c.Bulk(context.Background(), events); err == nil {
		t.Error("accepted more than the documented maximum")
	}
}

// The fixtures the Node and Python SDKs test against are generated from the Go
// signer. This checks they still describe what Go actually produces.
//
// Without it the fixture file could drift from the implementation, and the
// other two SDKs would go on passing against a signature the server no longer
// sends — the exact failure the fixtures exist to prevent, only quieter.
func TestTheCrossLanguageFixturesStillMatchTheSigner(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "sdk", "fixtures", "signatures.json"))
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}

	var fixtures []struct {
		Name       string   `json:"name"`
		Secret     string   `json:"secret"`
		Timestamp  int64    `json:"timestamp"`
		Body       string   `json:"body"`
		Header     string   `json:"header"`
		Valid      bool     `json:"valid"`
		Why        string   `json:"why"`
		VerifyWith []string `json:"verify_with"`
		RejectWith []string `json:"reject_with"`
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatalf("parse fixtures: %v", err)
	}
	if len(fixtures) < 4 {
		t.Fatalf("only %d fixtures; the file looks truncated", len(fixtures))
	}

	for _, f := range fixtures {
		at := time.Unix(f.Timestamp, 0)
		err := reconsync.Verify(f.Secret, f.Header, []byte(f.Body), at, reconsync.DefaultTolerance)

		switch {
		case f.Valid && err != nil:
			t.Errorf("%s: the Go signer rejects a fixture marked valid: %v", f.Name, err)
		case !f.Valid && err == nil:
			t.Errorf("%s: the Go signer accepts a fixture marked invalid (%s)", f.Name, f.Why)
		}

		// Rotation fixtures carry a signature per secret, so they are not what
		// single-secret Sign produces; they are checked by what must verify
		// them instead.
		if f.Valid && len(f.VerifyWith) == 0 && reconsync.Sign(f.Secret, at, []byte(f.Body)) != f.Header {
			t.Errorf("%s: the fixture header is not what Sign produces today", f.Name)
		}

		for _, s := range f.VerifyWith {
			if err := reconsync.Verify(s, f.Header, []byte(f.Body), at, reconsync.DefaultTolerance); err != nil {
				t.Errorf("%s: a secret that signed it does not verify: %v", f.Name, err)
			}
		}
		for _, s := range f.RejectWith {
			if err := reconsync.Verify(s, f.Header, []byte(f.Body), at, reconsync.DefaultTolerance); err == nil {
				t.Errorf("%s: a secret that did not sign it verified", f.Name)
			}
		}
	}
}
