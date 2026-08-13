package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/correlate"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/ingest"
	"github.com/nobledeveloper01/ReconSync/internal/metrics"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

type ingestFixture struct {
	server *ingest.Server
	store  store.Store
	pipe   *pipeline.Pipeline
	keyA   string // tenant A's secret
	keyB   string // tenant B's secret
}

type fixtureOpts struct {
	drills  ingest.DrillRunner
	metrics *metrics.Registry

	ruleSet   *rules.Set
	ready     func(ctx context.Context) error
	blockPipe bool // hold the worker so the queue fills, for backpressure
}

func newIngestFixture(t *testing.T, opts fixtureOpts) *ingestFixture {
	t.Helper()
	ctx := context.Background()

	s := store.NewMemory()
	seedTenants(t, s)

	ruleSet := opts.ruleSet
	if ruleSet == nil {
		ruleSet = rules.NewSet(nil)
	}
	ruleProvider := func(context.Context, string) (*rules.Set, error) { return ruleSet, nil }

	engine, err := correlate.New(s, correlate.Options{
		Rules: ruleProvider,
		Salt:  func(_ context.Context, id string) (string, error) { return "salt_" + id, nil },
	})
	if err != nil {
		t.Fatalf("correlate.New: %v", err)
	}

	cfg := pipeline.Config{Workers: 2, BatchSize: 5, FlushInterval: 5 * time.Millisecond, BufferSize: 500}
	handler := pipeline.Handler(pipeline.HandlerFunc(
		func(ctx context.Context, tenantID string, events []domain.Event) error {
			_, err := engine.Apply(ctx, tenantID, events)
			return err
		}))

	var block chan struct{}
	if opts.blockPipe {
		// One worker, buffer of one, held inside the handler.
		cfg = pipeline.Config{Workers: 1, BatchSize: 1, FlushInterval: time.Hour, BufferSize: 1}
		block = make(chan struct{})
		handler = pipeline.HandlerFunc(func(context.Context, string, []domain.Event) error {
			<-block
			return nil
		})
	}

	p, err := pipeline.New(handler, cfg)
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}
	p.Start(ctx)
	t.Cleanup(p.Close)

	// Registered after p.Close so it runs first: cleanups are LIFO, and Close
	// waits for the worker that is parked on this channel.
	if block != nil {
		t.Cleanup(func() { close(block) })
	}

	keys := map[string]string{}
	for i, tenantID := range []string{tenantA, tenantB} {
		key, err := auth.Generate(auth.EnvTest)
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if err := s.CreateAPIKey(ctx, tenantID, fmt.Sprintf("key_%d", i), key, nil); err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		keys[tenantID] = key.Secret
	}

	authenticator, err := auth.New(s, auth.Options{})
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	srv, err := ingest.New(ingest.Options{
		Sink:     p,
		Rules:    ruleProvider,
		Store:    s,
		Audit:    s,
		Reports:  s,
		Drills:   opts.drills,
		Claims:   s,
		Webhooks: s,
		Metrics:  opts.metrics,
		Auth:     authenticator,
		Ready:    opts.ready,
	})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	return &ingestFixture{server: srv, store: s, pipe: p, keyA: keys[tenantA], keyB: keys[tenantB]}
}

func (f *ingestFixture) do(t *testing.T, method, path, key string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	switch b := body.(type) {
	case nil:
		reader = nil
	case string:
		reader = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(raw)
	}

	r := httptest.NewRequestWithContext(context.Background(), method, path, reader)
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	r.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	return w
}

func decodeBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode response %q: %v", w.Body.String(), err)
	}
	return out
}

func errorCode(t *testing.T, w *httptest.ResponseRecorder) (code, field string) {
	t.Helper()
	body := decodeBody(t, w)
	errObj, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("response has no error object: %s", w.Body.String())
	}
	code, _ = errObj["code"].(string)
	field, _ = errObj["field"].(string)
	return code, field
}

func debitBody(txnID string) map[string]any {
	return map[string]any{
		"transaction_id":   txnID,
		"idempotency_key":  "dk-" + txnID,
		"transaction_type": "transfer",
		"provider":         "paystack",
		"amount_minor":     5_000_000,
		"currency":         "NGN",
		"debit_at":         time.Now().UTC().Format(time.RFC3339Nano),
		"customer_ref":     "usr_9931",
		"metadata":         map[string]any{"channel": "mobile"},
	}
}

func creditBody(txnID, status string) map[string]any {
	return map[string]any{
		"transaction_id":     txnID,
		"idempotency_key":    "ck-" + txnID,
		"credit_at":          time.Now().UTC().Format(time.RFC3339Nano),
		"provider_reference": "ps_ref_1",
		"status":             status,
	}
}

// --- authentication ---

func TestIngestRequiresAuthentication(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	for _, tc := range []struct{ name, key string }{
		{"no key", ""},
		{"garbage key", "rs_test_not-a-real-key"},
		{"wrong key", f.keyA + "tampered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodPost, "/v1/events/debit", tc.key, debitBody("TX1"))
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", w.Code)
			}
			code, _ := errorCode(t, w)
			if code != "unauthenticated" {
				t.Errorf("code = %q, want unauthenticated", code)
			}
		})
	}
}

func TestIngestRevokedKeyIsRejected(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	if err := f.store.RevokeAPIKey(context.Background(), tenantA, "key_0"); err != nil {
		t.Fatalf("RevokeAPIKey: %v", err)
	}

	// A cached verification could outlive revocation; a fresh key has none.
	if w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, debitBody("TX1")); w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 for a revoked key", w.Code)
	}
}

// --- debit ---

func TestIngestDebitAccepted(t *testing.T) {
	set := rules.NewSet([]rules.Rule{
		{ID: 1, TransactionType: "transfer", WindowSeconds: 120, Action: rules.ActionAutoReverse, Enabled: true},
	})
	f := newIngestFixture(t, fixtureOpts{ruleSet: set})

	w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, debitBody("TX1"))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", w.Code, w.Body.String())
	}

	body := decodeBody(t, w)
	if body["status"] != "accepted" {
		t.Errorf("status = %v, want accepted", body["status"])
	}
	if body["transaction_id"] != "TX1" {
		t.Errorf("transaction_id = %v, want TX1", body["transaction_id"])
	}
	// The response must report the window the rule actually granted.
	if got := body["window_seconds"]; got != float64(120) {
		t.Errorf("window_seconds = %v, want 120", got)
	}
	if _, ok := body["expected_completion_at"].(string); !ok {
		t.Error("expected_completion_at missing")
	}
	if w.Header().Get("X-Request-Id") == "" {
		t.Error("no X-Request-Id on the response")
	}

	// And it must actually reach storage through the pipeline.
	waitFor(t, func() bool {
		_, err := f.store.Get(context.Background(), tenantA, "TX1")
		return err == nil
	}, "debit never reached the store")
}

func TestIngestDebitValidation(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	cases := []struct {
		name   string
		mutate func(map[string]any)
		field  string
		code   string
	}{
		{"zero amount", func(b map[string]any) { b["amount_minor"] = 0 }, "amount_minor", "invalid_request"},
		{"negative amount", func(b map[string]any) { b["amount_minor"] = -5 }, "amount_minor", "invalid_request"},
		{"bad currency", func(b map[string]any) { b["currency"] = "ngn" }, "currency", "invalid_request"},
		{"missing transaction id", func(b map[string]any) { b["transaction_id"] = "" }, "transaction_id", "invalid_request"},
		{"missing type", func(b map[string]any) { b["transaction_type"] = "" }, "transaction_type", "invalid_request"},
		{"missing idempotency key", func(b map[string]any) { delete(b, "idempotency_key") }, "idempotency_key", "invalid_request"},
		{"card data in metadata", func(b map[string]any) {
			b["metadata"] = map[string]any{"note": "4111 1111 1111 1111"}
		}, "metadata.note", "sensitive_data_rejected"},
		{"denylisted metadata field", func(b map[string]any) {
			b["metadata"] = map[string]any{"cvv": "123"}
		}, "metadata.cvv", "sensitive_data_rejected"},
		{"debit far in the future", func(b map[string]any) {
			b["debit_at"] = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
		}, "debit_at", "invalid_request"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := debitBody("TX1")
			tc.mutate(body)

			w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
			}
			code, field := errorCode(t, w)
			if code != tc.code {
				t.Errorf("code = %q, want %q", code, tc.code)
			}
			if field != tc.field {
				t.Errorf("field = %q, want %q", field, tc.field)
			}
		})
	}
}

func TestIngestDebitAcceptsIdempotencyHeader(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	body := debitBody("TX1")
	delete(body, "idempotency_key")

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/events/debit", bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+f.keyA)
	r.Header.Set("Idempotency-Key", "550e8400-e29b-41d4-a716-446655440000")

	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", w.Code, w.Body.String())
	}
}

func TestIngestDebitBackfillSkipsClockCheck(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	body := debitBody("TX-OLD")
	body["debit_at"] = time.Now().UTC().Add(-90 * 24 * time.Hour).Format(time.RFC3339Nano)
	body["backfill"] = true

	if w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, body); w.Code != http.StatusAccepted {
		t.Fatalf("historical backfill rejected: %d %s", w.Code, w.Body.String())
	}
}

// --- credit ---

func TestIngestCreditAccepted(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	if w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, debitBody("TX1")); w.Code != http.StatusAccepted {
		t.Fatalf("debit: %d %s", w.Code, w.Body.String())
	}
	w := f.do(t, http.MethodPost, "/v1/events/credit", f.keyA, creditBody("TX1", "success"))
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", w.Code, w.Body.String())
	}

	waitFor(t, func() bool {
		txn, err := f.store.Get(context.Background(), tenantA, "TX1")
		return err == nil && txn.Status == domain.StatusCompleted
	}, "credit never settled the transaction")
}

func TestIngestCreditValidation(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	for _, tc := range []struct{ name, status, field string }{
		{"unknown verdict", "maybe", "status"},
		{"empty verdict", "", "status"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodPost, "/v1/events/credit", f.keyA, creditBody("TX1", tc.status))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", w.Code)
			}
			if _, field := errorCode(t, w); field != tc.field {
				t.Errorf("field = %q, want %q", field, tc.field)
			}
		})
	}
}

// --- bulk ---

func TestIngestBulk(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	events := []map[string]any{}
	for i := 0; i < 5; i++ {
		events = append(events, debitBody(fmt.Sprintf("BULK-%d", i)))
	}
	// One invalid event must not discard the valid ones.
	bad := debitBody("BULK-BAD")
	bad["amount_minor"] = 0
	events = append(events, bad)

	credit := creditBody("BULK-0", "success")
	credit["type"] = "credit"
	events = append(events, credit)

	w := f.do(t, http.MethodPost, "/v1/events/bulk", f.keyA, map[string]any{"events": events})
	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", w.Code, w.Body.String())
	}

	body := decodeBody(t, w)
	if got := body["accepted"]; got != float64(6) {
		t.Errorf("accepted = %v, want 6", got)
	}
	rejected, _ := body["rejected"].([]any)
	if len(rejected) != 1 {
		t.Fatalf("rejected = %v, want 1 entry", rejected)
	}
	if entry, ok := rejected[0].(map[string]any); ok {
		if entry["index"] != float64(5) {
			t.Errorf("rejected index = %v, want 5", entry["index"])
		}
		if entry["field"] != "amount_minor" {
			t.Errorf("rejected field = %v, want amount_minor", entry["field"])
		}
	}

	waitFor(t, func() bool {
		txn, err := f.store.Get(context.Background(), tenantA, "BULK-0")
		return err == nil && txn.Status == domain.StatusCompleted
	}, "bulk credit never settled its debit")
}

func TestIngestBulkLimits(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	if w := f.do(t, http.MethodPost, "/v1/events/bulk", f.keyA, map[string]any{"events": []any{}}); w.Code != http.StatusBadRequest {
		t.Errorf("empty bulk: status = %d, want 400", w.Code)
	}

	// Over the documented ceiling: rejected outright rather than truncated.
	events := make([]map[string]any, 0, ingest.MaxBulkEvents+1)
	for i := 0; i <= ingest.MaxBulkEvents; i++ {
		events = append(events, debitBody(fmt.Sprintf("OVER-%d", i)))
	}
	w := f.do(t, http.MethodPost, "/v1/events/bulk", f.keyA, map[string]any{"events": events})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized bulk: status = %d, want 400", w.Code)
	}
	if _, field := errorCode(t, w); field != "events" {
		t.Errorf("field = %q, want events", field)
	}
}

// --- backpressure and body limits ---

func TestIngestBackpressureReturns429(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{blockPipe: true})

	var got429 bool
	for i := 0; i < 50 && !got429; i++ {
		w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, debitBody(fmt.Sprintf("TX-%d", i)))
		if w.Code == http.StatusTooManyRequests {
			got429 = true
			code, _ := errorCode(t, w)
			if code != "backpressure" {
				t.Errorf("code = %q, want backpressure", code)
			}
			// Without Retry-After a client has no basis for backing off.
			if w.Header().Get("Retry-After") == "" {
				t.Error("429 without a Retry-After header")
			}
		}
	}
	if !got429 {
		t.Fatal("a saturated pipeline never produced a 429")
	}
}

func TestIngestRejectsOversizedAndMalformedBodies(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	huge := strings.Repeat("x", ingest.DefaultMaxBodyBytes+1024)
	w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA,
		`{"transaction_id":"`+huge+`"}`)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: status = %d, want 413", w.Code)
	}

	for _, body := range []string{"", "{not json", "[]"} {
		w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, w.Code)
		}
	}
}

// --- reversal confirmation ---

func TestIngestReversalCompleted(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	ctx := context.Background()

	// Unknown transaction.
	w := f.do(t, http.MethodPost, "/v1/events/reversal-completed", f.keyA,
		map[string]any{"transaction_id": "ABSENT"})
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown transaction: status = %d, want 404", w.Code)
	}

	mustUpsert(t, f.store, newDebitTxn(tenantA, "TX1", -time.Minute))

	// Still pending: confirming a reversal nobody triggered is a conflict, not
	// a server error.
	w = f.do(t, http.MethodPost, "/v1/events/reversal-completed", f.keyA,
		map[string]any{"transaction_id": "TX1"})
	if w.Code != http.StatusConflict {
		t.Fatalf("premature confirmation: status = %d, want 409: %s", w.Code, w.Body.String())
	}
	if code, _ := errorCode(t, w); code != "invalid_state" {
		t.Errorf("code = %q, want invalid_state", code)
	}

	// Drive it to reversal_pending, then confirm.
	if _, err := f.store.ClaimExpired(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}
	if _, err := f.store.MarkReversalPending(ctx, tenantA, "TX1", time.Now().UTC()); err != nil {
		t.Fatalf("MarkReversalPending: %v", err)
	}

	w = f.do(t, http.MethodPost, "/v1/events/reversal-completed", f.keyA,
		map[string]any{"transaction_id": "TX1", "completed_at": time.Now().UTC().Format(time.RFC3339Nano)})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	if body["status"] != "recorded" {
		t.Errorf("status = %v, want recorded", body["status"])
	}
	// Detection-to-confirmation is the number a regulator asks for.
	if _, ok := body["elapsed_ms"]; !ok {
		t.Error("elapsed_ms missing from the confirmation response")
	}
}

// --- reads and tenant isolation ---

func TestIngestGetAndListTransactions(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	mustUpsert(t, f.store, newDebitTxn(tenantA, "TX1", time.Minute))

	w := f.do(t, http.MethodGet, "/v1/transactions/TX1", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	if body["transaction_id"] != "TX1" || body["status"] != "pending_debit" {
		t.Errorf("body = %v, want TX1/pending_debit", body)
	}

	if w := f.do(t, http.MethodGet, "/v1/transactions/NOPE", f.keyA, nil); w.Code != http.StatusNotFound {
		t.Errorf("unknown transaction: status = %d, want 404", w.Code)
	}

	w = f.do(t, http.MethodGet, "/v1/transactions?status=pending_debit", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list: status = %d, want 200", w.Code)
	}
	listed, _ := decodeBody(t, w)["transactions"].([]any)
	if len(listed) != 1 {
		t.Errorf("listed %d transactions, want 1", len(listed))
	}

	for _, q := range []string{"", "?status=bogus", "?status=pending_debit&limit=0", "?status=pending_debit&limit=99999"} {
		if w := f.do(t, http.MethodGet, "/v1/transactions"+q, f.keyA, nil); w.Code != http.StatusBadRequest {
			t.Errorf("query %q: status = %d, want 400", q, w.Code)
		}
	}
}

// §8.1: tenant B's key must not reach tenant A's data, and must get 404 rather
// than 403 — a 403 would confirm the transaction exists.
func TestIngestTenantIsolation(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	mustUpsert(t, f.store, newDebitTxn(tenantA, "SECRET", time.Minute))

	if w := f.do(t, http.MethodGet, "/v1/transactions/SECRET", f.keyB, nil); w.Code != http.StatusNotFound {
		t.Errorf("cross-tenant read: status = %d, want 404", w.Code)
	}

	w := f.do(t, http.MethodGet, "/v1/transactions?status=pending_debit", f.keyB, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list as tenant B: status = %d", w.Code)
	}
	if listed, _ := decodeBody(t, w)["transactions"].([]any); len(listed) != 0 {
		t.Errorf("tenant B listed %d of tenant A's transactions", len(listed))
	}

	if w := f.do(t, http.MethodPost, "/v1/events/reversal-completed", f.keyB,
		map[string]any{"transaction_id": "SECRET"}); w.Code != http.StatusNotFound {
		t.Errorf("cross-tenant confirmation: status = %d, want 404", w.Code)
	}

	// A debit submitted under B's key must be stored against B, never A.
	if w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyB, debitBody("B-OWNED")); w.Code != http.StatusAccepted {
		t.Fatalf("debit as tenant B: %d", w.Code)
	}
	waitFor(t, func() bool {
		_, err := f.store.Get(context.Background(), tenantB, "B-OWNED")
		return err == nil
	}, "tenant B's debit never stored")
	if _, err := f.store.Get(context.Background(), tenantA, "B-OWNED"); !errors.Is(err, store.ErrNotFound) {
		t.Error("tenant B's debit leaked into tenant A")
	}
}

// --- operational endpoints ---

func TestIngestHealthAndReadiness(t *testing.T) {
	// Liveness must not depend on the database: pointing it at a dependency
	// turns a brief blip into a simultaneous restart of every pod.
	f := newIngestFixture(t, fixtureOpts{
		ready: func(context.Context) error { return errors.New("database unreachable") },
	})

	if w := f.do(t, http.MethodGet, "/healthz", "", nil); w.Code != http.StatusOK {
		t.Errorf("healthz = %d, want 200 even with a failing dependency", w.Code)
	}
	if w := f.do(t, http.MethodGet, "/readyz", "", nil); w.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d, want 503 when a dependency is down", w.Code)
	}

	healthy := newIngestFixture(t, fixtureOpts{ready: func(context.Context) error { return nil }})
	if w := healthy.do(t, http.MethodGet, "/readyz", "", nil); w.Code != http.StatusOK {
		t.Errorf("readyz = %d, want 200 when dependencies are up", w.Code)
	}
}

func TestIngestMetrics(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	if w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, debitBody("TX1")); w.Code != http.StatusAccepted {
		t.Fatalf("debit: %d", w.Code)
	}

	w := f.do(t, http.MethodGet, "/metrics", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q, want Prometheus text format", ct)
	}

	out := w.Body.String()
	for _, want := range []string{
		"# HELP reconsync_events_received_total",
		"# TYPE reconsync_events_received_total counter",
		"reconsync_events_dropped_total",
		"reconsync_ingest_queue_depth",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q:\n%s", want, out)
		}
	}
}

// The customer runs this themselves, and it recomputes every hash rather than
// trusting anything we previously claimed.
func TestIngestAuditVerify(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	ctx := context.Background()

	// An empty chain verifies trivially, which is honest: nothing to falsify.
	w := f.do(t, http.MethodGet, "/v1/audit/verify", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if body := decodeBody(t, w); body["verified"] != true {
		t.Errorf("empty chain reported unverified: %v", body)
	}

	for i := 0; i < 3; i++ {
		if _, err := f.store.AppendAudit(ctx, tenantA, &audit.Record{
			EventType: audit.EventDetected,
			Subject:   map[string]any{"type": "transaction", "id": "TX-1"},
			Payload:   map[string]any{"verdict": "orphaned"},
		}); err != nil {
			t.Fatalf("AppendAudit: %v", err)
		}
	}

	w = f.do(t, http.MethodGet, "/v1/audit/verify", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := decodeBody(t, w)
	if body["verified"] != true {
		t.Errorf("intact chain reported unverified: %v", body)
	}
	if body["records"] != float64(3) {
		t.Errorf("records = %v, want 3", body["records"])
	}

	// Tenant B has its own chain and cannot see tenant A's records.
	w = f.do(t, http.MethodGet, "/v1/audit/verify", f.keyB, nil)
	if body := decodeBody(t, w); body["records"] != float64(0) {
		t.Errorf("tenant B saw %v records of tenant A's chain", body["records"])
	}

	if w := f.do(t, http.MethodGet, "/v1/audit/verify", "", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated verify = %d, want 401", w.Code)
	}
	if w := f.do(t, http.MethodGet, "/v1/audit/verify?limit=0", f.keyA, nil); w.Code != http.StatusBadRequest {
		t.Errorf("limit=0 = %d, want 400", w.Code)
	}
}

func TestIngestComplianceReport(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	ctx := context.Background()

	// One detected transaction, so the report has something to measure.
	mustUpsert(t, f.store, newExpiredTxn(tenantA, "TX-LATE", 5*time.Minute, time.Minute))
	if _, err := f.store.ClaimExpired(ctx, time.Now().UTC(), 10); err != nil {
		t.Fatalf("ClaimExpired: %v", err)
	}

	w := f.do(t, http.MethodGet, "/v1/reports/reversal-compliance", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := decodeBody(t, w)
	if body["tenant_id"] != tenantA {
		t.Errorf("tenant_id = %v", body["tenant_id"])
	}
	// The default deadline must be stated, not implied.
	if body["reversal_deadline_seconds"] != float64(86400) {
		t.Errorf("deadline = %v, want 86400", body["reversal_deadline_seconds"])
	}
	if _, ok := body["compliance"]; !ok {
		t.Error("no compliance section")
	}

	// A short deadline turns the same data into a breach.
	w = f.do(t, http.MethodGet, "/v1/reports/reversal-compliance?deadline_seconds=1", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	compliance, _ := decodeBody(t, w)["compliance"].(map[string]any)
	if compliance["breached"] != float64(1) {
		t.Errorf("breached = %v, want 1 with a 1s deadline", compliance["breached"])
	}

	// CSV is what a compliance team actually works from.
	w = f.do(t, http.MethodGet, "/v1/reports/reversal-compliance?deadline_seconds=1&format=csv", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("csv status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/csv") {
		t.Errorf("content-type = %q, want text/csv", ct)
	}
	if !strings.Contains(w.Body.String(), "TX-LATE") {
		t.Errorf("csv missing the breach: %s", w.Body.String())
	}

	// PDF is in the specification but not built; say so rather than returning
	// something that is not a PDF.
	if w := f.do(t, http.MethodGet, "/v1/reports/reversal-compliance?format=pdf", f.keyA, nil); w.Code != http.StatusBadRequest {
		t.Errorf("pdf = %d, want 400", w.Code)
	}

	for _, q := range []string{"?from=nonsense", "?deadline_seconds=0", "?from=2026-08-10&to=2026-08-01"} {
		if w := f.do(t, http.MethodGet, "/v1/reports/reversal-compliance"+q, f.keyA, nil); w.Code != http.StatusBadRequest {
			t.Errorf("query %q = %d, want 400", q, w.Code)
		}
	}

	// Tenant B has its own, empty, report.
	w = f.do(t, http.MethodGet, "/v1/reports/reversal-compliance", f.keyB, nil)
	if totals, _ := decodeBody(t, w)["totals"].(map[string]any); totals["orphans_detected"] != float64(0) {
		t.Errorf("tenant B saw %v of tenant A's orphans", totals["orphans_detected"])
	}

	if w := f.do(t, http.MethodGet, "/v1/reports/reversal-compliance", "", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated = %d, want 401", w.Code)
	}
}

func TestIngestNewValidatesOptions(t *testing.T) {
	if _, err := ingest.New(ingest.Options{}); err == nil {
		t.Error("accepted empty options")
	}
}
