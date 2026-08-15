package tests

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

func TestWebhookEndpointLifecycleOverHTTP(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	// Create.
	w := f.do(t, http.MethodPost, "/v1/webhooks", f.keyA,
		map[string]any{"url": "https://customer.example.com/hook", "events": []string{"reversal.triggered"}})
	if w.Code != http.StatusCreated {
		t.Fatalf("create = %d, want 201: %s", w.Code, w.Body.String())
	}
	created := decodeBody(t, w)
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatal("no endpoint id returned")
	}
	if created["enabled"] != true {
		t.Errorf("enabled = %v, want true", created["enabled"])
	}

	// List.
	w = f.do(t, http.MethodGet, "/v1/webhooks", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d", w.Code)
	}
	eps, _ := decodeBody(t, w)["endpoints"].([]any)
	if len(eps) != 1 {
		t.Fatalf("listed %v, want one endpoint", eps)
	}

	// Disable, which is how you stop delivery without losing the history.
	w = f.do(t, http.MethodPatch, "/v1/webhooks/"+id, f.keyA, map[string]any{"enabled": false})
	if w.Code != http.StatusOK {
		t.Fatalf("patch = %d: %s", w.Code, w.Body.String())
	}
	stored, err := f.store.ListEndpoints(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	}
	if stored[0].Enabled {
		t.Error("endpoint is still enabled after being disabled")
	}

	// Delete.
	w = f.do(t, http.MethodDelete, "/v1/webhooks/"+id, f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d: %s", w.Code, w.Body.String())
	}
	if stored, _ := f.store.ListEndpoints(context.Background(), tenantA); len(stored) != 0 {
		t.Errorf("endpoint survived deletion: %v", stored)
	}

	// And deleting it twice is a 404, not a silent success.
	if w := f.do(t, http.MethodDelete, "/v1/webhooks/"+id, f.keyA, nil); w.Code != http.StatusNotFound {
		t.Errorf("second delete = %d, want 404", w.Code)
	}
}

// The endpoint answers the internet. Letting a caller register a private
// address would turn the dispatcher into an SSRF proxy against the deployment's
// own metadata service.
func TestWebhookAPIRefusesTheRelaxationsTheCLIAllows(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	for _, tc := range []struct {
		name, url, want string
	}{
		{"plaintext", "http://customer.example.com/hook", "https"},
		{"loopback", "https://127.0.0.1/hook", "public"},
		{"metadata service", "https://169.254.169.254/latest/meta-data", "public"},
		{"private range", "https://10.0.0.5/hook", "public"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := f.do(t, http.MethodPost, "/v1/webhooks", f.keyA, map[string]any{"url": tc.url})
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s = %d, want 400", tc.url, w.Code)
			}
			body, _ := decodeBody(t, w)["error"].(map[string]any)
			msg, _ := body["message"].(string)
			if !strings.Contains(msg, tc.want) {
				t.Errorf("message = %q, want it to mention %q", msg, tc.want)
			}
		})
	}
}

// Whoever can change the delivery target decides where every reversal payload
// goes. An ingest key must not be able to.
func TestWebhookAPIRequiresAnAdminScope(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})
	ctx := context.Background()

	// A key scoped to reporting events only, as a transaction service would hold.
	key, err := auth.Generate(auth.EnvTest)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := f.store.CreateAPIKey(ctx, tenantA, "key_ingest", key,
		[]string{auth.ScopeEventsWrite}); err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}

	w := f.do(t, http.MethodPost, "/v1/webhooks", key.Secret,
		map[string]any{"url": "https://attacker.example.com/hook"})
	if w.Code != http.StatusForbidden {
		t.Fatalf("create with an ingest key = %d, want 403: %s", w.Code, w.Body.String())
	}
	body, _ := decodeBody(t, w)["error"].(map[string]any)
	if msg, _ := body["message"].(string); !strings.Contains(msg, auth.ScopeEndpointsWrite) {
		t.Errorf("message = %q, want it to name the missing scope", msg)
	}

	// Nor read the list. Every route now declares the scope it needs, and
	// reading where reversals are delivered is reading, so it takes
	// reports:read — which an ingest key deliberately does not hold. This used
	// to be ungated, back when webhook writes were the only scoped route.
	if w := f.do(t, http.MethodGet, "/v1/webhooks", key.Secret, nil); w.Code != http.StatusForbidden {
		t.Errorf("list with an ingest key = %d, want 403", w.Code)
	}

	// A key that can read reports can read the list.
	reader, err := auth.Generate(auth.EnvTest)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := f.store.CreateAPIKey(ctx, tenantA, "key_reader", reader,
		[]string{auth.ScopeReportsRead}); err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if w := f.do(t, http.MethodGet, "/v1/webhooks", reader.Secret, nil); w.Code != http.StatusOK {
		t.Errorf("list with a reporting key = %d, want 200", w.Code)
	}

	// A key holding the scope may write.
	admin, err := auth.Generate(auth.EnvTest)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := f.store.CreateAPIKey(ctx, tenantA, "key_admin", admin,
		[]string{auth.ScopeEndpointsWrite}); err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if w := f.do(t, http.MethodPost, "/v1/webhooks", admin.Secret,
		map[string]any{"url": "https://customer.example.com/hook"}); w.Code != http.StatusCreated {
		t.Errorf("create with an admin key = %d, want 201: %s", w.Code, w.Body.String())
	}
}

// A subscription to an event that will never fire silently means "deliver
// nothing", which is worse than a rejection.
func TestWebhookAPIRejectsUnknownEvents(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	w := f.do(t, http.MethodPost, "/v1/webhooks", f.keyA, map[string]any{
		"url": "https://customer.example.com/hook", "events": []string{"reversal.triggerd"},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("typo in an event name = %d, want 400", w.Code)
	}
	// Every real event is accepted, including the newer integration ones.
	if w := f.do(t, http.MethodPost, "/v1/webhooks", f.keyA, map[string]any{
		"url":    "https://customer.example.com/hook",
		"events": []string{"reversal.triggered", "integration.silent", "integration.recovered"},
	}); w.Code != http.StatusCreated {
		t.Errorf("real events rejected = %d: %s", w.Code, w.Body.String())
	}
}

// One tenant must not touch another's delivery targets.
func TestWebhookAPIIsTenantScoped(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	w := f.do(t, http.MethodPost, "/v1/webhooks", f.keyA,
		map[string]any{"url": "https://a.example.com/hook"})
	if w.Code != http.StatusCreated {
		t.Fatalf("create = %d", w.Code)
	}
	id, _ := decodeBody(t, w)["id"].(string)

	// Tenant B sees nothing and can change nothing.
	if eps, _ := decodeBody(t, f.do(t, http.MethodGet, "/v1/webhooks", f.keyB, nil))["endpoints"].([]any); len(eps) != 0 {
		t.Errorf("tenant B listed tenant A's endpoints: %v", eps)
	}
	// 404 not 403: a 403 would confirm the endpoint exists.
	if w := f.do(t, http.MethodDelete, "/v1/webhooks/"+id, f.keyB, nil); w.Code != http.StatusNotFound {
		t.Errorf("cross-tenant delete = %d, want 404", w.Code)
	}
	if w := f.do(t, http.MethodPatch, "/v1/webhooks/"+id, f.keyB,
		map[string]any{"enabled": false}); w.Code != http.StatusNotFound {
		t.Errorf("cross-tenant patch = %d, want 404", w.Code)
	}
}

// --- store conformance ---

func testEndpointEnableAndDelete(t *testing.T, s store.Store) {
	seedTenants(t, s)
	ctx := context.Background()

	ep := &store.WebhookEndpoint{
		ID: "we_1", TenantID: tenantA, URL: "https://a.example.com/hook",
		SecretRef: "env://X", Events: []string{}, Enabled: true,
	}
	if err := s.CreateEndpoint(ctx, tenantA, ep); err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}

	if err := s.SetEndpointEnabled(ctx, tenantA, "we_1", false); err != nil {
		t.Fatalf("SetEndpointEnabled: %v", err)
	}
	got, err := s.ListEndpoints(ctx, tenantA)
	if err != nil {
		t.Fatalf("ListEndpoints: %v", err)
	}
	if len(got) != 1 || got[0].Enabled {
		t.Fatalf("endpoints = %+v, want one disabled", got)
	}

	// Another tenant cannot touch it, and the error is not-found rather than a
	// permission error that would confirm it exists.
	if err := s.SetEndpointEnabled(ctx, tenantB, "we_1", true); err == nil {
		t.Error("tenant B enabled tenant A's endpoint")
	}
	if err := s.DeleteEndpoint(ctx, tenantB, "we_1"); err == nil {
		t.Error("tenant B deleted tenant A's endpoint")
	}

	if err := s.DeleteEndpoint(ctx, tenantA, "we_1"); err != nil {
		t.Fatalf("DeleteEndpoint: %v", err)
	}
	if got, _ := s.ListEndpoints(ctx, tenantA); len(got) != 0 {
		t.Errorf("endpoints = %+v after delete, want none", got)
	}
	// Deleting twice is not-found, not a silent success.
	if err := s.DeleteEndpoint(ctx, tenantA, "we_1"); err == nil {
		t.Error("deleting an absent endpoint reported success")
	}
}
