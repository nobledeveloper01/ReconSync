package ingest

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/store"
	"github.com/nobledeveloper01/ReconSync/internal/webhook"
)

// Endpoint management over HTTP, so registering where reversals go does not
// require shell access to the server.
//
// Two rules shape the whole surface.
//
// First, it is gated on endpoints:write. An ingest key lives in the customer's
// transaction service, is handled by the most code, and leaks most easily — and
// whoever can change the delivery target decides where every reversal payload
// goes. That is not something the high-volume key should be able to do.
//
// Second, the development relaxations available on the CLI are not available
// here. reconsyncctl runs on the host, by someone with a shell; this endpoint
// answers the internet. Letting a remote caller register http://169.254.169.254
// would turn the dispatcher into an SSRF proxy against the deployment's own
// metadata service, so plaintext and private addresses are simply refused.

type endpointRequest struct {
	ID     string   `json:"id,omitempty"`
	URL    string   `json:"url"`
	Events []string `json:"events,omitempty"`
}

type endpointView struct {
	ID      string   `json:"id"`
	URL     string   `json:"url"`
	Events  []string `json:"events"`
	Enabled bool     `json:"enabled"`
}

type endpointPatch struct {
	Enabled *bool `json:"enabled"`
}

func endpointViewOf(ep *store.WebhookEndpoint) endpointView {
	events := ep.Events
	if events == nil {
		events = []string{}
	}
	return endpointView{ID: ep.ID, URL: ep.URL, Events: events, Enabled: ep.Enabled}
}

// requireEndpointAdmin resolves the principal and checks it may change delivery
// targets.
func (s *Server) requireEndpointAdmin(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return auth.Principal{}, false
	}
	if !principal.HasScope(auth.ScopeEndpointsWrite) {
		// 403 rather than 404: the caller is authenticated and the resource is
		// theirs. Hiding it would only make a permissions problem look like a
		// missing feature.
		s.writeError(w, r, http.StatusForbidden, "forbidden",
			"this key does not hold "+auth.ScopeEndpointsWrite+
				"; changing where reversals are delivered needs an admin key", "")
		return auth.Principal{}, false
	}
	return principal, true
}

func (s *Server) handleListWebhooks(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}

	eps, err := s.webhooks.ListEndpoints(r.Context(), principal.TenantID)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	out := make([]endpointView, 0, len(eps))
	for _, ep := range eps {
		out = append(out, endpointViewOf(ep))
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"endpoints": out})
}

func (s *Server) handleCreateWebhook(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireEndpointAdmin(w, r)
	if !ok {
		return
	}

	var req endpointRequest
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}
	if req.URL == "" {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "url is required", "url")
		return
	}

	// No relaxations. See the note at the top of this file.
	if err := webhook.ValidateEndpointURL(req.URL, false); err != nil {
		switch {
		case errors.Is(err, webhook.ErrInsecureScheme):
			s.writeError(w, r, http.StatusBadRequest, "invalid_request",
				"url must be https", "url")
		case errors.Is(err, webhook.ErrPrivateAddress):
			s.writeError(w, r, http.StatusBadRequest, "invalid_request",
				"url must resolve to a public address", "url")
		default:
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", err.Error(), "url")
		}
		return
	}

	for _, e := range req.Events {
		if !knownEvent(e) {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request",
				fmt.Sprintf("unknown event %q; an endpoint subscribed to nothing real would never fire", e),
				"events")
			return
		}
	}

	id := req.ID
	if id == "" {
		generated, err := newEndpointID()
		if err != nil {
			s.writeDomainError(w, r, err)
			return
		}
		id = generated
	}

	ep := &store.WebhookEndpoint{
		ID:       id,
		TenantID: principal.TenantID,
		URL:      req.URL,
		Events:   req.Events,
		Enabled:  true,
		// The secret is referenced, never carried. Accepting one over the API
		// would put a signing key in a request body, a log and a proxy.
		SecretRef: store.DefaultSecretRef,
	}
	if err := s.webhooks.CreateEndpoint(r.Context(), principal.TenantID, ep); err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			s.writeError(w, r, http.StatusConflict, "already_exists",
				fmt.Sprintf("endpoint %q already exists", id), "id")
			return
		}
		s.writeDomainError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusCreated, endpointViewOf(ep))
}

// handlePatchWebhook enables or disables delivery.
func (s *Server) handlePatchWebhook(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireEndpointAdmin(w, r)
	if !ok {
		return
	}

	var req endpointPatch
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}
	if req.Enabled == nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			"enabled is required", "enabled")
		return
	}

	id := r.PathValue("endpoint_id")
	if err := s.webhooks.SetEndpointEnabled(r.Context(), principal.TenantID, id, *req.Enabled); err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"id":      id,
		"enabled": *req.Enabled,
	})
}

func (s *Server) handleDeleteWebhook(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireEndpointAdmin(w, r)
	if !ok {
		return
	}

	id := r.PathValue("endpoint_id")
	if err := s.webhooks.DeleteEndpoint(r.Context(), principal.TenantID, id); err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"id":     id,
		"status": "deleted",
		"notice": "the delivery history for this endpoint went with it; disable instead of deleting to keep it",
	})
}

// knownEvent guards against a subscription to an event that will never fire —
// a typo that silently means "deliver nothing" is worse than a rejection.
func knownEvent(name string) bool {
	for _, e := range []webhook.EventType{
		webhook.EventReversalTriggered,
		webhook.EventReversalCompleted,
		webhook.EventReversalFailed,
		webhook.EventTransactionSuspect,
		webhook.EventTransactionSettled,
		webhook.EventSLAAtRisk,
		webhook.EventIntegrationSilent,
		webhook.EventIntegrationRecovered,
	} {
		if name == string(e) {
			return true
		}
	}
	return false
}

func newEndpointID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate endpoint id: %w", err)
	}
	return "we_" + hex.EncodeToString(b[:]), nil
}
