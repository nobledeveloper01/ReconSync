// Package ingest is the HTTP front door for events (§7.1).
package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/account"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/drill"
	"github.com/nobledeveloper01/ReconSync/internal/licence"
	"github.com/nobledeveloper01/ReconSync/internal/metrics"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// EventSink is the pipeline as this package needs it.
type EventSink interface {
	Submit(ev domain.Event) error
	Stats() pipeline.Stats
}

// RuleProvider returns the rules in force for a tenant, so the response can
// report the window a transaction was admitted under.
type RuleProvider func(ctx context.Context, tenantID string) (*rules.Set, error)

const (
	// DefaultMaxBodyBytes bounds a single-event request.
	DefaultMaxBodyBytes = 64 << 10 // 64 KiB

	// DefaultMaxBulkBodyBytes bounds a bulk request: 1000 events (§3.2 A3).
	DefaultMaxBulkBodyBytes = 8 << 20 // 8 MiB

	// MaxBulkEvents is the documented ceiling on one bulk call.
	MaxBulkEvents = 1000

	// maxClockSkew is how far ahead of us a debit may claim to have happened.
	// Anything further is a broken clock or bad data, not a real transaction.
	maxClockSkew = 5 * time.Minute
)

// Options configures a Server. Sink, Rules, Store and Auth are required.
type Options struct {
	Sink  EventSink
	Rules RuleProvider
	Store store.TransactionStore
	Auth  *auth.Authenticator

	// Audit backs GET /v1/audit/verify. Optional: without it the endpoint
	// reports that verification is unavailable rather than pretending to pass.
	Audit store.AuditStore

	// Reports backs GET /v1/reports/reversal-compliance. Optional.
	Reports store.ReportStore

	// Drills backs POST /v1/fire-drill. Optional.
	Drills DrillRunner

	// Claims backs the reversal claim interlock. Optional.
	Claims store.ClaimStore

	// Webhooks backs /v1/webhooks endpoint management. Optional.
	Webhooks store.WebhookStore

	// Metrics is what the background loops report into, exposed on /metrics.
	Metrics *metrics.Registry

	// Licence gates the commercial artefacts. Nil means unlicensed, which
	// serves everything — the behaviour every deployment had before licensing.
	Licence *licence.Checker

	// Dashboard is the built web app, served at the root. Nil serves no
	// dashboard, which is what a headless deployment wants.
	Dashboard DashboardFS

	// Accounts backs signing in with an email and password. Nil serves the API
	// to API keys only, which is what a headless deployment wants and what
	// every deployment had before user accounts existed.
	Accounts *account.Service

	// Ready reports dependency health for /readyz. Liveness never calls it.
	Ready func(ctx context.Context) error

	Logger           *slog.Logger
	Now              func() time.Time
	MaxBodyBytes     int64
	MaxBulkBodyBytes int64

	// RetryAfter is what a backpressured client is told to wait.
	RetryAfter time.Duration
}

// DrillRunner delivers a synthetic reversal to the tenant's own endpoints. An
// interface rather than the concrete runner, so the HTTP layer does not depend
// on the transport a drill happens to use.
type DrillRunner interface {
	Run(ctx context.Context, tenantID string) (drill.Report, error)
}

// Server serves the ingest API.
type Server struct {
	sink      EventSink
	rules     RuleProvider
	store     store.TransactionStore
	audit     store.AuditStore
	reports   store.ReportStore
	drills    DrillRunner
	claims    store.ClaimStore
	webhooks  store.WebhookStore
	metrics   *metrics.Registry
	licence   *licence.Checker
	dashboard DashboardFS
	auth      *auth.Authenticator
	accounts  *account.Service
	logins    *loginLimiter
	ready     func(ctx context.Context) error
	log       *slog.Logger
	now       func() time.Time
	handler   http.Handler

	maxBody     int64
	maxBulkBody int64
	retryAfter  time.Duration
}

// New builds a Server.
func New(opts Options) (*Server, error) {
	switch {
	case opts.Sink == nil:
		return nil, errors.New("ingest: sink is required")
	case opts.Rules == nil:
		return nil, errors.New("ingest: rule provider is required")
	case opts.Store == nil:
		return nil, errors.New("ingest: store is required")
	case opts.Auth == nil:
		return nil, errors.New("ingest: authenticator is required")
	}

	s := &Server{
		sink:        opts.Sink,
		rules:       opts.Rules,
		store:       opts.Store,
		audit:       opts.Audit,
		reports:     opts.Reports,
		drills:      opts.Drills,
		claims:      opts.Claims,
		webhooks:    opts.Webhooks,
		metrics:     opts.Metrics,
		licence:     opts.Licence,
		dashboard:   opts.Dashboard,
		auth:        opts.Auth,
		accounts:    opts.Accounts,
		logins:      newLoginLimiter(),
		ready:       opts.Ready,
		log:         opts.Logger,
		now:         opts.Now,
		maxBody:     opts.MaxBodyBytes,
		maxBulkBody: opts.MaxBulkBodyBytes,
		retryAfter:  opts.RetryAfter,
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.maxBody <= 0 {
		s.maxBody = DefaultMaxBodyBytes
	}
	if s.maxBulkBody <= 0 {
		s.maxBulkBody = DefaultMaxBulkBodyBytes
	}
	if s.retryAfter <= 0 {
		s.retryAfter = time.Second
	}

	s.handler = s.routes()
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) routes() http.Handler {
	read := auth.ScopeReportsRead
	write := auth.ScopeEventsWrite
	admin := auth.ScopeEndpointsWrite

	// Authenticated surface. Each route declares the scope it needs, so the
	// permission is visible next to the path rather than buried in a handler —
	// and a new route cannot be added without deciding who may call it.
	api := http.NewServeMux()
	api.Handle("POST /v1/events/debit", s.need(write, s.handleDebit))
	api.Handle("POST /v1/events/credit", s.need(write, s.handleCredit))
	api.Handle("POST /v1/events/bulk", s.need(write, s.handleBulk))
	api.Handle("POST /v1/events/reversal-completed", s.need(write, s.handleReversalCompleted))
	api.Handle("GET /v1/transactions", s.need(read, s.handleListTransactions))
	api.Handle("GET /v1/transactions/{transaction_id}", s.need(read, s.handleGetTransaction))
	api.Handle("GET /v1/licence", s.need(read, s.handleLicence))
	api.Handle("GET /v1/audit/verify", s.need(read, s.handleAuditVerify))
	api.Handle("GET /v1/audit/checkpoints", s.need(read, s.handleAuditCheckpoints))
	api.Handle("GET /v1/reports/reversal-compliance", s.need(read, s.handleComplianceReport))
	api.Handle("GET /v1/reports/providers", s.need(read, s.handleProviderScorecard))
	api.Handle("GET /v1/reports/exposure", s.need(read, s.handleExposure))
	api.Handle("GET /v1/reports/window-fit", s.need(read, s.handleWindowFit))
	api.Handle("POST /v1/fire-drill", s.need(write, s.handleFireDrill))
	api.Handle("POST /v1/reversals/{transaction_id}/claim", s.need(write, s.handleClaimReversal))
	api.Handle("POST /v1/reversals/{transaction_id}/claim/release", s.need(write, s.handleReleaseReversalClaim))
	if s.webhooks != nil {
		// Reading the list is reading; changing it decides where every reversal
		// payload goes, which is why only that half needs an admin.
		api.Handle("GET /v1/webhooks", s.need(read, s.handleListWebhooks))
		api.Handle("POST /v1/webhooks", s.need(admin, s.handleCreateWebhook))
		api.Handle("PATCH /v1/webhooks/{endpoint_id}", s.need(admin, s.handlePatchWebhook))
		api.Handle("DELETE /v1/webhooks/{endpoint_id}", s.need(admin, s.handleDeleteWebhook))
	}

	// The account a signed-in person manages. Any role may change their own
	// password and their own second factor — those are not privileges, they are
	// the means of keeping the account theirs.
	api.HandleFunc("GET /v1/auth/me", s.handleMe)
	api.HandleFunc("POST /v1/auth/logout", s.handleLogout)
	api.HandleFunc("POST /v1/auth/password", s.handleChangePassword)
	api.HandleFunc("POST /v1/auth/totp/begin", s.handleTOTPBegin)
	api.HandleFunc("POST /v1/auth/totp/confirm", s.handleTOTPConfirm)
	api.HandleFunc("POST /v1/auth/totp/disable", s.handleTOTPDisable)
	api.HandleFunc("GET /v1/auth/sessions", s.handleListSessions)
	api.HandleFunc("DELETE /v1/auth/sessions", s.handleRevokeSessions)

	// User administration checks the admin role inside each handler, because it
	// also has to scope every lookup to the caller's tenant.
	api.HandleFunc("GET /v1/users", s.handleListUsers)
	api.HandleFunc("POST /v1/users", s.handleCreateUser)
	api.HandleFunc("PATCH /v1/users/{user_id}", s.handleUpdateUser)
	api.HandleFunc("POST /v1/users/{user_id}/reset", s.handleIssueReset)

	root := http.NewServeMux()
	root.Handle("/v1/", s.authenticate(api))

	// Signing in and finishing a reset are the two things an unauthenticated
	// caller must be able to do. Both are rate limited by source address.
	root.HandleFunc("POST /v1/auth/login", s.handleLogin)
	root.HandleFunc("POST /v1/auth/reset", s.handleCompleteReset)

	// Operational endpoints are unauthenticated: a probe that needs a credential
	// fails for the wrong reasons during an incident.
	root.HandleFunc("GET /healthz", s.handleHealthz)
	root.HandleFunc("GET /readyz", s.handleReadyz)
	root.HandleFunc("GET /metrics", s.handleMetrics)

	// Last, so it claims only what the API has not: the root pattern would
	// otherwise swallow every path.
	s.mountDashboard(root)

	return s.recoverPanics(s.withRequestID(root))
}

// need wraps a handler in the scope it requires.
func (s *Server) need(scope string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := auth.PrincipalFrom(r.Context())
		if !ok {
			s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "not authenticated", "")
			return
		}
		if !principal.HasScope(scope) {
			// 403, not 404: the caller is authenticated and the resource is
			// theirs. Hiding it would make a permissions problem look like a
			// missing feature.
			s.writeError(w, r, http.StatusForbidden, "forbidden",
				"this caller does not hold "+scope, "")
			return
		}
		h(w, r)
	})
}

// recoverPanics keeps one bad request from taking down the process.
func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.ErrorContext(r.Context(), "panic serving request",
					slog.String("path", r.URL.Path),
					slog.Any("panic", rec))
				s.writeError(w, r, http.StatusInternalServerError, "internal", "internal error", "")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type requestIDKey struct{}

func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = newRequestID()
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

func newRequestID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "req_unknown"
	}
	return "req_" + hex.EncodeToString(b[:])
}

func requestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// authenticate resolves the caller, by session cookie or by API key.
//
// Two credentials, one Principal. A person's role becomes the same scopes an
// API key carries, so every handler downstream asks what the caller may do and
// never which kind of caller it is.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The cookie first. A browser that is signed in should not also need a
		// key, and a stale cookie alongside a valid key should not win.
		if principal, sess, ok := s.principalFromSession(r); ok {
			if !s.requireCSRF(w, r) {
				return
			}
			// Best effort: a failed touch costs a slightly stale "last seen",
			// which is not worth failing a request over.
			if err := s.accounts.Store().TouchSession(r.Context(), sess.TokenHash, s.now().UTC()); err != nil {
				s.log.WarnContext(r.Context(), "could not touch session", "error", err.Error())
			}
			next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
			return
		}

		token := auth.BearerToken(r)
		principal, err := s.auth.Authenticate(r.Context(), token)
		if err != nil {
			// Deliberately uniform: never reveal whether the key existed.
			s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), principal)))
	})
}

// --- response helpers ---

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Field     string `json:"field,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already sent, so this can only be logged.
		s.log.ErrorContext(r.Context(), "encode response", slog.String("error", err.Error()))
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, code, message, field string) {
	s.writeJSON(w, r, status, errorEnvelope{Error: apiError{
		Code:      code,
		Message:   message,
		Field:     field,
		RequestID: requestIDFrom(r.Context()),
	}})
}

// writeDomainError maps an error to the status a client should see.
//
// The mapping matters: a rejected replay or an out-of-order event is ordinary
// client behaviour and must not read as a server fault, or every SDK retry
// would look like an outage.
func (s *Server) writeDomainError(w http.ResponseWriter, r *http.Request, err error) {
	var ve domain.ValidationError
	if errors.As(err, &ve) {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", ve.Reason, ve.Field)
		return
	}

	var sde domain.SensitiveDataError
	if errors.As(err, &sde) {
		s.writeError(w, r, http.StatusBadRequest, "sensitive_data_rejected", sde.Reason, sde.Path)
		return
	}

	var ite domain.InvalidTransitionError
	if errors.As(err, &ite) {
		s.writeError(w, r, http.StatusConflict, "invalid_state",
			"transaction is "+ite.From.String()+" and cannot become "+ite.To.String(), "")
		return
	}

	switch {
	case errors.Is(err, store.ErrNotFound):
		s.writeError(w, r, http.StatusNotFound, "not_found", "transaction not found", "")
	case errors.Is(err, pipeline.ErrBackpressure):
		w.Header().Set("Retry-After", retryAfterSeconds(s.retryAfter))
		s.writeError(w, r, http.StatusTooManyRequests, "backpressure",
			"ingest queue is at capacity, retry shortly", "")
	case errors.Is(err, pipeline.ErrClosed):
		s.writeError(w, r, http.StatusServiceUnavailable, "shutting_down",
			"server is shutting down", "")
	default:
		s.log.ErrorContext(r.Context(), "unhandled request error", slog.String("error", err.Error()))
		s.writeError(w, r, http.StatusInternalServerError, "internal", "internal error", "")
	}
}

func retryAfterSeconds(d time.Duration) string {
	secs := int(d.Round(time.Second) / time.Second)
	if secs < 1 {
		secs = 1
	}
	return itoa(secs)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
