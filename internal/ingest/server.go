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

	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
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

	// Ready reports dependency health for /readyz. Liveness never calls it.
	Ready func(ctx context.Context) error

	Logger           *slog.Logger
	Now              func() time.Time
	MaxBodyBytes     int64
	MaxBulkBodyBytes int64

	// RetryAfter is what a backpressured client is told to wait.
	RetryAfter time.Duration
}

// Server serves the ingest API.
type Server struct {
	sink    EventSink
	rules   RuleProvider
	store   store.TransactionStore
	auth    *auth.Authenticator
	ready   func(ctx context.Context) error
	log     *slog.Logger
	now     func() time.Time
	handler http.Handler

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
		auth:        opts.Auth,
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
	// Authenticated surface.
	api := http.NewServeMux()
	api.HandleFunc("POST /v1/events/debit", s.handleDebit)
	api.HandleFunc("POST /v1/events/credit", s.handleCredit)
	api.HandleFunc("POST /v1/events/bulk", s.handleBulk)
	api.HandleFunc("POST /v1/events/reversal-completed", s.handleReversalCompleted)
	api.HandleFunc("GET /v1/transactions", s.handleListTransactions)
	api.HandleFunc("GET /v1/transactions/{transaction_id}", s.handleGetTransaction)

	root := http.NewServeMux()
	root.Handle("/v1/", s.authenticate(api))

	// Operational endpoints are unauthenticated: a probe that needs a credential
	// fails for the wrong reasons during an incident.
	root.HandleFunc("GET /healthz", s.handleHealthz)
	root.HandleFunc("GET /readyz", s.handleReadyz)
	root.HandleFunc("GET /metrics", s.handleMetrics)

	return s.recoverPanics(s.withRequestID(root))
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

// authenticate resolves the bearer key to a tenant.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
