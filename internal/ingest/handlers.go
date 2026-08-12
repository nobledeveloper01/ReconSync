package ingest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/drill"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
	"github.com/nobledeveloper01/ReconSync/internal/report"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// --- request bodies (§7.1) ---

type debitRequest struct {
	TransactionID   string         `json:"transaction_id"`
	IdempotencyKey  string         `json:"idempotency_key"`
	TransactionType string         `json:"transaction_type"`
	Provider        string         `json:"provider"`
	AmountMinor     int64          `json:"amount_minor"`
	Currency        string         `json:"currency"`
	DebitAt         time.Time      `json:"debit_at"`
	CustomerRef     string         `json:"customer_ref"`
	Metadata        map[string]any `json:"metadata"`
	Backfill        bool           `json:"backfill"`
}

type creditRequest struct {
	TransactionID     string    `json:"transaction_id"`
	IdempotencyKey    string    `json:"idempotency_key"`
	CreditAt          time.Time `json:"credit_at"`
	ProviderReference string    `json:"provider_reference"`
	Status            string    `json:"status"`
}

type reversalCompletedRequest struct {
	TransactionID string    `json:"transaction_id"`
	CompletedAt   time.Time `json:"completed_at"`
}

type bulkRequest struct {
	Events []bulkEvent `json:"events"`
}

// bulkEvent carries either leg. Debit fields and credit fields are disjoint, so
// the presence of a credit status is what distinguishes them.
type bulkEvent struct {
	Type string `json:"type"` // "debit" or "credit"
	debitRequest
	CreditAt          time.Time `json:"credit_at"`
	ProviderReference string    `json:"provider_reference"`
	CreditStatus      string    `json:"status"`
}

// --- responses ---

type debitAccepted struct {
	Status               string    `json:"status"`
	TransactionID        string    `json:"transaction_id"`
	ExpectedCompletionAt time.Time `json:"expected_completion_at"`
	WindowSeconds        int       `json:"window_seconds"`
}

type creditAccepted struct {
	Status        string `json:"status"`
	TransactionID string `json:"transaction_id"`
}

type bulkAccepted struct {
	Accepted int             `json:"accepted"`
	Rejected []bulkRejection `json:"rejected,omitempty"`
}

type bulkRejection struct {
	Index int    `json:"index"`
	Code  string `json:"code"`
	Error string `json:"error"`
	Field string `json:"field,omitempty"`
}

type reversalCompleted struct {
	Status              string    `json:"status"`
	TransactionID       string    `json:"transaction_id"`
	ReversalCompletedAt time.Time `json:"reversal_completed_at"`

	// ElapsedMS is detection to confirmation — the number a regulator asks for.
	ElapsedMS *int64 `json:"elapsed_ms,omitempty"`
}

type transactionView struct {
	TransactionID        string     `json:"transaction_id"`
	Status               string     `json:"status"`
	TransactionType      string     `json:"transaction_type"`
	Provider             string     `json:"provider,omitempty"`
	AmountMinor          int64      `json:"amount_minor"`
	Currency             string     `json:"currency"`
	DebitAt              time.Time  `json:"debit_at"`
	CreditAt             *time.Time `json:"credit_at,omitempty"`
	ExpectedCompletionAt time.Time  `json:"expected_completion_at"`
	DetectedAt           *time.Time `json:"detected_at,omitempty"`
	ReversalTriggeredAt  *time.Time `json:"reversal_triggered_at,omitempty"`
	ReversalCompletedAt  *time.Time `json:"reversal_completed_at,omitempty"`
	IsBackfill           bool       `json:"backfill,omitempty"`
}

func viewOf(t *domain.Transaction) transactionView {
	return transactionView{
		TransactionID:        t.TransactionID,
		Status:               t.Status.String(),
		TransactionType:      t.TransactionType,
		Provider:             t.Provider,
		AmountMinor:          t.AmountMinor,
		Currency:             t.Currency,
		DebitAt:              t.DebitAt,
		CreditAt:             t.CreditAt,
		ExpectedCompletionAt: t.ExpectedCompletionAt,
		DetectedAt:           t.DetectedAt,
		ReversalTriggeredAt:  t.ReversalTriggeredAt,
		ReversalCompletedAt:  t.ReversalCompletedAt,
		IsBackfill:           t.IsBackfill,
	}
}

// --- handlers ---

func (s *Server) handleDebit(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}

	var req debitRequest
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}

	ev, err := s.toDebitEvent(principal.TenantID, idempotencyKey(r, req.IdempotencyKey), &req)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	window, err := s.resolveWindow(r, ev)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	if err := s.sink.Submit(domain.NewDebitEvent(ev)); err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	s.writeJSON(w, r, http.StatusAccepted, debitAccepted{
		Status:               "accepted",
		TransactionID:        ev.TransactionID,
		ExpectedCompletionAt: ev.DebitAt.Add(window),
		WindowSeconds:        int(window / time.Second),
	})
}

func (s *Server) handleCredit(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}

	var req creditRequest
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}

	ev, err := s.toCreditEvent(principal.TenantID, idempotencyKey(r, req.IdempotencyKey), &req)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	if err := s.sink.Submit(domain.NewCreditEvent(ev)); err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	// 202 rather than the reconciled/duration_ms body sketched in §7.1:
	// correlation is asynchronous, so the outcome is not known yet. See
	// docs/adr/0003.
	s.writeJSON(w, r, http.StatusAccepted, creditAccepted{
		Status:        "accepted",
		TransactionID: ev.TransactionID,
	})
}

func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}

	var req bulkRequest
	if !s.decode(w, r, s.maxBulkBody, &req) {
		return
	}
	if len(req.Events) == 0 {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "events is required", "events")
		return
	}
	if len(req.Events) > MaxBulkEvents {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("at most %d events per request", MaxBulkEvents), "events")
		return
	}

	res := bulkAccepted{}
	for i, raw := range req.Events {
		ev, err := s.toBulkEvent(principal.TenantID, &raw)
		if err == nil {
			err = s.sink.Submit(ev)
		}
		if err != nil {
			// Backpressure applies to the whole request, not one event: stopping
			// here tells the client to retry rather than silently losing the tail.
			if errors.Is(err, pipeline.ErrBackpressure) || errors.Is(err, pipeline.ErrClosed) {
				s.writeDomainError(w, r, err)
				return
			}
			res.Rejected = append(res.Rejected, rejectionFor(i, err))
			continue
		}
		res.Accepted++
	}

	s.writeJSON(w, r, http.StatusAccepted, res)
}

func (s *Server) handleReversalCompleted(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}

	var req reversalCompletedRequest
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}

	ev := &domain.ReversalCompletedEvent{
		TenantID:      principal.TenantID,
		TransactionID: req.TransactionID,
		CompletedAt:   req.CompletedAt,
	}
	if ev.CompletedAt.IsZero() {
		ev.CompletedAt = s.now().UTC()
	}
	if err := ev.Validate(); err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	// Applied synchronously: this is a low-volume confirmation whose result the
	// caller needs, not a high-rate ingest event.
	txn, err := s.store.MarkReversalCompleted(r.Context(), ev.TenantID, ev.TransactionID, ev.CompletedAt)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	// Close the claim that authorised this, if one was taken. Best effort: a
	// confirmation that arrives without a claim is normal — the interlock is
	// opt-in, and refusing to record a real reversal because its bookkeeping is
	// missing would be the wrong trade.
	if s.claims != nil {
		if err := s.claims.ConfirmReversalClaim(r.Context(), ev.TenantID, ev.TransactionID, ev.CompletedAt); err != nil &&
			!errors.Is(err, store.ErrNotFound) {
			s.log.ErrorContext(r.Context(), "could not confirm the reversal claim",
				slog.String("tenant_id", ev.TenantID),
				slog.String("transaction_id", ev.TransactionID),
				slog.String("error", err.Error()))
		}
	}

	out := reversalCompleted{
		Status:        "recorded",
		TransactionID: txn.TransactionID,
	}
	if txn.ReversalCompletedAt != nil {
		out.ReversalCompletedAt = *txn.ReversalCompletedAt
		if txn.DetectedAt != nil {
			ms := txn.ReversalCompletedAt.Sub(*txn.DetectedAt).Milliseconds()
			out.ElapsedMS = &ms
		}
	}
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}

	txn, err := s.store.Get(r.Context(), principal.TenantID, r.PathValue("transaction_id"))
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, viewOf(txn))
}

func (s *Server) handleListTransactions(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}

	status := domain.Status(r.URL.Query().Get("status"))
	if !status.Valid() {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			"status must be a known transaction state", "status")
		return
	}

	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 1000 {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request",
				"limit must be between 1 and 1000", "limit")
			return
		}
		limit = n
	}

	txns, err := s.store.ListByStatus(r.Context(), principal.TenantID, status, limit)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	views := make([]transactionView, 0, len(txns))
	for _, t := range txns {
		views = append(views, viewOf(t))
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"transactions": views})
}

// handleAuditVerify walks the tenant's chain and reports whether it is intact.
//
// The customer runs this themselves, and it recomputes every hash from the
// stored content rather than trusting anything we previously wrote. A pass means
// no record was altered, replaced or removed since it was written.
func (s *Server) handleAuditVerify(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}
	if s.audit == nil {
		// Saying "unavailable" beats returning a pass this build cannot support.
		s.writeError(w, r, http.StatusNotImplemented, "unavailable",
			"audit verification is not configured on this deployment", "")
		return
	}

	limit := 10000
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 100000 {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request",
				"limit must be between 1 and 100000", "limit")
			return
		}
		limit = n
	}

	records, err := s.audit.ListAudit(r.Context(), principal.TenantID, limit)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	result := audit.VerifyChain(principal.TenantID, records)

	// A broken chain is a successful verification that found tampering, not a
	// failed request — 200 with verified:false. A 5xx would suggest our bug and
	// send the operator looking in the wrong place.
	s.writeJSON(w, r, http.StatusOK, result)
}

type claimRequest struct {
	ClaimedBy string `json:"claimed_by"`
}

type claimResponse struct {
	Granted       bool      `json:"granted"`
	TransactionID string    `json:"transaction_id"`
	ClaimToken    string    `json:"claim_token,omitempty"`
	ClaimedBy     string    `json:"claimed_by"`
	ClaimedAt     time.Time `json:"claimed_at"`
	Status        string    `json:"transaction_status"`

	// Instruction is what to do about it, in words, because the caller is a
	// worker about to move money.
	Instruction string `json:"instruction"`
}

// claimableStatus is the one state in which reversing is still the right advice.
//
// This is the point of the interlock beyond deduplication: between the webhook
// leaving and their worker acting, the rail may have confirmed the credit
// arrived. The claim is the last checkpoint before money moves, so it re-reads
// the current verdict rather than trusting a webhook that may be minutes old.
//
// Only reversal_pending, deliberately. An orphan has been detected but not yet
// advised on — corroboration may still settle it — and reversal_completed is
// reachable only from here, so claiming any other state would hand out a claim
// that could never be confirmed.
const claimableStatus = domain.StatusReversalPending

// handleClaimReversal grants exactly one caller the right to reverse.
func (s *Server) handleClaimReversal(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}
	if s.claims == nil {
		s.writeError(w, r, http.StatusNotImplemented, "unavailable",
			"reversal claims are not configured on this deployment", "")
		return
	}

	transactionID := r.PathValue("transaction_id")
	var req claimRequest
	if r.ContentLength > 0 && !s.decode(w, r, s.maxBody, &req) {
		return
	}
	if req.ClaimedBy == "" {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			"claimed_by is required: name the worker taking the claim, so an outstanding one can be traced", "claimed_by")
		return
	}

	txn, err := s.store.Get(r.Context(), principal.TenantID, transactionID)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	if txn.Status != claimableStatus {
		// 409, not 200-with-a-flag: a client that checks only the status code
		// then does the safe thing by default.
		s.writeError(w, r, http.StatusConflict, "not_reversible",
			fmt.Sprintf("this transaction is %s, not %s; there is no standing advice to reverse it",
				txn.Status, claimableStatus),
			"")
		return
	}

	token, err := newClaimToken()
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	claim, granted, err := s.claims.ClaimReversal(r.Context(), principal.TenantID,
		transactionID, req.ClaimedBy, token, s.now().UTC())
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	out := claimResponse{
		Granted:       granted,
		TransactionID: transactionID,
		ClaimedBy:     claim.ClaimedBy,
		ClaimedAt:     claim.ClaimedAt,
		Status:        txn.Status.String(),
	}
	if !granted {
		// Also 409. The whole value of this endpoint is that a naive client —
		// one that reverses whenever the call succeeds — cannot double-pay.
		out.Instruction = "another worker already holds this reversal; do not reverse"
		s.writeJSON(w, r, http.StatusConflict, out)
		return
	}

	out.ClaimToken = claim.ClaimToken
	out.Instruction = "you hold this reversal. Reverse it, then confirm with POST /v1/events/reversal-completed"
	s.writeJSON(w, r, http.StatusOK, out)
}

// handleReleaseReversalClaim frees a claim whose holder died before reversing.
func (s *Server) handleReleaseReversalClaim(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}
	if s.claims == nil {
		s.writeError(w, r, http.StatusNotImplemented, "unavailable",
			"reversal claims are not configured on this deployment", "")
		return
	}

	transactionID := r.PathValue("transaction_id")
	err := s.claims.ReleaseReversalClaim(r.Context(), principal.TenantID, transactionID)
	if errors.Is(err, store.ErrNotFound) {
		// Either there is no claim, or it is confirmed. Both mean the same
		// thing to the caller: there is nothing here that is safe to release.
		s.writeError(w, r, http.StatusConflict, "not_releasable",
			"no unconfirmed claim on this transaction; a confirmed claim is never released because the money has already moved", "")
		return
	}
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"status":         "released",
		"transaction_id": transactionID,
		"instruction":    "the reversal can be claimed again",
	})
}

func newClaimToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate claim token: %w", err)
	}
	return "rcl_" + hex.EncodeToString(b[:]), nil
}

// handleFireDrill delivers a synthetic reversal to the customer's own endpoints
// and reports what each did.
//
// Synchronous on purpose: an integration test whose result you have to go
// looking for does not get run.
func (s *Server) handleFireDrill(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}
	if s.drills == nil {
		s.writeError(w, r, http.StatusNotImplemented, "unavailable",
			"fire drills are not configured on this deployment", "")
		return
	}

	report, err := s.drills.Run(r.Context(), principal.TenantID)
	if errors.Is(err, drill.ErrNoEndpoint) {
		// Nothing registered is a real finding, not a server fault: it means a
		// genuine reversal would have nowhere to go either.
		s.writeError(w, r, http.StatusConflict, "no_endpoint",
			"no enabled endpoint subscribes to reversal.triggered, so a real reversal would not be delivered either", "")
		return
	}
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	// A failing drill is a successful test that found a problem. 200 with
	// failed > 0 — a 5xx would point the operator at us instead of at their
	// handler.
	s.writeJSON(w, r, http.StatusOK, report)
}

// maxReportCandidates bounds how many detected transactions one report examines.
// Exceeding it is a real event — that many failures in one period is a crisis,
// not a routine report — so it is surfaced, never silently absorbed.
const maxReportCandidates = 10000

// handleComplianceReport answers the question a regulator actually asks: show me
// every failed transfer in this period and prove you reversed it in time.
func (s *Server) handleComplianceReport(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "invalid api key", "")
		return
	}
	if s.reports == nil {
		s.writeError(w, r, http.StatusNotImplemented, "unavailable",
			"reporting is not configured on this deployment", "")
		return
	}

	q := r.URL.Query()
	now := s.now().UTC()

	// Defaults to the last 30 days, so the common case needs no parameters.
	from, err := parseDay(q.Get("from"), now.AddDate(0, 0, -30))
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			"from must be RFC3339 or YYYY-MM-DD", "from")
		return
	}
	to, err := parseDay(q.Get("to"), now)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			"to must be RFC3339 or YYYY-MM-DD", "to")
		return
	}
	if !from.Before(to) {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			"from must be before to", "from")
		return
	}

	// The mandated reversal window differs by regulator and transaction type, so
	// it is a parameter. Baking in one jurisdiction's number would quietly
	// produce wrong reports everywhere else.
	deadline := report.DefaultReversalDeadline
	if raw := q.Get("deadline_seconds"); raw != "" {
		secs, convErr := strconv.Atoi(raw)
		if convErr != nil || secs <= 0 {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request",
				"deadline_seconds must be a positive integer", "deadline_seconds")
			return
		}
		deadline = time.Duration(secs) * time.Second
	}

	counts, err := s.reports.CountByStatus(r.Context(), principal.TenantID, from, to)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	// One over the cap, so a tenant that exceeds it is detected rather than
	// silently reported on with understated counts.
	candidates, err := s.reports.ListReversalCandidates(r.Context(), principal.TenantID, from, to, maxReportCandidates+1)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	incomplete := len(candidates) > maxReportCandidates
	if incomplete {
		candidates = candidates[:maxReportCandidates]
	}

	doc := report.Compute(report.Input{
		TenantID:       principal.TenantID,
		From:           from,
		To:             to,
		CountsByStatus: counts,
		Reversals:      candidates,
		Incomplete:     incomplete,
	}, deadline, now)

	switch q.Get("format") {
	case "", "json":
		s.writeJSON(w, r, http.StatusOK, doc)
	case "csv":
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition",
			fmt.Sprintf("attachment; filename=\"reversal-compliance-%s.csv\"", from.Format("2006-01-02")))
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, doc.CSV())
	default:
		// PDF is in the specification but not built. Saying so beats returning
		// something that is not a PDF under a pdf request.
		s.writeError(w, r, http.StatusBadRequest, "invalid_request",
			"format must be json or csv; pdf is not yet supported", "format")
	}
}

// parseDay accepts a full timestamp or a plain date, since a compliance officer
// types the latter.
func parseDay(raw string, fallback time.Time) (time.Time, error) {
	if raw == "" {
		return fallback.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// handleHealthz reports process liveness only. It must never consult the
// database: pointing a liveness probe at a dependency turns a brief blip into a
// simultaneous restart of every pod (§11.1).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		if err := s.ready(r.Context()); err != nil {
			s.writeJSON(w, r, http.StatusServiceUnavailable, map[string]string{
				"status": "unready",
				"reason": "dependency unavailable",
			})
			return
		}
	}
	s.writeJSON(w, r, http.StatusOK, map[string]string{"status": "ready"})
}

// handleMetrics emits Prometheus text format by hand, so the binary carries no
// metrics client dependency (§7.3: every SDK dependency is one a customer's
// security team must approve).
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	st := s.sink.Stats()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	// value stays untyped so counters (uint64) and the depth gauge (int) can
	// share one table without a conversion.
	metrics := []struct {
		name, help, kind string
		value            any
	}{
		{"reconsync_events_received_total", "Events accepted onto the pipeline.", "counter", st.Received},
		{"reconsync_events_dropped_total", "Events refused by backpressure.", "counter", st.Dropped},
		{"reconsync_events_flushed_total", "Events handed to correlation.", "counter", st.Flushed},
		{"reconsync_batches_total", "Batches dispatched to correlation.", "counter", st.Batches},
		{"reconsync_handler_errors_total", "Batches the handler failed.", "counter", st.HandlerErrors},
		{"reconsync_ingest_queue_depth", "Events waiting in the ingest queue.", "gauge", st.QueueDepth},
	}

	var out strings.Builder
	for _, m := range metrics {
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", m.name, m.help, m.name, m.kind, m.name, m.value)
	}
	// A scrape that disconnects mid-write is not worth logging; nothing else can
	// be done once the status line has gone out.
	_, _ = io.WriteString(w, out.String())
}

// --- conversion and decoding ---

func (s *Server) toDebitEvent(tenantID, idemKey string, req *debitRequest) (*domain.DebitEvent, error) {
	ev := &domain.DebitEvent{
		TenantID:        tenantID,
		TransactionID:   req.TransactionID,
		IdempotencyKey:  idemKey,
		TransactionType: req.TransactionType,
		Provider:        req.Provider,
		AmountMinor:     req.AmountMinor,
		Currency:        req.Currency,
		DebitAt:         req.DebitAt.UTC(),
		CustomerRef:     req.CustomerRef,
		Metadata:        req.Metadata,
		IsBackfill:      req.Backfill,
	}
	if err := ev.Validate(); err != nil {
		return nil, err
	}
	// A debit cannot have happened meaningfully in the future. Rejecting it here
	// stops one skewed clock from setting a window that never closes.
	if !ev.IsBackfill && ev.DebitAt.After(s.now().UTC().Add(maxClockSkew)) {
		return nil, domain.ValidationError{Field: "debit_at", Reason: "is too far in the future"}
	}
	if err := domain.ScreenMetadata(ev.Metadata); err != nil {
		return nil, err
	}
	if err := domain.ScreenString("transaction_id", ev.TransactionID); err != nil {
		return nil, err
	}
	return ev, nil
}

func (s *Server) toCreditEvent(tenantID, idemKey string, req *creditRequest) (*domain.CreditEvent, error) {
	ev := &domain.CreditEvent{
		TenantID:          tenantID,
		TransactionID:     req.TransactionID,
		IdempotencyKey:    idemKey,
		CreditAt:          req.CreditAt.UTC(),
		ProviderReference: req.ProviderReference,
		Status:            domain.CreditStatus(req.Status),
	}
	if ev.CreditAt.IsZero() {
		ev.CreditAt = s.now().UTC()
	}
	if err := ev.Validate(); err != nil {
		return nil, err
	}
	if err := domain.ScreenString("provider_reference", ev.ProviderReference); err != nil {
		return nil, err
	}
	return ev, nil
}

func (s *Server) toBulkEvent(tenantID string, raw *bulkEvent) (domain.Event, error) {
	kind := raw.Type
	if kind == "" {
		// Infer from shape so a client need not restate what the fields say.
		if raw.CreditStatus != "" {
			kind = "credit"
		} else {
			kind = "debit"
		}
	}

	switch kind {
	case "debit":
		ev, err := s.toDebitEvent(tenantID, raw.IdempotencyKey, &raw.debitRequest)
		if err != nil {
			return domain.Event{}, err
		}
		return domain.NewDebitEvent(ev), nil

	case "credit":
		ev, err := s.toCreditEvent(tenantID, raw.IdempotencyKey, &creditRequest{
			TransactionID:     raw.TransactionID,
			IdempotencyKey:    raw.IdempotencyKey,
			CreditAt:          raw.CreditAt,
			ProviderReference: raw.ProviderReference,
			Status:            raw.CreditStatus,
		})
		if err != nil {
			return domain.Event{}, err
		}
		return domain.NewCreditEvent(ev), nil

	default:
		return domain.Event{}, domain.ValidationError{Field: "type", Reason: "must be debit or credit"}
	}
}

func (s *Server) resolveWindow(r *http.Request, ev *domain.DebitEvent) (time.Duration, error) {
	set, err := s.rules(r.Context(), ev.TenantID)
	if err != nil {
		return 0, fmt.Errorf("load rules: %w", err)
	}
	return set.Resolve(&domain.Transaction{
		TransactionType: ev.TransactionType,
		Provider:        ev.Provider,
		Currency:        ev.Currency,
		AmountMinor:     ev.AmountMinor,
	}).Window, nil
}

// decode reads a size-limited JSON body. It reports whether decoding succeeded,
// having already written the error response if it did not.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	// Unknown fields are allowed on purpose: a newer SDK sending a field this
	// build does not know must not fail.
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr):
			s.writeError(w, r, http.StatusRequestEntityTooLarge, "payload_too_large",
				"request body exceeds the limit", "")
		case errors.Is(err, io.EOF):
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "request body is empty", "")
		default:
			s.writeError(w, r, http.StatusBadRequest, "invalid_request", "request body is not valid JSON", "")
		}
		return false
	}
	return true
}

// idempotencyKey prefers the header, which is where §7.1 puts it, and falls
// back to the body so bulk events can carry their own.
func idempotencyKey(r *http.Request, fromBody string) string {
	if h := r.Header.Get("Idempotency-Key"); h != "" {
		return h
	}
	return fromBody
}

func rejectionFor(index int, err error) bulkRejection {
	out := bulkRejection{Index: index, Code: "invalid_request", Error: err.Error()}

	var ve domain.ValidationError
	if errors.As(err, &ve) {
		out.Field = ve.Field
		return out
	}
	var sde domain.SensitiveDataError
	if errors.As(err, &sde) {
		out.Code = "sensitive_data_rejected"
		out.Field = sde.Path
		return out
	}
	if errors.Is(err, store.ErrNotFound) {
		out.Code = "not_found"
	}
	return out
}
