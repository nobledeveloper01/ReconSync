package ingest

import (
	"errors"
	"net/http"

	"github.com/nobledeveloper01/ReconSync/internal/account"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
)

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Code     string `json:"code,omitempty"`
}

type sessionView struct {
	Email       string   `json:"email"`
	Role        string   `json:"role"`
	TenantID    string   `json:"tenant_id"`
	Scopes      []string `json:"scopes"`
	TOTPEnabled bool     `json:"totp_enabled"`
	CSRF        string   `json:"csrf_token"`
}

// handleLogin signs a person in, in one or two steps.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.accounts == nil {
		s.writeError(w, r, http.StatusNotImplemented, "unavailable",
			"user accounts are not configured on this deployment", "")
		return
	}

	var req loginRequest
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}

	// Per-source throttling, which the per-account lockout cannot do: one
	// source spraying one common password across many accounts never trips a
	// single account's counter.
	source := clientIP(r)
	if !s.logins.allow(source, s.now()) {
		s.writeError(w, r, http.StatusTooManyRequests, "slow_down",
			"too many failed sign-ins from this address; wait a minute", "")
		return
	}

	attempt, err := s.accounts.Authenticate(r.Context(), req.Email, req.Password)
	if err != nil {
		s.logins.fail(source, s.now())
		s.errorForAccount(w, r, err)
		return
	}

	if attempt.NeedsTOTP {
		if req.Code == "" {
			// No session yet. A half-authenticated request must not be able to
			// read anything, so this is a plain "now prove the second factor"
			// rather than a partially privileged state.
			s.writeJSON(w, r, http.StatusOK, map[string]any{
				"totp_required": true,
				"user_id":       attempt.User.ID,
			})
			return
		}
		if _, err := s.accounts.VerifySecondFactor(r.Context(), attempt.User.ID, req.Code); err != nil {
			// A wrong code is a failed sign-in like any other: without this, a
			// correct password plus a million guessed codes would never be
			// throttled by source.
			s.logins.fail(source, s.now())
			s.errorForAccount(w, r, err)
			return
		}
	}

	s.issueSession(w, r, attempt.User)
}

// issueSession mints the cookies and returns who the caller now is.
func (s *Server) issueSession(w http.ResponseWriter, r *http.Request, u *account.User) {
	token, sess, err := s.accounts.StartSession(r.Context(), u, r.UserAgent(), clientIP(r))
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	csrf, _, err := account.NewToken()
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	setSessionCookie(w, r, token, sess.ExpiresAt)
	setCSRFCookie(w, r, csrf, sess.ExpiresAt)

	s.writeJSON(w, r, http.StatusOK, sessionView{
		Email: u.Email, Role: string(u.Role), TenantID: u.TenantID,
		Scopes: u.Role.Scopes(), TOTPEnabled: u.TOTPEnabled, CSRF: csrf,
	})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && s.accounts != nil {
		if err := s.accounts.EndSession(r.Context(), c.Value); err != nil {
			s.log.WarnContext(r.Context(), "could not delete session", "error", err.Error())
		}
	}
	clearAuthCookies(w, r)
	s.writeJSON(w, r, http.StatusOK, map[string]any{"status": "signed out"})
}

// handleMe reports who the caller is, which is what the dashboard asks on load.
func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "not signed in", "")
		return
	}
	if s.accounts == nil {
		// An API key rather than a person. Still a valid caller, so answer with
		// what is true rather than pretending there is a user.
		s.writeJSON(w, r, http.StatusOK, sessionView{
			TenantID: principal.TenantID, Scopes: principal.Scopes,
		})
		return
	}

	u, err := s.accounts.Store().UserByID(r.Context(), principal.KeyID)
	if err != nil {
		s.writeJSON(w, r, http.StatusOK, sessionView{
			TenantID: principal.TenantID, Scopes: principal.Scopes,
		})
		return
	}
	csrf := ""
	if c, err := r.Cookie(csrfCookie); err == nil {
		csrf = c.Value
	}
	s.writeJSON(w, r, http.StatusOK, sessionView{
		Email: u.Email, Role: string(u.Role), TenantID: u.TenantID,
		Scopes: u.Role.Scopes(), TOTPEnabled: u.TOTPEnabled, CSRF: csrf,
	})
}

// --- second factor ---

func (s *Server) handleTOTPBegin(w http.ResponseWriter, r *http.Request) {
	u, ok := s.currentUser(w, r)
	if !ok {
		return
	}
	secret, uri, err := s.accounts.BeginTOTPEnrolment(r.Context(), u, "ReconSync")
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	// The secret is always returned alongside, so a scanner that will not read
	// the code is an inconvenience rather than a dead end.
	svg, err := account.QRSVG(uri)
	if err != nil {
		s.log.WarnContext(r.Context(), "could not render the enrolment QR", "error", err.Error())
		svg = ""
	}

	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"secret": secret,
		"uri":    uri,
		"qr":     svg,
		"notice": "scan this, then confirm with a code. Two-factor is not enabled until a code verifies.",
	})
}

func (s *Server) handleTOTPConfirm(w http.ResponseWriter, r *http.Request) {
	u, ok := s.currentUser(w, r)
	if !ok {
		return
	}
	var req struct {
		Code string `json:"code"`
	}
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}

	codes, err := s.accounts.ConfirmTOTPEnrolment(r.Context(), u.ID, req.Code)
	if err != nil {
		s.errorForAccount(w, r, err)
		return
	}
	// Shown exactly once. They are stored hashed, so this is the only moment
	// they can be read — which is why the notice says so.
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"enabled":        true,
		"recovery_codes": codes,
		"notice": "save these now. Each works once, and they are the only way in if you " +
			"lose your authenticator. They cannot be shown again.",
	})
}

func (s *Server) handleTOTPDisable(w http.ResponseWriter, r *http.Request) {
	u, ok := s.currentUser(w, r)
	if !ok {
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}
	// The password again: turning off a second factor is exactly what someone
	// who borrowed an unlocked laptop would do first.
	if !account.VerifyPassword(u.PasswordHash, req.Password) {
		s.errorForAccount(w, r, account.ErrInvalidCredentials)
		return
	}
	if err := s.accounts.Store().DisableTOTP(r.Context(), u.ID); err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"enabled": false})
}

// --- password ---

func (s *Server) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	u, ok := s.currentUser(w, r)
	if !ok {
		return
	}
	var req struct {
		Current string `json:"current_password"`
		New     string `json:"new_password"`
	}
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}
	if err := s.accounts.ChangePassword(r.Context(), u.ID, req.Current, req.New); err != nil {
		s.errorForAccount(w, r, err)
		return
	}
	// Every session ended, including this one: a password change is usually a
	// response to a suspected compromise, and leaving the intruder signed in
	// defeats the point.
	clearAuthCookies(w, r)
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"status": "changed",
		"notice": "every session was signed out, including this one. Sign in again.",
	})
}

func (s *Server) handleCompleteReset(w http.ResponseWriter, r *http.Request) {
	if s.accounts == nil {
		s.writeError(w, r, http.StatusNotImplemented, "unavailable", "user accounts are not configured", "")
		return
	}
	var req struct {
		Token    string `json:"token"`
		Password string `json:"password"`
	}
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}
	source := clientIP(r)
	if !s.logins.allow(source, s.now()) {
		s.writeError(w, r, http.StatusTooManyRequests, "slow_down", "too many attempts", "")
		return
	}

	if _, err := s.accounts.CompleteReset(r.Context(), req.Token, req.Password); err != nil {
		s.logins.fail(source, s.now())
		if errors.Is(err, account.ErrNotFound) {
			// Expired, already used, or never existed — one answer, because
			// distinguishing them turns the endpoint into a token oracle.
			s.writeError(w, r, http.StatusBadRequest, "invalid_token",
				"that reset link is not valid; ask an administrator for a new one", "token")
			return
		}
		s.errorForAccount(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"status": "password set"})
}

// --- users, admin only ---

func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	users, err := s.accounts.Store().ListUsers(r.Context(), principal.TenantID)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	out := make([]map[string]any, 0, len(users))
	for _, u := range users {
		out = append(out, map[string]any{
			"id": u.ID, "email": u.Email, "role": string(u.Role),
			"totp_enabled": u.TOTPEnabled, "disabled": u.DisabledAt != nil,
			"last_login_at": u.LastLoginAt, "created_at": u.CreatedAt,
		})
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"users": out})
}

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}

	u, err := s.accounts.Create(r.Context(), principal.TenantID, req.Email, req.Password,
		account.Role(req.Role))
	if err != nil {
		s.errorForAccount(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusCreated, map[string]any{
		"id": u.ID, "email": u.Email, "role": string(u.Role),
	})
}

func (s *Server) handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	id := r.PathValue("user_id")

	var req struct {
		Role     *string `json:"role,omitempty"`
		Disabled *bool   `json:"disabled,omitempty"`
	}
	if !s.decode(w, r, s.maxBody, &req) {
		return
	}

	// An admin cannot demote or disable themselves. Not paternalism: the last
	// admin doing either leaves a tenant with nobody who can manage users, and
	// the only way back is the CLI on the server.
	if id == principal.KeyID && (req.Role != nil || req.Disabled != nil) {
		s.writeError(w, r, http.StatusConflict, "self_change",
			"change your own role or status from another admin account, so a tenant is never left without one", "")
		return
	}

	if req.Role != nil {
		role := account.Role(*req.Role)
		if !role.Valid() {
			s.writeError(w, r, http.StatusBadRequest, "invalid_request",
				"role must be viewer, operator or admin", "role")
			return
		}
		if err := s.accounts.Store().UpdateUserRole(r.Context(), principal.TenantID, id, role); err != nil {
			s.errorForAccount(w, r, err)
			return
		}
	}
	if req.Disabled != nil {
		if err := s.accounts.Store().SetUserDisabled(r.Context(), principal.TenantID, id, *req.Disabled); err != nil {
			s.errorForAccount(w, r, err)
			return
		}
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"id": id, "status": "updated"})
}

// handleIssueReset hands an admin a single-use link for a locked-out colleague.
func (s *Server) handleIssueReset(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.requireAdmin(w, r)
	if !ok {
		return
	}
	id := r.PathValue("user_id")

	target, err := s.accounts.Store().UserByID(r.Context(), id)
	if err != nil || target.TenantID != principal.TenantID {
		s.writeError(w, r, http.StatusNotFound, "not_found", "no such user", "")
		return
	}

	token, expires, err := s.accounts.IssueReset(r.Context(), target.ID)
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"reset_token": token,
		"expires_at":  expires,
		"notice": "hand this to them over a channel you trust. It works once, expires in an hour, " +
			"and any earlier link for this user has stopped working.",
	})
}

func (s *Server) handleListSessions(w http.ResponseWriter, r *http.Request) {
	u, ok := s.currentUser(w, r)
	if !ok {
		return
	}
	sessions, err := s.accounts.Store().ListSessions(r.Context(), u.ID, s.now().UTC())
	if err != nil {
		s.writeDomainError(w, r, err)
		return
	}

	current := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		current = account.HashToken(c.Value)
	}
	out := make([]map[string]any, 0, len(sessions))
	for _, sess := range sessions {
		out = append(out, map[string]any{
			"user_agent": sess.UserAgent, "ip": sess.IP,
			"created_at": sess.CreatedAt, "last_seen_at": sess.LastSeenAt,
			// So a user can tell which row is the browser they are looking at,
			// and therefore which ones are not.
			"current": sess.TokenHash == current,
		})
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"sessions": out})
}

func (s *Server) handleRevokeSessions(w http.ResponseWriter, r *http.Request) {
	u, ok := s.currentUser(w, r)
	if !ok {
		return
	}
	if err := s.accounts.Store().DeleteUserSessions(r.Context(), u.ID); err != nil {
		s.writeDomainError(w, r, err)
		return
	}
	clearAuthCookies(w, r)
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"status": "all sessions ended",
		"notice": "including this one.",
	})
}

// --- helpers ---

// currentUser resolves the signed-in person, or writes the error.
func (s *Server) currentUser(w http.ResponseWriter, r *http.Request) (*account.User, bool) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok || s.accounts == nil {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "not signed in", "")
		return nil, false
	}
	u, err := s.accounts.Store().UserByID(r.Context(), principal.KeyID)
	if err != nil {
		// An API key, not a person. These endpoints are about an account, so
		// there is nothing to act on.
		s.writeError(w, r, http.StatusForbidden, "not_a_user",
			"this endpoint manages a user account; sign in rather than using an API key", "")
		return nil, false
	}
	return u, true
}

func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	principal, ok := auth.PrincipalFrom(r.Context())
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, "unauthenticated", "not signed in", "")
		return auth.Principal{}, false
	}
	if s.accounts == nil {
		s.writeError(w, r, http.StatusNotImplemented, "unavailable", "user accounts are not configured", "")
		return auth.Principal{}, false
	}
	if !principal.HasScope(auth.ScopeEndpointsWrite) {
		s.writeError(w, r, http.StatusForbidden, "forbidden",
			"managing users needs an administrator account", "")
		return auth.Principal{}, false
	}
	return principal, true
}
