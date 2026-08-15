package ingest

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/account"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
)

// The browser side of authentication.
//
// Two credentials now reach this server: an API key on the Authorization header
// for services, and a session cookie for people. They resolve to the same
// Principal, so every handler downstream asks one question — what may this
// caller do — and never which kind of caller it is.

const (
	sessionCookie = "reconsync_session"

	// csrfCookie is readable by the page; csrfHeader is what the page sends
	// back. A cross-site request can cause the cookie to be sent but cannot
	// read it to set the header, which is what makes the pair work.
	csrfCookie = "reconsync_csrf"
	csrfHeader = "X-ReconSync-CSRF"
)

// setSessionCookie writes the session cookie for a signed-in browser.
func setSessionCookie(w http.ResponseWriter, r *http.Request, token string, expires time.Time) {
	//nolint:gosec // Secure follows the actual connection; see isSecure
	http.SetCookie(w, &http.Cookie{
		Name:  sessionCookie,
		Value: token,
		Path:  "/",
		// The page never needs to read this, and not being able to is what
		// stops an injected script from stealing the session outright.
		HttpOnly: true,
		// Strict, not Lax: nothing about this dashboard is a link someone
		// follows from elsewhere, so there is no flow to break.
		SameSite: http.SameSiteStrictMode,
		Secure:   isSecure(r),
		Expires:  expires,
	})
}

func setCSRFCookie(w http.ResponseWriter, r *http.Request, token string, expires time.Time) {
	//nolint:gosec // deliberately readable: the page has to echo it back
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookie,
		Value:    token,
		Path:     "/",
		HttpOnly: false, // the page has to read it to echo it back
		SameSite: http.SameSiteStrictMode,
		Secure:   isSecure(r),
		Expires:  expires,
	})
}

func clearAuthCookies(w http.ResponseWriter, r *http.Request) {
	for _, name := range []string{sessionCookie, csrfCookie} {
		//nolint:gosec // an expiry, carrying no value to protect
		http.SetCookie(w, &http.Cookie{
			Name: name, Value: "", Path: "/", MaxAge: -1,
			HttpOnly: name == sessionCookie,
			SameSite: http.SameSiteStrictMode,
			Secure:   isSecure(r),
		})
	}
}

// isSecure reports whether the connection reached us over TLS.
//
// Marking the cookie Secure on a plain HTTP development server would stop the
// browser sending it at all, so this follows the actual connection rather than
// hard-coding either answer.
func isSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		if first, _, ok := strings.Cut(fwd, ","); ok {
			return strings.TrimSpace(first)
		}
		return strings.TrimSpace(fwd)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// principalFromSession resolves a session cookie to a Principal.
func (s *Server) principalFromSession(r *http.Request) (auth.Principal, *account.Session, bool) {
	if s.accounts == nil {
		return auth.Principal{}, nil, false
	}
	c, err := r.Cookie(sessionCookie)
	if err != nil || c.Value == "" {
		return auth.Principal{}, nil, false
	}

	sess, user, err := s.accounts.Resolve(r.Context(), c.Value)
	if err != nil {
		return auth.Principal{}, nil, false
	}

	// The role becomes scopes, so a handler asking HasScope gets the same
	// answer whether the caller is a person or a service.
	return auth.Principal{
		TenantID: user.TenantID,
		KeyID:    user.ID,
		Scopes:   user.Role.Scopes(),
	}, sess, true
}

// requireCSRF checks the double-submit token on state-changing requests.
//
// A session cookie is sent by the browser on any request to this origin,
// including one triggered by another site. SameSite=Strict already blocks that
// in every browser that honours it; this is the belt to that pair of braces,
// and costs one header.
func (s *Server) requireCSRF(w http.ResponseWriter, r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}
	// Only cookie-authenticated calls need it. A service sending an API key is
	// not a browser and cannot be induced to send one implicitly.
	if _, err := r.Cookie(sessionCookie); err != nil {
		return true
	}

	c, err := r.Cookie(csrfCookie)
	if err != nil || c.Value == "" || c.Value != r.Header.Get(csrfHeader) {
		s.writeError(w, r, http.StatusForbidden, "csrf",
			"missing or mismatched CSRF token", "")
		return false
	}
	return true
}

// loginLimiter throttles failed sign-ins per address.
//
// The per-account lockout stops one account being ground down; this stops one
// source spraying a common password across many accounts, which the lockout
// alone would never notice because no single account accumulates failures.
//
// Only failures count. Charging successful sign-ins to the same budget would
// throttle an office of twenty people arriving at nine o'clock behind one
// egress address — punishing the ordinary case to slow an attack that is, by
// definition, made of failures.
type loginLimiter struct {
	failures map[string][]time.Time
	mu       chan struct{}
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{failures: map[string][]time.Time{}, mu: make(chan struct{}, 1)}
}

const (
	loginWindow    = time.Minute
	loginMaxFails  = 10
	limiterMaxKeys = 10_000
)

// allow reports whether another attempt from this source is permitted.
func (l *loginLimiter) allow(key string, now time.Time) bool {
	l.mu <- struct{}{}
	defer func() { <-l.mu }()

	return len(l.recent(key, now)) < loginMaxFails
}

// fail records a failed attempt.
func (l *loginLimiter) fail(key string, now time.Time) {
	l.mu <- struct{}{}
	defer func() { <-l.mu }()

	l.failures[key] = append(l.recent(key, now), now)

	// Bounded: without this an attacker rotating source addresses grows the map
	// until the process dies, which is a denial of service via the thing meant
	// to prevent one.
	if len(l.failures) > limiterMaxKeys {
		cutoff := now.Add(-loginWindow)
		for k, v := range l.failures {
			if len(v) == 0 || v[len(v)-1].Before(cutoff) {
				delete(l.failures, k)
			}
		}
	}
}

// recent returns this source's failures inside the window, dropping the rest.
// The caller holds the lock.
func (l *loginLimiter) recent(key string, now time.Time) []time.Time {
	cutoff := now.Add(-loginWindow)
	kept := l.failures[key][:0]
	for _, t := range l.failures[key] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	l.failures[key] = kept
	return kept
}

// errorForAccount maps a sign-in failure to a response.
func (s *Server) errorForAccount(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, account.ErrInvalidCredentials):
		// One message for unknown address, wrong password and disabled account
		// alike. Anything more tells an attacker which addresses are real.
		s.writeError(w, r, http.StatusUnauthorized, "invalid_credentials",
			"that email and password did not match", "")
	case errors.Is(err, account.ErrLocked):
		s.writeError(w, r, http.StatusTooManyRequests, "locked",
			"too many attempts; the account is locked for a few minutes", "")
	case errors.Is(err, account.ErrTOTPInvalid):
		s.writeError(w, r, http.StatusUnauthorized, "invalid_code",
			"that code did not verify", "code")
	case errors.Is(err, account.ErrEmailTaken):
		s.writeError(w, r, http.StatusConflict, "email_taken",
			"that email already has an account", "email")
	case errors.Is(err, account.ErrNotFound):
		s.writeError(w, r, http.StatusNotFound, "not_found", "not found", "")
	default:
		s.writeDomainError(w, r, err)
	}
}
