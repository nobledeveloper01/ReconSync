package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/account"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
	"github.com/nobledeveloper01/ReconSync/internal/correlate"
	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/ingest"
	"github.com/nobledeveloper01/ReconSync/internal/pipeline"
	"github.com/nobledeveloper01/ReconSync/internal/rules"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

// The browser side of authentication, end to end against a real database.
//
// These use Postgres rather than the in-memory store on purpose: user accounts
// are only implemented there, and a session that is revocable in a map but not
// in the database would be a test that proves nothing.

type authFixture struct {
	server   *ingest.Server
	accounts *account.Service
	store    *store.Postgres
	tenant   string
	now      time.Time
}

func newAuthFixture(t *testing.T) *authFixture {
	t.Helper()

	pool := testPool(t)
	truncate(t, pool)
	s := store.NewPostgres(pool)
	ctx := context.Background()

	tenant := fmt.Sprintf("tenant_auth_%d", time.Now().UnixNano())
	if err := s.EnsureTenant(ctx, tenant, "Auth Test", string(auth.EnvTest)); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	ruleSet := rules.NewSet(nil)
	ruleProvider := func(context.Context, string) (*rules.Set, error) { return ruleSet, nil }
	engine, err := correlate.New(s, correlate.Options{
		Rules: ruleProvider,
		Salt:  func(_ context.Context, id string) (string, error) { return "salt_" + id, nil },
	})
	if err != nil {
		t.Fatalf("correlate.New: %v", err)
	}

	p, err := pipeline.New(pipeline.HandlerFunc(
		func(ctx context.Context, tenantID string, events []domain.Event) error {
			_, err := engine.Apply(ctx, tenantID, events)
			return err
		}), pipeline.Config{Workers: 1, BatchSize: 5, FlushInterval: 5 * time.Millisecond, BufferSize: 100})
	if err != nil {
		t.Fatalf("pipeline.New: %v", err)
	}
	p.Start(ctx)
	t.Cleanup(p.Close)

	authenticator, err := auth.New(s, auth.Options{})
	if err != nil {
		t.Fatalf("auth.New: %v", err)
	}

	f := &authFixture{accounts: nil, tenant: tenant, now: time.Now().UTC()}
	// A controllable clock, so lockout expiry and session expiry can be tested
	// without the test sleeping for fifteen minutes.
	f.accounts = account.NewService(s, func() time.Time { return f.now })

	srv, err := ingest.New(ingest.Options{
		Sink:     p,
		Rules:    ruleProvider,
		Store:    s,
		Reports:  s,
		Webhooks: s,
		Auth:     authenticator,
		Accounts: f.accounts,
		Now:      func() time.Time { return f.now },
	})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	f.server = srv
	f.store = s
	return f
}

// browser keeps the cookies a real one would, so CSRF and session handling are
// exercised rather than bypassed.
type browser struct {
	f       *authFixture
	cookies map[string]string
}

func (f *authFixture) browser() *browser {
	return &browser{f: f, cookies: map[string]string{}}
}

func (b *browser) do(t *testing.T, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(raw)
	}

	r := httptest.NewRequestWithContext(context.Background(), method, path, reader)
	r.Header.Set("Content-Type", "application/json")
	for name, value := range b.cookies {
		r.AddCookie(&http.Cookie{Name: name, Value: value})
	}
	if csrf, ok := b.cookies["reconsync_csrf"]; ok {
		r.Header.Set("X-ReconSync-CSRF", csrf)
	}

	w := httptest.NewRecorder()
	b.f.server.ServeHTTP(w, r)

	for _, c := range w.Result().Cookies() {
		if c.MaxAge < 0 {
			delete(b.cookies, c.Name)
			continue
		}
		b.cookies[c.Name] = c.Value
	}
	return w
}

func (b *browser) login(t *testing.T, email, password string) *httptest.ResponseRecorder {
	t.Helper()
	return b.do(t, http.MethodPost, "/v1/auth/login",
		map[string]string{"email": email, "password": password})
}

func decodeJSON(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return out
}

const testPassword = "correct horse battery staple"

// The whole point of a server-side session: signing in gives a cookie the page
// cannot read, and signing out kills it immediately everywhere.
func TestSignInIssuesARevocableSession(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "admin@example.com", testPassword, account.RoleViewer); err != nil {
		t.Fatalf("Create: %v", err)
	}

	b := f.browser()
	w := b.login(t, "admin@example.com", testPassword)
	if w.Code != http.StatusOK {
		t.Fatalf("login = %d: %s", w.Code, w.Body.String())
	}

	// The first account for a tenant is an admin whatever was asked for,
	// because otherwise nobody could grant the first admin their role.
	if role := decodeJSON(t, w)["role"]; role != "admin" {
		t.Errorf("first user role = %v, want admin", role)
	}

	var sessionCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "reconsync_session" {
			sessionCookie = c
		}
	}
	if sessionCookie == nil {
		t.Fatal("no session cookie was set")
	}
	if !sessionCookie.HttpOnly {
		t.Error("the session cookie is readable by script, so an injected script could steal it")
	}
	if sessionCookie.SameSite != http.SameSiteStrictMode {
		t.Error("the session cookie is not SameSite=Strict")
	}

	// The cookie authenticates the API with no key at all.
	if w := b.do(t, http.MethodGet, "/v1/transactions?status=orphaned", nil); w.Code != http.StatusOK {
		t.Fatalf("GET /v1/transactions with session = %d: %s", w.Code, w.Body.String())
	}

	// Signing out revokes it on the server, not merely in the browser: a copy
	// of the cookie taken beforehand must stop working too.
	stolen := f.browser()
	stolen.cookies["reconsync_session"] = sessionCookie.Value
	stolen.cookies["reconsync_csrf"] = b.cookies["reconsync_csrf"]

	if w := b.do(t, http.MethodPost, "/v1/auth/logout", map[string]string{}); w.Code != http.StatusOK {
		t.Fatalf("logout = %d: %s", w.Code, w.Body.String())
	}
	if w := stolen.do(t, http.MethodGet, "/v1/transactions?status=orphaned", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("a copy of a signed-out session still works: %d", w.Code)
	}
}

// A cookie is sent by the browser whoever caused the request, so a
// state-changing call needs proof the page itself made it.
func TestStateChangingCallsNeedTheCSRFToken(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "admin@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}
	b := f.browser()
	if w := b.login(t, "admin@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("login = %d: %s", w.Code, w.Body.String())
	}

	// A forged request carries the cookies but cannot read them to set the
	// header, which is exactly what this drops.
	forged := f.browser()
	forged.cookies["reconsync_session"] = b.cookies["reconsync_session"]
	forged.cookies["reconsync_csrf"] = b.cookies["reconsync_csrf"]

	r := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/users",
		strings.NewReader(`{"email":"mallory@example.com","password":"`+testPassword+`","role":"admin"}`))
	r.Header.Set("Content-Type", "application/json")
	for name, value := range forged.cookies {
		r.AddCookie(&http.Cookie{Name: name, Value: value})
	}
	w := httptest.NewRecorder()
	f.server.ServeHTTP(w, r)

	if w.Code != http.StatusForbidden {
		t.Fatalf("a request without the CSRF header = %d, want 403: %s", w.Code, w.Body.String())
	}

	// Reading is still allowed without it: a GET changes nothing, and requiring
	// the header there would break every plain link.
	if w := forged.do(t, http.MethodGet, "/v1/users", nil); w.Code != http.StatusOK {
		t.Errorf("GET with a session = %d, want 200", w.Code)
	}
}

// Roles map onto the same scopes API keys carry, so a viewer signing in cannot
// do what a viewer key cannot do.
func TestRolesAreEnforcedOverHTTP(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	// The first account is forced to admin, so make it and then the others.
	if _, err := f.accounts.Create(ctx, f.tenant, "boss@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}
	for _, u := range []struct {
		email string
		role  account.Role
	}{
		{"viewer@example.com", account.RoleViewer},
		{"operator@example.com", account.RoleOperator},
	} {
		if _, err := f.accounts.Create(ctx, f.tenant, u.email, testPassword, u.role); err != nil {
			t.Fatalf("Create %s: %v", u.email, err)
		}
	}

	cases := []struct {
		email string
		readTransactions,
		writeEvents,
		manageUsers int
	}{
		{"viewer@example.com", http.StatusOK, http.StatusForbidden, http.StatusForbidden},
		{"operator@example.com", http.StatusOK, http.StatusAccepted, http.StatusForbidden},
		{"boss@example.com", http.StatusOK, http.StatusAccepted, http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.email, func(t *testing.T) {
			b := f.browser()
			if w := b.login(t, tc.email, testPassword); w.Code != http.StatusOK {
				t.Fatalf("login = %d: %s", w.Code, w.Body.String())
			}

			if w := b.do(t, http.MethodGet, "/v1/transactions?status=orphaned", nil); w.Code != tc.readTransactions {
				t.Errorf("GET /v1/transactions = %d, want %d", w.Code, tc.readTransactions)
			}

			// A distinct id per role, so a duplicate is never what is measured.
			ev := debitBody("txn_" + strings.Split(tc.email, "@")[0])
			if w := b.do(t, http.MethodPost, "/v1/events/debit", ev); w.Code != tc.writeEvents {
				t.Errorf("POST /v1/events/debit = %d, want %d: %s", w.Code, tc.writeEvents, w.Body.String())
			}

			if w := b.do(t, http.MethodGet, "/v1/users", nil); w.Code != tc.manageUsers {
				t.Errorf("GET /v1/users = %d, want %d", w.Code, tc.manageUsers)
			}
		})
	}
}

// Six digits is a million guesses, which unthrottled is a few minutes of
// traffic. The lockout is what makes the second factor worth having.
func TestRepeatedFailuresLockTheAccount(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "target@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}
	b := f.browser()

	for i := 0; i < account.MaxFailedLogins; i++ {
		w := b.login(t, "target@example.com", "not the password")
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d = %d, want 401: %s", i+1, w.Code, w.Body.String())
		}
	}

	// Now even the right password is refused, and says so plainly rather than
	// claiming the password is wrong — the person is almost always the owner.
	w := b.login(t, "target@example.com", testPassword)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("after %d failures the correct password = %d, want 429: %s",
			account.MaxFailedLogins, w.Code, w.Body.String())
	}

	// It clears on its own, so a fat-fingered password is not a support ticket.
	f.now = f.now.Add(account.LockDuration + time.Minute)
	if w := b.login(t, "target@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("after the lock expired = %d, want 200: %s", w.Code, w.Body.String())
	}
}

// Enrolment stores the secret but does not enable it until a code verifies, so
// a wrong clock cannot lock someone out of their own account during setup.
func TestTwoFactorEnrolmentAndChallenge(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	u, err := f.accounts.Create(ctx, f.tenant, "admin@example.com", testPassword, account.RoleAdmin)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	b := f.browser()
	if w := b.login(t, "admin@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("login = %d: %s", w.Code, w.Body.String())
	}

	w := b.do(t, http.MethodPost, "/v1/auth/totp/begin", map[string]string{})
	if w.Code != http.StatusOK {
		t.Fatalf("begin = %d: %s", w.Code, w.Body.String())
	}
	secret, _ := decodeJSON(t, w)["secret"].(string)
	if secret == "" {
		t.Fatal("no secret was returned")
	}

	// Not yet on: a second sign-in must not demand a code the person cannot
	// produce.
	fresh := f.browser()
	if w := fresh.login(t, "admin@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("sign-in during enrolment = %d, want 200", w.Code)
	}
	if _, asked := decodeJSON(t, fresh.do(t, http.MethodGet, "/v1/auth/me", nil))["totp_enabled"].(bool); !asked {
		t.Error("me did not report totp_enabled")
	}

	// A wrong code does not enable it either.
	if w := b.do(t, http.MethodPost, "/v1/auth/totp/confirm", map[string]string{"code": "000000"}); w.Code == http.StatusOK {
		t.Fatal("a wrong code enabled two-factor")
	}

	code, err := account.TOTPCode(secret, f.now)
	if err != nil {
		t.Fatalf("TOTPCode: %v", err)
	}
	w = b.do(t, http.MethodPost, "/v1/auth/totp/confirm", map[string]string{"code": code})
	if w.Code != http.StatusOK {
		t.Fatalf("confirm = %d: %s", w.Code, w.Body.String())
	}
	codes, _ := decodeJSON(t, w)["recovery_codes"].([]any)
	if len(codes) == 0 {
		t.Fatal("no recovery codes were issued")
	}

	// From here the password alone is not enough.
	next := f.browser()
	w = next.login(t, "admin@example.com", testPassword)
	if w.Code != http.StatusOK {
		t.Fatalf("password step = %d: %s", w.Code, w.Body.String())
	}
	body := decodeJSON(t, w)
	if required, _ := body["totp_required"].(bool); !required {
		t.Fatalf("the password alone signed in with two-factor on: %v", body)
	}
	// And critically, no session came with it.
	if _, ok := next.cookies["reconsync_session"]; ok {
		t.Fatal("a session was issued before the second factor was proved")
	}
	if w := next.do(t, http.MethodGet, "/v1/transactions?status=orphaned", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("a half-authenticated caller read data: %d", w.Code)
	}

	code, err = account.TOTPCode(secret, f.now)
	if err != nil {
		t.Fatalf("TOTPCode: %v", err)
	}
	w = next.do(t, http.MethodPost, "/v1/auth/login", map[string]string{
		"email": "admin@example.com", "password": testPassword, "code": code,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("second factor = %d: %s", w.Code, w.Body.String())
	}
	if _, ok := next.cookies["reconsync_session"]; !ok {
		t.Fatal("no session after the second factor verified")
	}

	// A recovery code is the lost-phone path, and is spent on use.
	recovery := codes[0].(string)
	lost := f.browser()
	if w := lost.login(t, "admin@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("password step = %d", w.Code)
	}
	w = lost.do(t, http.MethodPost, "/v1/auth/login", map[string]string{
		"email": "admin@example.com", "password": testPassword, "code": recovery,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("recovery code = %d: %s", w.Code, w.Body.String())
	}

	again := f.browser()
	if w := again.login(t, "admin@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("password step = %d", w.Code)
	}
	if w := again.do(t, http.MethodPost, "/v1/auth/login", map[string]string{
		"email": "admin@example.com", "password": testPassword, "code": recovery,
	}); w.Code == http.StatusOK {
		t.Error("a recovery code worked twice")
	}
	_ = u
}

// The recovery flow has to work without SMTP, because a self-hosted deployment
// that only recovers when someone configured mail does not recover.
func TestAdminIssuedResetIsSingleUse(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "boss@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}
	forgetful, err := f.accounts.Create(ctx, f.tenant, "forgetful@example.com", testPassword, account.RoleOperator)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	boss := f.browser()
	if w := boss.login(t, "boss@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("login = %d: %s", w.Code, w.Body.String())
	}

	w := boss.do(t, http.MethodPost, "/v1/users/"+forgetful.ID+"/reset", map[string]string{})
	if w.Code != http.StatusOK {
		t.Fatalf("issue reset = %d: %s", w.Code, w.Body.String())
	}
	token, _ := decodeJSON(t, w)["reset_token"].(string)
	if token == "" {
		t.Fatal("no reset token")
	}

	const newPassword = "a different long passphrase"
	anon := f.browser()
	if w := anon.do(t, http.MethodPost, "/v1/auth/reset",
		map[string]string{"token": token, "password": newPassword}); w.Code != http.StatusOK {
		t.Fatalf("complete reset = %d: %s", w.Code, w.Body.String())
	}

	if w := anon.login(t, "forgetful@example.com", newPassword); w.Code != http.StatusOK {
		t.Fatalf("sign-in with the new password = %d: %s", w.Code, w.Body.String())
	}

	// Spent. Otherwise a link forwarded in an email thread stays a way in.
	if w := f.browser().do(t, http.MethodPost, "/v1/auth/reset",
		map[string]string{"token": token, "password": "yet another passphrase"}); w.Code == http.StatusOK {
		t.Error("a reset token worked twice")
	}

	// The old password is dead, which is the other half of a reset meaning
	// anything.
	if w := f.browser().login(t, "forgetful@example.com", testPassword); w.Code == http.StatusOK {
		t.Error("the old password still works after a reset")
	}
}

// Disabling someone has to mean now, not when their session happens to expire.
func TestDisablingAUserEndsTheirSessionImmediately(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "boss@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}
	leaver, err := f.accounts.Create(ctx, f.tenant, "leaver@example.com", testPassword, account.RoleOperator)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	them := f.browser()
	if w := them.login(t, "leaver@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("login = %d: %s", w.Code, w.Body.String())
	}
	if w := them.do(t, http.MethodGet, "/v1/transactions?status=orphaned", nil); w.Code != http.StatusOK {
		t.Fatalf("read before disabling = %d", w.Code)
	}

	boss := f.browser()
	if w := boss.login(t, "boss@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("login = %d", w.Code)
	}
	if w := boss.do(t, http.MethodPatch, "/v1/users/"+leaver.ID,
		map[string]any{"disabled": true}); w.Code != http.StatusOK {
		t.Fatalf("disable = %d: %s", w.Code, w.Body.String())
	}

	if w := them.do(t, http.MethodGet, "/v1/transactions?status=orphaned", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("a disabled user still reads data: %d", w.Code)
	}
	// And cannot sign back in, with the same message an unknown address gets.
	if w := f.browser().login(t, "leaver@example.com", testPassword); w.Code != http.StatusUnauthorized {
		t.Errorf("a disabled user signed in: %d", w.Code)
	}
}

// The last admin demoting or disabling themselves would leave a tenant with
// nobody who can manage users, recoverable only from a shell on the server.
func TestAnAdminCannotLockThemselvesOut(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	boss, err := f.accounts.Create(ctx, f.tenant, "boss@example.com", testPassword, account.RoleAdmin)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	b := f.browser()
	if w := b.login(t, "boss@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("login = %d", w.Code)
	}

	for _, patch := range []map[string]any{{"role": "viewer"}, {"disabled": true}} {
		if w := b.do(t, http.MethodPatch, "/v1/users/"+boss.ID, patch); w.Code != http.StatusConflict {
			t.Errorf("self %v = %d, want 409", patch, w.Code)
		}
	}
	// Still an admin afterwards.
	if w := b.do(t, http.MethodGet, "/v1/users", nil); w.Code != http.StatusOK {
		t.Errorf("GET /v1/users = %d after a refused self-change", w.Code)
	}
}

// A password change is usually a response to a suspected compromise, so leaving
// other sessions alive defeats the point.
func TestChangingAPasswordEndsEverySession(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "admin@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}

	laptop := f.browser()
	phone := f.browser()
	for _, b := range []*browser{laptop, phone} {
		if w := b.login(t, "admin@example.com", testPassword); w.Code != http.StatusOK {
			t.Fatalf("login = %d: %s", w.Code, w.Body.String())
		}
	}

	const newPassword = "an entirely different phrase"
	if w := laptop.do(t, http.MethodPost, "/v1/auth/password", map[string]string{
		"current_password": testPassword, "new_password": newPassword,
	}); w.Code != http.StatusOK {
		t.Fatalf("change password = %d: %s", w.Code, w.Body.String())
	}

	if w := phone.do(t, http.MethodGet, "/v1/transactions?status=orphaned", nil); w.Code != http.StatusUnauthorized {
		t.Errorf("the other session survived a password change: %d", w.Code)
	}

	// The current password is required, so a borrowed session cannot take the
	// account over outright.
	fresh := f.browser()
	if w := fresh.login(t, "admin@example.com", newPassword); w.Code != http.StatusOK {
		t.Fatalf("sign-in with the new password = %d", w.Code)
	}
	if w := fresh.do(t, http.MethodPost, "/v1/auth/password", map[string]string{
		"current_password": "wrong", "new_password": "another long passphrase here",
	}); w.Code != http.StatusUnauthorized {
		t.Errorf("changed the password without the current one: %d", w.Code)
	}
}

// A tenant's users are its own. Two tenants sharing a deployment must not be
// able to see, still less manage, each other's people.
func TestUserAdministrationIsTenantScoped(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	other := f.tenant + "_other"
	if err := f.store.EnsureTenant(ctx, other, "Other", string(auth.EnvTest)); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	if _, err := f.accounts.Create(ctx, f.tenant, "boss@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}
	theirs, err := f.accounts.Create(ctx, other, "theirs@example.com", testPassword, account.RoleAdmin)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	b := f.browser()
	if w := b.login(t, "boss@example.com", testPassword); w.Code != http.StatusOK {
		t.Fatalf("login = %d", w.Code)
	}

	if body := decodeJSON(t, b.do(t, http.MethodGet, "/v1/users", nil)); strings.Contains(
		fmt.Sprint(body["users"]), "theirs@example.com") {
		t.Error("one tenant's admin can see another tenant's users")
	}
	if w := b.do(t, http.MethodPatch, "/v1/users/"+theirs.ID,
		map[string]any{"role": "viewer"}); w.Code == http.StatusOK {
		t.Error("one tenant's admin changed another tenant's user")
	}
	if w := b.do(t, http.MethodPost, "/v1/users/"+theirs.ID+"/reset",
		map[string]string{}); w.Code == http.StatusOK {
		t.Error("one tenant's admin issued a reset for another tenant's user")
	}
}

// An unknown address and a wrong password must be indistinguishable, or the
// endpoint becomes a way to find out who has an account.
func TestSignInDoesNotRevealWhichAddressesExist(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "real@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}

	unknown := f.browser().login(t, "nobody@example.com", testPassword)
	wrong := f.browser().login(t, "real@example.com", "not the password")

	if unknown.Code != wrong.Code {
		t.Errorf("status differs: unknown %d, wrong password %d", unknown.Code, wrong.Code)
	}
	if unknown.Body.String() != wrong.Body.String() {
		// Compared after stripping the request id, which differs by design.
		strip := func(w *httptest.ResponseRecorder) string {
			body := decodeJSON(t, w)
			if e, ok := body["error"].(map[string]any); ok {
				delete(e, "request_id")
			}
			raw, _ := json.Marshal(body)
			return string(raw)
		}
		if strip(unknown) != strip(wrong) {
			t.Errorf("body differs:\n unknown: %s\n wrong:   %s", strip(unknown), strip(wrong))
		}
	}
}

// A deployment with no user accounts must keep working exactly as it did
// before they existed.
func TestAPIKeysStillWorkWithoutAccounts(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	if w := f.do(t, http.MethodGet, "/v1/transactions?status=orphaned", f.keyA, nil); w.Code != http.StatusOK {
		t.Fatalf("API key read = %d: %s", w.Code, w.Body.String())
	}
	// And signing in says so rather than failing obscurely.
	if w := f.do(t, http.MethodPost, "/v1/auth/login", "",
		map[string]string{"email": "a@b.com", "password": testPassword}); w.Code != http.StatusNotImplemented {
		t.Errorf("login without accounts configured = %d, want 501: %s", w.Code, w.Body.String())
	}
}

// An office of twenty people arriving at nine o'clock shares one egress
// address. Charging their successful sign-ins to the anti-brute-force budget
// would throttle the ordinary case to slow an attack made entirely of failures.
func TestSuccessfulSignInsDoNotSpendTheRateLimit(t *testing.T) {
	f := newAuthFixture(t)
	ctx := context.Background()

	if _, err := f.accounts.Create(ctx, f.tenant, "first@example.com", testPassword, account.RoleAdmin); err != nil {
		t.Fatalf("Create: %v", err)
	}
	for i := 0; i < 20; i++ {
		if _, err := f.accounts.Create(ctx, f.tenant,
			fmt.Sprintf("user%d@example.com", i), testPassword, account.RoleViewer); err != nil {
			t.Fatalf("Create: %v", err)
		}
	}

	// All from the same address, as httptest gives every request.
	for i := 0; i < 20; i++ {
		email := fmt.Sprintf("user%d@example.com", i)
		if w := f.browser().login(t, email, testPassword); w.Code != http.StatusOK {
			t.Fatalf("%s signed in = %d, want 200: %s", email, w.Code, w.Body.String())
		}
	}

	// Failures still accumulate, which is what the limit is for.
	for i := 0; i < 12; i++ {
		f.browser().login(t, fmt.Sprintf("guess%d@example.com", i), "spray")
	}
	if w := f.browser().login(t, "first@example.com", testPassword); w.Code != http.StatusTooManyRequests {
		t.Errorf("after a spray of failures = %d, want 429", w.Code)
	}
}
