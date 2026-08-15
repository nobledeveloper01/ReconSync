package account

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// Service is where the decisions about authenticating a person live.
type Service struct {
	store Store
	now   func() time.Time
}

// NewService builds one.
func NewService(s Store, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{store: s, now: now}
}

// Attempt is the outcome of a sign-in step.
type Attempt struct {
	User *User

	// NeedsTOTP means the password was right and a code is now required. No
	// session exists yet: a half-authenticated request must not be able to read
	// anything.
	NeedsTOTP bool
}

// Authenticate checks an email and password.
//
// Every failure returns ErrInvalidCredentials, whether the address is unknown,
// the password wrong, or the account disabled. Distinguishing them tells an
// attacker which addresses are real, which is the first step of every
// credential stuffing run.
func (s *Service) Authenticate(ctx context.Context, email, password string) (*Attempt, error) {
	now := s.now().UTC()

	u, err := s.store.UserByEmail(ctx, email)
	if errors.Is(err, ErrNotFound) {
		// Hash anyway. Without this an unknown address returns in microseconds
		// while a real one spends 64 MiB of argon2 — a timing gap wide enough
		// to enumerate every account from a laptop.
		WasteTime()
		return nil, ErrInvalidCredentials
	}
	if err != nil {
		return nil, err
	}

	if u.DisabledAt != nil {
		WasteTime()
		return nil, ErrInvalidCredentials
	}
	if u.LockedUntil != nil && now.Before(*u.LockedUntil) {
		// Said plainly rather than hidden as a wrong password: the person is
		// almost always the real owner, and leaving them to guess for fifteen
		// minutes helps nobody.
		return nil, ErrLocked
	}

	if !VerifyPassword(u.PasswordHash, password) {
		if _, err := s.store.RecordLoginFailure(ctx, u.ID, now); err != nil {
			return nil, err
		}
		return nil, ErrInvalidCredentials
	}

	if u.TOTPEnabled {
		// Not yet signed in. The counter is deliberately not reset here — a
		// correct password followed by a wrong code should still count against
		// a brute force.
		return &Attempt{User: u, NeedsTOTP: true}, nil
	}

	if err := s.store.RecordLoginSuccess(ctx, u.ID, now); err != nil {
		return nil, err
	}
	return &Attempt{User: u}, nil
}

// VerifySecondFactor checks a TOTP code or a recovery code.
func (s *Service) VerifySecondFactor(ctx context.Context, userID, code string) (*User, error) {
	now := s.now().UTC()

	u, err := s.store.UserByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if !u.Active(now) {
		return nil, ErrLocked
	}
	if !u.TOTPEnabled {
		return nil, errors.New("account: second factor is not enabled")
	}

	if VerifyTOTP(u.TOTPSecret, code, now) {
		if err := s.store.RecordLoginSuccess(ctx, u.ID, now); err != nil {
			return nil, err
		}
		return u, nil
	}

	// A recovery code is the lost-phone path, and is spent on use.
	if err := s.store.UseRecoveryCode(ctx, u.ID, HashToken(normaliseCode(code)), now); err == nil {
		if err := s.store.RecordLoginSuccess(ctx, u.ID, now); err != nil {
			return nil, err
		}
		return u, nil
	}

	// Counted, because six digits is a million guesses and unthrottled that is
	// a few minutes of traffic.
	if _, err := s.store.RecordLoginFailure(ctx, u.ID, now); err != nil {
		return nil, err
	}
	return nil, ErrTOTPInvalid
}

func normaliseCode(v string) string {
	out := make([]rune, 0, len(v))
	for _, r := range v {
		if r == ' ' || r == '\t' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		out = append(out, r)
	}
	return string(out)
}

// StartSession issues a session for an authenticated user.
func (s *Service) StartSession(ctx context.Context, u *User, userAgent, ip string) (token string, sess *Session, err error) {
	token, hash, err := NewToken()
	if err != nil {
		return "", nil, err
	}
	now := s.now().UTC()
	sess = &Session{
		TokenHash: hash,
		UserID:    u.ID,
		UserAgent: truncate(userAgent, 400),
		IP:        truncate(ip, 60),
		ExpiresAt: now.Add(SessionLifetime),
	}
	if err := s.store.CreateSession(ctx, sess); err != nil {
		return "", nil, err
	}
	return token, sess, nil
}

// Resolve turns a cookie value into the user it authenticates.
func (s *Service) Resolve(ctx context.Context, token string) (*Session, *User, error) {
	if token == "" {
		return nil, nil, ErrNotFound
	}
	return s.store.SessionByToken(ctx, HashToken(token), s.now().UTC())
}

// EndSession signs one browser out.
func (s *Service) EndSession(ctx context.Context, token string) error {
	return s.store.DeleteSession(ctx, HashToken(token))
}

// Create makes a user. The first account for a tenant is an admin, because
// otherwise nobody could grant the first admin their role.
func (s *Service) Create(ctx context.Context, tenantID, email, password string, role Role) (*User, error) {
	if err := ValidateEmail(email); err != nil {
		return nil, err
	}
	if err := ValidatePassword(password); err != nil {
		return nil, err
	}

	n, err := s.store.CountUsers(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		role = RoleAdmin
	}
	if !role.Valid() {
		return nil, fmt.Errorf("account: unknown role %q", role)
	}

	hash, err := HashPassword(password)
	if err != nil {
		return nil, err
	}
	id, err := newID("usr")
	if err != nil {
		return nil, err
	}

	u := &User{
		ID: id, TenantID: tenantID, Email: NormaliseEmail(email),
		PasswordHash: hash, Role: role,
	}
	if err := s.store.CreateUser(ctx, u); err != nil {
		return nil, err
	}
	return u, nil
}

// BeginTOTPEnrolment mints a secret and returns the URI to scan.
//
// The secret is stored but not enabled: requiring a valid code before turning
// it on is what stops someone whose authenticator clock is wrong from locking
// themselves out during setup.
func (s *Service) BeginTOTPEnrolment(ctx context.Context, u *User, issuer string) (secret, uri string, err error) {
	secret, err = NewTOTPSecret()
	if err != nil {
		return "", "", err
	}
	if err := s.store.BeginTOTP(ctx, u.ID, secret); err != nil {
		return "", "", err
	}
	return secret, TOTPURI(issuer, u.Email, secret), nil
}

// ConfirmTOTPEnrolment enables 2FA once a code proves the device works, and
// returns the recovery codes, which are shown exactly once.
func (s *Service) ConfirmTOTPEnrolment(ctx context.Context, userID, code string) ([]string, error) {
	u, err := s.store.UserByID(ctx, userID)
	if err != nil {
		return nil, err
	}
	if u.TOTPSecret == "" {
		return nil, errors.New("account: start enrolment first")
	}
	if !VerifyTOTP(u.TOTPSecret, code, s.now().UTC()) {
		return nil, ErrTOTPInvalid
	}

	codes, err := NewRecoveryCodes()
	if err != nil {
		return nil, err
	}
	hashes := make([]string, len(codes))
	for i, c := range codes {
		hashes[i] = HashToken(c)
	}
	if err := s.store.ReplaceRecoveryCodes(ctx, u.ID, hashes); err != nil {
		return nil, err
	}
	if err := s.store.EnableTOTP(ctx, u.ID); err != nil {
		return nil, err
	}
	return codes, nil
}

// ChangePassword requires the current one, so a borrowed session cannot take
// over the account outright.
func (s *Service) ChangePassword(ctx context.Context, userID, current, next string) error {
	u, err := s.store.UserByID(ctx, userID)
	if err != nil {
		return err
	}
	if !VerifyPassword(u.PasswordHash, current) {
		return ErrInvalidCredentials
	}
	if err := ValidatePassword(next); err != nil {
		return err
	}
	hash, err := HashPassword(next)
	if err != nil {
		return err
	}
	return s.store.SetPassword(ctx, u.ID, hash)
}

// IssueReset mints a single-use link for an admin or the CLI to hand over.
//
// Not emailed: this is self-hosted with no mail dependency, and a recovery flow
// that only works when someone configured SMTP is not a recovery flow.
func (s *Service) IssueReset(ctx context.Context, userID string) (token string, expires time.Time, err error) {
	token, hash, err := NewToken()
	if err != nil {
		return "", time.Time{}, err
	}
	expires = s.now().UTC().Add(ResetLifetime)
	if err := s.store.CreateReset(ctx, userID, hash, expires); err != nil {
		return "", time.Time{}, err
	}
	return token, expires, nil
}

// CompleteReset spends a token and sets the new password.
func (s *Service) CompleteReset(ctx context.Context, token, password string) (*User, error) {
	if err := ValidatePassword(password); err != nil {
		return nil, err
	}
	u, err := s.store.ConsumeReset(ctx, HashToken(token), s.now().UTC())
	if err != nil {
		return nil, err
	}
	hash, err := HashPassword(password)
	if err != nil {
		return nil, err
	}
	if err := s.store.SetPassword(ctx, u.ID, hash); err != nil {
		return nil, err
	}
	return u, nil
}

// Store exposes the store for the handlers that only read.
func (s *Service) Store() Store { return s.store }

func newID(prefix string) (string, error) {
	var b [9]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("account: generate id: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(b[:]), nil
}

func truncate(v string, n int) string {
	if len(v) <= n {
		return v
	}
	return v[:n]
}
