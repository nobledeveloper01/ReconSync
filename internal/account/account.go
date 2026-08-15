// Package account is the human side of authentication: who someone is, what
// they may do, and how they prove both.
//
// Deliberately separate from internal/auth, which authenticates machines. An
// API key is a long-lived secret held by a transaction service; a user is a
// person who logs in, proves a second factor, and can be locked out
// immediately. The two need different lifecycles, and giving services
// passwords or people long-lived secrets is how that goes wrong.
package account

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/auth"
)

// Role is what a user may do.
//
// Three, not a permission matrix. A role nobody can describe in a sentence gets
// assigned by guesswork, and the guess is usually "give them admin".
type Role string

const (
	// RoleViewer reads reports and transactions. The auditor, the analyst.
	RoleViewer Role = "viewer"

	// RoleOperator additionally acts on transactions — claiming a reversal,
	// running a fire drill. The person on call.
	RoleOperator Role = "operator"

	// RoleAdmin additionally changes where reversals are delivered and manages
	// users. The smallest group.
	RoleAdmin Role = "admin"
)

// scopes maps a role onto the permissions that already exist.
//
// Reusing the API key scopes rather than inventing a parallel system means one
// answer to "may this caller do X", whether the caller is a person or a
// service. Two systems would eventually disagree, and the disagreement would be
// discovered by someone doing something they should not have been able to.
var scopes = map[Role][]string{
	RoleViewer:   {auth.ScopeReportsRead},
	RoleOperator: {auth.ScopeReportsRead, auth.ScopeEventsWrite},
	RoleAdmin:    {auth.ScopeReportsRead, auth.ScopeEventsWrite, auth.ScopeEndpointsWrite},
}

// Valid reports whether a role is one we know.
func (r Role) Valid() bool {
	_, ok := scopes[r]
	return ok
}

// Scopes returns the permissions a role carries.
func (r Role) Scopes() []string {
	return append([]string(nil), scopes[r]...)
}

// Can reports whether a role holds a scope.
func (r Role) Can(scope string) bool {
	for _, s := range scopes[r] {
		if s == scope {
			return true
		}
	}
	return false
}

// Roles lists them, weakest first, for a UI that has to offer a choice.
func Roles() []Role { return []Role{RoleViewer, RoleOperator, RoleAdmin} }

// User is a person who can sign in.
type User struct {
	ID           string
	TenantID     string
	Email        string
	PasswordHash string
	Role         Role

	TOTPSecret  string
	TOTPEnabled bool

	FailedLogins int
	LockedUntil  *time.Time
	DisabledAt   *time.Time
	LastLoginAt  *time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Active reports whether the account may sign in at all.
func (u *User) Active(now time.Time) bool {
	if u.DisabledAt != nil {
		return false
	}
	return u.LockedUntil == nil || now.After(*u.LockedUntil)
}

// Session is a signed-in browser.
type Session struct {
	TokenHash  string
	UserID     string
	UserAgent  string
	IP         string
	CreatedAt  time.Time
	LastSeenAt time.Time
	ExpiresAt  time.Time
}

// Lifetimes.
const (
	// SessionLifetime is how long a session lasts before a fresh sign-in.
	// A day, not a month: this is a view onto money movement, and a laptop
	// left open in a café should not still be signed in next week.
	SessionLifetime = 24 * time.Hour

	// ResetLifetime bounds a password reset link.
	ResetLifetime = time.Hour

	// MaxFailedLogins before the account is locked.
	MaxFailedLogins = 5

	// LockDuration is how long a locked account stays locked. Long enough to
	// make a six-digit code impractical to guess, short enough that a real
	// person who fat-fingered their password is not filing a support ticket.
	LockDuration = 15 * time.Minute
)

// Errors a caller has to distinguish.
var (
	// ErrInvalidCredentials is returned for a wrong password, an unknown
	// email, and a disabled account alike.
	//
	// Deliberately one error. Distinguishing them tells an attacker which
	// addresses have accounts, which is the first step of every credential
	// stuffing run.
	ErrInvalidCredentials = errors.New("account: invalid credentials")

	// ErrLocked means too many failures.
	ErrLocked = errors.New("account: temporarily locked")

	// ErrTOTPRequired means the password was right and a second factor is now
	// needed.
	ErrTOTPRequired = errors.New("account: second factor required")

	// ErrTOTPInvalid means the code did not verify.
	ErrTOTPInvalid = errors.New("account: invalid code")
)

// NormaliseEmail lowercases and trims, so one address is one account.
func NormaliseEmail(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

// ValidateEmail is structural rather than a deliverability check.
func ValidateEmail(v string) error {
	v = NormaliseEmail(v)
	at := strings.IndexByte(v, '@')
	switch {
	case v == "":
		return errors.New("account: email is required")
	case at <= 0 || at == len(v)-1:
		return errors.New("account: email must contain a name and a domain")
	case strings.Contains(v, " "):
		return errors.New("account: email must not contain spaces")
	case len(v) > 320:
		return errors.New("account: email is too long")
	}
	return nil
}

// MinPasswordLength is the floor.
//
// Length, not composition rules. Requiring a symbol and a digit produces
// "Password1!" — memorable to nobody and guessable by everything. Twelve
// characters of anything beats eight characters of theatre.
const MinPasswordLength = 12

// ValidatePassword checks a new password.
func ValidatePassword(pw string) error {
	if len([]rune(pw)) < MinPasswordLength {
		return fmt.Errorf("account: password must be at least %d characters", MinPasswordLength)
	}
	if len(pw) > 1024 {
		// Hashing is deliberately expensive, so an unbounded password is a way
		// to spend the server's CPU.
		return errors.New("account: password is too long")
	}
	return nil
}
