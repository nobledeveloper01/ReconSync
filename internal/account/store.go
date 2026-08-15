package account

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound means no such user, session or token.
var ErrNotFound = errors.New("account: not found")

// ErrEmailTaken means the address already has an account.
var ErrEmailTaken = errors.New("account: email already registered")

// Store persists people, their sessions and their recovery material.
//
// Its own interface rather than part of store.Store, and implemented only
// against Postgres. The in-memory store exists so the reconciliation logic can
// be tested without a database; user management has no such need, and
// duplicating fifteen methods to satisfy a symmetry nobody uses would be cost
// without benefit.
type Store interface {
	CreateUser(ctx context.Context, u *User) error
	UserByEmail(ctx context.Context, email string) (*User, error)
	UserByID(ctx context.Context, id string) (*User, error)
	ListUsers(ctx context.Context, tenantID string) ([]*User, error)
	UpdateUserRole(ctx context.Context, tenantID, id string, role Role) error
	SetUserDisabled(ctx context.Context, tenantID, id string, disabled bool) error
	SetPassword(ctx context.Context, id, passwordHash string) error

	// RecordLoginFailure increments the counter and locks the account once it
	// passes the threshold, returning the lock time if one was applied.
	RecordLoginFailure(ctx context.Context, id string, at time.Time) (*time.Time, error)
	RecordLoginSuccess(ctx context.Context, id string, at time.Time) error

	// BeginTOTP stores a secret without enabling it, so a user who cannot
	// produce a valid code is never locked out by their own enrolment.
	BeginTOTP(ctx context.Context, id, secret string) error
	EnableTOTP(ctx context.Context, id string) error
	DisableTOTP(ctx context.Context, id string) error

	ReplaceRecoveryCodes(ctx context.Context, userID string, hashes []string) error
	UseRecoveryCode(ctx context.Context, userID, hash string, at time.Time) error
	CountRecoveryCodes(ctx context.Context, userID string) (int, error)

	CreateSession(ctx context.Context, s *Session) error
	SessionByToken(ctx context.Context, tokenHash string, now time.Time) (*Session, *User, error)
	TouchSession(ctx context.Context, tokenHash string, at time.Time) error
	DeleteSession(ctx context.Context, tokenHash string) error
	DeleteUserSessions(ctx context.Context, userID string) error
	ListSessions(ctx context.Context, userID string, now time.Time) ([]*Session, error)

	CreateReset(ctx context.Context, userID, tokenHash string, expires time.Time) error
	ConsumeReset(ctx context.Context, tokenHash string, at time.Time) (*User, error)

	// CountUsers reports how many accounts a tenant has, so the first one can
	// be made an admin and later ones cannot silently become one.
	CountUsers(ctx context.Context, tenantID string) (int, error)
}
