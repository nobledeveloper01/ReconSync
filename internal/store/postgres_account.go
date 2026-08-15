package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/nobledeveloper01/ReconSync/internal/account"
)

const userColumns = `id, tenant_id, email, password_hash, role, totp_secret, totp_enabled,
	failed_logins, locked_until, disabled_at, last_login_at, created_at, updated_at`

func scanUser(row scanner) (*account.User, error) {
	var (
		u      account.User
		role   string
		secret *string
	)
	err := row.Scan(&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &role, &secret,
		&u.TOTPEnabled, &u.FailedLogins, &u.LockedUntil, &u.DisabledAt, &u.LastLoginAt,
		&u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, err
	}
	u.Role = account.Role(role)
	if secret != nil {
		u.TOTPSecret = *secret
	}
	return &u, nil
}

func (p *Postgres) CreateUser(ctx context.Context, u *account.User) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO users (id, tenant_id, email, password_hash, role)
		VALUES ($1, $2, $3, $4, $5)`,
		u.ID, u.TenantID, account.NormaliseEmail(u.Email), u.PasswordHash, string(u.Role))
	if err != nil {
		// A duplicate address is ordinary client behaviour, not a fault.
		if strings.Contains(err.Error(), "idx_users_email") || strings.Contains(err.Error(), "duplicate key") {
			return account.ErrEmailTaken
		}
		return fmt.Errorf("create user: %w", err)
	}
	return nil
}

func (p *Postgres) UserByEmail(ctx context.Context, email string) (*account.User, error) {
	u, err := scanUser(p.pool.QueryRow(ctx,
		`SELECT `+userColumns+` FROM users WHERE lower(email) = lower($1)`, account.NormaliseEmail(email)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, account.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("user by email: %w", err)
	}
	return u, nil
}

func (p *Postgres) UserByID(ctx context.Context, id string) (*account.User, error) {
	u, err := scanUser(p.pool.QueryRow(ctx, `SELECT `+userColumns+` FROM users WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, account.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("user by id: %w", err)
	}
	return u, nil
}

func (p *Postgres) ListUsers(ctx context.Context, tenantID string) ([]*account.User, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT `+userColumns+` FROM users WHERE tenant_id = $1 ORDER BY lower(email)`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var out []*account.User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (p *Postgres) CountUsers(ctx context.Context, tenantID string) (int, error) {
	var n int
	if err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM users WHERE tenant_id = $1`, tenantID).Scan(&n); err != nil {
		return 0, fmt.Errorf("count users: %w", err)
	}
	return n, nil
}

func (p *Postgres) UpdateUserRole(ctx context.Context, tenantID, id string, role account.Role) error {
	return p.touchUser(ctx, `UPDATE users SET role = $3, updated_at = now()
		WHERE tenant_id = $1 AND id = $2`, tenantID, id, string(role))
}

func (p *Postgres) SetUserDisabled(ctx context.Context, tenantID, id string, disabled bool) error {
	var at any
	if disabled {
		at = time.Now().UTC()
	}
	if err := p.touchUser(ctx, `UPDATE users SET disabled_at = $3, updated_at = now()
		WHERE tenant_id = $1 AND id = $2`, tenantID, id, at); err != nil {
		return err
	}
	if !disabled {
		return nil
	}
	// Disabling has to end their sessions now. Leaving them signed in until
	// expiry is the difference between "revoked" and "revoked tomorrow".
	return p.DeleteUserSessions(ctx, id)
}

func (p *Postgres) touchUser(ctx context.Context, sql string, args ...any) error {
	tag, err := p.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return account.ErrNotFound
	}
	return nil
}

func (p *Postgres) SetPassword(ctx context.Context, id, passwordHash string) error {
	if err := p.touchUser(ctx, `UPDATE users
		SET password_hash = $2, failed_logins = 0, locked_until = NULL, updated_at = now()
		WHERE id = $1`, id, passwordHash); err != nil {
		return err
	}
	// Every other session ends. A password change is usually a response to a
	// suspected compromise, and leaving the attacker's session alive defeats it.
	return p.DeleteUserSessions(ctx, id)
}

func (p *Postgres) RecordLoginFailure(ctx context.Context, id string, at time.Time) (*time.Time, error) {
	var locked *time.Time
	err := p.pool.QueryRow(ctx, `
		UPDATE users
		SET failed_logins = failed_logins + 1,
		    locked_until = CASE
		        WHEN failed_logins + 1 >= $2 THEN $3::timestamptz
		        ELSE locked_until
		    END,
		    updated_at = now()
		WHERE id = $1
		RETURNING locked_until`,
		id, account.MaxFailedLogins, at.Add(account.LockDuration).UTC()).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, account.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("record login failure: %w", err)
	}
	return locked, nil
}

func (p *Postgres) RecordLoginSuccess(ctx context.Context, id string, at time.Time) error {
	return p.touchUser(ctx, `UPDATE users
		SET failed_logins = 0, locked_until = NULL, last_login_at = $2, updated_at = now()
		WHERE id = $1`, id, at.UTC())
}

func (p *Postgres) BeginTOTP(ctx context.Context, id, secret string) error {
	// Enabled stays false: a secret nobody has proved they can generate codes
	// from must not become a requirement to sign in.
	return p.touchUser(ctx, `UPDATE users
		SET totp_secret = $2, totp_enabled = false, updated_at = now()
		WHERE id = $1`, id, secret)
}

func (p *Postgres) EnableTOTP(ctx context.Context, id string) error {
	return p.touchUser(ctx, `UPDATE users SET totp_enabled = true, updated_at = now()
		WHERE id = $1 AND totp_secret IS NOT NULL`, id)
}

func (p *Postgres) DisableTOTP(ctx context.Context, id string) error {
	if err := p.touchUser(ctx, `UPDATE users
		SET totp_enabled = false, totp_secret = NULL, updated_at = now()
		WHERE id = $1`, id); err != nil {
		return err
	}
	_, err := p.pool.Exec(ctx, `DELETE FROM user_recovery_codes WHERE user_id = $1`, id)
	return err
}

func (p *Postgres) ReplaceRecoveryCodes(ctx context.Context, userID string, hashes []string) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin recovery codes: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM user_recovery_codes WHERE user_id = $1`, userID); err != nil {
		return fmt.Errorf("clear recovery codes: %w", err)
	}
	for _, h := range hashes {
		if _, err := tx.Exec(ctx,
			`INSERT INTO user_recovery_codes (user_id, code_hash) VALUES ($1, $2)`, userID, h); err != nil {
			return fmt.Errorf("store recovery code: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// UseRecoveryCode spends a code, and only ever once.
func (p *Postgres) UseRecoveryCode(ctx context.Context, userID, hash string, at time.Time) error {
	tag, err := p.pool.Exec(ctx, `
		UPDATE user_recovery_codes SET used_at = $3
		WHERE user_id = $1 AND code_hash = $2 AND used_at IS NULL`, userID, hash, at.UTC())
	if err != nil {
		return fmt.Errorf("use recovery code: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return account.ErrNotFound
	}
	return nil
}

func (p *Postgres) CountRecoveryCodes(ctx context.Context, userID string) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx,
		`SELECT count(*) FROM user_recovery_codes WHERE user_id = $1 AND used_at IS NULL`,
		userID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count recovery codes: %w", err)
	}
	return n, nil
}

func (p *Postgres) CreateSession(ctx context.Context, s *account.Session) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO user_sessions (token_hash, user_id, user_agent, ip, expires_at)
		VALUES ($1, $2, $3, $4, $5)`,
		s.TokenHash, s.UserID, s.UserAgent, s.IP, s.ExpiresAt.UTC())
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

// SessionByToken resolves a cookie to a session and the user it belongs to.
func (p *Postgres) SessionByToken(ctx context.Context, tokenHash string, now time.Time) (*account.Session, *account.User, error) {
	var s account.Session
	u := &account.User{}
	var role string
	var secret *string

	err := p.pool.QueryRow(ctx, `
		SELECT s.token_hash, s.user_id, s.user_agent, s.ip, s.created_at, s.last_seen_at, s.expires_at,
		       u.id, u.tenant_id, u.email, u.password_hash, u.role, u.totp_secret, u.totp_enabled,
		       u.failed_logins, u.locked_until, u.disabled_at, u.last_login_at, u.created_at, u.updated_at
		FROM user_sessions s
		JOIN users u ON u.id = s.user_id
		WHERE s.token_hash = $1 AND s.expires_at > $2`, tokenHash, now.UTC()).
		Scan(&s.TokenHash, &s.UserID, &s.UserAgent, &s.IP, &s.CreatedAt, &s.LastSeenAt, &s.ExpiresAt,
			&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &role, &secret, &u.TOTPEnabled,
			&u.FailedLogins, &u.LockedUntil, &u.DisabledAt, &u.LastLoginAt, &u.CreatedAt, &u.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, account.ErrNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("session by token: %w", err)
	}

	u.Role = account.Role(role)
	if secret != nil {
		u.TOTPSecret = *secret
	}
	// A disabled account's session is dead even if the row still exists,
	// because disabling and session cleanup are two writes and a request can
	// land between them.
	if u.DisabledAt != nil {
		return nil, nil, account.ErrNotFound
	}
	return &s, u, nil
}

func (p *Postgres) TouchSession(ctx context.Context, tokenHash string, at time.Time) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE user_sessions SET last_seen_at = $2 WHERE token_hash = $1`, tokenHash, at.UTC())
	return err
}

func (p *Postgres) DeleteSession(ctx context.Context, tokenHash string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM user_sessions WHERE token_hash = $1`, tokenHash)
	return err
}

func (p *Postgres) DeleteUserSessions(ctx context.Context, userID string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM user_sessions WHERE user_id = $1`, userID)
	return err
}

func (p *Postgres) ListSessions(ctx context.Context, userID string, now time.Time) ([]*account.Session, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT token_hash, user_id, user_agent, ip, created_at, last_seen_at, expires_at
		FROM user_sessions
		WHERE user_id = $1 AND expires_at > $2
		ORDER BY last_seen_at DESC`, userID, now.UTC())
	if err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}
	defer rows.Close()

	var out []*account.Session
	for rows.Next() {
		var s account.Session
		if err := rows.Scan(&s.TokenHash, &s.UserID, &s.UserAgent, &s.IP,
			&s.CreatedAt, &s.LastSeenAt, &s.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan session: %w", err)
		}
		out = append(out, &s)
	}
	return out, rows.Err()
}

func (p *Postgres) CreateReset(ctx context.Context, userID, tokenHash string, expires time.Time) error {
	// Any earlier link stops working. Two live reset links means a stale one in
	// an old message still opens the account.
	if _, err := p.pool.Exec(ctx,
		`DELETE FROM password_resets WHERE user_id = $1 AND used_at IS NULL`, userID); err != nil {
		return fmt.Errorf("clear resets: %w", err)
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO password_resets (token_hash, user_id, expires_at) VALUES ($1, $2, $3)`,
		tokenHash, userID, expires.UTC())
	if err != nil {
		return fmt.Errorf("create reset: %w", err)
	}
	return nil
}

// ConsumeReset spends a reset token, and only ever once.
func (p *Postgres) ConsumeReset(ctx context.Context, tokenHash string, at time.Time) (*account.User, error) {
	var userID string
	err := p.pool.QueryRow(ctx, `
		UPDATE password_resets SET used_at = $2
		WHERE token_hash = $1 AND used_at IS NULL AND expires_at > $2
		RETURNING user_id`, tokenHash, at.UTC()).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, account.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("consume reset: %w", err)
	}
	return p.UserByID(ctx, userID)
}

var _ account.Store = (*Postgres)(nil)
