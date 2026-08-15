-- Human accounts for the dashboard.
--
-- Separate from api_keys on purpose. A key is a machine credential held by a
-- transaction service; a user is a person who logs in, can be made to prove a
-- second factor, and can be locked out without redeploying anything. Conflating
-- them would mean either giving people long-lived secrets or giving services
-- passwords.
CREATE TABLE users (
    id            TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,

    -- Stored lowercased; the unique index is what stops "Ada@x" and "ada@x"
    -- becoming two accounts that each think they own the address.
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,

    -- viewer | operator | admin. Checked in the application against a single
    -- table of role to permission, so adding a role is one change in one place.
    role          TEXT NOT NULL,

    -- TOTP. The secret is only meaningful once enabled: enrolment writes it,
    -- and a verified code flips the flag. Writing both at once would lock a
    -- user out of their own account if their authenticator clock was wrong.
    totp_secret   TEXT,
    totp_enabled  BOOLEAN NOT NULL DEFAULT false,

    -- Brute force protection. A six digit code is 10^6 guesses, which is
    -- minutes of unthrottled traffic.
    failed_logins INTEGER NOT NULL DEFAULT 0,
    locked_until  TIMESTAMPTZ,

    disabled_at   TIMESTAMPTZ,
    last_login_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_users_email ON users (lower(email));
CREATE INDEX idx_users_tenant ON users (tenant_id);

-- Server-side sessions rather than a self-contained token.
--
-- A signed token that carries its own claims cannot be revoked before it
-- expires. For a dashboard onto financial records, "sign this person out now"
-- has to actually mean now — when someone is fired, or a laptop is lost.
CREATE TABLE user_sessions (
    -- The token is never stored, only its SHA-256. A database disclosure then
    -- yields no usable session, the same reasoning as the API key table.
    token_hash   TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Recorded so a user can see their own sessions and recognise one that is
    -- not theirs.
    user_agent   TEXT NOT NULL DEFAULT '',
    ip           TEXT NOT NULL DEFAULT '',

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_sessions_user ON user_sessions (user_id);
CREATE INDEX idx_sessions_expiry ON user_sessions (expires_at);

-- Single-use recovery codes, for the authenticator that was on the lost phone.
-- Hashed like passwords: a code is a credential.
CREATE TABLE user_recovery_codes (
    user_id   TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code_hash TEXT NOT NULL,
    used_at   TIMESTAMPTZ,

    PRIMARY KEY (user_id, code_hash)
);

-- Password resets. Issued by an admin or the CLI rather than emailed, because
-- this is self-hosted and a recovery flow that needs a mail server configured
-- is not a recovery flow.
CREATE TABLE password_resets (
    token_hash TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at    TIMESTAMPTZ
);

CREATE INDEX idx_resets_user ON password_resets (user_id);
