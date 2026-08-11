-- ReconSync initial schema (§5).
-- Money is always BIGINT in minor units. Never float, never numeric-from-string.

CREATE TABLE tenants (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    environment  TEXT NOT NULL CHECK (environment IN ('test','live')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    prefix       TEXT NOT NULL UNIQUE,  -- plaintext, for lookup and display
    hash         TEXT NOT NULL,         -- argon2id
    scopes       TEXT[] NOT NULL DEFAULT '{}',
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_tenant ON api_keys (tenant_id);

-- Status values must stay in step with internal/domain/status.go.
CREATE TABLE transactions (
    id                      BIGSERIAL PRIMARY KEY,
    tenant_id               TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    transaction_id          TEXT NOT NULL,
    idempotency_key         TEXT NOT NULL,
    transaction_type        TEXT NOT NULL,
    provider                TEXT NOT NULL DEFAULT '',
    amount_minor            BIGINT NOT NULL CHECK (amount_minor > 0),
    currency                CHAR(3) NOT NULL,
    status                  TEXT NOT NULL CHECK (status IN (
                                'pending_debit','pending_unknown','completed','suspect',
                                'orphaned','reversal_pending','reversal_completed','reversal_failed')),
    debit_at                TIMESTAMPTZ NOT NULL,
    credit_at               TIMESTAMPTZ,
    expected_completion_at  TIMESTAMPTZ NOT NULL,
    detected_at             TIMESTAMPTZ,
    reversal_triggered_at   TIMESTAMPTZ,
    reversal_completed_at   TIMESTAMPTZ,
    customer_ref_hash       TEXT NOT NULL DEFAULT '',
    metadata                JSONB NOT NULL DEFAULT '{}',
    is_backfill             BOOLEAN NOT NULL DEFAULT false,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (tenant_id, transaction_id),
    UNIQUE (tenant_id, idempotency_key)
);

-- Partial index: predicate must match domain.Status.IsOpen(). The scheduler only
-- ever scans open rows, so this stays small regardless of history.
CREATE INDEX idx_txn_due ON transactions (expected_completion_at)
    WHERE status IN ('pending_debit','pending_unknown');

CREATE INDEX idx_txn_tenant_status ON transactions (tenant_id, status, debit_at DESC);

CREATE TABLE reconciliation_rules (
    id                BIGSERIAL PRIMARY KEY,
    tenant_id         TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    transaction_type  TEXT,          -- NULL matches any
    provider          TEXT,
    currency          CHAR(3),
    min_amount_minor  BIGINT,
    max_amount_minor  BIGINT,
    window_seconds    INTEGER NOT NULL CHECK (window_seconds > 0),
    action            TEXT NOT NULL DEFAULT 'auto_reverse'
                      CHECK (action IN ('auto_reverse','alert_only','investigate')),
    priority          INTEGER NOT NULL DEFAULT 0,  -- higher wins
    enabled           BOOLEAN NOT NULL DEFAULT true,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_rules_tenant ON reconciliation_rules (tenant_id, enabled, priority DESC);

CREATE TABLE webhook_endpoints (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    url         TEXT NOT NULL,
    secret_ref  TEXT NOT NULL,  -- KMS reference, never the secret
    events      TEXT[] NOT NULL DEFAULT '{}',
    enabled     BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_endpoints_tenant ON webhook_endpoints (tenant_id, enabled);

CREATE TABLE webhook_deliveries (
    id             BIGSERIAL PRIMARY KEY,
    tenant_id      TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    endpoint_id    TEXT NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    transaction_id TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    attempt        INTEGER NOT NULL DEFAULT 0,
    status         TEXT NOT NULL CHECK (status IN ('pending','delivered','failed','dead_letter')),
    response_code  INTEGER,
    response_body  TEXT,  -- truncated to 1KB by the dispatcher
    duration_ms    INTEGER,
    next_retry_at  TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_deliveries_due ON webhook_deliveries (next_retry_at)
    WHERE status = 'pending';

CREATE INDEX idx_deliveries_tenant ON webhook_deliveries (tenant_id, status, created_at DESC);

-- Append-only. Immutability is enforced below, not just promised (§8.3).
CREATE TABLE audit_records (
    id          BIGSERIAL PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor       JSONB NOT NULL DEFAULT '{}',
    subject     JSONB NOT NULL DEFAULT '{}',
    payload     JSONB NOT NULL DEFAULT '{}',
    prev_hash   TEXT,
    hash        TEXT NOT NULL
);

CREATE INDEX idx_audit_tenant ON audit_records (tenant_id, id);

-- Fails loudly even for a superuser mistake; grants alone would not.
CREATE OR REPLACE FUNCTION audit_records_immutable() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_records is append-only: % denied', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER audit_records_no_update_delete
    BEFORE UPDATE OR DELETE ON audit_records
    FOR EACH ROW EXECUTE FUNCTION audit_records_immutable();

-- Row triggers do not fire on TRUNCATE, so it would otherwise walk straight
-- through the guard above.
CREATE TRIGGER audit_records_no_truncate
    BEFORE TRUNCATE ON audit_records
    FOR EACH STATEMENT EXECUTE FUNCTION audit_records_immutable();
