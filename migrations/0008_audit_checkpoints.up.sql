-- Signed statements that a tenant's chain reached a given hash at a given seq.
--
-- The row itself is worth little: someone who can rewrite audit_records can
-- rewrite this table too. Its value comes entirely from the signature, made with
-- a key that is not in the database, and from the copy published somewhere the
-- attacker does not own. This table is the local cache of a fact that lives
-- elsewhere.
--
-- No foreign key to tenants, for the same reason audit_records has none: the
-- record of what a tenant did must outlive the tenant.
CREATE TABLE audit_checkpoints (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    seq        BIGINT NOT NULL,
    hash       TEXT NOT NULL,
    taken_at   TIMESTAMPTZ NOT NULL,
    signature  TEXT NOT NULL,
    public_key TEXT NOT NULL,

    -- One checkpoint per (tenant, seq). Re-signing the same head is a no-op
    -- rather than an ever-growing pile of identical rows.
    UNIQUE (tenant_id, seq)
);

CREATE INDEX idx_audit_checkpoints_latest ON audit_checkpoints (tenant_id, seq DESC);
