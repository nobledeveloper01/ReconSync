-- Turn audit_records into a verifiable hash chain.
--
-- Immutability triggers already stop rows being edited or removed. They do not
-- prove nothing was *replaced* — a row deleted and reinserted through a path
-- that bypassed the trigger leaves no trace. Chaining each record to the one
-- before it makes any such gap detectable: every hash after the tampered record
-- stops matching.
--
-- seq is per tenant and gapless, so a missing record is visible as a hole rather
-- than being invisible by construction.

-- The immutability trigger blocks the backfill below, so it stands down for the
-- length of this migration only.
ALTER TABLE audit_records DISABLE TRIGGER audit_records_no_update_delete;

ALTER TABLE audit_records ADD COLUMN IF NOT EXISTS seq BIGINT;

-- Existing rows predate the chain. Number them in insertion order so the
-- constraint below can be applied; they will verify as unchained, which is
-- accurate rather than a false claim of integrity.
WITH numbered AS (
    SELECT id, row_number() OVER (PARTITION BY tenant_id ORDER BY id) AS n
    FROM audit_records
    WHERE seq IS NULL
)
UPDATE audit_records a
SET seq = numbered.n
FROM numbered
WHERE a.id = numbered.id;

ALTER TABLE audit_records ENABLE TRIGGER audit_records_no_update_delete;

ALTER TABLE audit_records ALTER COLUMN seq SET NOT NULL;

-- The chain is per tenant, and this is what makes concurrent appends safe: two
-- writers racing for the same seq both compute the same prev_hash, and the
-- loser's insert is rejected rather than silently forking the chain.
ALTER TABLE audit_records ADD CONSTRAINT audit_records_tenant_seq_key UNIQUE (tenant_id, seq);

-- Verification walks a tenant's chain in order.
CREATE INDEX IF NOT EXISTS idx_audit_chain ON audit_records (tenant_id, seq);
