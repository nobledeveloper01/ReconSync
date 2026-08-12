-- Silence episodes: one open row per tenant that has stopped sending.
--
-- The primary key is the interlock. Every replica sweeps every tenant, so
-- without it a tenant going quiet would be alerted once per replica per tick.
-- Whoever wins the insert owns the alert; the losers see a conflict and stay
-- quiet. The row is deleted when events resume, which both closes the episode
-- and re-arms the alert for the next one.
CREATE TABLE tenant_silence (
    tenant_id    TEXT PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    silent_since TIMESTAMPTZ NOT NULL,
    notified_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- An alert about the integration itself concerns no transaction. Carrying a
-- placeholder id would put a transaction that does not exist into the delivery
-- log, so the column becomes nullable instead.
ALTER TABLE webhook_deliveries ALTER COLUMN transaction_id DROP NOT NULL;
