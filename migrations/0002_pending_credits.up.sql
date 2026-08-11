-- Credits can arrive before their debit when the customer's transaction service
-- reports legs out of order (§3.2 A2). Park them here and apply on debit
-- arrival, rather than dropping them.

CREATE TABLE pending_credits (
    id                 BIGSERIAL PRIMARY KEY,
    tenant_id          TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    transaction_id     TEXT NOT NULL,
    idempotency_key    TEXT NOT NULL,
    credit_at          TIMESTAMPTZ NOT NULL,
    provider_reference TEXT NOT NULL DEFAULT '',
    status             TEXT NOT NULL CHECK (status IN ('success','failed','unknown')),
    parked_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- First verdict wins; a duplicate cannot overwrite it.
    UNIQUE (tenant_id, transaction_id)
);

-- For the reaper that clears credits whose debit never turned up.
CREATE INDEX idx_pending_credits_parked ON pending_credits (parked_at);
