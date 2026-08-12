-- The exactly-once interlock between our advice and their money movement.
--
-- A reversal webhook can arrive more than once: a retry after a timeout the
-- customer actually processed, a dead-letter replay, two of their workers
-- picking up the same job. Today we say "reverse this" and hope they
-- deduplicate. This makes it our guarantee instead: exactly one claim per
-- transaction succeeds, and the primary key is what enforces it.
CREATE TABLE reversal_claims (
    tenant_id      TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    transaction_id TEXT NOT NULL,

    -- Returned to the winner. Their worker carries it through to confirmation,
    -- so a confirmation can be tied to the claim that authorised it.
    claim_token    TEXT NOT NULL,

    -- Whoever asked, in their words: a worker id, a pod name, a job id. Echoed
    -- back to the loser so "who already has this" is answerable at 3am.
    claimed_by     TEXT NOT NULL,
    claimed_at     TIMESTAMPTZ NOT NULL,

    confirmed_at   TIMESTAMPTZ,

    PRIMARY KEY (tenant_id, transaction_id)
);

CREATE INDEX idx_reversal_claims_outstanding ON reversal_claims (tenant_id, claimed_at)
    WHERE confirmed_at IS NULL;
