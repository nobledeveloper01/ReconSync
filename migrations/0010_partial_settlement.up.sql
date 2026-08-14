-- Whether the credit that arrived was actually the whole amount.
--
-- Until now a credit event carried no amount at all, so a ₦50,000 debit settled
-- by a ₦10,000 credit was marked completed and the customer was quietly short
-- ₦40,000. The product exists to notice a customer is out of pocket, and it
-- could not notice being partly out of pocket.
--
-- Both columns are nullable or defaulted, so every existing row and every client
-- that does not send an amount behaves exactly as before.

-- What we expect to arrive, which is not always what was debited: a transfer
-- with a fee credits less than it debits, and that is correct rather than short.
-- Null means "the debited amount", which is the single-leg case.
ALTER TABLE transactions ADD COLUMN expected_credit_minor BIGINT;

-- What has actually arrived so far, accumulated across credits so a split
-- settlement adds up rather than the last one winning.
ALTER TABLE transactions ADD COLUMN credited_minor BIGINT NOT NULL DEFAULT 0;

-- A partial settlement is money still outstanding, so it is worth finding
-- cheaply among transactions that are otherwise open.
CREATE INDEX idx_transactions_partial ON transactions (tenant_id, expected_completion_at)
    WHERE credited_minor > 0 AND status IN ('pending_debit', 'pending_unknown');

-- A credit that arrives before its debit is parked here, and without this
-- column the amount was lost on the round trip: the credit came back with no
-- amount and settled the transaction in full. Found by running it, not by
-- reading it.
ALTER TABLE pending_credits ADD COLUMN amount_minor BIGINT NOT NULL DEFAULT 0;

-- Which credits have already been counted.
--
-- Accumulation is not naturally idempotent the way the old path was: a settled
-- transaction rejects further credits, but a running total happily adds the
-- same credit twice. The pipeline can legitimately deliver one twice — a credit
-- that overtakes its debit is parked and later drained, and a client retry is
-- ordinary behaviour — so without this a ₦10,000 credit could settle a ₦20,000
-- transfer. Double-counting money is the exact failure this product exists to
-- prevent.
--
-- The primary key is the interlock, checked in the same transaction as the
-- accumulation.
CREATE TABLE credit_applications (
    tenant_id       TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    transaction_id  TEXT NOT NULL,
    amount_minor    BIGINT NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, idempotency_key)
);
