-- When we warned that a transaction was approaching its regulatory deadline.
--
-- On the transaction rather than in its own table because it is a property of
-- the transaction, and because the claim that fires the warning has to be
-- atomic with recording it: several replicas sweep the same rows, and a
-- separate table would need a join to stay exactly-once.
--
-- Null means never warned, which is also what every existing row gets — so no
-- backfill fires a warning for history.
ALTER TABLE transactions ADD COLUMN sla_warned_at TIMESTAMPTZ;

-- Partial: only unwarned transactions are ever scanned for, and they are a
-- shrinking minority of a large table.
CREATE INDEX idx_transactions_sla_unwarned ON transactions (debit_at)
    WHERE sla_warned_at IS NULL;
