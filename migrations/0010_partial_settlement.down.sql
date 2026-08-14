DROP TABLE IF EXISTS credit_applications;

DO $$
BEGIN
    IF to_regclass('public.transactions') IS NOT NULL THEN
        DROP INDEX IF EXISTS idx_transactions_partial;
        ALTER TABLE transactions DROP COLUMN IF EXISTS expected_credit_minor;
        ALTER TABLE transactions DROP COLUMN IF EXISTS credited_minor;
    END IF;
    IF to_regclass('public.pending_credits') IS NOT NULL THEN
        ALTER TABLE pending_credits DROP COLUMN IF EXISTS amount_minor;
    END IF;
END $$;
