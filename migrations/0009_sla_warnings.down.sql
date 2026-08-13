DO $$
BEGIN
    IF to_regclass('public.transactions') IS NOT NULL THEN
        DROP INDEX IF EXISTS idx_transactions_sla_unwarned;
        ALTER TABLE transactions DROP COLUMN IF EXISTS sla_warned_at;
    END IF;
END $$;
