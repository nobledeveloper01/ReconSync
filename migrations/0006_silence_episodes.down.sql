DROP TABLE IF EXISTS tenant_silence;

-- Guarded like 0005: the harness rolls every migration back on a fresh database,
-- where webhook_deliveries does not exist yet.
--
-- Restoring NOT NULL would fail on any integration alert already queued, so the
-- placeholder goes in first. Nothing reads it — the down migration exists so the
-- schema can be rolled back, not so the rows stay meaningful.
DO $$
BEGIN
    IF to_regclass('public.webhook_deliveries') IS NOT NULL THEN
        UPDATE webhook_deliveries SET transaction_id = '' WHERE transaction_id IS NULL;
        ALTER TABLE webhook_deliveries ALTER COLUMN transaction_id SET NOT NULL;
    END IF;
END $$;
