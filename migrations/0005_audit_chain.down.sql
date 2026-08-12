-- Guarded so this is safe to run against a database where 0001 has not been
-- applied. Test harnesses roll every migration back before rolling forward, so a
-- bare ALTER TABLE here fails on a fresh database.
DO $$
BEGIN
    IF to_regclass('public.audit_records') IS NOT NULL THEN
        DROP INDEX IF EXISTS idx_audit_chain;
        ALTER TABLE audit_records DROP CONSTRAINT IF EXISTS audit_records_tenant_seq_key;
        ALTER TABLE audit_records DROP COLUMN IF EXISTS seq;
    END IF;
END $$;
