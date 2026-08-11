-- Lookup path for authentication: prefix is already UNIQUE, but auth reads it on
-- every request and must never fall back to a scan.
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (prefix) WHERE revoked_at IS NULL;
