-- Per-tenant, per-minute record of how intact our own view of the event stream
-- was.
--
-- Detection concludes failure from the absence of a credit event. That is only
-- sound if we actually received everything the tenant sent. When we drop events
-- under backpressure, or a batch fails to apply, our view has a hole — and every
-- transaction whose window overlaps that hole is unreliable.
--
-- Without this, a burst of backpressure silently becomes a burst of reversals.

CREATE TABLE ingest_health (
    tenant_id      TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    bucket         TIMESTAMPTZ NOT NULL,  -- minute-truncated
    received       BIGINT NOT NULL DEFAULT 0,
    dropped        BIGINT NOT NULL DEFAULT 0,
    handler_errors BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, bucket)
);

-- The detection sweep asks "was this tenant's view intact between these two
-- instants", so the lookup is by tenant and bucket range.
CREATE INDEX idx_ingest_health_gaps ON ingest_health (tenant_id, bucket)
    WHERE dropped > 0 OR handler_errors > 0;

-- Retention sweeps and the silence check scan by time alone.
CREATE INDEX idx_ingest_health_bucket ON ingest_health (bucket);
