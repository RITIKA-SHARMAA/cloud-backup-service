-- Catalog schema. Every statement is idempotent so it can run on every start.

CREATE TABLE IF NOT EXISTS policies (
    id             bigserial PRIMARY KEY,
    name           text        NOT NULL UNIQUE,
    source_type    text        NOT NULL CHECK (source_type IN ('dir', 'postgres')),
    source         text        NOT NULL,
    schedule       text        NOT NULL,
    retention_days int         NOT NULL CHECK (retention_days > 0),
    next_run_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id               bigserial PRIMARY KEY,
    kind             text        NOT NULL CHECK (kind IN ('backup', 'restore', 'gc')),
    policy_id        bigint      REFERENCES policies (id) ON DELETE SET NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    -- The same logical request always carries the same key, so enqueueing twice creates one job.
    idempotency_key  text        NOT NULL UNIQUE,
    state            text        NOT NULL DEFAULT 'queued'
                                 CHECK (state IN ('queued', 'running', 'succeeded', 'failed')),
    attempts         int         NOT NULL DEFAULT 0,
    max_attempts     int         NOT NULL DEFAULT 5,
    run_after        timestamptz NOT NULL DEFAULT now(),
    -- Lease: which worker holds the job and until when. A worker that dies stops renewing it,
    -- and once it expires another worker may take the job over.
    lease_owner      text,
    lease_expires_at timestamptz,
    last_error       text,
    result           jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Workers look for runnable jobs constantly; this partial index keeps that cheap no matter how
-- many finished jobs pile up.
CREATE INDEX IF NOT EXISTS jobs_runnable ON jobs (run_after, id) WHERE state IN ('queued', 'running');

CREATE TABLE IF NOT EXISTS snapshots (
    id             text PRIMARY KEY,
    policy_id      bigint REFERENCES policies (id) ON DELETE SET NULL,
    job_id         bigint REFERENCES jobs (id) ON DELETE SET NULL,
    source_type    text        NOT NULL,
    source         text        NOT NULL,
    created_at     timestamptz NOT NULL,
    logical_bytes  bigint      NOT NULL,
    uploaded_bytes bigint      NOT NULL,
    new_chunks     int         NOT NULL,
    reused_chunks  int         NOT NULL,
    duration_ms    bigint      NOT NULL
);
