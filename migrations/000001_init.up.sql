CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    priority        SMALLINT    NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 3),
    status          TEXT        NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','QUEUED','RUNNING','SUCCEEDED','RETRYING','DEAD','CANCELLED')),
    cron_expr       TEXT,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_run_at     TIMESTAMPTZ,
    timeout_seconds INT         NOT NULL DEFAULT 60 CHECK (timeout_seconds BETWEEN 1 AND 3600),
    max_retries     INT         NOT NULL DEFAULT 3 CHECK (max_retries BETWEEN 0 AND 10),
    attempt         INT         NOT NULL DEFAULT 0,
    last_error      TEXT        NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX jobs_status_next_run_at_idx ON jobs (status, next_run_at) WHERE status IN ('PENDING','RETRYING');

CREATE TABLE job_runs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id      UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    worker_id   TEXT        NOT NULL,
    attempt     INT         NOT NULL,
    status      TEXT        NOT NULL,
    error       TEXT        NOT NULL DEFAULT '',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX job_runs_job_attempt_idx ON job_runs (job_id, attempt);

CREATE TABLE workers (
    id             TEXT PRIMARY KEY,
    hostname       TEXT        NOT NULL,
    concurrency    INT         NOT NULL,
    alive          BOOLEAN     NOT NULL DEFAULT true,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_jobs_updated_at BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
