-- A worker's heartbeat must be able to re-create its own row.
--
-- internal/store.Beat used to be a plain UPDATE, and a row only appeared via the
-- one-time UpsertWorker call at startup. So if the workers table was cleaned
-- (a demo reset, an operator tidying up, a restored dump) while workers were
-- running, every heartbeat then failed with "not found", for the rest of the
-- process's life. Worse, recovery saw the missing rows as dead workers and
-- reclaimed jobs that were running perfectly well - the demo reported "38
-- recovered from a dead worker" without a single worker being killed.
--
-- A no-op for existing data; it exists so the fix is visible in the schema
-- history. The real change is in store.Beat, which now upserts.

-- Keep the default that new rows start alive, and make the intent explicit.
ALTER TABLE workers ALTER COLUMN alive SET DEFAULT true;

-- Find dead workers quickly when recovery sweeps.
CREATE INDEX IF NOT EXISTS workers_alive_last_heartbeat_idx
    ON workers (alive, last_heartbeat);
