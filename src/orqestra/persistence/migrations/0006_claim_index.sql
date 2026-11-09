-- The claim index (DEC-069, DEC-033).
--
-- Additive: one partial index on tasks for the claim's discovery statement,
-- which looks for the first queued task by created_at, then id. Only queued rows
-- are indexed, so finished tasks never grow it, and the statement reads the
-- queue in index order and stops at the first eligible row instead of scanning
-- and sorting every task. The statement names state = 'queued' as a literal, so
-- the planner can use this index for prepared (generic) plans too. No data,
-- constraint or other index changes.
--
-- Building it takes tasks in SHARE mode: task writes (submission, claims,
-- completions, recovery) wait until it is built, for a time that grows with the
-- number of tasks; reads and row locks (a claim's discovery) continue. It may
-- run while workers run. Like 0002 to 0004, it takes its lock first and with
-- NOWAIT, so it fails at once instead of waiting while an open transaction has
-- written to tasks. CREATE INDEX CONCURRENTLY cannot run inside the runner's
-- transaction (DEC-017).
--
-- Workers check the history before they start (DEC-051): code with this
-- migration refuses a database without it, and code from before it refuses a
-- database with it. Migrate, then start the new code.

DO $$
BEGIN
    LOCK TABLE tasks IN SHARE MODE NOWAIT;
EXCEPTION WHEN lock_not_available THEN
    RAISE EXCEPTION 'migration 0006 could not lock tasks because an open transaction has '
        'written to it; retry when it is idle, or stop application processes first'
        USING ERRCODE = 'lock_not_available';
END
$$;

CREATE INDEX tasks_claim_order ON tasks (created_at, id) WHERE state = 'queued';
