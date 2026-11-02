-- Durable retry eligibility (ADR-0003; Day 4A, DEC-053).
--
-- Additive: tasks gain available_at, the database time from which a queued task
-- may be claimed. The default, now(), is evaluated once when this migration runs
-- and stored without rewriting the table, so every existing row, and every task
-- submitted later, is eligible at once. Only a requeue (a transient failure, or a
-- lease_expired or deadline_exceeded revocation) moves it into the future. No
-- state, constraint or index changes, and no task is touched.
--
-- Application versions are a separate matter the database cannot enforce: code
-- from before this slice ignores available_at and would retry requeued tasks at
-- once. Stop all application processes, apply this migration, then start only
-- code from this slice or later. A worker that was already running before the
-- migration is not stopped by the startup history check (DEC-051).
--
-- Like 0002 and 0003, it takes its lock before any change and with NOWAIT, so it
-- fails at once instead of waiting while any session uses the table. It changes
-- only tasks, so it does not lock attempts.

DO $$
BEGIN
    LOCK TABLE tasks IN ACCESS EXCLUSIVE MODE NOWAIT;
EXCEPTION WHEN lock_not_available THEN
    RAISE EXCEPTION 'migration 0004 could not lock tasks because another session is using it; '
        'stop application processes and end open transactions, then retry'
        USING ERRCODE = 'lock_not_available';
END
$$;

ALTER TABLE tasks ADD COLUMN available_at timestamptz NOT NULL DEFAULT now();
