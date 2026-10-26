-- Attempt leases, execution deadlines and revocation (ADR-0003; Day 3A Slice 2).
--
-- Quiescent upgrade only: stop every worker and claimer, let running attempts
-- finish, apply this migration, then deploy code that writes leases. Code
-- from before this migration cannot read revoked attempts, and its claims are
-- rejected afterwards by attempts_lease_required.
--
-- This migration never waits for a table lock while holding one, so it can
-- neither stall lifecycle writers behind it nor take part in a deadlock. It
-- takes every lock it needs up front, in the lifecycle's task-before-attempt
-- order and already in the strongest mode its DDL needs, with NOWAIT: if any
-- session is using the tables, it fails at once and changes nothing. It then
-- refuses to run while any task or attempt is running. It never expires or
-- revokes legacy work, and finished attempts keep their history without a lease.

DO $$
BEGIN
    -- Each statement below must see work committed before the locks were taken.
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'migration 0002 requires READ COMMITTED isolation, not %',
            current_setting('transaction_isolation')
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

DO $$
BEGIN
    LOCK TABLE tasks IN EXCLUSIVE MODE NOWAIT;
    LOCK TABLE attempts IN ACCESS EXCLUSIVE MODE NOWAIT;
EXCEPTION WHEN lock_not_available THEN
    RAISE EXCEPTION 'migration 0002 needs a quiescent database: tasks or attempts is in use; '
        'stop workers and claimers, end open transactions, then retry'
        USING ERRCODE = 'lock_not_available';
END
$$;

DO $$
BEGIN
    IF EXISTS (SELECT FROM tasks WHERE state = 'running')
        OR EXISTS (SELECT FROM attempts WHERE state = 'running') THEN
        RAISE EXCEPTION 'migration 0002 refuses to run while tasks or attempts are running; '
            'stop claimers and let running attempts finish, then retry'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

-- lease_expires_at and deadline_at are set together when an attempt is
-- claimed, from one claim instant. The lease may only grow, up to the
-- deadline; the deadline never changes. Finished attempts from before this
-- migration have neither.
ALTER TABLE attempts
    ADD COLUMN lease_expires_at timestamptz,
    ADD COLUMN deadline_at      timestamptz,
    ADD COLUMN revoked_reason   text,
    DROP CONSTRAINT attempts_state_valid,
    ADD CONSTRAINT attempts_state_valid
        CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled', 'revoked')),
    ADD CONSTRAINT attempts_lease_iff_deadline
        CHECK ((lease_expires_at IS NULL) = (deadline_at IS NULL)),
    -- Every running attempt can be recovered, and every revocation keeps the
    -- facts it was decided on.
    ADD CONSTRAINT attempts_lease_required
        CHECK (state NOT IN ('running', 'revoked') OR lease_expires_at IS NOT NULL),
    ADD CONSTRAINT attempts_lease_after_start CHECK (lease_expires_at > started_at),
    ADD CONSTRAINT attempts_lease_within_deadline CHECK (lease_expires_at <= deadline_at),
    ADD CONSTRAINT attempts_revoked_reason_valid
        CHECK (revoked_reason IN ('lease_expired', 'deadline_exceeded')),
    ADD CONSTRAINT attempts_revoked_reason_iff_revoked
        CHECK ((revoked_reason IS NOT NULL) = (state = 'revoked')),
    -- The reason follows from the stored lease: a lease capped at the deadline
    -- exceeded it; any other expired lease was not renewed in time.
    ADD CONSTRAINT attempts_revoked_reason_matches_lease
        CHECK (revoked_reason IS NULL
            OR (revoked_reason = 'deadline_exceeded') = (lease_expires_at = deadline_at));

-- The execution deadline is the one bound a live worker cannot extend, so
-- the database refuses any change to it, like the generation guard in 0001.
CREATE FUNCTION attempts_guard_deadline() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'an attempt''s deadline_at cannot change (% -> %)',
        OLD.deadline_at, NEW.deadline_at
        USING ERRCODE = 'check_violation', CONSTRAINT = 'attempts_deadline_immutable';
END
$$;

CREATE TRIGGER attempts_deadline_immutable
    BEFORE UPDATE ON attempts
    FOR EACH ROW WHEN (OLD.deadline_at IS DISTINCT FROM NEW.deadline_at)
    EXECUTE FUNCTION attempts_guard_deadline();
