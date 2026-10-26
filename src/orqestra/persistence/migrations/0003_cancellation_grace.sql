-- Cancellation-grace revocation reason (ADR-0003; Day 3A Slice 3).
--
-- Additive: attempts may now be revoked as 'cancellation_grace_expired'. No
-- column, index or other constraint changes. Every stored row already satisfies
-- the widened rule, and running attempts are not affected, so the migration
-- needs no quiescence of its own. attempts_revoked_reason_matches_lease is kept:
-- it already admits the new reason exactly when the lease ended before the
-- deadline, which is when it is assigned (a lease that reached its deadline is
-- 'deadline_exceeded').
--
-- Application versions are a separate matter the database cannot enforce: code
-- from before this slice cannot read the new reason, and does not stop
-- heartbeats after a cancellation request. Stop all application processes,
-- apply this migration, then start only code from this slice or later.
--
-- Like 0002, it takes its lock before any change and with NOWAIT, so it fails
-- at once instead of waiting while any session uses the table. It changes
-- only attempts, so it does not lock tasks.

DO $$
BEGIN
    LOCK TABLE attempts IN ACCESS EXCLUSIVE MODE NOWAIT;
EXCEPTION WHEN lock_not_available THEN
    RAISE EXCEPTION 'migration 0003 could not lock attempts because another session is using it; '
        'stop application processes and end open transactions, then retry'
        USING ERRCODE = 'lock_not_available';
END
$$;

ALTER TABLE attempts
    DROP CONSTRAINT attempts_revoked_reason_valid,
    ADD CONSTRAINT attempts_revoked_reason_valid
        CHECK (revoked_reason IN ('lease_expired', 'deadline_exceeded', 'cancellation_grace_expired'));
