-- Provider usage ledger: append-only intent, result and closure records.
--
-- Additive: three new, append-only tables record what provider calls an attempt
-- made. provider_calls holds each call's intent, written before the call;
-- provider_call_results holds what the provider answered or how it failed, with
-- any amount and unit exactly as the provider reported them; and
-- attempt_usage_closures says whether an attempt's capture is complete. A call
-- whose outcome is unknown has no result row. job_id and task_id are copied from
-- the attempt when an intent is recorded, so attribution survives fencing and
-- recovery, and no trigger here reads tasks (DEC-036). No existing row,
-- column, constraint or index changes.
--
-- The only reference to an engine table is provider_calls -> attempts, ON
-- DELETE RESTRICT. Adding it needs SHARE ROW EXCLUSIVE on attempts, the weakest
-- mode that can: it conflicts only with sessions writing attempts (ROW
-- EXCLUSIVE and above), not with plain readers or with FOR UPDATE row-lock
-- holders (ROW SHARE). Like 0002 to 0004, the lock is taken first and with
-- NOWAIT, so the migration fails at once, changing nothing, instead of waiting
-- behind a writer. It never locks or alters tasks.
--
-- Rollout: apply this migration, then restart workers on code from this slice
-- or later. Code from before this slice never uses these tables. A worker's
-- startup check requires the database's migration history to match its code
-- exactly (DEC-051), so code that does not match the schema refuses to start.

DO $$
BEGIN
    LOCK TABLE attempts IN SHARE ROW EXCLUSIVE MODE NOWAIT;
EXCEPTION WHEN lock_not_available THEN
    RAISE EXCEPTION 'migration 0005 could not lock attempts because another session is writing it; '
        'end open transactions that write attempts, then retry'
        USING ERRCODE = 'lock_not_available';
END
$$;

-- The intent of one provider call, recorded before the call is made.
CREATE TABLE provider_calls (
    attempt_id              uuid        NOT NULL,
    call_seq                integer     NOT NULL,
    job_id                  uuid        NOT NULL,
    task_id                 uuid        NOT NULL,
    provider                text        NOT NULL,
    route_id                text        NOT NULL,
    prompt_version          text,
    capability              text        NOT NULL,
    model                   text,
    field_kind              text,
    item_index              integer,
    -- The caller's route estimate in opaque units, not accounting.
    estimated_cost_units    bigint,
    attempt_state_at_record text        NOT NULL,
    recorded_at             timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT provider_calls_pkey PRIMARY KEY (attempt_id, call_seq),
    CONSTRAINT provider_calls_attempt_fk
        FOREIGN KEY (attempt_id) REFERENCES attempts (id) ON DELETE RESTRICT,
    CONSTRAINT provider_calls_call_seq_range CHECK (call_seq >= 0),
    CONSTRAINT provider_calls_provider_valid CHECK (provider ~ '^[A-Za-z0-9._-]{1,64}$'),
    CONSTRAINT provider_calls_route_id_valid CHECK (route_id ~ '^[A-Za-z0-9._-]{1,64}$'),
    CONSTRAINT provider_calls_prompt_version_valid
        CHECK (prompt_version ~ '^[A-Za-z0-9._-]{1,64}$'),
    CONSTRAINT provider_calls_capability_valid
        CHECK (btrim(capability) <> '' AND char_length(capability) <= 256),
    CONSTRAINT provider_calls_model_valid
        CHECK (btrim(model) <> '' AND char_length(model) <= 128),
    CONSTRAINT provider_calls_field_kind_valid
        CHECK (field_kind IN ('quantity', 'unit_amount', 'total')),
    CONSTRAINT provider_calls_item_index_range CHECK (item_index >= 0),
    CONSTRAINT provider_calls_estimated_cost_units_range CHECK (estimated_cost_units >= 0),
    CONSTRAINT provider_calls_attempt_state_valid
        CHECK (attempt_state_at_record IN ('running', 'succeeded', 'failed', 'cancelled', 'revoked'))
);

CREATE INDEX provider_calls_job_id ON provider_calls (job_id);

-- What one call's provider answered, or how it failed. reported_amount has no
-- type modifier, so it is stored exactly as given and never rounded.
CREATE TABLE provider_call_results (
    attempt_id              uuid        NOT NULL,
    call_seq                integer     NOT NULL,
    outcome                 text        NOT NULL,
    failure_code            text,
    reported_amount         numeric,
    reported_unit           text,
    model_served            text,
    provider_generation_id  text,
    input_tokens            bigint,
    output_tokens           bigint,
    latency_ms              integer,
    attempt_state_at_record text        NOT NULL,
    recorded_at             timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT provider_call_results_pkey PRIMARY KEY (attempt_id, call_seq),
    CONSTRAINT provider_call_results_call_fk FOREIGN KEY (attempt_id, call_seq)
        REFERENCES provider_calls (attempt_id, call_seq) ON DELETE RESTRICT,
    CONSTRAINT provider_call_results_outcome_valid CHECK (outcome IN ('answered', 'failed')),
    CONSTRAINT provider_call_results_failure_code_valid
        CHECK (failure_code IN
            ('timeout', 'rate_limited', 'unavailable', 'rejected', 'unsupported_request')),
    CONSTRAINT provider_call_results_failure_code_iff_failed
        CHECK ((failure_code IS NOT NULL) = (outcome = 'failed')),
    -- NaN compares above every number and Infinity is not below 1e20, so both
    -- are refused; -Infinity is below 0.
    CONSTRAINT provider_call_results_reported_amount_valid
        CHECK (reported_amount >= 0 AND reported_amount <> 'NaN' AND reported_amount < 1e20
            AND scale(reported_amount) <= 18),
    CONSTRAINT provider_call_results_reported_unit_valid
        CHECK (btrim(reported_unit) <> '' AND char_length(reported_unit) <= 64),
    CONSTRAINT provider_call_results_amount_iff_unit
        CHECK ((reported_amount IS NULL) = (reported_unit IS NULL)),
    CONSTRAINT provider_call_results_model_served_valid
        CHECK (btrim(model_served) <> '' AND char_length(model_served) <= 128),
    CONSTRAINT provider_call_results_generation_id_valid
        CHECK (btrim(provider_generation_id) <> '' AND char_length(provider_generation_id) <= 128),
    CONSTRAINT provider_call_results_input_tokens_range CHECK (input_tokens >= 0),
    CONSTRAINT provider_call_results_output_tokens_range CHECK (output_tokens >= 0),
    CONSTRAINT provider_call_results_latency_ms_range CHECK (latency_ms >= 0),
    CONSTRAINT provider_call_results_attempt_state_valid
        CHECK (attempt_state_at_record IN ('running', 'succeeded', 'failed', 'cancelled', 'revoked'))
);

-- Whether an attempt's capture is complete. It has no foreign key: the trigger
-- below checks that the attempt exists, and attempts are never deleted.
CREATE TABLE attempt_usage_closures (
    attempt_id   uuid        NOT NULL,
    capture      text        NOT NULL,
    call_count   integer,
    closed_by    text        NOT NULL,
    capture_tier text        NOT NULL,
    recorded_at  timestamptz NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT attempt_usage_closures_pkey PRIMARY KEY (attempt_id),
    CONSTRAINT attempt_usage_closures_capture_valid CHECK (capture IN ('complete', 'incomplete')),
    CONSTRAINT attempt_usage_closures_call_count_range CHECK (call_count >= 0),
    CONSTRAINT attempt_usage_closures_call_count_iff_complete
        CHECK ((call_count IS NOT NULL) = (capture = 'complete')),
    CONSTRAINT attempt_usage_closures_closed_by_valid CHECK (closed_by IN ('worker', 'recovery')),
    CONSTRAINT attempt_usage_closures_capture_tier_valid
        CHECK (capture_tier IN ('t1', 't2', 't3')),
    -- Recovery cannot vouch for a complete capture below tier t3.
    CONSTRAINT attempt_usage_closures_recovery_tier
        CHECK (NOT (closed_by = 'recovery' AND capture_tier IN ('t1', 't2') AND capture = 'complete'))
);

-- A closure must name an existing attempt, and a complete one must count every
-- recorded intent, each with its result. A closure already committed for the
-- attempt is left to the primary key (or ON CONFLICT), so a replay that arrives
-- after a later intent is still a replay. Reads only attempts and the ledger,
-- without row locks; never tasks.
CREATE FUNCTION attempt_usage_closures_check_consistent() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    intents    integer;
    unanswered integer;
BEGIN
    IF EXISTS (SELECT FROM attempt_usage_closures WHERE attempt_id = NEW.attempt_id) THEN
        RETURN NEW;
    END IF;
    IF NOT EXISTS (SELECT FROM attempts WHERE id = NEW.attempt_id) THEN
        RAISE EXCEPTION 'attempt % does not exist', NEW.attempt_id
            USING ERRCODE = 'check_violation',
                CONSTRAINT = 'attempt_usage_closures_attempt_exists';
    END IF;
    IF NEW.capture = 'complete' THEN
        SELECT count(*), count(*) FILTER (WHERE r.attempt_id IS NULL)
        INTO intents, unanswered
        FROM provider_calls c
        LEFT JOIN provider_call_results r
            ON r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq
        WHERE c.attempt_id = NEW.attempt_id;
        IF intents <> NEW.call_count OR unanswered <> 0 THEN
            RAISE EXCEPTION 'a complete capture of attempt % must count % intents, each with '
                'its result; it counts %, and % have no result',
                NEW.attempt_id, intents, NEW.call_count, unanswered
                USING ERRCODE = 'check_violation',
                    CONSTRAINT = 'attempt_usage_closures_complete_capture';
        END IF;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER attempt_usage_closures_consistent
    BEFORE INSERT ON attempt_usage_closures
    FOR EACH ROW EXECUTE FUNCTION attempt_usage_closures_check_consistent();

-- The ledger is append-only, like the deadline guard in 0002: no row changes or
-- disappears once recorded.
CREATE FUNCTION provider_usage_refuse_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'check_violation', CONSTRAINT = TG_TABLE_NAME || '_immutable';
END
$$;

CREATE TRIGGER provider_calls_immutable
    BEFORE UPDATE OR DELETE ON provider_calls
    FOR EACH ROW EXECUTE FUNCTION provider_usage_refuse_change();
CREATE TRIGGER provider_calls_immutable_truncate
    BEFORE TRUNCATE ON provider_calls
    FOR EACH STATEMENT EXECUTE FUNCTION provider_usage_refuse_change();

CREATE TRIGGER provider_call_results_immutable
    BEFORE UPDATE OR DELETE ON provider_call_results
    FOR EACH ROW EXECUTE FUNCTION provider_usage_refuse_change();
CREATE TRIGGER provider_call_results_immutable_truncate
    BEFORE TRUNCATE ON provider_call_results
    FOR EACH STATEMENT EXECUTE FUNCTION provider_usage_refuse_change();

CREATE TRIGGER attempt_usage_closures_immutable
    BEFORE UPDATE OR DELETE ON attempt_usage_closures
    FOR EACH ROW EXECUTE FUNCTION provider_usage_refuse_change();
CREATE TRIGGER attempt_usage_closures_immutable_truncate
    BEFORE TRUNCATE ON attempt_usage_closures
    FOR EACH STATEMENT EXECUTE FUNCTION provider_usage_refuse_change();
