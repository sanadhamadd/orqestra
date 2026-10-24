-- Initial schema: durable Job -> Task -> Attempt records (ADR-0003).
--
-- State values mirror orqestra.domain.states and orqestra.domain.policy; an
-- integration test fails if they drift. Timestamps come from the database
-- clock. Every constraint is named so a violation identifies the exact rule.

CREATE TABLE jobs (
    id              uuid        PRIMARY KEY,
    idempotency_key text,
    kind            text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_idempotency_key_unique UNIQUE (idempotency_key),
    CONSTRAINT jobs_idempotency_key_not_blank CHECK (btrim(idempotency_key) <> ''),
    CONSTRAINT jobs_kind_not_blank CHECK (btrim(kind) <> '')
);

CREATE TABLE tasks (
    id                  uuid        PRIMARY KEY,
    job_id              uuid        NOT NULL,
    task_key            text        NOT NULL,
    state               text        NOT NULL DEFAULT 'queued',
    payload             jsonb       NOT NULL,
    result              jsonb,
    max_attempts        integer     NOT NULL,
    -- Number of attempts started so far, and the task's ownership generation:
    -- attempt N holds generation N, and authoritative writes must present it.
    attempt_count       integer     NOT NULL DEFAULT 0,
    cancel_requested_at timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz,
    CONSTRAINT tasks_job_fk FOREIGN KEY (job_id) REFERENCES jobs (id) ON DELETE RESTRICT,
    CONSTRAINT tasks_job_task_key_unique UNIQUE (job_id, task_key),
    CONSTRAINT tasks_task_key_not_blank CHECK (btrim(task_key) <> ''),
    CONSTRAINT tasks_state_valid
        CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT tasks_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 100),
    CONSTRAINT tasks_attempt_count_range CHECK (attempt_count BETWEEN 0 AND max_attempts),
    -- A queued task must remain claimable; exhausting retries means failing.
    CONSTRAINT tasks_queued_has_attempts_left
        CHECK (state <> 'queued' OR attempt_count < max_attempts),
    CONSTRAINT tasks_running_has_attempt CHECK (state <> 'running' OR attempt_count >= 1),
    CONSTRAINT tasks_result_iff_succeeded CHECK ((result IS NOT NULL) = (state = 'succeeded')),
    -- Cancellation is requested before it happens; a queued task is cancelled
    -- at once, so it never carries a pending request.
    CONSTRAINT tasks_cancel_request_not_queued
        CHECK (cancel_requested_at IS NULL OR state <> 'queued'),
    CONSTRAINT tasks_cancelled_was_requested
        CHECK (state <> 'cancelled' OR cancel_requested_at IS NOT NULL),
    CONSTRAINT tasks_finished_iff_terminal
        CHECK ((finished_at IS NOT NULL) = (state IN ('succeeded', 'failed', 'cancelled')))
);

CREATE TABLE attempts (
    id             uuid        PRIMARY KEY,
    task_id        uuid        NOT NULL,
    attempt_number integer     NOT NULL,
    worker_id      text        NOT NULL,
    state          text        NOT NULL DEFAULT 'running',
    failure_kind   text,
    error_type     text,
    error_message  text,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    CONSTRAINT attempts_task_fk FOREIGN KEY (task_id) REFERENCES tasks (id) ON DELETE RESTRICT,
    CONSTRAINT attempts_task_number_unique UNIQUE (task_id, attempt_number),
    CONSTRAINT attempts_number_range CHECK (attempt_number BETWEEN 1 AND 100),
    CONSTRAINT attempts_worker_id_not_blank CHECK (btrim(worker_id) <> ''),
    CONSTRAINT attempts_state_valid
        CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT attempts_failure_kind_valid
        CHECK (failure_kind IN ('transient', 'permanent', 'unclassified')),
    CONSTRAINT attempts_failure_kind_iff_failed
        CHECK ((failure_kind IS NOT NULL) = (state = 'failed')),
    CONSTRAINT attempts_failed_has_error_type CHECK (state <> 'failed' OR error_type IS NOT NULL),
    CONSTRAINT attempts_error_details_only_when_failed
        CHECK (state = 'failed' OR (error_type IS NULL AND error_message IS NULL)),
    CONSTRAINT attempts_finished_iff_terminal
        CHECK ((finished_at IS NOT NULL) = (state <> 'running'))
);

-- Ownership: at most one live attempt per task, and at most one attempt whose
-- success can be the task's authoritative result.
CREATE UNIQUE INDEX attempts_one_running_per_task ON attempts (task_id) WHERE state = 'running';
CREATE UNIQUE INDEX attempts_one_succeeded_per_task ON attempts (task_id)
    WHERE state = 'succeeded';

-- Fencing guard. attempt_count starts at 0 and changes only when a task is
-- claimed (queued -> running), by exactly +1; a claim must advance it. This
-- protects the generation against decreases, skipped values and unrelated
-- increments from any writer. It does not restate the transition table and
-- does not replace conditional writes that check the generation.
CREATE FUNCTION tasks_guard_attempt_count() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.attempt_count <> 0 THEN
            RAISE EXCEPTION 'a new task must start with attempt_count 0, got %',
                NEW.attempt_count
                USING ERRCODE = 'check_violation', CONSTRAINT = 'tasks_attempt_count_fencing';
        END IF;
    ELSIF OLD.state = 'queued' AND NEW.state = 'running' THEN
        IF NEW.attempt_count <> OLD.attempt_count + 1 THEN
            RAISE EXCEPTION 'a claim must advance attempt_count by exactly 1 (% -> %)',
                OLD.attempt_count, NEW.attempt_count
                USING ERRCODE = 'check_violation', CONSTRAINT = 'tasks_attempt_count_fencing';
        END IF;
    ELSIF NEW.attempt_count <> OLD.attempt_count THEN
        RAISE EXCEPTION 'attempt_count may change only when a task is claimed (% -> %)',
            OLD.attempt_count, NEW.attempt_count
            USING ERRCODE = 'check_violation', CONSTRAINT = 'tasks_attempt_count_fencing';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER tasks_attempt_count_fencing
    BEFORE INSERT OR UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION tasks_guard_attempt_count();
