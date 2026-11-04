"""Usage closure written by the lifecycle, against real PostgreSQL (DEC-057).

``complete_attempt`` closes an attempt's capture with the worker's closure and
``revoke_next_expired`` with one it derives for the kinds it is told about, each in
the transaction that ends the attempt: the closure commits or rolls back with the
transition. Recovery keeps any existing closure and never raises a replay conflict.
An intent waits behind the lifecycle's lock on its attempt and then records the
committed state, so a call recorded after a closure is flagged late and never counted
by it. ``record_late_usage`` and ``capture_of`` report what a late fact means.

Deterministic tests hold one transaction at a known point with a test-only gate
trigger and observe the competitor waiting in ``pg_locks``.
"""

import secrets
import uuid
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from functools import partial
from typing import Any, cast

import psycopg
import pytest
from psycopg import errors, sql
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow

from orqestra.domain.jobs import (
    Attempt,
    AttemptCancelled,
    AttemptOutcome,
    Task,
)
from orqestra.domain.leases import LeasePolicy, RevocationReason
from orqestra.domain.retry import RetryPolicy
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence.lifecycle import (
    AttemptCompletion,
    AttemptFinishedError,
    AttemptRevocation,
    AttemptRevokedError,
    CompletionEvidence,
    StaleAttemptError,
    complete_attempt,
    get_attempt,
    read_completion_evidence,
    request_cancellation,
    revoke_next_expired,
    start_attempt,
)
from orqestra.persistence.migrate import load_migrations, migrate
from orqestra.persistence.provider_usage import (
    AttemptCapture,
    AttemptUsage,
    CallIntentNotFoundError,
    CaptureNotCompleteError,
    CaptureState,
    CaptureTier,
    ClosedBy,
    LateUsage,
    Recorded,
    UsageClosure,
    UsageReplayConflictError,
    capture_of,
    read_attempt_usage,
    record_call_intent,
    record_call_result,
    record_late_usage,
    record_usage_closure,
)
from tests.integration.test_attempt_lifecycle import (
    BRIEF_GRACE,
    DEADLINE_EXPIRING,
    EXPIRING,
    GRACE,
    KIND,
    PERMANENT,
    POLICY,
    RETRY,
    SUCCESS,
    TRANSIENT,
    Snapshot,
    assert_invariants,
    complete,
    connect,
    jittered,
    new_task,
    no_wait,
    race,
    renew,
    revoke,
    snapshot,
    task_now,
    until_expired,
    wait_for_lock,
)
from tests.integration.test_provider_usage_pg import (
    LEDGER_TABLES,
    a_closure,
    an_answer,
    an_intent,
    count,
    incomplete,
    insert_closure,
    rows,
    running,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
Gate = Callable[[str, str], Callable[[], None]]

CLOSURES = "attempt_usage_closures"
T3_FOR_KIND: Mapping[str, CaptureTier] = {KIND: CaptureTier.T3}
OTHER_KIND = "purchase_order_matching"
# Test value only (DEC-034): a delay far longer than any test, so "later than now" is exact.
HOUR = RetryPolicy(base_delay=timedelta(hours=1), max_delay=timedelta(hours=1))


@pytest.fixture
def checked(db: Conn) -> Iterator[Conn]:
    """The migrated test connection; the lifecycle invariants must hold when the test ends."""
    yield db
    assert_invariants(db)


@pytest.fixture
def worker(schema_url: str) -> Iterator[Callable[[], Conn]]:
    """Open extra connections, each a separate session; closed at teardown."""
    opened: list[Conn] = []

    def open_connection() -> Conn:
        conn = connect(schema_url)
        opened.append(conn)
        return conn

    yield open_connection
    for conn in opened:
        conn.close()


@pytest.fixture
def gate(checked: Conn) -> Iterator[Gate]:
    """``gate(event, table)`` holds every ``event`` on ``table`` until released.

    A test-only statement trigger takes a shared advisory lock that this fixture holds
    exclusively, so a transaction stops at that statement while keeping every lock it
    already holds. Returns the function that releases it.
    """
    held: list[int] = []

    def close(event: str, table: str) -> Callable[[], None]:
        key = secrets.randbits(62)
        name = sql.Identifier(f"gate_{len(held)}_{event.lower()}_{table}")
        checked.execute(
            sql.SQL("""
                CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN PERFORM pg_advisory_xact_lock_shared({key}); RETURN NULL; END $$;
                CREATE TRIGGER {name} BEFORE {event} ON {table}
                    FOR EACH STATEMENT EXECUTE FUNCTION {name}();
            """).format(
                name=name, key=sql.Literal(key), event=sql.SQL(event), table=sql.Identifier(table)
            )
        )
        checked.execute("SELECT pg_advisory_lock(%s)", (key,))
        held.append(key)

        def release() -> None:
            if key in held:
                checked.execute("SELECT pg_advisory_unlock(%s)", (key,))
                held.remove(key)

        return release

    yield close
    for key in list(held):
        checked.execute("SELECT pg_advisory_unlock(%s)", (key,))


# --- helpers ----------------------------------------------------------------------------


def finish(
    db: Conn,
    attempt: Attempt,
    outcome: AttemptOutcome,
    closure: UsageClosure | None,
    retry: RetryPolicy = RETRY,
) -> AttemptCompletion:
    return complete_attempt(
        db,
        attempt.id,
        generation=attempt.attempt_number,
        outcome=outcome,
        retry_policy=retry,
        usage_closure=closure,
    )


def sweep(
    db: Conn, tiers: Mapping[str, CaptureTier] | None = T3_FOR_KIND
) -> AttemptRevocation | None:
    return revoke_next_expired(db, retry_policy=RETRY, usage_capture=tiers)


def expired(db: Conn, policy: LeasePolicy = EXPIRING) -> tuple[Task, Attempt]:
    """A task whose running attempt's lease has already expired by the database clock."""
    task = new_task(db)
    attempt = start_attempt(db, task.id, "w", policy=policy)
    until_expired(db, attempt)
    return task, attempt


def calls(db: Conn, attempt: Attempt, *, answered: int, unanswered: int = 0) -> None:
    """Record ``answered`` calls with their results, then ``unanswered`` without."""
    for seq in range(answered + unanswered):
        record_call_intent(db, an_intent(attempt.id, seq))
        if seq < answered:
            record_call_result(db, an_answer(attempt.id, seq))


def write_call(conn: Conn, attempt_id: uuid.UUID, seq: int) -> Recorded[Any]:
    record_call_intent(conn, an_intent(attempt_id, seq))
    return record_call_result(conn, an_answer(attempt_id, seq))


def ledger(db: Conn) -> Snapshot:
    return {table: rows(db, table) for table in LEDGER_TABLES}


def usage_of(db: Conn, attempt_id: uuid.UUID) -> AttemptUsage:
    usage = read_attempt_usage(db, attempt_id)
    assert usage is not None
    return usage


def stored_closure(db: Conn, attempt_id: uuid.UUID) -> UsageClosure | None:
    closure = usage_of(db, attempt_id).closure
    return None if closure is None else closure.closure


def by_recovery(
    attempt_id: uuid.UUID, tier: CaptureTier = CaptureTier.T3, call_count: int | None = None
) -> UsageClosure:
    return UsageClosure(
        attempt_id=attempt_id,
        capture=CaptureState.INCOMPLETE if call_count is None else CaptureState.COMPLETE,
        closed_by=ClosedBy.RECOVERY,
        capture_tier=tier,
        call_count=call_count,
    )


def assert_unchanged(db: Conn, engine: Snapshot, usage: Snapshot) -> None:
    assert snapshot(db) == engine
    assert ledger(db) == usage
    assert db.info.transaction_status is TransactionStatus.IDLE


def end_with(operation: str, attempt: Attempt) -> Callable[[Conn], object]:
    """The operation that ends ``attempt`` (with one answered call) and closes its capture."""
    if operation == "recover":
        return sweep
    closure = a_closure(attempt.id, call_count=1)
    return partial(finish, attempt=attempt, outcome=SUCCESS, closure=closure)


def poison(db: Conn, event: str, table: str) -> None:
    """A test-only trigger that fails every ``event`` on ``table``."""
    db.execute(
        sql.SQL("""
            CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'poisoned'; END $$;
            CREATE TRIGGER poison BEFORE {event} ON {table}
                FOR EACH ROW EXECUTE FUNCTION poison();
        """).format(event=sql.SQL(event), table=sql.Identifier(table))
    )


# --- 1. compatibility -------------------------------------------------------------------


def test_without_the_new_arguments_nothing_is_written_to_the_ledger(checked: Conn) -> None:
    _, done = running(checked)
    calls(checked, done, answered=1)
    _, lost = expired(checked)
    before = ledger(checked)

    completion = complete(checked, done, SUCCESS)
    revocation = revoke(checked)
    assert revocation.attempt.id == lost.id
    assert (completion.usage_closure, revocation.usage_closure) == (None, None)
    assert ledger(checked) == before
    assert capture_of(usage_of(checked, lost.id)) is AttemptCapture.NOT_CAPTURED
    assert capture_of(usage_of(checked, done.id)) is AttemptCapture.LOWER_BOUND


def test_an_attempt_from_before_the_ledger_is_not_captured(schema_url: str) -> None:
    migrate(schema_url, list(load_migrations()[:4]))
    with connect(schema_url) as conn:
        task = new_task(conn)
        attempt = start_attempt(conn, task.id, "w", policy=POLICY)
        complete(conn, attempt, SUCCESS)
    migrate(schema_url)
    with connect(schema_url) as conn:
        usage = usage_of(conn, attempt.id)
        assert (usage.calls, usage.closure) == ((), None)
        assert capture_of(usage) is AttemptCapture.NOT_CAPTURED
        assert_invariants(conn)


# --- 2. the worker's closure, by outcome ------------------------------------------------


@pytest.mark.parametrize(
    ("outcome", "task_state"),
    [
        (SUCCESS, TaskState.SUCCEEDED),
        (PERMANENT, TaskState.FAILED),
        (TRANSIENT, TaskState.QUEUED),
        (AttemptCancelled(), TaskState.CANCELLED),
    ],
    ids=["succeeded", "failed", "requeued", "cancelled"],
)
def test_every_outcome_commits_its_closure_with_the_transition(
    checked: Conn, outcome: AttemptOutcome, task_state: TaskState
) -> None:
    task, attempt = running(checked)
    calls(checked, attempt, answered=1)
    if isinstance(outcome, AttemptCancelled):
        request_cancellation(checked, task.id, grace=GRACE)
    closure = a_closure(attempt.id, call_count=1)

    completion = finish(checked, attempt, outcome, closure, retry=HOUR)
    assert completion.usage_closure is not None
    assert completion.usage_closure.created
    assert completion.usage_closure.record.closure == closure
    assert completion.attempt == get_attempt(checked, attempt.id)
    assert completion.attempt.state is outcome.state
    assert task_now(checked, task.id).state is task_state
    if task_state is TaskState.QUEUED:
        retry_at = checked.execute(
            "SELECT available_at > statement_timestamp() FROM tasks WHERE id = %s", (task.id,)
        ).fetchone()
        assert retry_at == (True,)
    assert stored_closure(checked, attempt.id) == closure
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.CLOSED


def test_a_cancelled_outcome_without_a_request_writes_nothing(checked: Conn) -> None:
    _, attempt = running(checked)
    engine, usage = snapshot(checked), ledger(checked)
    with pytest.raises(ValueError, match="cancellation request"):
        finish(checked, attempt, AttemptCancelled(), incomplete(attempt.id))
    assert_unchanged(checked, engine, usage)


# --- 3. rollback atomicity --------------------------------------------------------------


@pytest.mark.parametrize(
    ("answered", "unanswered"), [(1, 0), (1, 1)], ids=["miscounted", "unresolved"]
)
def test_a_closure_that_does_not_match_the_ledger_rolls_back_the_completion(
    checked: Conn, answered: int, unanswered: int
) -> None:
    task, attempt = running(checked)
    calls(checked, attempt, answered=answered, unanswered=unanswered)
    engine, usage = snapshot(checked), ledger(checked)
    with pytest.raises(CaptureNotCompleteError):
        finish(checked, attempt, SUCCESS, a_closure(attempt.id, call_count=2))
    assert_unchanged(checked, engine, usage)

    # The documented fallback: complete again with an incomplete closure.
    completion = finish(checked, attempt, SUCCESS, incomplete(attempt.id))
    assert completion.usage_closure is not None
    assert completion.usage_closure.created
    assert completion.task.state is TaskState.SUCCEEDED
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.LOWER_BOUND


def test_a_different_stored_closure_rolls_back_the_completion(checked: Conn) -> None:
    _, attempt = running(checked)
    standalone = record_usage_closure(
        checked, incomplete(attempt.id, capture_tier=CaptureTier.T2)
    ).record
    engine, usage = snapshot(checked), ledger(checked)
    with pytest.raises(UsageReplayConflictError):
        finish(checked, attempt, SUCCESS, incomplete(attempt.id))
    assert_unchanged(checked, engine, usage)

    replay = finish(checked, attempt, SUCCESS, standalone.closure)
    assert replay.usage_closure == Recorded(standalone, created=False)


@pytest.mark.parametrize(
    ("event", "table"),
    [("UPDATE", "tasks"), ("INSERT", CLOSURES)],
    ids=["task-update", "closure-insert"],
)
@pytest.mark.parametrize("operation", ["complete", "recover"])
def test_a_failure_after_the_attempt_was_written_rolls_back_everything(
    checked: Conn, operation: str, event: str, table: str
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=1)
    run = end_with(operation, attempt)
    poison(checked, event, table)
    engine, usage = snapshot(checked), ledger(checked)
    with pytest.raises(errors.RaiseException, match="poisoned"):
        run(checked)
    assert_unchanged(checked, engine, usage)

    checked.execute(sql.SQL("DROP TRIGGER poison ON {}").format(sql.Identifier(table)))
    assert run(checked) is not None
    assert stored_closure(checked, attempt.id) is not None


@pytest.mark.parametrize("operation", ["complete", "recover"])
def test_a_connection_lost_at_the_closure_insert_records_nothing(
    checked: Conn, gate: Gate, worker: Callable[[], Conn], operation: str
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=1)
    run = end_with(operation, attempt)
    engine, usage = snapshot(checked), ledger(checked)
    release = gate("INSERT", CLOSURES)
    conn = worker()
    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(run, conn)
        wait_for_lock(checked, conn, "advisory")  # attempt and task written, closure not yet
        checked.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            pending.result(timeout=20)
    release()
    assert_unchanged(checked, engine, usage)
    assert run(checked) is not None  # it can simply be tried again


def test_completion_evidence_matches_exactly_when_the_closure_exists(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    _, attempt = running(checked)
    closure = a_closure(attempt.id, call_count=0)

    def evidence() -> CompletionEvidence:
        return read_completion_evidence(
            checked, attempt.id, generation=attempt.attempt_number, outcome=SUCCESS
        )

    release = gate("INSERT", CLOSURES)
    conn = worker()
    with ThreadPoolExecutor(1) as pool:
        lost = pool.submit(finish, conn, attempt, SUCCESS, closure)
        wait_for_lock(checked, conn, "advisory")
        checked.execute("SELECT pg_terminate_backend(%s)", (conn.info.backend_pid,))
        with pytest.raises(psycopg.OperationalError):
            lost.result(timeout=20)
    release()
    assert (evidence(), stored_closure(checked, attempt.id)) == (CompletionEvidence.RUNNING, None)

    finish(checked, attempt, SUCCESS, closure)
    assert (evidence(), stored_closure(checked, attempt.id)) == (
        CompletionEvidence.MATCHES,
        closure,
    )


# --- 4. fencing -------------------------------------------------------------------------


def test_a_stale_generation_writes_no_closure(checked: Conn) -> None:
    task = new_task(checked)
    first = start_attempt(checked, task.id, "w", policy=POLICY)
    finish(checked, first, TRANSIENT, None)
    second = start_attempt(checked, task.id, "w", policy=POLICY)
    engine, usage = snapshot(checked), ledger(checked)
    with pytest.raises(StaleAttemptError):
        complete_attempt(
            checked,
            second.id,
            generation=first.attempt_number,
            outcome=SUCCESS,
            retry_policy=RETRY,
            usage_closure=incomplete(second.id),
        )
    assert_unchanged(checked, engine, usage)


def test_a_revoked_workers_completion_is_refused_and_the_recovery_closure_stands(
    checked: Conn,
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=1)
    revocation = sweep(checked)
    assert revocation is not None
    assert revocation.usage_closure is not None
    assert revocation.usage_closure.record.closure == by_recovery(attempt.id, call_count=1)
    engine, usage = snapshot(checked), ledger(checked)
    with pytest.raises(AttemptRevokedError):
        finish(checked, attempt, SUCCESS, a_closure(attempt.id, call_count=1))
    assert_unchanged(checked, engine, usage)


def test_a_duplicate_completion_leaves_one_closure(checked: Conn) -> None:
    _, attempt = running(checked)
    closure = incomplete(attempt.id)
    finish(checked, attempt, SUCCESS, closure)
    engine, usage = snapshot(checked), ledger(checked)
    with pytest.raises(AttemptFinishedError) as info:
        finish(checked, attempt, SUCCESS, closure)
    assert type(info.value) is AttemptFinishedError
    assert_unchanged(checked, engine, usage)
    assert count(checked, CLOSURES) == 1


# --- 5. recovery, by kind and tier ------------------------------------------------------


def test_an_unmapped_kind_gets_no_closure_and_stays_not_captured(checked: Conn) -> None:
    # A single fleet-wide T3 would close this attempt as complete(0): "no calls".
    _, attempt = expired(checked)
    revocation = sweep(checked, {OTHER_KIND: CaptureTier.T3})
    assert revocation is not None
    assert revocation.attempt.state is AttemptState.REVOKED
    assert revocation.usage_closure is None
    assert count(checked, CLOSURES) == 0
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.NOT_CAPTURED


@pytest.mark.parametrize(
    ("answered", "unanswered", "call_count", "capture"),
    [
        (2, 0, 2, AttemptCapture.CLOSED),
        (1, 1, None, AttemptCapture.LOWER_BOUND),
        (0, 0, 0, AttemptCapture.CLOSED),
    ],
    ids=["all-answered", "one-unanswered", "no-calls"],
)
def test_t3_recovery_closes_complete_only_when_every_call_has_its_result(
    checked: Conn, answered: int, unanswered: int, call_count: int | None, capture: AttemptCapture
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=answered, unanswered=unanswered)
    revocation = sweep(checked)
    assert revocation is not None
    expected = by_recovery(attempt.id, call_count=call_count)
    assert revocation.usage_closure is not None
    assert revocation.usage_closure.created
    assert revocation.usage_closure.record.closure == expected
    assert stored_closure(checked, attempt.id) == expected
    assert capture_of(usage_of(checked, attempt.id)) is capture


@pytest.mark.parametrize("tier", [CaptureTier.T1, CaptureTier.T2])
def test_recovery_below_t3_always_closes_incomplete(checked: Conn, tier: CaptureTier) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=1)
    revocation = sweep(checked, {KIND: tier, OTHER_KIND: CaptureTier.T3})
    assert revocation is not None
    assert revocation.usage_closure is not None
    assert revocation.usage_closure.record.closure == by_recovery(attempt.id, tier)
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.LOWER_BOUND


@pytest.mark.parametrize(
    "reason",
    [
        RevocationReason.LEASE_EXPIRED,
        RevocationReason.DEADLINE_EXCEEDED,
        RevocationReason.CANCELLATION_GRACE_EXPIRED,
    ],
)
def test_every_revocation_reason_closes_the_capture(
    checked: Conn, reason: RevocationReason
) -> None:
    task = new_task(checked)
    policy = {
        RevocationReason.LEASE_EXPIRED: EXPIRING,
        RevocationReason.DEADLINE_EXCEEDED: DEADLINE_EXPIRING,
        RevocationReason.CANCELLATION_GRACE_EXPIRED: POLICY,
    }[reason]
    attempt = start_attempt(checked, task.id, "w", policy=policy)
    calls(checked, attempt, answered=1)
    if reason is RevocationReason.CANCELLATION_GRACE_EXPIRED:
        request_cancellation(checked, task.id, grace=BRIEF_GRACE)
        shortened = get_attempt(checked, attempt.id)
        assert shortened is not None
        until_expired(checked, shortened)
    else:
        until_expired(checked, attempt)

    revocation = sweep(checked)
    assert revocation is not None
    assert revocation.attempt.revoked_reason is reason
    if reason is RevocationReason.CANCELLATION_GRACE_EXPIRED:
        assert revocation.task.state is TaskState.CANCELLED
    assert revocation.usage_closure is not None
    assert revocation.usage_closure.record.closure == by_recovery(attempt.id, call_count=1)


def test_recovery_keeps_an_existing_closure_and_still_revokes(checked: Conn) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=0, unanswered=1)
    standalone = record_usage_closure(checked, incomplete(attempt.id)).record
    revocation = sweep(checked)
    assert revocation is not None
    assert revocation.attempt.state is AttemptState.REVOKED
    assert revocation.usage_closure == Recorded(standalone, created=False)
    assert count(checked, CLOSURES) == 1


@pytest.mark.parametrize("ending", ["commit", "rollback"])
def test_recovery_behind_an_uncommitted_closure_waits_and_never_conflicts(
    checked: Conn, worker: Callable[[], Conn], ending: str
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=0, unanswered=1)
    holder, recoverer = worker(), worker()
    holder.execute("BEGIN")
    assert insert_closure(holder, attempt.id, "incomplete", None, "worker", "t1") == 1
    with ThreadPoolExecutor(1) as pool:
        recovering = pool.submit(sweep, recoverer)
        wait_for_lock(checked, recoverer, "transactionid")  # its insert waits for the holder
        holder.execute(ending.upper())
        revocation = recovering.result(timeout=20)

    assert revocation is not None
    assert revocation.attempt.state is AttemptState.REVOKED
    assert revocation.usage_closure is not None
    stored = stored_closure(checked, attempt.id)
    assert revocation.usage_closure.record.closure == stored
    if ending == "commit":  # the standalone closure stands, unchanged
        assert not revocation.usage_closure.created
        assert stored == incomplete(attempt.id)
    else:
        assert revocation.usage_closure.created
        assert stored == by_recovery(attempt.id)


def test_an_invalid_tier_map_is_refused_before_anything_is_revoked(checked: Conn) -> None:
    expired(checked)
    engine = snapshot(checked)
    with pytest.raises(ValueError, match="at least one"):
        sweep(checked, {})
    # Inside a caller's transaction the check still comes first, before the guard.
    with checked.transaction(), pytest.raises(TypeError, match="CaptureTier"):
        sweep(checked, cast(Any, {KIND: "t3"}))
    assert snapshot(checked) == engine


# --- 6. late writes ---------------------------------------------------------------------


def test_facts_after_a_worker_closure_are_late_and_leave_it_a_lower_bound(checked: Conn) -> None:
    _, attempt = running(checked)
    calls(checked, attempt, answered=1)
    completion = finish(checked, attempt, SUCCESS, a_closure(attempt.id, call_count=1))
    assert completion.usage_closure is not None
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.CLOSED
    closures = rows(checked, CLOSURES)

    intent = record_call_intent(checked, an_intent(attempt.id, 1))
    late = record_late_usage(checked, an_answer(attempt.id, 1))
    assert intent.record.late
    assert intent.record.attempt_state_at_record is AttemptState.SUCCEEDED
    assert late.after_closure
    assert late.result.record.late
    assert late.closure == completion.usage_closure.record
    assert rows(checked, CLOSURES) == closures
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.LOWER_BOUND


def test_facts_after_a_recovery_closure_are_late_and_leave_it_a_lower_bound(
    checked: Conn,
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=0, unanswered=1)
    revocation = sweep(checked)
    assert revocation is not None
    assert revocation.usage_closure is not None
    closures = rows(checked, CLOSURES)

    intent = record_call_intent(checked, an_intent(attempt.id, 1))
    late = record_late_usage(checked, an_answer(attempt.id, 0))  # the unresolved call
    assert intent.record.attempt_state_at_record is AttemptState.REVOKED
    assert late.after_closure
    assert late.result.created
    assert late.result.record.attempt_state_at_record is AttemptState.REVOKED
    assert late.closure == revocation.usage_closure.record
    assert late.closure.closure.capture is CaptureState.INCOMPLETE
    assert rows(checked, CLOSURES) == closures
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.LOWER_BOUND


def test_a_result_recorded_while_running_is_not_after_a_closure(checked: Conn) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    first = record_late_usage(checked, an_answer(attempt.id))
    assert first.result.created
    assert not first.result.record.late
    assert (first.after_closure, first.closure) == (False, None)
    again = record_late_usage(checked, an_answer(attempt.id))
    assert not again.result.created
    assert again.result.record == first.result.record
    with pytest.raises(UsageReplayConflictError):
        record_late_usage(checked, an_answer(attempt.id, latency_ms=41))
    with pytest.raises(CallIntentNotFoundError):
        record_late_usage(checked, an_answer(attempt.id, 5))
    assert count(checked, "provider_call_results") == 1


def test_an_intent_that_waited_behind_a_completion_records_the_committed_state(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    # Fails without FOR KEY SHARE OF a: the intent would read 'running' from its
    # snapshot and only wait at the foreign key check, after choosing its flag.
    _, attempt = running(checked)
    release = gate("UPDATE", "tasks")
    completer, writer = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        completing = pool.submit(finish, completer, attempt, SUCCESS, a_closure(attempt.id))
        wait_for_lock(checked, completer, "advisory")  # attempt locked and finished, task not yet
        writing = pool.submit(record_call_intent, writer, an_intent(attempt.id))
        wait_for_lock(checked, writer, "transactionid")  # waits for the attempt row
        release()
        completion = completing.result(timeout=20)
        recorded = writing.result(timeout=20)

    assert recorded.record.attempt_state_at_record is AttemptState.SUCCEEDED
    assert recorded.record.late
    assert completion.usage_closure is not None
    assert completion.usage_closure.record.closure.call_count == 0
    assert capture_of(usage_of(checked, attempt.id)) is AttemptCapture.LOWER_BOUND


# --- 7. duplicates and concurrency ------------------------------------------------------


def test_concurrent_completions_with_one_closure_record_exactly_one(
    schema_url: str, checked: Conn
) -> None:
    _, attempt = running(checked)
    calls(checked, attempt, answered=1)
    closure = a_closure(attempt.id, call_count=1)
    outcomes = race(
        schema_url, [partial(finish, attempt=attempt, outcome=SUCCESS, closure=closure)] * 8
    )
    winners = [o for o in outcomes if isinstance(o, AttemptCompletion)]
    losers = [o for o in outcomes if not isinstance(o, AttemptCompletion)]
    assert len(winners) == 1, outcomes
    assert all(type(o) is AttemptFinishedError for o in losers), losers
    assert count(checked, CLOSURES) == 1
    assert stored_closure(checked, attempt.id) == closure


def test_concurrent_late_results_store_one_row(schema_url: str, checked: Conn) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    finish(checked, attempt, SUCCESS, incomplete(attempt.id))
    outcomes = race(schema_url, [partial(record_late_usage, result=an_answer(attempt.id))] * 8)
    assert all(isinstance(o, LateUsage) for o in outcomes), outcomes
    late = cast(list[LateUsage], outcomes)
    assert sum(o.result.created for o in late) == 1
    assert len({o.result.record for o in late}) == 1
    assert all(o.after_closure and o.result.record.late for o in late)
    assert count(checked, "provider_call_results") == 1


@pytest.mark.parametrize(
    ("tier", "ending"),
    [("t1", "commit"), ("t2", "commit"), ("t2", "rollback")],
    ids=["same-committed", "different-committed", "different-rolled-back"],
)
def test_a_completion_behind_an_uncommitted_closure(
    checked: Conn, worker: Callable[[], Conn], tier: str, ending: str
) -> None:
    _, attempt = running(checked)
    closure = incomplete(attempt.id)  # by the worker at t1
    other = incomplete(attempt.id, capture_tier=CaptureTier.T2)
    holder, completer = worker(), worker()
    holder.execute("BEGIN")
    assert insert_closure(holder, attempt.id, "incomplete", None, "worker", tier) == 1
    completion: AttemptCompletion | None = None
    with ThreadPoolExecutor(1) as pool:
        completing = pool.submit(finish, completer, attempt, SUCCESS, closure)
        wait_for_lock(checked, completer, "transactionid")  # its insert waits for the holder
        holder.execute(ending.upper())
        if tier == "t2" and ending == "commit":
            with pytest.raises(UsageReplayConflictError):
                completing.result(timeout=20)
        else:
            completion = completing.result(timeout=20)

    attempt_state = cast(Attempt, get_attempt(checked, attempt.id)).state
    if completion is None:  # rolled back: the attempt still runs; the other closure stands
        assert attempt_state is AttemptState.RUNNING
        assert stored_closure(checked, attempt.id) == other
        return
    assert attempt_state is AttemptState.SUCCEEDED
    assert completion.usage_closure is not None
    assert completion.usage_closure.created is (ending == "rollback")
    assert stored_closure(checked, attempt.id) == closure


@pytest.mark.parametrize("first", ["recover", "complete"])
def test_completion_and_recovery_with_closures_in_either_order(
    checked: Conn, gate: Gate, worker: Callable[[], Conn], first: str
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=1)
    closure = a_closure(attempt.id, call_count=1)
    release = gate("UPDATE", "tasks")
    recoverer, completer = worker(), worker()
    with ThreadPoolExecutor(2) as pool:
        if first == "recover":
            recovering = pool.submit(sweep, recoverer)
            wait_for_lock(checked, recoverer, "advisory")  # attempt revoked, task not yet
            completing = pool.submit(finish, completer, attempt, SUCCESS, closure)
            wait_for_lock(checked, completer, "transactionid")
            release()
            assert recovering.result(timeout=20) is not None
            with pytest.raises(AttemptRevokedError):
                completing.result(timeout=20)
            winner = ClosedBy.RECOVERY
        else:
            completing = pool.submit(finish, completer, attempt, SUCCESS, closure)
            wait_for_lock(checked, completer, "advisory")  # attempt finished, task not yet
            assert sweep(no_wait(recoverer)) is None  # skips the locked task
            release()
            assert completing.result(timeout=20).usage_closure is not None
            assert sweep(recoverer) is None
            winner = ClosedBy.WORKER
    assert count(checked, CLOSURES) == 1
    stored = stored_closure(checked, attempt.id)
    assert stored is not None
    assert stored.closed_by is winner


# --- 8. retry attribution ---------------------------------------------------------------


def test_each_attempt_keeps_its_own_calls_across_a_retry(checked: Conn) -> None:
    task = new_task(checked)
    first = start_attempt(checked, task.id, "w", policy=POLICY)
    calls(checked, first, answered=2)
    finish(checked, first, TRANSIENT, a_closure(first.id, call_count=2))
    second = start_attempt(checked, task.id, "w", policy=POLICY)
    calls(checked, second, answered=1)
    finish(checked, second, SUCCESS, a_closure(second.id, call_count=1))

    one, two = usage_of(checked, first.id), usage_of(checked, second.id)
    assert [c.call.intent.call_seq for c in one.calls] == [0, 1]
    assert [c.call.intent.call_seq for c in two.calls] == [0]  # call_seq restarts
    attribution = {(c.call.job_id, c.call.task_id) for c in (*one.calls, *two.calls)}
    assert attribution == {(task.job_id, task.id)}
    assert capture_of(one) is capture_of(two) is AttemptCapture.CLOSED

    with pytest.raises(StaleAttemptError):
        finish(checked, first, SUCCESS, incomplete(first.id))
    record_call_intent(checked, an_intent(first.id, 2))
    late = record_late_usage(checked, an_answer(first.id, 2))
    assert late.after_closure
    assert late.result.record.attempt_state_at_record is AttemptState.FAILED
    assert [c.call.intent.call_seq for c in usage_of(checked, first.id).calls] == [0, 1, 2]
    after = usage_of(checked, second.id)
    assert (after.calls, after.closure) == (two.calls, two.closure)
    assert capture_of(usage_of(checked, first.id)) is AttemptCapture.LOWER_BOUND


# --- 9. bounded locks -------------------------------------------------------------------


def test_a_gated_completion_bounds_heartbeats_and_blocks_neither_results_nor_recovery(
    checked: Conn, gate: Gate, worker: Callable[[], Conn]
) -> None:
    _, attempt = expired(checked)
    calls(checked, attempt, answered=1, unanswered=1)
    release = gate("INSERT", CLOSURES)
    completer = worker()
    beater, writer, recoverer = no_wait(worker()), no_wait(worker()), no_wait(worker())
    with ThreadPoolExecutor(1) as pool:
        completing = pool.submit(finish, completer, attempt, SUCCESS, incomplete(attempt.id))
        wait_for_lock(checked, completer, "advisory")  # attempt and task written, closure not yet
        with pytest.raises(errors.LockNotAvailable) as info:
            renew(beater, attempt)
        assert info.value.sqlstate == "55P03"
        assert record_call_result(writer, an_answer(attempt.id, 1)).created  # no wait
        assert sweep(recoverer) is None  # skips the locked task
        release()
        completion = completing.result(timeout=20)
    assert completion.usage_closure is not None
    assert completion.usage_closure.created
    assert get_attempt(checked, attempt.id) == completion.attempt  # the heartbeat changed nothing


def test_heartbeats_completion_recovery_writers_and_cancellation_never_deadlock(
    schema_url: str, checked: Conn
) -> None:
    # Unforced races on one task with seeded start jitter. Whatever the interleaving,
    # nothing deadlocks (a DeadlockDetected would escape race()), exactly one of
    # completion and recovery ends the attempt, the winner's closure is the one
    # stored, and a complete closure counts exactly the intents recorded while the
    # attempt ran. No standalone closure takes part, so that count is exact (it is
    # not for a closure written by record_usage_closure on a running attempt).
    for n in range(40):
        task = new_task(checked, max_attempts=2)
        ends = n % 4 != 0
        attempt = start_attempt(checked, task.id, "w", policy=EXPIRING if ends else POLICY)
        record_call_intent(checked, an_intent(attempt.id, 0))
        if n % 2:
            record_call_result(checked, an_answer(attempt.id, 0))
        if ends:
            until_expired(checked, attempt)
        participants: dict[str, Callable[[Conn], Any]] = {
            "renew": partial(renew, attempt=attempt),
            "complete": partial(
                finish,
                attempt=attempt,
                outcome=SUCCESS if n % 3 else TRANSIENT,
                closure=incomplete(attempt.id),
            ),
            "revoke": sweep,
            "write": partial(write_call, attempt_id=attempt.id, seq=1),
            "cancel": partial(request_cancellation, task_id=task.id, grace=GRACE),
        }
        if n % 4 == 2:
            del participants["cancel"]
        elif n % 4 == 3:
            del participants["renew"]
        results = dict(
            zip(
                participants,
                race(
                    schema_url,
                    [
                        jittered(call, seed=n * 10 + i)
                        for i, call in enumerate(participants.values())
                    ],
                ),
                strict=True,
            )
        )
        completed, revoked = results["complete"], results["revoke"]
        assert isinstance(completed, AttemptCompletion) != isinstance(revoked, AttemptRevocation)
        if isinstance(revoked, AttemptRevocation):
            assert isinstance(completed, AttemptRevokedError)

        usage = usage_of(checked, attempt.id)
        assert usage.attempt_state is not AttemptState.RUNNING
        assert usage.closure is not None  # a lifecycle closure exactly when terminal
        stored = usage.closure.closure
        won = isinstance(completed, AttemptCompletion)
        assert stored.closed_by is (ClosedBy.WORKER if won else ClosedBy.RECOVERY)
        if stored.capture is CaptureState.COMPLETE:
            assert stored.call_count == len([c for c in usage.calls if not c.call.late])
        assert_invariants(checked)
