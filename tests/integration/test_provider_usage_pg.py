"""The provider usage ledger against real PostgreSQL (migration 0005).

Writes are idempotent per key and keep amounts exactly, scale included. They copy
the job and task of the attempt, and they are accepted, flagged late, after the
attempt stopped running, without changing it or its task. The ledger is
append-only and reaches the engine only through provider_calls -> attempts. A read
comes from one read-only snapshot. Migration 0005 is applied to a genuine 0004
schema: it fails at once, changing nothing, while a session writes attempts, and
does not wait for readers, row-lock holders or task locks.
"""

import threading
import time
import uuid
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import Any, cast

import psycopg
import pytest
from psycopg import errors, sql
from psycopg.rows import TupleRow

from orqestra.calls.provider import FieldKind
from orqestra.domain.jobs import Attempt, Task
from orqestra.domain.states import AttemptState
from orqestra.persistence.lifecycle import NotFoundError, get_attempt, start_attempt
from orqestra.persistence.migrate import (
    Migration,
    MigrationError,
    load_migrations,
    migrate,
    verify_current,
)
from orqestra.persistence.provider_usage import (
    CallFailure,
    CallIntent,
    CallIntentNotFoundError,
    CallOutcome,
    CallResult,
    CaptureNotCompleteError,
    CaptureState,
    CaptureTier,
    ClosedBy,
    Recorded,
    UsageClosure,
    UsageReplayConflictError,
    read_attempt_usage,
    record_call_intent,
    record_call_result,
    record_usage_closure,
)
from tests.integration.test_attempt_lifecycle import (
    EXPIRING,
    POLICY,
    SUCCESS,
    assert_invariants,
    complete,
    connect,
    new_task,
    race,
    revoke,
    snapshot,
    until_expired,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

LEDGER_TABLES = ("provider_calls", "provider_call_results", "attempt_usage_closures")
MICRO_CREDITS = "\N{MICRO SIGN}-credits"


@pytest.fixture
def checked(db: Conn) -> Iterator[Conn]:
    """The migrated test connection; the lifecycle invariants must hold when the test ends."""
    yield db
    assert_invariants(db)


# --- helpers ----------------------------------------------------------------------------


def running(db: Conn) -> tuple[Task, Attempt]:
    task = new_task(db)
    return task, start_attempt(db, task.id, "w", policy=POLICY)


def an_intent(attempt_id: uuid.UUID, call_seq: int = 0, **changes: Any) -> CallIntent:
    fields: dict[str, Any] = {
        "attempt_id": attempt_id,
        "call_seq": call_seq,
        "provider": "mock",
        "route_id": "route-a",
        "capability": "amount.parse",
        "prompt_version": "v1",
        "model": "model-1",
        "field_kind": FieldKind.TOTAL,
        "item_index": 0,
        "estimated_cost_units": 3,
    }
    fields.update(changes)
    return CallIntent(**fields)


def an_answer(attempt_id: uuid.UUID, call_seq: int = 0, **changes: Any) -> CallResult:
    fields: dict[str, Any] = {
        "attempt_id": attempt_id,
        "call_seq": call_seq,
        "outcome": CallOutcome.ANSWERED,
        "reported_amount": Decimal("0.25"),
        "reported_unit": "credits",
        "model_served": "model-1",
        "provider_generation_id": "gen-1",
        "input_tokens": 10,
        "output_tokens": 2,
        "latency_ms": 40,
    }
    fields.update(changes)
    return CallResult(**fields)


def a_closure(attempt_id: uuid.UUID, **changes: Any) -> UsageClosure:
    fields: dict[str, Any] = {
        "attempt_id": attempt_id,
        "capture": CaptureState.COMPLETE,
        "closed_by": ClosedBy.WORKER,
        "capture_tier": CaptureTier.T1,
        "call_count": 0,
    }
    fields.update(changes)
    return UsageClosure(**fields)


def incomplete(attempt_id: uuid.UUID, **changes: Any) -> UsageClosure:
    return a_closure(attempt_id, capture=CaptureState.INCOMPLETE, call_count=None, **changes)


def rows(db: Conn, table: str) -> list[tuple[Any, ...]]:
    return db.execute(
        sql.SQL("SELECT * FROM {} ORDER BY 1, 2").format(sql.Identifier(table))
    ).fetchall()


def count(db: Conn, table: str) -> int:
    row = db.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))).fetchone()
    assert row is not None
    return int(row[0])


def insert_closure(
    db: Conn,
    attempt_id: uuid.UUID,
    capture: str,
    call_count: int | None,
    closed_by: str,
    tier: str,
    *,
    on_conflict: bool = False,
) -> int:
    """A raw closure insert, past the module's checks; returns the number of rows inserted."""
    statement = (
        "INSERT INTO attempt_usage_closures (attempt_id, capture, call_count, closed_by, "
        "capture_tier) VALUES (%s, %s, %s, %s, %s)"
    )
    if on_conflict:
        statement += " ON CONFLICT DO NOTHING"
    return db.execute(statement, (attempt_id, capture, call_count, closed_by, tier)).rowcount


def insert_row(db: Conn, table: str, fields: dict[str, Any]) -> None:
    db.execute(
        sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(name) for name in fields),
            sql.SQL(", ").join([sql.Placeholder()] * len(fields)),
        ),
        tuple(fields.values()),
    )


# --- recording --------------------------------------------------------------------------


def test_an_intent_records_its_attempts_job_and_task(checked: Conn) -> None:
    task, attempt = running(checked)
    recorded = record_call_intent(checked, an_intent(attempt.id))
    assert recorded.created
    stored = recorded.record
    assert (stored.job_id, stored.task_id) == (task.job_id, task.id)
    assert stored.attempt_state_at_record is AttemptState.RUNNING
    assert not stored.late
    row = checked.execute(
        "SELECT job_id, task_id, attempt_state_at_record, recorded_at, field_kind "
        "FROM provider_calls WHERE attempt_id = %s AND call_seq = 0",
        (attempt.id,),
    ).fetchone()
    assert row == (task.job_id, task.id, "running", stored.recorded_at, "total")


def test_results_and_closures_are_recorded(checked: Conn) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id, 0))
    record_call_intent(checked, an_intent(attempt.id, 1, model=None, field_kind=None))
    answer = record_call_result(checked, an_answer(attempt.id, 0))
    failure = record_call_result(
        checked,
        CallResult(
            attempt_id=attempt.id,
            call_seq=1,
            outcome=CallOutcome.FAILED,
            failure_code=CallFailure.UNSUPPORTED_REQUEST,
        ),
    )
    closure = record_usage_closure(checked, a_closure(attempt.id, call_count=2))
    assert (answer.created, failure.created, closure.created) == (True, True, True)
    assert not answer.record.late
    assert count(checked, "provider_call_results") == 2


def test_an_exact_replay_is_acknowledged_without_a_second_row(checked: Conn) -> None:
    _, attempt = running(checked)

    def write_all() -> list[Recorded[Any]]:
        return [
            record_call_intent(checked, an_intent(attempt.id)),
            record_call_result(checked, an_answer(attempt.id)),
            record_usage_closure(checked, a_closure(attempt.id, call_count=1)),
        ]

    first = write_all()
    before = {table: rows(checked, table) for table in LEDGER_TABLES}
    again = write_all()
    assert [r.created for r in first] == [True, True, True]
    assert [r.created for r in again] == [False, False, False]
    assert [r.record for r in again] == [r.record for r in first]
    assert {table: rows(checked, table) for table in LEDGER_TABLES} == before


@pytest.mark.parametrize(
    "change",
    [
        {"capability": "amount.other"},
        {"model": None},
        {"prompt_version": "v2"},
        {"field_kind": FieldKind.QUANTITY},
        {"item_index": 1},
        {"estimated_cost_units": 4},
        {"provider": "other"},
    ],
)
def test_an_intent_with_different_content_is_a_conflict(
    checked: Conn, change: dict[str, Any]
) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    before = rows(checked, "provider_calls")
    with pytest.raises(UsageReplayConflictError, match=f"attempt {attempt.id} call 0") as info:
        record_call_intent(checked, an_intent(attempt.id, **change))
    assert "amount" not in str(info.value)  # ids only, never content
    assert rows(checked, "provider_calls") == before


@pytest.mark.parametrize(
    "change",
    [
        {"outcome": CallOutcome.FAILED, "failure_code": CallFailure.TIMEOUT},
        {"reported_amount": Decimal("0.26")},
        {"reported_unit": "tokens"},
        {"reported_amount": None, "reported_unit": None},
        {"model_served": "model-2"},
        {"provider_generation_id": None},
        {"input_tokens": None},
        {"latency_ms": 41},
    ],
)
def test_a_result_with_different_content_is_a_conflict(
    checked: Conn, change: dict[str, Any]
) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    record_call_result(checked, an_answer(attempt.id))
    before = rows(checked, "provider_call_results")
    with pytest.raises(UsageReplayConflictError, match=f"attempt {attempt.id} call 0"):
        record_call_result(checked, an_answer(attempt.id, **change))
    assert rows(checked, "provider_call_results") == before


@pytest.mark.parametrize(
    "change",
    [
        {"capture_tier": CaptureTier.T2},
        {"closed_by": ClosedBy.RECOVERY, "capture_tier": CaptureTier.T3},
        {"capture": CaptureState.INCOMPLETE, "call_count": None},
    ],
)
def test_a_closure_with_different_content_is_a_conflict(
    checked: Conn, change: dict[str, Any]
) -> None:
    _, attempt = running(checked)
    record_usage_closure(checked, a_closure(attempt.id))
    before = rows(checked, "attempt_usage_closures")
    with pytest.raises(UsageReplayConflictError, match=f"attempt {attempt.id}"):
        record_usage_closure(checked, a_closure(attempt.id, **change))
    assert rows(checked, "attempt_usage_closures") == before


@pytest.mark.parametrize("table", LEDGER_TABLES)
def test_concurrent_writers_of_one_key_store_one_row(
    schema_url: str, checked: Conn, table: str
) -> None:
    _, attempt = running(checked)
    if table != "provider_calls":
        record_call_intent(checked, an_intent(attempt.id))
    if table == "attempt_usage_closures":
        record_call_result(checked, an_answer(attempt.id))
    writes: dict[str, Callable[[Conn], Any]] = {
        "provider_calls": lambda conn: record_call_intent(conn, an_intent(attempt.id)),
        "provider_call_results": lambda conn: record_call_result(conn, an_answer(attempt.id)),
        "attempt_usage_closures": lambda conn: record_usage_closure(
            conn, a_closure(attempt.id, call_count=1)
        ),
    }
    outcomes = race(schema_url, [writes[table]] * 8)
    assert all(isinstance(outcome, Recorded) for outcome in outcomes), outcomes
    assert sum(outcome.created for outcome in outcomes) == 1
    assert len({outcome.record for outcome in outcomes}) == 1
    assert count(checked, table) == 1


# --- amounts ----------------------------------------------------------------------------


def test_an_amount_is_stored_exactly_and_its_scale_is_in_its_identity(checked: Conn) -> None:
    _, attempt = running(checked)
    big = Decimal("99999999999999999999.999999999999999999")
    for seq in (0, 1, 2):
        record_call_intent(checked, an_intent(attempt.id, seq))
    record_call_result(checked, an_answer(attempt.id, 0, reported_amount=Decimal("1.50")))
    record_call_result(checked, an_answer(attempt.id, 1, reported_amount=Decimal("100")))
    record_call_result(checked, an_answer(attempt.id, 2, reported_amount=big))
    stored = checked.execute(
        "SELECT call_seq, reported_amount::text, scale(reported_amount) "
        "FROM provider_call_results WHERE attempt_id = %s ORDER BY call_seq",
        (attempt.id,),
    ).fetchall()
    assert stored == [(0, "1.50", 2), (1, "100", 0), (2, str(big), 18)]

    with pytest.raises(UsageReplayConflictError):
        record_call_result(checked, an_answer(attempt.id, 0, reported_amount=Decimal("1.5")))
    assert not record_call_result(
        checked, an_answer(attempt.id, 0, reported_amount=Decimal("1.50"))
    ).created
    assert not record_call_result(
        checked, an_answer(attempt.id, 1, reported_amount=Decimal("1E+2"))
    ).created
    with pytest.raises(UsageReplayConflictError):
        record_call_result(checked, an_answer(attempt.id, 1, reported_amount=Decimal("100.0")))


@pytest.mark.parametrize(
    "amount", ["NaN", "Infinity", "-Infinity", "-1", "1e20", "0.0000000000000000001"]
)
def test_the_database_refuses_an_amount_that_is_not_finite_unsigned_and_bounded(
    checked: Conn, amount: str
) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    with pytest.raises(errors.CheckViolation) as info:
        checked.execute(
            "INSERT INTO provider_call_results (attempt_id, call_seq, outcome, reported_amount, "
            "reported_unit, attempt_state_at_record) "
            "VALUES (%s, 0, 'answered', %s::numeric, 'credits', 'running')",
            (attempt.id, amount),
        )
    assert info.value.diag.constraint_name == "provider_call_results_reported_amount_valid"


@pytest.mark.parametrize(
    ("column", "value", "constraint"),
    [
        ("call_seq", -1, "provider_calls_call_seq_range"),
        ("provider", "x" * 65, "provider_calls_provider_valid"),
        ("route_id", "a b", "provider_calls_route_id_valid"),
        ("prompt_version", "v\n", "provider_calls_prompt_version_valid"),
        ("capability", "   ", "provider_calls_capability_valid"),
        ("capability", "x" * 257, "provider_calls_capability_valid"),
        ("model", "", "provider_calls_model_valid"),
        ("field_kind", "price", "provider_calls_field_kind_valid"),
        ("item_index", -1, "provider_calls_item_index_range"),
        ("estimated_cost_units", -1, "provider_calls_estimated_cost_units_range"),
        ("attempt_state_at_record", "paused", "provider_calls_attempt_state_valid"),
    ],
)
def test_the_database_backs_up_the_intent_rules(
    checked: Conn, column: str, value: object, constraint: str
) -> None:
    task, attempt = running(checked)
    fields: dict[str, Any] = {
        "attempt_id": attempt.id,
        "call_seq": 0,
        "job_id": task.job_id,
        "task_id": task.id,
        "provider": "mock",
        "route_id": "r",
        "capability": "c",
        "attempt_state_at_record": "running",
    }
    fields[column] = value
    with pytest.raises(errors.CheckViolation) as info:
        insert_row(checked, "provider_calls", fields)
    assert info.value.diag.constraint_name == constraint


@pytest.mark.parametrize(
    ("changes", "constraint"),
    [
        ({"outcome": "unknown"}, "provider_call_results_outcome_valid"),
        ({"failure_code": "timeout"}, "provider_call_results_failure_code_iff_failed"),
        ({"outcome": "failed"}, "provider_call_results_failure_code_iff_failed"),
        (
            {"outcome": "failed", "failure_code": "oops"},
            "provider_call_results_failure_code_valid",
        ),
        ({"reported_unit": "credits"}, "provider_call_results_amount_iff_unit"),
        ({"reported_amount": Decimal(1)}, "provider_call_results_amount_iff_unit"),
        (
            {"reported_amount": Decimal(1), "reported_unit": "  "},
            "provider_call_results_reported_unit_valid",
        ),
        ({"model_served": ""}, "provider_call_results_model_served_valid"),
        ({"provider_generation_id": "x" * 129}, "provider_call_results_generation_id_valid"),
        ({"input_tokens": -1}, "provider_call_results_input_tokens_range"),
        ({"output_tokens": -1}, "provider_call_results_output_tokens_range"),
        ({"latency_ms": -1}, "provider_call_results_latency_ms_range"),
        ({"attempt_state_at_record": "paused"}, "provider_call_results_attempt_state_valid"),
    ],
)
def test_the_database_backs_up_the_result_rules(
    checked: Conn, changes: dict[str, Any], constraint: str
) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    fields: dict[str, Any] = {
        "attempt_id": attempt.id,
        "call_seq": 0,
        "outcome": "answered",
        "attempt_state_at_record": "running",
        **changes,
    }
    with pytest.raises(errors.CheckViolation) as info:
        insert_row(checked, "provider_call_results", fields)
    assert info.value.diag.constraint_name == constraint


# --- references -------------------------------------------------------------------------


def test_records_must_name_an_existing_attempt_and_call(checked: Conn) -> None:
    task, attempt = running(checked)
    missing = uuid.uuid4()
    with pytest.raises(NotFoundError) as info:
        record_call_intent(checked, an_intent(missing))
    assert (info.value.kind, info.value.record_id) == ("attempt", missing)
    with pytest.raises(CallIntentNotFoundError, match=f"attempt {attempt.id} call 5"):
        record_call_result(checked, an_answer(attempt.id, 5))
    with pytest.raises(NotFoundError):
        record_usage_closure(checked, a_closure(missing))
    with pytest.raises(errors.ForeignKeyViolation):
        insert_row(
            checked,
            "provider_call_results",
            {
                "attempt_id": attempt.id,
                "call_seq": 5,
                "outcome": "answered",
                "attempt_state_at_record": "running",
            },
        )
    with pytest.raises(errors.ForeignKeyViolation):
        insert_row(
            checked,
            "provider_calls",
            {
                "attempt_id": missing,
                "call_seq": 0,
                "job_id": task.job_id,
                "task_id": task.id,
                "provider": "mock",
                "route_id": "r",
                "capability": "c",
                "attempt_state_at_record": "running",
            },
        )
    with pytest.raises(errors.CheckViolation) as raw:
        insert_closure(checked, missing, "incomplete", None, "worker", "t1")
    assert raw.value.diag.constraint_name == "attempt_usage_closures_attempt_exists"
    assert [count(checked, table) for table in LEDGER_TABLES] == [0, 0, 0]


def test_provider_calls_is_the_only_reference_to_an_engine_table(checked: Conn) -> None:
    references = checked.execute(
        "SELECT conrelid::regclass::text, conname::text, confrelid::regclass::text "
        "FROM pg_constraint WHERE contype = 'f' "
        "AND connamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())"
    ).fetchall()
    assert set(references) == {
        ("tasks", "tasks_job_fk", "jobs"),
        ("attempts", "attempts_task_fk", "tasks"),
        ("provider_calls", "provider_calls_attempt_fk", "attempts"),
        ("provider_call_results", "provider_call_results_call_fk", "provider_calls"),
    }


# --- lateness ---------------------------------------------------------------------------


def test_writes_after_the_attempt_was_revoked_are_kept_and_flagged_late(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=EXPIRING)
    assert not record_call_intent(checked, an_intent(attempt.id, 0)).record.late
    until_expired(checked, attempt)
    revoke(checked)
    engine = snapshot(checked)

    late_intent = record_call_intent(checked, an_intent(attempt.id, 1))
    late_result = record_call_result(checked, an_answer(attempt.id, 0))
    closure = incomplete(attempt.id, closed_by=ClosedBy.RECOVERY)
    late_closure = record_usage_closure(checked, closure)
    assert (late_intent.created, late_result.created, late_closure.created) == (True, True, True)
    assert late_intent.record.late
    assert late_result.record.late
    assert late_intent.record.attempt_state_at_record is AttemptState.REVOKED
    assert late_result.record.attempt_state_at_record is AttemptState.REVOKED
    assert (late_intent.record.job_id, late_intent.record.task_id) == (task.job_id, task.id)

    assert not record_call_intent(checked, an_intent(attempt.id, 1)).created
    assert not record_call_result(checked, an_answer(attempt.id, 0)).created
    assert not record_usage_closure(checked, closure).created
    usage = read_attempt_usage(checked, attempt.id)
    assert usage is not None
    assert [call.call.late for call in usage.calls] == [False, True]
    assert snapshot(checked) == engine  # no task or attempt changed
    revoked = get_attempt(checked, attempt.id)
    assert revoked is not None
    assert revoked.state is AttemptState.REVOKED


# --- append-only ------------------------------------------------------------------------


@pytest.mark.parametrize("table", LEDGER_TABLES)
def test_the_ledger_is_append_only(checked: Conn, table: str) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    record_call_result(checked, an_answer(attempt.id))
    record_usage_closure(checked, a_closure(attempt.id, call_count=1))
    before = rows(checked, table)
    name = sql.Identifier(table)
    for statement in (
        sql.SQL("UPDATE {} SET recorded_at = recorded_at").format(name),
        sql.SQL("DELETE FROM {}").format(name),
        sql.SQL("TRUNCATE {} CASCADE").format(name),
    ):
        with pytest.raises(errors.CheckViolation) as info:
            checked.execute(statement)
        assert info.value.diag.constraint_name == f"{table}_immutable"
    assert rows(checked, table) == before


def test_an_attempt_with_recorded_calls_cannot_be_removed(checked: Conn) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    with pytest.raises(errors.ForeignKeyViolation):
        checked.execute("DELETE FROM attempts WHERE id = %s", (attempt.id,))
    with pytest.raises(errors.CheckViolation) as info:
        checked.execute("TRUNCATE attempts CASCADE")
    assert info.value.diag.constraint_name == "provider_calls_immutable"
    assert get_attempt(checked, attempt.id) is not None


# --- closures ---------------------------------------------------------------------------


@pytest.mark.parametrize("tier", ["t1", "t2"])
def test_the_database_refuses_a_complete_recovery_closure_below_t3(
    checked: Conn, tier: str
) -> None:
    _, attempt = running(checked)
    with pytest.raises(errors.CheckViolation) as info:
        insert_closure(checked, attempt.id, "complete", 0, "recovery", tier)
    assert info.value.diag.constraint_name == "attempt_usage_closures_recovery_tier"


def test_a_complete_closure_must_count_every_intent_with_its_result(checked: Conn) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id, 0))
    record_call_intent(checked, an_intent(attempt.id, 1))
    record_call_result(checked, an_answer(attempt.id, 0))
    for call_count in (2, 1, 0, 3):  # 2 is the right count, but call 1 has no result
        with pytest.raises(errors.CheckViolation) as info:
            insert_closure(checked, attempt.id, "complete", call_count, "worker", "t1")
        assert info.value.diag.constraint_name == "attempt_usage_closures_complete_capture"
    with pytest.raises(CaptureNotCompleteError) as refused:
        record_usage_closure(checked, a_closure(attempt.id, call_count=2))
    assert (refused.value.intents, refused.value.unanswered) == (2, 1)

    record_call_result(checked, an_answer(attempt.id, 1))
    with pytest.raises(CaptureNotCompleteError) as miscounted:
        record_usage_closure(checked, a_closure(attempt.id, call_count=3))
    assert (miscounted.value.intents, miscounted.value.unanswered) == (2, 0)
    assert count(checked, "attempt_usage_closures") == 0
    assert insert_closure(checked, attempt.id, "complete", 2, "recovery", "t3") == 1


def test_a_zero_call_closure_and_an_incomplete_one_are_accepted(checked: Conn) -> None:
    _, first = running(checked)
    _, second = running(checked)
    assert insert_closure(checked, first.id, "complete", 0, "worker", "t1") == 1
    record_call_intent(checked, an_intent(second.id))  # its outcome is unknown: no result
    closure = incomplete(second.id, closed_by=ClosedBy.RECOVERY)
    assert record_usage_closure(checked, closure).created


def test_a_closure_replayed_after_a_late_intent_is_still_a_replay(checked: Conn) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id, 0))
    record_call_result(checked, an_answer(attempt.id, 0))
    closure = a_closure(attempt.id, call_count=1)
    first = record_usage_closure(checked, closure)
    complete(checked, attempt, SUCCESS)
    late = record_call_intent(checked, an_intent(attempt.id, 1))
    assert late.created
    assert late.record.late
    assert late.record.attempt_state_at_record is AttemptState.SUCCEEDED

    again = record_usage_closure(checked, closure)
    assert not again.created
    assert again.record == first.record
    # The trigger leaves an existing closure to ON CONFLICT, though it no longer counts every call.
    skipped = insert_closure(checked, attempt.id, "complete", 1, "worker", "t1", on_conflict=True)
    assert skipped == 0
    with pytest.raises(errors.UniqueViolation):
        insert_closure(checked, attempt.id, "complete", 1, "worker", "t1")


# --- reads ------------------------------------------------------------------------------


def test_reading_a_missing_attempt_returns_none(checked: Conn) -> None:
    assert read_attempt_usage(checked, uuid.uuid4()) is None


def test_an_attempt_with_nothing_recorded_reads_as_empty(checked: Conn) -> None:
    task, attempt = running(checked)
    usage = read_attempt_usage(checked, attempt.id)
    assert usage is not None
    assert (usage.calls, usage.closure) == ((), None)
    assert (usage.attempt_id, usage.job_id, usage.task_id, usage.attempt_state) == (
        attempt.id,
        task.job_id,
        task.id,
        AttemptState.RUNNING,
    )


def test_a_read_returns_calls_in_order_with_results_exactly_as_stored(checked: Conn) -> None:
    _, attempt = running(checked)
    big = Decimal("99999999999999999999.999999999999999999")
    for seq in (2, 0, 1):
        record_call_intent(checked, an_intent(attempt.id, seq, item_index=seq))
    answer = record_call_result(
        checked,
        an_answer(attempt.id, 0, reported_amount=Decimal("1.50"), reported_unit=MICRO_CREDITS),
    ).record
    failure = record_call_result(
        checked,
        CallResult(
            attempt_id=attempt.id,
            call_seq=2,
            outcome=CallOutcome.FAILED,
            failure_code=CallFailure.RATE_LIMITED,
            reported_amount=big,
            reported_unit="credits",
        ),
    ).record
    closure = record_usage_closure(checked, incomplete(attempt.id)).record

    usage = read_attempt_usage(checked, attempt.id)
    assert usage is not None
    assert [call.call.intent for call in usage.calls] == [
        an_intent(attempt.id, seq, item_index=seq) for seq in (0, 1, 2)
    ]
    first, second, third = usage.calls
    assert first.result == answer
    assert first.result is not None
    assert str(first.result.result.reported_amount) == "1.50"
    assert first.result.result.reported_unit == MICRO_CREDITS
    assert second.result is None  # an intent whose outcome is unknown
    assert third.result == failure
    assert third.result is not None
    assert str(third.result.result.reported_amount) == str(big)
    assert usage.closure == closure


class RecordingCursor:
    """Passes statements to a real cursor and logs them; runs a hook after the first SELECT."""

    def __init__(
        self, cursor: psycopg.Cursor[Any], log: list[str], hook: Callable[[], None]
    ) -> None:
        self.cursor = cursor
        self.log = log
        self.hook: Callable[[], None] | None = hook

    def __enter__(self) -> "RecordingCursor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.cursor.close()

    def execute(self, query: str, params: Any = None) -> "RecordingCursor":
        self.cursor.execute(query, params)
        self.log.append(query.strip())
        if self.hook is not None and query.lstrip().startswith("SELECT"):
            hook, self.hook = self.hook, None
            hook()
        return self

    def fetchone(self) -> Any:
        return self.cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self.cursor.fetchall()


class RecordingConnection:
    """The parts of a connection ``read_attempt_usage`` uses, with recording cursors."""

    def __init__(self, conn: Conn, log: list[str], hook: Callable[[], None]) -> None:
        self.conn = conn
        self.log = log
        self.hook = hook

    @property
    def info(self) -> Any:
        return self.conn.info

    def transaction(self) -> Any:
        return self.conn.transaction()

    def cursor(self, *, row_factory: Any) -> RecordingCursor:
        return RecordingCursor(self.conn.cursor(row_factory=row_factory), self.log, self.hook)


def test_a_read_is_one_read_only_snapshot(schema_url: str, checked: Conn) -> None:
    _, attempt = running(checked)
    record_call_intent(checked, an_intent(attempt.id))
    log: list[str] = []

    def commit_a_result() -> None:  # from another session, after the snapshot was taken
        assert record_call_result(checked, an_answer(attempt.id)).created

    with psycopg.connect(schema_url, autocommit=True) as conn:
        usage = read_attempt_usage(
            cast(Any, RecordingConnection(conn, log, commit_a_result)), attempt.id
        )
    assert log[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert len(log) == 4
    assert all(statement.startswith("SELECT") for statement in log[1:])
    assert usage is not None
    (call,) = usage.calls
    assert call.result is None  # committed after the snapshot's first read
    assert count(checked, "provider_call_results") == 1
    later = read_attempt_usage(checked, attempt.id)
    assert later is not None
    assert later.calls[0].result is not None


def test_reads_and_writes_refuse_a_caller_transaction(schema_url: str, checked: Conn) -> None:
    _, attempt = running(checked)
    with psycopg.connect(schema_url) as conn:
        conn.execute("SELECT 1")  # the caller's transaction is now open
        with pytest.raises(RuntimeError, match="own transaction"):
            read_attempt_usage(conn, attempt.id)
        with pytest.raises(RuntimeError, match="own transaction"):
            record_call_intent(conn, an_intent(attempt.id))
        with pytest.raises(RuntimeError, match="own transaction"):
            record_usage_closure(conn, a_closure(attempt.id))
        conn.rollback()
    assert [count(checked, table) for table in LEDGER_TABLES] == [0, 0, 0]


# --- locks ------------------------------------------------------------------------------


def test_writes_and_reads_never_wait_for_a_lock_on_the_task(schema_url: str, checked: Conn) -> None:
    task, attempt = running(checked)
    with connect(schema_url) as holder, connect(schema_url) as writer:
        writer.execute("SET lock_timeout = '1s'")  # a wait fails the test instead
        holder.execute("BEGIN")
        holder.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task.id,))
        assert record_call_intent(writer, an_intent(attempt.id)).created
        assert record_call_result(writer, an_answer(attempt.id)).created
        assert record_usage_closure(writer, a_closure(attempt.id, call_count=1)).created
        assert read_attempt_usage(writer, attempt.id) is not None
        holder.execute("ROLLBACK")


def test_an_intent_waits_for_a_lifecycle_lock_on_its_own_attempt(
    schema_url: str, checked: Conn
) -> None:
    _, attempt = running(checked)
    with connect(schema_url) as holder, connect(schema_url) as writer:
        writer.execute("SET lock_timeout = '200ms'")
        holder.execute("BEGIN")
        holder.execute("SELECT id FROM attempts WHERE id = %s FOR UPDATE", (attempt.id,))
        with pytest.raises(errors.LockNotAvailable):
            record_call_intent(writer, an_intent(attempt.id))
        holder.execute("ROLLBACK")
        assert record_call_intent(writer, an_intent(attempt.id)).created


# --- migration 0005 ---------------------------------------------------------------------

MIGRATIONS = load_migrations()
UP_TO_0004 = list(MIGRATIONS[:4])
UP_TO_0005 = list(MIGRATIONS[:5])  # never a later migration: it has its own tests
USAGE = MIGRATIONS[4]


@pytest.fixture
def at_0004(schema_url: str) -> Iterator[Callable[[], Conn]]:
    """Bring ``schema_url`` to migration 0004; return a factory of connections to it."""
    migrate(schema_url, UP_TO_0004)
    opened: list[Conn] = []

    def open_connection() -> Conn:
        conn = connect(schema_url)
        opened.append(conn)
        return conn

    yield open_connection
    for conn in opened:
        conn.close()


def versions(conn: Conn) -> list[int]:
    return [int(v) for (v,) in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")]


def ledger_tables(conn: Conn) -> set[str]:
    found = conn.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname = current_schema() "
        "AND tablename = ANY(%s)",
        (list(LEDGER_TABLES),),
    ).fetchall()
    return {str(name) for (name,) in found}


def upgrade(url: str) -> tuple[Migration, ...] | Exception:
    """Run the upgrade to 0005; return what it applied or raised. A blocked upgrade fails."""
    outcome: list[tuple[Migration, ...] | Exception] = []

    def run() -> None:
        try:
            outcome.append(migrate(url, UP_TO_0005))
        except Exception as exc:  # returned to the test thread, which inspects it
            outcome.append(exc)

    upgrading = threading.Thread(target=run, daemon=True)
    upgrading.start()
    upgrading.join(timeout=10)
    if upgrading.is_alive():
        pytest.fail("the upgrade waited for a lock instead of deciding at once")
    (result,) = outcome
    return result


def test_upgrades_a_populated_0004_schema_keeping_every_row(
    schema_url: str, at_0004: Callable[[], Conn]
) -> None:
    conn = at_0004()
    task, attempt = running(conn)
    done_task = new_task(conn)
    complete(conn, start_attempt(conn, done_task.id, "w", policy=POLICY), SUCCESS)
    new_task(conn)  # queued
    before = snapshot(conn)
    assert ledger_tables(conn) == set()

    applied = upgrade(schema_url)
    assert isinstance(applied, tuple), applied
    assert [m.label for m in applied] == [USAGE.label]
    assert versions(conn) == [1, 2, 3, 4, 5]
    assert ledger_tables(conn) == set(LEDGER_TABLES)
    assert snapshot(conn) == before  # every job, task and attempt as it was
    recorded = record_call_intent(conn, an_intent(attempt.id))
    assert (recorded.record.job_id, recorded.record.task_id) == (task.job_id, task.id)
    assert_invariants(conn)


def test_fails_at_once_while_a_session_writes_attempts_and_changes_nothing(
    schema_url: str, at_0004: Callable[[], Conn]
) -> None:
    conn, writer = at_0004(), at_0004()
    _, attempt = running(conn)
    before = snapshot(conn)
    writer.execute("BEGIN")  # an open lifecycle write of attempts
    writer.execute("UPDATE attempts SET worker_id = worker_id WHERE id = %s", (attempt.id,))

    refused = upgrade(schema_url)
    assert isinstance(refused, MigrationError), refused
    assert refused.sqlstate == "55P03"
    assert "could not lock attempts" in str(refused)
    assert versions(conn) == [1, 2, 3, 4]  # rolled back: no history row
    assert ledger_tables(conn) == set()  # and no table
    writer.execute("ROLLBACK")
    assert snapshot(conn) == before

    retried = upgrade(schema_url)
    assert isinstance(retried, tuple), retried
    assert [m.label for m in retried] == [USAGE.label]


def test_does_not_wait_for_readers_attempt_row_locks_or_task_locks(
    schema_url: str, at_0004: Callable[[], Conn]
) -> None:
    conn = at_0004()
    task, attempt = running(conn)
    row_lock, reader, task_lock = at_0004(), at_0004(), at_0004()
    for holder in (row_lock, reader, task_lock):
        holder.execute("BEGIN")
    row_lock.execute("SELECT id FROM attempts WHERE id = %s FOR UPDATE", (attempt.id,))
    reader.execute("SELECT count(*) FROM attempts")
    task_lock.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task.id,))

    started = time.monotonic()
    applied = upgrade(schema_url)
    assert isinstance(applied, tuple), applied
    assert [m.label for m in applied] == [USAGE.label]
    assert time.monotonic() - started < 5
    for holder in (row_lock, reader, task_lock):
        holder.execute("ROLLBACK")
    assert versions(conn) == [1, 2, 3, 4, 5]


def test_a_repeated_upgrade_changes_nothing_and_the_history_verifies(
    schema_url: str, at_0004: Callable[[], Conn]
) -> None:
    conn = at_0004()
    applied = upgrade(schema_url)
    assert isinstance(applied, tuple), applied
    assert len(applied) == 1
    assert migrate(schema_url, UP_TO_0005) == ()
    assert versions(conn) == [1, 2, 3, 4, 5]
    verify_current(conn, UP_TO_0005)  # the exact, read-only history check a worker runs
