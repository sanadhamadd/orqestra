"""The worker's durable usage sink against PostgreSQL, run in this process (DEC-059).

A real ``Worker`` runs a ``metered`` handler that makes its calls through
``ProviderCalls`` and the sink's recorder. Its billable provider double never
answers before it has checked, on its own connection, whether its intent row has
committed, and records what it saw, so "no call without a row" is read from
PostgreSQL, not inferred. Database trouble is injected with test triggers or the
``PgProxy``. As in ``test_worker_runtime.py``, the watchdog only records and
``terminate`` raises; the real exits are in ``test_worker_process.py``.
"""

import functools
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from typing import Any, NoReturn

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.ai.provider import (
    FieldKind,
    ProviderRequest,
    ProviderResponse,
    ProviderUsage,
    make_request,
)
from orqestra.ai.recording import MAX_CALL_SEQ
from orqestra.domain.jobs import JsonValue, Task
from orqestra.persistence.lifecycle import request_cancellation, revoke_next_expired
from orqestra.persistence.provider_usage import (
    AttemptCapture,
    AttemptUsage,
    CaptureState,
    CaptureTier,
    ClosedBy,
    capture_of,
    read_attempt_usage,
)
from orqestra.worker.fence import ExitCode, Expiry, Guard, Watchdog
from orqestra.worker.handlers import HandlerCancelled, HandlerRegistry, TaskContext
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.usage import UsageSink
from orqestra.workloads.invoice.verify import ProviderCalls
from tests.integration.pg_proxy import PgProxy
from tests.integration.test_attempt_lifecycle import assert_invariants
from tests.integration.test_worker_runtime import (
    WAIT,
    Probe,
    Terminated,
    events,
    eventually,
    rows,
    submit,
    task_states,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

# Test values only (DEC-034): heartbeat 2 s + bound 0.5 s + margin 1 s < lease 6 s, and a
# usage write may start in the 1.5 s of each heartbeat interval outside the reserve.
SETTINGS = WorkerSettings(
    _env_file=None,
    lease=timedelta(seconds=6),
    execution_timeout=timedelta(minutes=5),
    heartbeat_interval=timedelta(seconds=2),
    db_operation_timeout=timedelta(milliseconds=500),
    lock_timeout=timedelta(milliseconds=200),
    fence_margin=timedelta(seconds=1),
    poll_interval=timedelta(milliseconds=100),
    recovery_interval=timedelta(milliseconds=300),
    recovery_batch_limit=5,
    shutdown_timeout=timedelta(seconds=5),
    retry_base_delay=timedelta(milliseconds=50),
    retry_max_delay=timedelta(milliseconds=200),
)
KIND = "metered"
ROUTE = "route-a"
PROVIDER = "fake-provider"
PROMPT = "test-v1"
INTENT = b"INSERT INTO provider_calls ("
RESULT = b"INSERT INTO provider_call_results ("
REQUEST: ProviderRequest = make_request(
    capability="interpret_amount",
    route_id=ROUTE,
    field_kind=FieldKind.TOTAL,
    raw="12",
    timeout=timedelta(seconds=5),
)
INTENT_COMMITTED = (
    "SELECT EXISTS (SELECT FROM provider_calls WHERE attempt_id = %s AND call_seq = %s)"
)


def new_sink() -> UsageSink:
    return UsageSink([KIND], {ROUTE: PROVIDER}, prompt_version=PROMPT)


@dataclass
class Metered:
    """A handler making ``payload["calls"]`` provider calls through the sink, and its provider.

    With ``release``, each call stays in flight until it is set; with ``stop``, the
    handler makes no further call once it is set. ``invocations`` holds, per call
    made, the attempt, its ``call_seq`` and whether its intent row had committed.
    """

    url: str
    sink: UsageSink = field(default_factory=new_sink)
    usage: ProviderUsage | None = field(default_factory=lambda: ProviderUsage("0.25", "tokens"))
    release: threading.Event | None = None
    stop: threading.Event | None = None
    invocations: list[tuple[uuid.UUID, int, bool]] = field(default_factory=list)
    in_flight: threading.Event = field(default_factory=threading.Event)

    def handler(self, ctx: TaskContext, payload: JsonValue) -> JsonValue:
        assert isinstance(payload, dict)
        count = payload["calls"]
        assert isinstance(count, int)
        calls = ProviderCalls(
            attempt_id=ctx.attempt_id, cap=MAX_CALL_SEQ, recorder=self.sink.recorder
        )
        with psycopg.connect(self.url, autocommit=True) as conn:
            provider = FakeProvider(self, conn, ctx.attempt_id, calls)
            for _ in range(count):
                if ctx.cancellation.is_set() or (self.stop is not None and self.stop.is_set()):
                    break
                calls.call(provider, REQUEST, item_index=calls.made, estimated_cost_units=3)
        if ctx.cancellation.is_set():
            raise HandlerCancelled
        return {"calls": calls.made}

    def made(self) -> int:
        return len(self.invocations)


class FakeProvider:
    """A billable provider double; it never reaches a real provider."""

    def __init__(
        self, metered: Metered, conn: Conn, attempt_id: uuid.UUID, calls: ProviderCalls
    ) -> None:
        self.metered, self.conn, self.attempt_id, self.calls = metered, conn, attempt_id, calls

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        seq = self.calls.made - 1  # ProviderCalls counts the call once its intent is recorded
        row = self.conn.execute(INTENT_COMMITTED, (self.attempt_id, seq)).fetchone()
        assert row is not None
        self.metered.invocations.append((self.attempt_id, seq, bool(row[0])))
        self.metered.in_flight.set()
        if self.metered.release is not None:
            assert self.metered.release.wait(60)
        return ProviderResponse(value="12", usage=self.metered.usage)


@contextmanager
def metered_worker(
    url: str,
    metered: Metered,
    settings: WorkerSettings = SETTINGS,
    *,
    expiries_seen: list[list[Expiry]] | None = None,
) -> Iterator[Probe]:
    """Run a sink-wired worker; ``expiries_seen`` gets the watchdog's expiries at each end."""
    expiries: list[Expiry] = []
    terminations: list[tuple[ExitCode, str]] = []

    def terminate(code: ExitCode, event: str, message: str, context: Any) -> NoReturn:
        if expiries_seen is not None:
            expiries_seen.append(list(expiries))
        terminations.append((code, event))
        raise Terminated(event)

    worker = Worker(
        registry=HandlerRegistry({KIND: metered.handler}),
        settings=settings,
        connect=functools.partial(connect_supervisor, url, settings),
        watchdog=Watchdog(terminate=expiries.append),
        terminate=terminate,
        usage=metered.sink,
    )
    failures: list[BaseException] = []

    def run() -> None:
        try:
            worker.run()
        except Terminated:
            pass
        except BaseException as exc:  # reported by the test
            failures.append(exc)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    probe = Probe(worker, expiries, terminations, thread, failures)
    try:
        yield probe
    finally:
        if metered.release is not None:
            metered.release.set()
        worker.request_stop()
        thread.join(WAIT)
        assert not thread.is_alive(), "the worker did not stop"
        assert failures == []


@pytest.fixture
def checked(db: Conn) -> Iterator[Conn]:
    yield db
    assert_invariants(db)


def attempt_ids(db: Conn, task: Task) -> list[uuid.UUID]:
    found = rows(db, "SELECT id FROM attempts WHERE task_id = %s ORDER BY attempt_number", task.id)
    return [row[0] for row in found]


def usage_of(db: Conn, task: Task, generation: int = 1) -> AttemptUsage:
    usage = read_attempt_usage(db, attempt_ids(db, task)[generation - 1])
    assert usage is not None
    return usage


def closure(usage: AttemptUsage) -> tuple[Any, ...] | None:
    if usage.closure is None:
        return None
    c = usage.closure.closure
    return (c.capture, c.call_count, c.closed_by, c.capture_tier)


def no_call_without_its_row(metered: Metered) -> bool:
    return all(committed for _, _, committed in metered.invocations)


def attempt_state(db: Conn, task: Task) -> tuple[Any, ...]:
    ((state, failure_kind),) = rows(
        db, "SELECT state, failure_kind FROM attempts WHERE task_id = %s", task.id
    )
    return state, failure_kind


def install(db: Conn, name: str, table: str, errcode: str, *, once: bool = False) -> None:
    """A test trigger that refuses inserts into ``table`` with ``errcode`` (once, or always)."""
    condition = f"nextval('{name}_seq') = 1" if once else "true"
    db.execute(f"CREATE SEQUENCE {name}_seq")
    db.execute(f"""
        CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF {condition} THEN
                RAISE EXCEPTION 'refused for the test' USING ERRCODE = '{errcode}';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER {name} BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {name}();
    """)


# --- the happy path ---------------------------------------------------------------------


def test_every_call_has_its_intent_and_result_and_the_capture_closes_complete(
    schema_url: str, checked: Conn
) -> None:
    metered = Metered(schema_url)
    (task,) = submit(checked, KIND, {"calls": 3})
    with metered_worker(schema_url, metered) as probe:
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    assert probe.terminations == []
    assert metered.made() == 3
    assert no_call_without_its_row(metered)
    usage = usage_of(checked, task)
    assert [call.call.intent.call_seq for call in usage.calls] == [0, 1, 2]
    for call in usage.calls:
        assert (call.call.intent.provider, call.call.intent.prompt_version) == (PROVIDER, PROMPT)
        assert not call.call.late
        assert call.result is not None
        assert call.result.result.reported_amount == Decimal("0.25")
        assert call.result.result.reported_unit == "tokens"
    assert closure(usage) == (CaptureState.COMPLETE, 3, ClosedBy.WORKER, CaptureTier.T3)
    assert capture_of(usage) is AttemptCapture.CLOSED


# --- cancellation (O1) ----------------------------------------------------------------------


def test_a_cancellation_while_an_intent_waits_withdraws_it_no_row_and_no_call(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    install(checked, "refuse_intent", "provider_calls", "40001")  # retried, never recorded
    metered = Metered(schema_url)
    (task,) = submit(checked, KIND, {"calls": 1})
    with metered_worker(schema_url, metered) as probe:
        eventually(lambda: "usage_write_retry" in events(caplog), "a retried intent")
        request_cancellation(checked, task.id, grace=timedelta(seconds=30))
        eventually(lambda: task_states(checked, [task]) == [("cancelled", 1)], "the cancellation")
    assert probe.terminations == []
    assert "usage_intent_withdrawn" in events(caplog)
    assert metered.invocations == []  # no call
    usage = usage_of(checked, task)
    assert usage.calls == ()  # no row
    assert closure(usage) == (CaptureState.COMPLETE, 0, ClosedBy.WORKER, CaptureTier.T3)
    checked.execute("DROP TRIGGER refuse_intent ON provider_calls")


def test_a_cancellation_confirmed_during_a_call_still_records_its_result(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    metered = Metered(schema_url, release=threading.Event())
    (task,) = submit(checked, KIND, {"calls": 5})
    with metered_worker(schema_url, metered) as probe:
        assert metered.in_flight.wait(WAIT)
        request_cancellation(checked, task.id, grace=timedelta(seconds=30))
        eventually(lambda: "cancellation_requested" in events(caplog), "the cancellation notice")
        assert metered.release is not None
        metered.release.set()  # the call in flight ends; its result is written
        eventually(lambda: task_states(checked, [task]) == [("cancelled", 1)], "the cancellation")
    assert probe.terminations == []
    assert metered.made() == 1
    assert no_call_without_its_row(metered)
    usage = usage_of(checked, task)
    assert [call.result is not None for call in usage.calls] == [True]
    assert closure(usage) == (CaptureState.COMPLETE, 1, ClosedBy.WORKER, CaptureTier.T3)
    assert attempt_state(checked, task) == ("cancelled", None)


# --- lost answers, retries and refusals -------------------------------------------------------


@pytest.mark.parametrize("marker", [INTENT, RESULT], ids=["intent", "result"])
@pytest.mark.parametrize("committed", [True, False], ids=["committed", "rolled-back"])
def test_a_lost_usage_answer_is_replayed_to_exactly_one_row_and_one_call(
    schema_url: str,
    checked: Conn,
    caplog: pytest.LogCaptureFixture,
    marker: bytes,
    committed: bool,
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    metered = Metered(schema_url)
    (task,) = submit(checked, KIND, {"calls": 2})
    with PgProxy(schema_url) as proxy:
        proxy.drop_commit(marker, committed=committed)
        with metered_worker(proxy.url(schema_url), metered) as probe:
            eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    assert proxy.drop_count == 1
    assert "usage_write_ambiguous" in events(caplog)
    assert probe.terminations == []
    assert [seq for _, seq, _ in metered.invocations] == [0, 1]  # one call per call_seq
    assert no_call_without_its_row(metered)
    usage = usage_of(checked, task)
    assert [(c.call.intent.call_seq, c.result is not None) for c in usage.calls] == [
        (0, True),
        (1, True),
    ]
    assert closure(usage) == (CaptureState.COMPLETE, 2, ClosedBy.WORKER, CaptureTier.T3)


def test_a_retryable_usage_error_is_retried_without_failing_the_task(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    install(checked, "deadlock_once", "provider_calls", "40P01", once=True)
    metered = Metered(schema_url)
    (task,) = submit(checked, KIND, {"calls": 1})
    with metered_worker(schema_url, metered) as probe:
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    assert rows(checked, "SELECT last_value FROM deadlock_once_seq") == [(2,)]  # refused once
    assert "usage_write_retry" in events(caplog)
    assert probe.terminations == []
    assert metered.made() == 1
    assert no_call_without_its_row(metered)
    assert closure(usage_of(checked, task)) == (
        CaptureState.COMPLETE,
        1,
        ClosedBy.WORKER,
        CaptureTier.T3,
    )
    checked.execute("DROP TRIGGER deadlock_once ON provider_calls")


# Test values only: no heartbeat is due near the 6 s deadline (one at 4 s, the next at 8 s),
# so the usage step meets the deadline first; 4 + 0.5 + 1 < 6.
SHORT = SETTINGS.model_copy(
    update={
        "lease": timedelta(seconds=6),
        "execution_timeout": timedelta(seconds=6),
        "heartbeat_interval": timedelta(seconds=4),
    }
)


def test_an_intent_unrecordable_until_the_deadline_ends_as_unrecorded_and_no_call(
    schema_url: str, checked: Conn
) -> None:
    install(checked, "refuse_intent", "provider_calls", "40001")
    metered = Metered(schema_url)
    (task,) = submit(checked, KIND, {"calls": 1})
    seen: list[list[Expiry]] = []
    with metered_worker(schema_url, metered, SHORT, expiries_seen=seen) as probe:
        eventually(lambda: probe.terminations, "the worker to end", timeout=WAIT + 6)
    # Exit 8 from the supervisor, never 5 from the handler's later backstop (O2).
    assert probe.terminations[0] == (ExitCode.UNRECORDED, "usage_intent_unrecorded")
    # The order a real process sees: the ownership fence (lease capped at the deadline,
    # minus the margin) had expired first, which there ends the process as exit 3.
    assert Guard.OWNERSHIP in [expiry.guard for expiry in seen[0]]
    assert metered.invocations == []
    assert usage_of(checked, task).calls == ()
    assert attempt_state(checked, task) == ("running", None)  # never failed
    checked.execute("DROP TRIGGER refuse_intent ON provider_calls")


def test_a_rejected_intent_is_a_defect_and_never_a_task_failure(
    schema_url: str, checked: Conn
) -> None:
    install(checked, "reject_intent", "provider_calls", "23514")  # check_violation
    metered = Metered(schema_url)
    (task,) = submit(checked, KIND, {"calls": 1})
    with metered_worker(schema_url, metered) as probe:
        eventually(lambda: probe.terminations, "the worker to end")
    assert probe.terminations[0] == (ExitCode.DEFECT, "usage_write_rejected")
    assert metered.invocations == []
    assert attempt_state(checked, task) == ("running", None)
    checked.execute("DROP TRIGGER reject_intent ON provider_calls")


# --- late records ------------------------------------------------------------------------------

T3 = {KIND: CaptureTier.T3}


def revoke_at_once(db: Conn, task: Task) -> Any:
    """End the attempt's lease now (a 1 µs cancellation grace) and let recovery revoke it.

    This takes milliseconds, so the worker's held write resumes long before its next
    heartbeat is due: the write in hand, not a heartbeat, meets the revocation first.
    """
    request_cancellation(db, task.id, grace=timedelta(microseconds=1))
    return eventually(
        lambda: revoke_next_expired(db, retry_policy=SETTINGS.retry_policy, usage_capture=T3),
        "recovery to revoke the attempt",
    )


def test_an_intent_recorded_after_revocation_fences_the_worker_without_a_call(
    schema_url: str, checked: Conn
) -> None:
    metered = Metered(schema_url)
    (task,) = submit(checked, KIND, {"calls": 1}, max_attempts=1)
    with PgProxy(schema_url) as proxy:
        proxy.hold_on(INTENT)  # the supervisor's intent write waits here
        with metered_worker(proxy.url(schema_url), metered) as probe:
            assert proxy.held.wait(WAIT)
            revocation = revoke_at_once(checked, task)
            proxy.resume()
            eventually(lambda: probe.terminations, "the worker to end")
    assert probe.terminations[0] == (ExitCode.FENCED, "usage_intent_late")
    assert metered.invocations == []  # no call after ownership ended
    assert revocation.usage_closure is not None
    usage = usage_of(checked, task)
    # The late row is kept: a conservative unresolved call, so never "closed".
    assert [(c.call.late, c.result) for c in usage.calls] == [(True, None)]
    assert closure(usage) == (CaptureState.COMPLETE, 0, ClosedBy.RECOVERY, CaptureTier.T3)
    assert capture_of(usage) is AttemptCapture.LOWER_BOUND


def test_a_result_recorded_after_revocation_is_accepted(schema_url: str, checked: Conn) -> None:
    metered = Metered(schema_url)
    # One attempt only: once revoked, the task is not claimed again.
    (task,) = submit(checked, KIND, {"calls": 1}, max_attempts=1)

    def late_result() -> bool:
        calls = usage_of(checked, task).calls
        return bool(calls) and calls[0].result is not None

    with PgProxy(schema_url) as proxy:
        proxy.hold_on(RESULT)  # the call was made; its result write waits here
        with metered_worker(proxy.url(schema_url), metered) as probe:
            assert proxy.held.wait(WAIT)
            revoke_at_once(checked, task)
            proxy.resume()
            eventually(late_result, "the late result")
    # Then the outcome is discarded, or a heartbeat fences the worker first.
    assert probe.terminations in ([], [(ExitCode.FENCED, "ownership_lost")])
    assert metered.made() == 1
    assert no_call_without_its_row(metered)
    usage = usage_of(checked, task)
    (call,) = usage.calls
    assert call.result is not None
    assert call.result.late
    assert closure(usage) == (CaptureState.INCOMPLETE, None, ClosedBy.RECOVERY, CaptureTier.T3)
    assert capture_of(usage) is AttemptCapture.LOWER_BOUND
    assert task_states(checked, [task]) == [("cancelled", 1)]  # revoked at the grace's end


# --- an amount the ledger cannot store ------------------------------------------------------------


def test_an_unrepresentable_amount_leaves_the_call_unresolved_and_the_task_succeeds(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("DEBUG")
    amount = "100000000000000000000"  # 1E+20: the ledger's limit is exclusive
    metered = Metered(schema_url, usage=ProviderUsage(amount, "secret-unit"))
    (task,) = submit(checked, KIND, {"calls": 1})
    with metered_worker(schema_url, metered) as probe:
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    assert probe.terminations == []
    usage = usage_of(checked, task)
    assert [c.result for c in usage.calls] == [None]  # never clamped or zeroed
    assert closure(usage) == (CaptureState.INCOMPLETE, None, ClosedBy.WORKER, CaptureTier.T3)
    assert capture_of(usage) is AttemptCapture.LOWER_BOUND
    (logged,) = [
        r for r in caplog.records if getattr(r, "event", "") == "usage_result_unrepresentable"
    ]
    assert getattr(logged, "attempt_id", None) == str(usage.attempt_id)
    for record in caplog.records:
        text = record.getMessage() + repr(getattr(record, "__dict__", {}))
        assert amount not in text
        assert "secret-unit" not in text


# --- heartbeats come first --------------------------------------------------------------------


def test_heartbeats_keep_their_cadence_while_records_are_written(
    schema_url: str, checked: Conn
) -> None:
    metered = Metered(schema_url, stop=threading.Event())
    (task,) = submit(checked, KIND, {"calls": MAX_CALL_SEQ})
    with metered_worker(schema_url, metered) as probe:
        leases: list[Any] = []
        made: list[int] = []

        def renewals() -> bool:
            found = rows(
                checked,
                "SELECT lease_expires_at FROM attempts WHERE task_id = %s AND state = 'running'",
                task.id,
            )
            if found and (not leases or found[0][0] != leases[-1]):
                leases.append(found[0][0])
                made.append(metered.made())
            return len(leases) >= 4

        eventually(renewals, "three renewals while calls are made")
        assert made == sorted(made)
        assert made[-1] > made[0]  # calls went on between renewals
        assert probe.expiries == []  # no guard ever expired: the fence moved on time
        assert metered.stop is not None
        metered.stop.set()
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    assert probe.terminations == []
    assert no_call_without_its_row(metered)
    assert closure(usage_of(checked, task)) == (
        CaptureState.COMPLETE,
        metered.made(),
        ClosedBy.WORKER,
        CaptureTier.T3,
    )


def test_a_worker_without_a_sink_writes_no_usage(schema_url: str, checked: Conn) -> None:
    # The sink's absence changes nothing: no closure on completion, none on recovery.
    def echo(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        return payload

    def terminate(code: ExitCode, event: str, message: str, context: Any) -> NoReturn:
        raise Terminated(event)

    (task,) = submit(checked, "echo", {"n": 1})
    expiries: list[Expiry] = []
    worker = Worker(
        registry=HandlerRegistry({"echo": echo}),
        settings=SETTINGS,
        connect=functools.partial(connect_supervisor, schema_url, SETTINGS),
        watchdog=Watchdog(terminate=expiries.append),
        terminate=terminate,
    )
    runner = threading.Thread(target=worker.run, daemon=True)
    runner.start()
    try:
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    finally:
        worker.request_stop()
        runner.join(WAIT)
    assert worker.finished
    assert usage_of(checked, task).closure is None
