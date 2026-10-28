"""The single-slot worker runtime against PostgreSQL, run in this process (DEC-045).

Each worker runs on a thread with a recording watchdog and a ``terminate`` that
raises instead of ending the process; the process-level tests in
``test_worker_process.py`` show the real exits. Handlers wait on events the
test sets, and the test waits for PostgreSQL to show each state (bounded), so
no assertion depends on timing luck.
"""

import functools
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, NoReturn

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.domain.errors import PermanentError, TransientError
from orqestra.domain.jobs import JobSubmission, JsonValue, Task, TaskSpec
from orqestra.domain.leases import LeasePolicy
from orqestra.persistence.jobs import list_tasks, submit_job
from orqestra.persistence.lifecycle import (
    request_cancellation,
    revoke_next_expired,
    start_attempt,
)
from orqestra.worker.fence import ExitCode, Expiry, Guard, Watchdog
from orqestra.worker.handlers import HandlerCancelled, HandlerRegistry, TaskContext
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from tests.integration.pg_proxy import PgProxy
from tests.integration.test_attempt_lifecycle import assert_invariants

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

# Test values only (DEC-034). Generous database bounds keep a slow CI runner
# from tripping the recording watchdog; the process tests use tighter ones.
SETTINGS = WorkerSettings(
    _env_file=None,
    lease=timedelta(seconds=5),
    execution_timeout=timedelta(minutes=5),
    heartbeat_interval=timedelta(seconds=1),
    db_operation_timeout=timedelta(milliseconds=900),
    fence_margin=timedelta(seconds=1),
    poll_interval=timedelta(milliseconds=100),
    recovery_interval=timedelta(milliseconds=300),
    recovery_batch_limit=5,
    shutdown_timeout=timedelta(seconds=5),
)
WAIT = 20.0  # the bound on every wait for a state to appear


@pytest.fixture
def checked(db: Conn) -> Iterator[Conn]:
    """The migrated test connection; the lifecycle invariants must hold when the test ends."""
    yield db
    assert_invariants(db)


def eventually[T](check: Callable[[], T | None], what: str, timeout: float = WAIT) -> T:
    """Poll ``check`` until it returns something truthy; fail after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.02)
    pytest.fail(f"timed out waiting for {what}")


class Terminated(Exception):  # noqa: N818
    pass


@dataclass
class Gate:
    """Handlers wait on ``release``; ``runs`` records the attempts that ran."""

    release: threading.Event = field(default_factory=threading.Event)
    runs: list[uuid.UUID] = field(default_factory=list)

    def handler(self, ctx: TaskContext, payload: JsonValue) -> JsonValue:
        self.runs.append(ctx.attempt_id)
        assert self.release.wait(60)
        return {"ran": str(ctx.attempt_id)}


@dataclass
class Probe:
    worker: Worker
    expiries: list[Expiry]
    terminations: list[tuple[ExitCode, str]]
    thread: threading.Thread
    failures: list[BaseException]

    def ownership_deadline(self) -> float | None:
        return self.worker._watchdog.deadline(Guard.OWNERSHIP)


@contextmanager
def running_worker(
    url: str,
    handlers: dict[str, Callable[[TaskContext, JsonValue], JsonValue]],
    settings: WorkerSettings = SETTINGS,
) -> Iterator[Probe]:
    expiries: list[Expiry] = []
    terminations: list[tuple[ExitCode, str]] = []

    def terminate(code: ExitCode, event: str, message: str, context: Any) -> NoReturn:
        terminations.append((code, event))
        raise Terminated(event)

    worker = Worker(
        registry=HandlerRegistry(handlers),
        settings=settings,
        connect=functools.partial(connect_supervisor, url, settings),
        watchdog=Watchdog(terminate=expiries.append),
        terminate=terminate,
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
        worker.request_stop()
        thread.join(WAIT)
        assert not thread.is_alive(), "the worker did not stop"
        assert failures == []


def submit(db: Conn, kind: str, *payloads: JsonValue, max_attempts: int = 3) -> list[Task]:
    specs = tuple(TaskSpec(f"t{i}", payload, max_attempts) for i, payload in enumerate(payloads))
    job = submit_job(db, JobSubmission(kind, specs)).job
    return sorted(list_tasks(db, job.id), key=lambda task: task.task_key)


def rows(db: Conn, query: str, *params: object) -> list[tuple[Any, ...]]:
    return db.execute(query, params).fetchall()


def task_states(db: Conn, tasks: list[Task]) -> list[tuple[str, int]]:
    return rows(
        db,
        "SELECT state, attempt_count FROM tasks WHERE id = ANY(%s) ORDER BY task_key",
        [t.id for t in tasks],
    )


def attempts_of(db: Conn, task: Task) -> list[tuple[Any, ...]]:
    return rows(
        db,
        "SELECT attempt_number, state, failure_kind, error_type, worker_id, revoked_reason "
        "FROM attempts WHERE task_id = %s ORDER BY attempt_number",
        task.id,
    )


def running_attempt(db: Conn, task: Task) -> tuple[Any, ...] | None:
    found = rows(
        db,
        "SELECT id, lease_expires_at, started_at FROM attempts "
        "WHERE task_id = %s AND state = 'running'",
        task.id,
    )
    return found[0] if found else None


def echo(ctx: TaskContext, payload: JsonValue) -> JsonValue:
    return payload


# --- claiming and completing ------------------------------------------------------


def test_supported_tasks_are_completed_and_others_stay_queued(
    schema_url: str, checked: Conn
) -> None:
    supported = submit(checked, "echo", {"n": 1}, None, [1, 2])
    unsupported = submit(checked, "other", {"n": 2}, {"n": 3})
    with running_worker(schema_url, {"echo": echo}) as probe:
        eventually(
            lambda: task_states(checked, supported) == [("succeeded", 1)] * 3, "the supported tasks"
        )
    results = rows(
        checked,
        "SELECT result FROM tasks WHERE id = ANY(%s) ORDER BY task_key",
        [t.id for t in supported],
    )
    assert [r[0] for r in results] == [{"n": 1}, None, [1, 2]]  # None is JSON null (DEC-030)
    assert task_states(checked, unsupported) == [("queued", 0)] * 2
    assert rows(
        checked,
        "SELECT count(*) FROM attempts WHERE task_id = ANY(%s)",
        [t.id for t in unsupported],
    ) == [(0,)]
    assert {a[4] for t in supported for a in attempts_of(checked, t)} == {probe.worker.worker_id}
    assert probe.expiries == []
    assert probe.terminations == []


def test_concurrent_workers_never_share_a_generation_or_hold_two_tasks(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    payloads: list[JsonValue] = [{"i": i} for i in range(6)]
    tasks = submit(checked, "gated", *payloads)
    with (
        running_worker(schema_url, {"gated": gate.handler}) as first,
        running_worker(schema_url, {"gated": gate.handler}) as second,
        running_worker(schema_url, {"gated": gate.handler}) as third,
    ):
        per_worker = eventually(
            lambda: (
                rows(
                    checked,
                    "SELECT worker_id, count(*) FROM attempts "
                    "WHERE state = 'running' GROUP BY worker_id",
                )
                if len(gate.runs) == 3
                else None
            ),
            "three handlers to run",
        )
        # Each worker holds exactly one task; the other three are untouched.
        assert sorted(per_worker) == sorted((w.worker.worker_id, 1) for w in (first, second, third))
        assert sorted(task_states(checked, tasks)) == [("queued", 0)] * 3 + [("running", 1)] * 3
        gate.release.set()
        eventually(lambda: task_states(checked, tasks) == [("succeeded", 1)] * 6, "every task")
    assert len(gate.runs) == len(set(gate.runs)) == 6
    for task in tasks:
        ((number, state, *_),) = attempts_of(checked, task)  # exactly one attempt each
        assert (number, state) == (1, "succeeded")
    assert all(not w.expiries and not w.terminations for w in (first, second, third))


def test_no_second_task_is_claimed_while_a_handler_runs(schema_url: str, checked: Conn) -> None:
    gate = Gate()
    first, second = submit(checked, "gated", {"i": 1}, {"i": 2})
    with running_worker(schema_url, {"gated": gate.handler}) as probe:
        leases: set[object] = set()

        def renewed_twice() -> bool:
            found = running_attempt(checked, first) or running_attempt(checked, second)
            if found is not None:
                leases.add(found[1])
            return len(leases) >= 3

        eventually(renewed_twice, "two renewals of the first task")
        assert sorted(task_states(checked, [first, second])) == [("queued", 0), ("running", 1)]
        gate.release.set()
        eventually(
            lambda: task_states(checked, [first, second]) == [("succeeded", 1)] * 2, "both tasks"
        )
    (earlier_end, later_start) = rows(
        checked,
        "SELECT min(finished_at), max(started_at) FROM attempts WHERE task_id = ANY(%s)",
        [first.id, second.id],
    )[0]
    assert later_start >= earlier_end  # one at a time
    assert probe.expiries == []


# --- heartbeats and recovery ---------------------------------------------------------


def test_heartbeats_keep_a_slow_handler_alive(schema_url: str, checked: Conn) -> None:
    gate = Gate()
    (task,) = submit(checked, "gated", {})
    with running_worker(schema_url, {"gated": gate.handler}) as probe:
        leases: list[datetime] = []

        def renewals() -> bool:
            found = running_attempt(checked, task)
            if found is not None and (not leases or found[1] != leases[-1]):
                leases.append(found[1])
            return len(leases) >= 4

        eventually(renewals, "three renewals")
        assert leases == sorted(leases)  # the lease only ever moves later
        deadline = probe.ownership_deadline()
        assert deadline is not None
        assert deadline > time.monotonic()  # confirmed renewals moved it
        gate.release.set()
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    ((number, state, _, _, _, revoked),) = attempts_of(checked, task)
    assert (number, state, revoked) == (1, "succeeded", None)
    assert probe.expiries == []


def test_a_busy_worker_recovers_another_workers_expired_attempt(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    (own,) = submit(checked, "gated", {})
    (orphan,) = submit(checked, "orphaned", {})  # a kind this worker does not run
    dead = start_attempt(
        checked,
        orphan.id,
        "dead-worker",
        policy=LeasePolicy(
            lease=timedelta(milliseconds=300), execution_timeout=timedelta(minutes=1)
        ),
    )
    with running_worker(schema_url, {"gated": gate.handler}) as probe:
        both = eventually(
            lambda: rows(
                checked,
                "SELECT d.state, d.revoked_reason, o.state FROM attempts d, attempts o "
                "WHERE d.id = %s AND o.task_id = %s AND d.state = 'revoked'",
                dead.id,
                own.id,
            ),
            "the expired attempt to be revoked",
        )
        # Revoked by this worker while its own handler was still running.
        assert both == [("revoked", "lease_expired", "running")]
        assert len(gate.runs) == 1
        assert task_states(checked, [orphan]) == [("queued", 1)]
        gate.release.set()
        eventually(lambda: task_states(checked, [own]) == [("succeeded", 1)], "the own task")
    assert probe.expiries == []


# --- handler outcomes and database errors ---------------------------------------------


def failing(ctx: TaskContext, payload: JsonValue) -> JsonValue:
    assert isinstance(payload, dict)
    match payload["mode"]:
        case "transient":
            raise TransientError("later")
        case "permanent":
            raise PermanentError("never")
        case "bug":
            raise KeyError("missing")
        case "cancelled":
            raise HandlerCancelled  # without a request
        case _:
            return float("nan")


def test_handler_failures_follow_the_existing_classification(
    schema_url: str, checked: Conn
) -> None:
    modes = ["transient", "permanent", "bug", "cancelled", "invalid"]
    payloads: list[JsonValue] = [{"mode": m} for m in modes]
    tasks = submit(checked, "failing", *payloads, max_attempts=1)
    with running_worker(schema_url, {"failing": failing}) as probe:
        eventually(lambda: task_states(checked, tasks) == [("failed", 1)] * 5, "every failure")
    recorded = [attempts_of(checked, t)[0][2:4] for t in tasks]
    assert recorded == [
        ("transient", "orqestra.domain.errors.TransientError"),
        ("permanent", "orqestra.domain.errors.PermanentError"),
        ("unclassified", "builtins.KeyError"),
        ("unclassified", "orqestra.worker.handlers.HandlerCancelled"),
        ("unclassified", "orqestra.worker.handlers.InvalidHandlerResult"),
    ]
    assert probe.terminations == []
    assert_invariants(checked)


def test_a_lost_supervisor_connection_is_not_a_handler_failure(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    (task,) = submit(checked, "gated", {})
    with running_worker(schema_url, {"gated": gate.handler}) as probe:
        first = eventually(lambda: running_attempt(checked, task), "the task to run")
        conn = probe.worker._conn
        assert conn is not None
        killed = conn.info.backend_pid
        assert rows(checked, "SELECT pg_terminate_backend(%s)", killed) == [(True,)]
        # The next heartbeat fails unconfirmed; the worker reconnects and renews again.
        eventually(
            lambda: (
                (found := running_attempt(checked, task)) is not None
                and found[1] > first[1]
                and probe.worker._conn is not None
                and probe.worker._conn.info.backend_pid != killed
            ),
            "a renewal on a new connection",
        )
        gate.release.set()
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    ((_, state, failure_kind, *_),) = attempts_of(checked, task)
    assert (state, failure_kind) == ("succeeded", None)
    assert probe.expiries == []
    assert probe.terminations == []


def test_a_rejected_valid_outcome_ends_the_worker_without_a_failure(
    schema_url: str, checked: Conn
) -> None:
    checked.execute("""
        CREATE FUNCTION reject_success() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'rejected for the test' USING ERRCODE = 'check_violation'; END $$;
        CREATE TRIGGER reject_success BEFORE UPDATE ON attempts FOR EACH ROW
            WHEN (NEW.state = 'succeeded') EXECUTE FUNCTION reject_success();
    """)
    (task,) = submit(checked, "echo", {"x": 1})
    with running_worker(schema_url, {"echo": echo}) as probe:
        eventually(lambda: probe.terminations, "the worker to end")
    assert probe.terminations == [(ExitCode.DEFECT, "completion_rejected")]
    ((_, state, failure_kind, *_),) = attempts_of(checked, task)
    assert (state, failure_kind) == ("running", None)  # nothing recorded as a handler failure
    checked.execute("DROP TRIGGER reject_success ON attempts")


def test_a_retryable_completion_error_is_retried_without_rerunning_the_handler(
    schema_url: str, checked: Conn
) -> None:
    # A sequence survives the rollback of the transaction it fails, so only the
    # first completion is rejected.
    checked.execute("""
        CREATE SEQUENCE completions;
        CREATE FUNCTION deadlock_once() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF nextval('completions') = 1 THEN
                RAISE EXCEPTION 'deadlock for the test' USING ERRCODE = 'deadlock_detected';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER deadlock_once BEFORE UPDATE ON attempts FOR EACH ROW
            WHEN (NEW.state = 'succeeded') EXECUTE FUNCTION deadlock_once();
    """)
    gate = Gate()
    gate.release.set()
    (task,) = submit(checked, "gated", {})
    with running_worker(schema_url, {"gated": gate.handler}) as probe:
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    assert len(gate.runs) == 1
    assert rows(checked, "SELECT last_value FROM completions") == [(2,)]  # rejected once
    assert probe.terminations == []
    assert probe.expiries == []
    checked.execute("DROP TRIGGER deadlock_once ON attempts")


def test_unconfirmed_heartbeats_never_extend_the_fence(schema_url: str, checked: Conn) -> None:
    gate = Gate()
    (task,) = submit(checked, "gated", {})
    with running_worker(schema_url, {"gated": gate.handler}) as probe:
        eventually(lambda: running_attempt(checked, task), "the task to run")
        armed = eventually(probe.ownership_deadline, "the fence")
        # From now on every renewal is rejected by the server: no heartbeat is confirmed.
        checked.execute("""
            CREATE FUNCTION refuse_renewal() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'renewal refused for the test'; END $$;
            CREATE TRIGGER refuse_renewal BEFORE UPDATE ON attempts FOR EACH ROW
                WHEN (NEW.lease_expires_at > OLD.lease_expires_at)
                EXECUTE FUNCTION refuse_renewal();
        """)
        frozen = eventually(probe.ownership_deadline, "the fence")
        assert frozen >= armed
        expiry = eventually(
            lambda: probe.expiries,
            "the fence to expire",
            timeout=SETTINGS.lease.total_seconds() + WAIT,
        )
        ((_, lease_end, _),) = [running_attempt(checked, task) or (None, None, None)]
        # It expired at the last confirmed deadline, never moved by the refused heartbeats.
        assert expiry[0].guard is Guard.OWNERSHIP
        assert expiry[0].deadline == frozen
        assert lease_end is not None
        assert rows(checked, "SELECT clock_timestamp() < %s", lease_end) == [(True,)]
        checked.execute("DROP TRIGGER refuse_renewal ON attempts")
        gate.release.set()


# --- stalls and ambiguous completions (through a proxy) -------------------------------------


# The supervisor calls a busy worker makes; which one meets the stall depends on the schedule.
BUSY_CALLS = {"renew_lease", "revoke_next_expired"}


def test_a_stalled_supervisor_call_is_ended_by_the_operation_guard(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    submit(checked, "gated", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"gated": gate.handler}) as probe,
    ):
        try:
            eventually(lambda: len(gate.runs) == 1, "the handler to start")
            proxy.stall()
            (expiry,) = eventually(lambda: probe.expiries, "the operation guard")
            assert expiry.guard is Guard.OPERATION
            assert expiry.context["operation"] in BUSY_CALLS
            assert expiry.context["worker_id"] == probe.worker.worker_id
        finally:
            proxy.resume()  # never leave the worker stuck, whatever the assertions found
            gate.release.set()


# --- reconciling a lost completion answer (DEC-047) ------------------------------------------

# Only the completion evidence read joins the attempt to its task like this.
EVIDENCE_READ = b"JOIN tasks t ON t.id = a.task_id"


@dataclass
class Returning:
    """A gated handler that returns ``value`` and records each attempt it ran for."""

    value: JsonValue
    gate: Gate = field(default_factory=Gate)

    def handler(self, ctx: TaskContext, payload: JsonValue) -> JsonValue:
        self.gate.handler(ctx, payload)
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


def events(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [str(getattr(r, "event", "")) for r in caplog.records if r.name == "orqestra.worker"]


def attempt_rows(db: Conn, task: Task) -> list[tuple[Any, ...]]:
    return rows(
        db,
        "SELECT attempt_number, state, failure_kind, revoked_reason FROM attempts "
        "WHERE task_id = %s ORDER BY attempt_number",
        task.id,
    )


@pytest.mark.parametrize(
    ("value", "task_state", "attempt_state", "failure_kind"),
    [
        ({"total": 3}, "succeeded", "succeeded", None),
        (None, "succeeded", "succeeded", None),  # JSON null is a result
        (1e308, "succeeded", "succeeded", None),  # jsonb stores 309 digits
        (PermanentError("bad invoice"), "failed", "failed", "permanent"),
        (TransientError("later"), "failed", "failed", "transient"),  # max_attempts=1
    ],
    ids=["object", "json-null", "1e308", "permanent", "transient"],
)
def test_a_committed_completion_whose_answer_was_lost_is_reconciled(
    schema_url: str,
    checked: Conn,
    caplog: pytest.LogCaptureFixture,
    value: Any,
    task_state: str,
    attempt_state: str,
    failure_kind: str | None,
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    returning = Returning(value)
    first, second = submit(checked, "returning", {"i": 1}, {"i": 2}, max_attempts=1)
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the first handler")
        proxy.drop_completion(committed=True)
        returning.gate.release.set()
        # Reconciled, the worker carries on and runs the next task: no exit.
        eventually(
            lambda: sorted(task_states(checked, [first, second])) == [(task_state, 1)] * 2,
            "both tasks",
        )
    assert probe.terminations == []
    assert proxy.drop_count == 1
    assert len(returning.gate.runs) == 2  # once per task, never again for the first
    assert attempt_rows(checked, first) == [(1, attempt_state, failure_kind, None)]
    assert "completion_reconciled" in events(caplog)


def test_an_uncommitted_completion_whose_answer_was_lost_is_written_again(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    returning = Returning({"ok": True})
    (task,) = submit(checked, "returning", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=False)
        returning.gate.release.set()
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    assert probe.terminations == []
    assert len(returning.gate.runs) == 1
    assert rows(checked, "SELECT result FROM tasks WHERE id = %s", task.id) == [({"ok": True},)]
    recorded = events(caplog)
    assert recorded.index("completion_ambiguous") < recorded.index("attempt_completed")
    assert "completion_reconciled" not in recorded  # it had not committed: written again


def test_repeated_lost_answers_are_reconciled_without_rerunning(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    returning = Returning({"ok": True})
    (task,) = submit(checked, "returning", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=False)  # the first write rolls back
        proxy.drop_completion(committed=True)  # the second commits, its answer lost too
        returning.gate.release.set()
        eventually(lambda: "completion_reconciled" in events(caplog), "reconciliation")
    assert proxy.drop_count == 2
    assert probe.terminations == []
    assert len(returning.gate.runs) == 1
    assert task_states(checked, [task]) == [("succeeded", 1)]
    assert attempt_rows(checked, task) == [(1, "succeeded", None, None)]


def test_recovery_that_revoked_first_wins_and_the_outcome_is_discarded(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    returning = Returning({"ok": True})
    (task,) = submit(checked, "returning", {}, max_attempts=1)
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=False, then_refuse=True)  # no reconnection yet
        returning.gate.release.set()
        assert proxy.dropped.wait(WAIT)
        # No heartbeats while completing: the lease runs out and recovery revokes it.
        eventually(lambda: revoke_next_expired(checked), "the lease to expire and be revoked")
        proxy.accept()
        eventually(lambda: "outcome_discarded" in events(caplog), "the outcome to be discarded")
    assert probe.terminations == []
    assert len(returning.gate.runs) == 1  # never rerun
    assert attempt_rows(checked, task) == [(1, "revoked", None, "lease_expired")]
    assert task_states(checked, [task]) == [("failed", 1)]  # its only attempt was revoked


@pytest.mark.parametrize(
    "claimed_again", [False, True], ids=["revoked", "revoked-and-claimed-again"]
)
def test_recovery_that_wins_between_the_evidence_and_the_rewrite_is_respected(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture, claimed_again: bool
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    returning = Returning({"ok": True})
    # With no attempt left after the revocation, this worker cannot claim the task
    # again; with one left, another worker claims it first.
    (task,) = submit(checked, "returning", {}, max_attempts=2 if claimed_again else 1)
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=False, then_refuse=True)
        returning.gate.release.set()
        assert proxy.dropped.wait(WAIT)
        # Let the worker reconnect and read "still running", then hold its rewrite.
        proxy.hold_on(b"BEGIN")
        proxy.accept()
        assert proxy.held.wait(WAIT)
        eventually(lambda: revoke_next_expired(checked), "the lease to expire and be revoked")
        if claimed_again:
            start_attempt(
                checked,
                task.id,
                "another-worker",
                policy=LeasePolicy(timedelta(minutes=5), timedelta(minutes=5)),
            )
        proxy.resume()  # the rewrite is now refused; the evidence decides
        eventually(lambda: "outcome_discarded" in events(caplog), "the outcome to be discarded")
    assert probe.terminations == []
    assert len(returning.gate.runs) == 1  # the first attempt's handler, once
    first = attempt_rows(checked, task)[0]
    assert first == (1, "revoked", None, "lease_expired")
    expected = [("running", 2)] if claimed_again else [("failed", 1)]
    assert task_states(checked, [task]) == expected


def test_a_success_reconciled_after_a_cancellation_request_still_stands(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    returning = Returning({"ok": True})
    (task,) = submit(checked, "returning", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=False, then_refuse=True)
        returning.gate.release.set()
        assert proxy.dropped.wait(WAIT)
        # A request commits while the worker cannot reach the database.
        request_cancellation(checked, task.id, grace=timedelta(minutes=5))
        proxy.accept()
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the task")
    # The evidence showed it still running; the rewrite of the success won (DEC-020).
    assert probe.terminations == []
    assert len(returning.gate.runs) == 1
    assert rows(
        checked, "SELECT cancel_requested_at IS NOT NULL FROM tasks WHERE id = %s", task.id
    ) == [(True,)]


def test_a_lost_connection_while_reading_the_evidence_is_retried(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    returning = Returning({"ok": True})
    (task,) = submit(checked, "returning", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=True)
        proxy.cut_on(EVIDENCE_READ)  # the first evidence read loses its connection
        returning.gate.release.set()
        eventually(lambda: "completion_reconciled" in events(caplog), "reconciliation")
    assert proxy.cut.is_set()
    assert "evidence_unread" in events(caplog)
    assert probe.terminations == []
    assert len(returning.gate.runs) == 1
    assert task_states(checked, [task]) == [("succeeded", 1)]


def test_a_conflicting_recorded_result_is_a_defect(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    returning = Returning(True)
    (task,) = submit(checked, "returning", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=True, then_refuse=True)
        returning.gate.release.set()
        assert proxy.dropped.wait(WAIT)
        # The stored true becomes 1 behind the worker's back: equal in Python, not in jsonb.
        checked.execute("UPDATE tasks SET result = '1'::jsonb WHERE id = %s", (task.id,))
        proxy.accept()
        eventually(lambda: probe.terminations, "the worker to end")
    assert probe.terminations == [(ExitCode.DEFECT, "completion_conflicts")]
    assert len(returning.gate.runs) == 1


def test_an_unresolved_reconciliation_ends_at_the_attempts_deadline(
    schema_url: str, checked: Conn
) -> None:
    short = WorkerSettings(
        _env_file=None, **{**SETTINGS.model_dump(), "execution_timeout": SETTINGS.lease}
    )
    returning = Returning({"ok": True})
    first, second = submit(checked, "returning", {"i": 1}, {"i": 2})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"returning": returning.handler}, short) as probe,
    ):
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        proxy.drop_completion(committed=False, then_refuse=True)  # never reachable again
        returning.gate.release.set()
        eventually(lambda: probe.terminations, "the worker to end", timeout=WAIT + 10)
    assert probe.terminations == [(ExitCode.AMBIGUOUS, "reconciliation_unresolved")]
    assert len(returning.gate.runs) == 1  # never rerun
    assert sorted(task_states(checked, [first, second])) == [("queued", 0), ("running", 1)]


# --- cooperative cancellation (DEC-048) --------------------------------------------------


@dataclass
class Cooperative:
    """Waits for its cancellation signal, then does ``then``; records what it saw.

    With ``hold`` set, it waits for ``proceed`` after seeing the signal, so a test
    can act while the request is known and the handler has not ended yet.
    """

    then: str = "cancel"  # cancel | succeed | transient | permanent
    hold: bool = False
    runs: list[uuid.UUID] = field(default_factory=list)
    observed: threading.Event = field(default_factory=threading.Event)
    proceed: threading.Event = field(default_factory=threading.Event)

    def handler(self, ctx: TaskContext, payload: JsonValue) -> JsonValue:
        self.runs.append(ctx.attempt_id)
        if not ctx.cancellation.wait(60):
            return {"never": "told"}
        self.observed.set()
        if self.hold:
            assert self.proceed.wait(60)
        match self.then:
            case "cancel":
                raise HandlerCancelled
            case "transient":
                raise TransientError("stopping")
            case "permanent":
                raise PermanentError("stopping")
            case _:
                return {"finished": "anyway"}


GRACE = timedelta(seconds=30)  # test value: far longer than a heartbeat interval


def lease_of(db: Conn, task: Task) -> Any:
    return rows(db, "SELECT lease_expires_at FROM attempts WHERE task_id = %s", task.id)[0][0]


@pytest.mark.parametrize(
    ("then", "attempt_state", "failure_kind", "task_state"),
    [
        ("cancel", "cancelled", None, "cancelled"),
        ("succeed", "succeeded", None, "succeeded"),  # a success may still win (DEC-020)
        ("transient", "failed", "transient", "cancelled"),  # a request cancels, never requeues
        ("permanent", "failed", "permanent", "failed"),  # the failure policy is unchanged
    ],
    ids=["cancelled", "success-stands", "transient-cancels", "permanent-fails"],
)
def test_a_handler_told_of_a_committed_request_ends_by_the_existing_policy(
    schema_url: str,
    checked: Conn,
    caplog: pytest.LogCaptureFixture,
    then: str,
    attempt_state: str,
    failure_kind: str | None,
    task_state: str,
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    cooperative = Cooperative(then)
    (task,) = submit(checked, "cooperative", {})
    with running_worker(schema_url, {"cooperative": cooperative.handler}) as probe:
        eventually(lambda: len(cooperative.runs) == 1, "the handler")
        request_cancellation(checked, task.id, grace=GRACE)
        assert cooperative.observed.wait(WAIT)  # told at the next heartbeat
        eventually(lambda: task_states(checked, [task]) == [(task_state, 1)], "the outcome")
    assert attempt_rows(checked, task) == [(1, attempt_state, failure_kind, None)]  # not revoked
    assert probe.terminations == []  # the worker carries on
    assert probe.expiries == []
    assert len(cooperative.runs) == 1
    assert "cancellation_requested" in events(caplog)


def test_a_request_after_the_completion_committed_changes_nothing(
    schema_url: str, checked: Conn
) -> None:
    returning = Returning({"ok": True})
    returning.gate.release.set()
    (task,) = submit(checked, "returning", {})
    with running_worker(schema_url, {"returning": returning.handler}):
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the success")
        result = request_cancellation(checked, task.id, grace=GRACE)
    assert result.outcome.value == "already_finished"
    assert task_states(checked, [task]) == [("succeeded", 1)]
    assert attempt_rows(checked, task) == [(1, "succeeded", None, None)]


def test_a_success_after_a_committed_request_still_stands(schema_url: str, checked: Conn) -> None:
    # The request commits first; the handler (which ignores the signal) then succeeds.
    returning = Returning({"ok": True})
    (task,) = submit(checked, "returning", {})
    with running_worker(schema_url, {"returning": returning.handler}) as probe:
        eventually(lambda: len(returning.gate.runs) == 1, "the handler")
        assert request_cancellation(checked, task.id, grace=GRACE).outcome.value == "requested"
        returning.gate.release.set()
        eventually(lambda: task_states(checked, [task]) == [("succeeded", 1)], "the success")
    assert rows(
        checked, "SELECT cancel_requested_at IS NOT NULL FROM tasks WHERE id = %s", task.id
    ) == [(True,)]
    assert probe.terminations == []


def test_a_grace_that_ends_before_the_next_heartbeat_fences_without_notice(
    schema_url: str, checked: Conn
) -> None:
    cooperative = Cooperative()
    (task,) = submit(checked, "cooperative", {})
    with running_worker(schema_url, {"cooperative": cooperative.handler}) as probe:
        eventually(lambda: len(cooperative.runs) == 1, "the handler")
        # A 1 µs grace has ended before any heartbeat can see the request.
        request_cancellation(checked, task.id, grace=timedelta(microseconds=1))
        eventually(lambda: probe.terminations, "the worker to fence itself")
    assert probe.terminations[0] == (ExitCode.FENCED, "ownership_lost")
    assert not cooperative.observed.is_set()  # no notice: it was never guaranteed
    assert attempt_rows(checked, task) == [(1, "running", None, None)]  # nothing recorded
    revocation = revoke_next_expired(checked)  # recovery then cancels it (DEC-040)
    assert revocation is not None
    assert task_states(checked, [task]) == [("cancelled", 1)]


def test_a_repeated_request_neither_extends_the_grace_nor_runs_the_handler_again(
    schema_url: str, checked: Conn
) -> None:
    cooperative = Cooperative(hold=True)
    (task,) = submit(checked, "cooperative", {})
    with running_worker(schema_url, {"cooperative": cooperative.handler}) as probe:
        eventually(lambda: len(cooperative.runs) == 1, "the handler")
        request_cancellation(checked, task.id, grace=GRACE)
        assert cooperative.observed.wait(WAIT)
        first_lease = lease_of(checked, task)
        again = request_cancellation(checked, task.id, grace=timedelta(hours=1))
        assert again.outcome.value == "already_requested"
        assert lease_of(checked, task) == first_lease  # not extended
        cooperative.proceed.set()
        eventually(lambda: task_states(checked, [task]) == [("cancelled", 1)], "the task")
    assert len(cooperative.runs) == 1
    assert probe.terminations == []


def test_a_cancelled_outcome_whose_answer_was_lost_is_reconciled(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    cooperative = Cooperative()
    (task,) = submit(checked, "cooperative", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"cooperative": cooperative.handler}) as probe,
    ):
        eventually(lambda: len(cooperative.runs) == 1, "the handler")
        proxy.drop_completion(committed=True)  # the cancelled completion commits, unheard
        request_cancellation(checked, task.id, grace=GRACE)
        eventually(lambda: "completion_reconciled" in events(caplog), "reconciliation")
    assert proxy.drop_count == 1
    assert task_states(checked, [task]) == [("cancelled", 1)]
    assert attempt_rows(checked, task) == [(1, "cancelled", None, None)]
    assert len(cooperative.runs) == 1
    assert probe.terminations == []


# --- graceful shutdown (DEC-049) ---------------------------------------------------------


def stopped(probe: Probe) -> bool:
    return not probe.thread.is_alive()


def draining(probe: Probe) -> bool:
    return probe.worker._watchdog.deadline(Guard.DRAIN) is not None


def untouched(db: Conn, task: Task) -> bool:
    """Shutdown fabricated nothing: no cancellation request and no recorded failure."""
    return rows(
        db,
        "SELECT t.cancel_requested_at IS NULL AND bool_and(a.failure_kind IS NULL) "
        "FROM tasks t JOIN attempts a ON a.task_id = t.id WHERE t.id = %s "
        "GROUP BY t.cancel_requested_at",
        task.id,
    ) == [(True,)]


def test_a_draining_worker_renews_records_and_claims_nothing_more(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    first, second = submit(checked, "gated", {"i": 1}, {"i": 2})
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(seconds=30)})
    with running_worker(schema_url, {"gated": gate.handler}, settings) as probe:
        eventually(lambda: len(gate.runs) == 1, "the first handler")
        probe.worker.request_stop()
        eventually(lambda: draining(probe), "the drain")
        # Same job, same created_at: which task is claimed first depends on its random id.
        (claimed,) = [t for t in (first, second) if running_attempt(checked, t)]
        lease = lease_of(checked, claimed)
        eventually(lambda: lease_of(checked, claimed) > lease, "a renewal while draining")
        gate.release.set()
        eventually(lambda: stopped(probe), "the worker to stop")
    assert probe.worker.finished
    assert sorted(task_states(checked, [first, second])) == [("queued", 0), ("succeeded", 1)]
    assert len(gate.runs) == 1
    assert probe.expiries == []
    assert probe.terminations == []


@pytest.mark.parametrize(("max_attempts", "after"), [(3, ("queued", 1)), (1, ("failed", 1))])
def test_a_drain_that_runs_out_leaves_the_attempt_to_recovery(
    schema_url: str, checked: Conn, max_attempts: int, after: tuple[str, int]
) -> None:
    gate = Gate()
    (task,) = submit(checked, "gated", {}, max_attempts=max_attempts)
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(seconds=1)})
    with running_worker(schema_url, {"gated": gate.handler}, settings) as probe:
        eventually(lambda: len(gate.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: stopped(probe), "the drain to run out")
        assert probe.terminations[0] == (ExitCode.SHUTDOWN, "drain_deadline_passed")
        assert all(expiry.guard is Guard.DRAIN for expiry in probe.expiries)
        assert task_states(checked, [task]) == [("running", 1)]
        assert untouched(checked, task)
        gate.release.set()  # too late: nothing supervises it any more
    # Recovery revokes it once its lease ends; it used one attempt (DEC-038).
    revocation = eventually(lambda: revoke_next_expired(checked), "the lease to end")
    assert revocation.attempt.revoked_reason is not None
    assert revocation.attempt.revoked_reason.value == "lease_expired"
    assert task_states(checked, [task]) == [after]
    assert untouched(checked, task)
    assert len(gate.runs) == 1


def test_a_lost_completion_answer_while_draining_is_reconciled(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    (task,) = submit(checked, "gated", {})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"gated": gate.handler}) as probe,
    ):
        eventually(lambda: len(gate.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: draining(probe), "the drain")
        proxy.drop_completion(committed=True)
        gate.release.set()
        eventually(lambda: stopped(probe), "the worker to stop")
    assert probe.worker.finished
    assert proxy.drop_count == 1
    assert task_states(checked, [task]) == [("succeeded", 1)]
    assert len(gate.runs) == 1
    assert probe.terminations == []


def test_an_outage_while_draining_ends_at_the_drain_deadline(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    (task,) = submit(checked, "gated", {})
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(seconds=2)})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"gated": gate.handler}, settings) as probe,
    ):
        eventually(lambda: len(gate.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: draining(probe), "the drain")
        proxy.drop_completion(committed=False, then_refuse=True)  # unreachable from now on
        gate.release.set()
        eventually(lambda: stopped(probe), "the drain to run out")
    # Long before the attempt's 5-minute deadline would have ended reconciliation.
    assert probe.terminations[0] == (ExitCode.SHUTDOWN, "drain_deadline_passed")
    assert task_states(checked, [task]) == [("running", 1)]  # nothing committed
    assert untouched(checked, task)


def test_a_supervisor_blocked_while_draining_is_ended_by_the_drain_guard(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    submit(checked, "gated", {})
    # The drain ends 0.5 s after the latch; any call started after it, 0.9 s after its start.
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(milliseconds=500)})
    with (
        PgProxy(schema_url) as proxy,
        running_worker(proxy.url(schema_url), {"gated": gate.handler}, settings) as probe,
    ):
        eventually(lambda: len(gate.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: draining(probe), "the drain")
        proxy.stall()  # the next supervisor call never gets an answer
        gate.release.set()
        eventually(lambda: probe.expiries, "the watchdog")
        assert probe.expiries[0].guard is Guard.DRAIN
        proxy.resume()
        eventually(lambda: stopped(probe), "the worker to end")


def test_a_cooperative_cancellation_while_draining_ends_cancelled(
    schema_url: str, checked: Conn
) -> None:
    cooperative = Cooperative()
    (task,) = submit(checked, "cooperative", {})
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(seconds=30)})
    with running_worker(schema_url, {"cooperative": cooperative.handler}, settings) as probe:
        eventually(lambda: len(cooperative.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: draining(probe), "the drain")
        assert not cooperative.observed.is_set()  # shutdown alone never tells it
        request_cancellation(checked, task.id, grace=GRACE)
        eventually(lambda: stopped(probe), "the worker to stop")
    assert cooperative.observed.is_set()
    assert task_states(checked, [task]) == [("cancelled", 1)]
    assert attempt_rows(checked, task) == [(1, "cancelled", None, None)]
    assert probe.terminations == []


def test_a_cancellation_grace_shorter_than_the_drain_fences_first(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()  # ignores the request
    (task,) = submit(checked, "gated", {})
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(seconds=30)})
    with running_worker(schema_url, {"gated": gate.handler}, settings) as probe:
        eventually(lambda: len(gate.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: draining(probe), "the drain")
        request_cancellation(checked, task.id, grace=timedelta(seconds=2))
        eventually(lambda: probe.expiries, "the fence")
        assert probe.expiries[0].guard is Guard.OWNERSHIP  # the grace, not the drain
        revocation = eventually(lambda: revoke_next_expired(checked), "the grace to run out")
        assert revocation.attempt.revoked_reason is not None
        assert revocation.attempt.revoked_reason.value == "cancellation_grace_expired"
        gate.release.set()  # its outcome is discarded: recovery already decided
        eventually(lambda: stopped(probe), "the worker to stop")
    assert task_states(checked, [task]) == [("cancelled", 1)]


def test_a_stop_during_an_in_flight_claim_leaves_the_attempt_to_recovery(
    schema_url: str, checked: Conn, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level("INFO", logger="orqestra.worker")
    gate = Gate()
    (task,) = submit(checked, "gated", {})
    with PgProxy(schema_url) as proxy:
        proxy.hold_on(b"INSERT INTO attempts")  # the claim, before it commits
        with running_worker(proxy.url(schema_url), {"gated": gate.handler}) as probe:
            assert proxy.held.wait(WAIT)
            probe.worker.request_stop()
            proxy.resume()  # the claim commits
            eventually(lambda: stopped(probe), "the worker to stop")
    assert probe.worker.finished
    assert gate.runs == []  # the claimed task's handler never started
    assert probe.terminations == []
    unstarted = running_attempt(checked, task)  # still running, never started
    assert unstarted is not None
    attempt_id = unstarted[0]
    warnings = [
        r for r in caplog.records if getattr(r, "event", "") == "stopped_with_unstarted_claim"
    ]
    assert [getattr(r, "attempt_id", None) for r in warnings] == [str(attempt_id)]
    revocation = eventually(lambda: revoke_next_expired(checked), "the lease to end")
    assert revocation.attempt.revoked_reason is not None
    assert revocation.attempt.revoked_reason.value == "lease_expired"
    assert task_states(checked, [task]) == [("queued", 1)]


def test_a_zero_shutdown_timeout_abandons_a_running_task_at_once(
    schema_url: str, checked: Conn
) -> None:
    gate = Gate()
    (task,) = submit(checked, "gated", {})
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(0)})
    with running_worker(schema_url, {"gated": gate.handler}, settings) as probe:
        eventually(lambda: len(gate.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: stopped(probe), "the worker to end")
        assert probe.terminations[0] == (ExitCode.SHUTDOWN, "drain_deadline_passed")
        gate.release.set()
    assert task_states(checked, [task]) == [("running", 1)]
    assert untouched(checked, task)


def test_shutdown_never_tells_the_handler_or_records_anything(
    schema_url: str, checked: Conn
) -> None:
    cooperative = Cooperative()
    (task,) = submit(checked, "cooperative", {})
    settings = SETTINGS.model_copy(update={"shutdown_timeout": timedelta(seconds=1)})
    with running_worker(schema_url, {"cooperative": cooperative.handler}, settings) as probe:
        eventually(lambda: len(cooperative.runs) == 1, "the handler")
        probe.worker.request_stop()
        eventually(lambda: stopped(probe), "the drain to run out")
    assert not cooperative.observed.is_set()
    assert probe.terminations[0] == (ExitCode.SHUTDOWN, "drain_deadline_passed")
    assert task_states(checked, [task]) == [("running", 1)]
    assert untouched(checked, task)
