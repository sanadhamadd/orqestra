"""An idle worker waits out a slow database; an owning one still ends itself (DEC-074).

A real ``Worker`` runs in this process against PostgreSQL, as in
``test_worker_runtime.py``: its watchdog only records, and ``terminate`` raises. The
database is made slow in two ways: ``PgProxy`` holds every byte (a frozen server, as
TCP sees one), or a test trigger keeps a claim's statement running on the server
past the operation bound. Every assertion about what was claimed reads PostgreSQL.
"""

import functools
import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, NoReturn

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.domain.jobs import JsonValue
from orqestra.worker.fence import ExitCode, Expiry, Guard, Watchdog
from orqestra.worker.handlers import HandlerRegistry, TaskContext
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from tests.integration.pg_proxy import PgProxy
from tests.integration.test_worker_runtime import (
    SETTINGS,
    WAIT,
    Gate,
    Probe,
    Terminated,
    events,
    eventually,
    rows,
    submit,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

BOUND = SETTINGS.db_operation_timeout.total_seconds()
# Only the claim's first statement names the job kinds this way.
CLAIM = b"jobs.kind = ANY("
# Only a heartbeat's renewal sets the lease like this.
RENEWAL = b"SET lease_expires_at = $"


def echo(ctx: TaskContext, payload: JsonValue) -> JsonValue:
    return {"echo": payload}


@contextmanager
def running(
    connect: Callable[[], Any],
    handlers: dict[str, Callable[[TaskContext, JsonValue], JsonValue]],
    settings: WorkerSettings = SETTINGS,
) -> Iterator[Probe]:
    """A worker on ``connect``, run until the test ends (``running_worker``'s twin)."""
    expiries: list[Expiry] = []
    terminations: list[tuple[ExitCode, str]] = []

    def terminate(code: ExitCode, event: str, message: str, context: Any) -> NoReturn:
        terminations.append((code, event))
        raise Terminated(event)

    worker = Worker(
        registry=HandlerRegistry(handlers),
        settings=settings,
        connect=connect,
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


def supervisor(url: str, settings: WorkerSettings = SETTINGS) -> Callable[[], Any]:
    return functools.partial(connect_supervisor, url, settings)


def succeeded(db: Conn) -> int:
    return int(rows(db, "SELECT count(*) FROM tasks WHERE state = 'succeeded'")[0][0])


@pytest.fixture
def worker_logs(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.INFO, logger="orqestra.worker")
    return caplog


def test_an_idle_claim_held_past_its_bound_is_abandoned_and_the_worker_claims_after(
    schema_url: str, db: Conn, worker_logs: pytest.LogCaptureFixture
) -> None:
    with PgProxy(schema_url) as proxy:
        proxy.hold_on(CLAIM)  # the next claim stalls, and with it every connection
        with running(supervisor(proxy.url(schema_url)), {"echo": echo}) as probe:
            try:
                eventually(
                    lambda: "idle_operation_abandoned" in events(worker_logs),
                    "the abandoned claim",
                )
            finally:
                proxy.resume()  # before the limit: the settle must reach the server
            submit(db, "echo", {"n": 1})
            eventually(lambda: succeeded(db) == 1, "the task, claimed after the stall")
            assert probe.expiries == []
            assert probe.terminations == []
            seen = events(worker_logs)
            assert seen.index("idle_operation_abandoned") < seen.index("claim_uncertain")
    assert rows(db, "SELECT count(*) FROM attempts WHERE state = 'running'") == [(0,)]


def test_a_claim_still_running_on_the_server_is_ended_and_never_commits(
    schema_url: str, db: Conn, worker_logs: pytest.LogCaptureFixture
) -> None:
    # The claim's insert sleeps 10 bounds: past the abort, the back-off, the reconnect
    # and the termination, even on a loaded machine.
    db.execute(
        "CREATE FUNCTION slow_claim() RETURNS trigger LANGUAGE plpgsql AS "
        f"$$ BEGIN PERFORM pg_sleep({10 * BOUND}); RETURN NEW; END $$"
    )
    db.execute(
        "CREATE TRIGGER slow_claim BEFORE INSERT ON attempts "
        "FOR EACH ROW EXECUTE FUNCTION slow_claim()"
    )
    (task,) = submit(db, "echo", {"n": 1})
    connect_plain = supervisor(schema_url)

    def without_a_statement_timeout() -> Any:
        conn = connect_plain()
        conn.execute("SET statement_timeout = 0")  # so the server never ends it itself
        return conn

    with running(without_a_statement_timeout, {"echo": echo}) as probe:
        eventually(lambda: "claim_uncertain" in events(worker_logs), "the settled claim")
        # The abandoned claim's backend is gone, and its insert never committed.
        assert rows(
            db,
            "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'orqestra-worker' "
            "AND state = 'active' AND query LIKE 'INSERT INTO attempts%%'",
        ) == [(0,)]
        assert rows(db, "SELECT count(*) FROM attempts") == [(0,)]
        assert rows(db, "SELECT state, attempt_count FROM tasks WHERE id = %s", task.id) == [
            ("queued", 0)
        ]
        db.execute("DROP TRIGGER slow_claim ON attempts")
        eventually(lambda: succeeded(db) == 1, "the task, claimed after the hold")
        assert probe.expiries == []
        assert probe.terminations == []
    assert rows(db, "SELECT attempt_number, state FROM attempts") == [(1, "succeeded")]


def test_idle_stalls_up_to_the_limit_end_the_process_with_exit_4(
    schema_url: str, worker_logs: pytest.LogCaptureFixture
) -> None:
    settings = SETTINGS.model_copy(update={"idle_stall_limit": 2})
    with PgProxy(schema_url) as proxy:
        proxy.hold_on(CLAIM)  # never resumed while the worker runs: the database stays frozen
        try:
            with running(supervisor(proxy.url(schema_url), settings), {}, settings) as probe:
                eventually(lambda: probe.terminations, "the stall limit")
                assert probe.terminations == [(ExitCode.STALLED, "idle_stall_limit_reached")]
                assert probe.expiries == []  # each stall was abandoned, none fatal
                assert events(worker_logs).count("idle_operation_abandoned") == 1
        finally:
            proxy.resume()


def test_a_stalled_heartbeat_while_a_task_is_owned_still_ends_the_process(
    schema_url: str, db: Conn
) -> None:
    gate = Gate()
    submit(db, "gated", {})
    with (
        PgProxy(schema_url) as proxy,
        running(supervisor(proxy.url(schema_url)), {"gated": gate.handler}) as probe,
    ):
        try:
            eventually(lambda: len(gate.runs) == 1, "the handler to start")
            proxy.hold_on(RENEWAL)
            (expiry,) = eventually(lambda: probe.expiries, "the operation guard")
            assert expiry.guard is Guard.OPERATION  # DEC-045's fail-fast, unchanged
            assert expiry.context["operation"] == "renew_lease"
        finally:
            proxy.resume()
            gate.release.set()
