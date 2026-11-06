"""A worker process for the process-level tests: ``python -m tests.integration.worker_process``.

Test-only. It runs a real ``Worker`` under ``run_until_signalled``, so SIGTERM
and SIGINT stop it gracefully (DEC-049); its default ``terminate`` and watchdog
end the process with ``os._exit``. It works against the URL in
``ORQESTRA_TEST_WORKER_URL`` (possibly through a ``PgProxy``), with timing
from the ``ORQESTRA_WORKER_*`` variables. Its handlers coordinate with the
test through their own connection to ``ORQESTRA_TEST_HANDLER_URL``:

- ``gated``: records the run in ``handler_runs``, then waits for the advisory
  lock named by the payload's ``gate``, which the test holds until it releases
  the handler;
- ``abort``: records the run, then raises ``SystemExit`` (a control-plane signal);
- ``cooperative``: records the run, then waits for its cancellation signal and
  raises ``HandlerCancelled`` once told (DEC-048);
- ``flaky``: records the run, then raises ``TransientError`` (DEC-053);
- ``metered``: makes provider calls through ``ProviderCalls`` and the worker's
  ``UsageSink`` (DEC-059): ``payload["first_attempt_calls"]`` calls in the first
  attempt, ``payload["calls"]`` in any later one. Its billable provider double
  first records, in ``provider_invocations``, whether the call's intent row had
  committed (read on its own connection); with ``payload["gate"]`` it then waits
  for that advisory lock, so the call stays in flight.
"""

import functools
import os
import uuid
from datetime import timedelta

import psycopg

from orqestra.calls.provider import (
    FieldKind,
    ProviderRequest,
    ProviderResponse,
    ProviderUsage,
    make_request,
)
from orqestra.calls.recording import MAX_CALL_SEQ
from orqestra.domain.errors import TransientError
from orqestra.domain.jobs import JsonValue
from orqestra.logging import configure_logging
from orqestra.worker.handlers import Handler, HandlerCancelled, HandlerRegistry, TaskContext
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.shutdown import run_until_signalled
from orqestra.worker.usage import UsageSink
from orqestra.workloads.invoice.verify import ProviderCalls

HANDLER_RUNS = (
    "CREATE TABLE handler_runs "
    "(attempt_id uuid NOT NULL, ran_at timestamptz NOT NULL DEFAULT clock_timestamp())"
)
PROVIDER_INVOCATIONS = (
    "CREATE TABLE provider_invocations (attempt_id uuid NOT NULL, call_seq integer NOT NULL, "
    "intent_committed boolean NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp())"
)
METERED_ROUTE = "route-a"
METERED_PROVIDER = "fake-provider"
_REQUEST: ProviderRequest = make_request(
    capability="interpret_amount",
    route_id=METERED_ROUTE,
    field_kind=FieldKind.TOTAL,
    raw="12",
    timeout=timedelta(seconds=5),
)
_INVOKED = (
    "INSERT INTO provider_invocations (attempt_id, call_seq, intent_committed) "
    "SELECT %s, %s, EXISTS (SELECT FROM provider_calls WHERE attempt_id = %s AND call_seq = %s)"
)


def _record_run(context: TaskContext) -> psycopg.Connection[tuple[object, ...]]:
    conn = psycopg.connect(os.environ["ORQESTRA_TEST_HANDLER_URL"], autocommit=True)
    conn.execute("INSERT INTO handler_runs (attempt_id) VALUES (%s)", (context.attempt_id,))
    return conn


def gated(context: TaskContext, payload: JsonValue) -> JsonValue:
    assert isinstance(payload, dict)
    with _record_run(context) as conn:
        conn.execute("SELECT pg_advisory_lock_shared(%s)", (payload["gate"],))
    return {"released": True}


def abort(context: TaskContext, payload: JsonValue) -> JsonValue:
    _record_run(context).close()
    raise SystemExit(9)


def cooperative(context: TaskContext, payload: JsonValue) -> JsonValue:
    _record_run(context).close()
    if context.cancellation.wait(60):
        raise HandlerCancelled
    return {"never": "told"}


def flaky(context: TaskContext, payload: JsonValue) -> JsonValue:
    _record_run(context).close()
    raise TransientError("try again later")


class _BillableDouble:
    """A billable provider double; it never reaches a real provider."""

    def __init__(
        self,
        conn: psycopg.Connection[tuple[object, ...]],
        attempt_id: uuid.UUID,
        calls: ProviderCalls,
        gate: object,
    ) -> None:
        self.conn, self.attempt_id, self.calls, self.gate = conn, attempt_id, calls, gate

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        seq = self.calls.made - 1  # ProviderCalls counts a call once its intent is recorded
        self.conn.execute(_INVOKED, (self.attempt_id, seq, self.attempt_id, seq))
        if self.gate is not None:
            self.conn.execute("SELECT pg_advisory_lock_shared(%s)", (self.gate,))
            self.conn.execute("SELECT pg_advisory_unlock_shared(%s)", (self.gate,))
        return ProviderResponse(value="12", usage=ProviderUsage("0.25", "tokens"))


def metered(sink: UsageSink) -> Handler:
    def handle(context: TaskContext, payload: JsonValue) -> JsonValue:
        assert isinstance(payload, dict)
        key = "calls" if context.attempt_number > 1 else "first_attempt_calls"
        count = payload.get(key, payload["calls"])
        assert isinstance(count, int)
        calls = ProviderCalls(
            attempt_id=context.attempt_id, cap=MAX_CALL_SEQ, recorder=sink.recorder
        )
        with psycopg.connect(os.environ["ORQESTRA_TEST_HANDLER_URL"], autocommit=True) as conn:
            provider = _BillableDouble(conn, context.attempt_id, calls, payload.get("gate"))
            for _ in range(count):
                if context.cancellation.is_set():
                    raise HandlerCancelled
                calls.call(provider, _REQUEST, item_index=calls.made, estimated_cost_units=3)
        return {"calls": calls.made}

    return handle


def main() -> None:
    configure_logging("INFO")
    settings = WorkerSettings(_env_file=None)
    sink = UsageSink(["metered"], {METERED_ROUTE: METERED_PROVIDER}, prompt_version="test-v1")
    registry = HandlerRegistry(
        {
            "gated": gated,
            "abort": abort,
            "cooperative": cooperative,
            "flaky": flaky,
            "metered": metered(sink),
        }
    )
    url = os.environ["ORQESTRA_TEST_WORKER_URL"]
    worker = Worker(
        registry=registry,
        settings=settings,
        connect=functools.partial(connect_supervisor, url, settings),
        usage=sink,
    )
    run_until_signalled(worker)  # until a signal stops it, or the process ends


if __name__ == "__main__":
    main()
