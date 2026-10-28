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
  raises ``HandlerCancelled`` once told (DEC-048).
"""

import functools
import os

import psycopg

from orqestra.domain.jobs import JsonValue
from orqestra.logging import configure_logging
from orqestra.worker.handlers import HandlerCancelled, HandlerRegistry, TaskContext
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.shutdown import run_until_signalled

HANDLER_RUNS = (
    "CREATE TABLE handler_runs "
    "(attempt_id uuid NOT NULL, ran_at timestamptz NOT NULL DEFAULT clock_timestamp())"
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


def main() -> None:
    configure_logging("INFO")
    settings = WorkerSettings(_env_file=None)
    registry = HandlerRegistry({"gated": gated, "abort": abort, "cooperative": cooperative})
    url = os.environ["ORQESTRA_TEST_WORKER_URL"]
    worker = Worker(
        registry=registry,
        settings=settings,
        connect=functools.partial(connect_supervisor, url, settings),
    )
    run_until_signalled(worker)  # until a signal stops it, or the process ends


if __name__ == "__main__":
    main()
