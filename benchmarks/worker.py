"""The benchmark worker for the ``invoice`` profile: ``python -m benchmarks.worker``.

Not packaged, and registered nowhere: ``orqestra worker run`` still runs only
the diagnostics (DEC-062). It is built like the process-level test worker
(``tests/integration/worker_process.py``): a real ``Worker`` under
``run_until_signalled``, so SIGTERM and SIGINT stop it gracefully (DEC-049),
with its timing from the ``ORQESTRA_WORKER_*`` variables and its database from
``ORQESTRA_DATABASE_URL``, exactly as the shipped command reads them.

It runs ``invoice_verification_handler`` on one route, ``route-a``, answered by
``SeparatorReader``. That double is billable, so every call goes through the
worker's ``UsageSink`` into the usage ledger at tier T3 (DEC-057 to DEC-059),
as a real provider's would. It answers at once and never touches a network,
so nothing measured with it says anything about a real provider's latency,
cost or reliability.

Before it connects, it checks with ``check_provider_call_budget`` that the
call cap, each call's timeout and the usage handoffs fit in the execution
timeout, and exits 1 if they do not.
"""

import argparse
import functools
import json
import os
import sys
from collections.abc import Sequence
from datetime import timedelta

from pydantic import TypeAdapter, ValidationError

from benchmarks.provider import SeparatorReader
from orqestra.calls.routing import RouteOption
from orqestra.logging import configure_logging
from orqestra.worker.handlers import HandlerRegistry
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.shutdown import run_until_signalled
from orqestra.worker.usage import UsageSink, check_provider_call_budget
from orqestra.workloads.invoice.handler import JOB_KIND, invoice_verification_handler
from orqestra.workloads.invoice.verify import REQUIRED_CAPABILITY

DATABASE_URL_ENV = "ORQESTRA_DATABASE_URL"
ROUTE = "route-a"
PROVIDER = "bench-double"
PROMPT_VERSION = "bench-v1"
DEFAULT_CAP = 16
DEFAULT_CALL_TIMEOUT = "PT0.1S"

_DURATION = TypeAdapter(timedelta)


def parse_duration(text: str) -> timedelta:
    """An ISO 8601 duration such as ``PT0.1S``, parsed as the worker settings parse theirs."""
    try:
        return _DURATION.validate_python(text)
    except ValidationError:
        msg = f"not an ISO 8601 duration: {text!r}"
        raise argparse.ArgumentTypeError(msg) from None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.worker", description=__doc__)
    parser.add_argument(
        "--cap", type=int, default=DEFAULT_CAP, help="provider calls allowed per attempt"
    )
    parser.add_argument(
        "--call-timeout",
        type=parse_duration,
        default=parse_duration(DEFAULT_CALL_TIMEOUT),
        help="each provider call's timeout, as an ISO 8601 duration",
    )
    return parser


def build_worker(
    url: str, settings: WorkerSettings, *, cap: int, call_timeout: timedelta
) -> Worker:
    """A worker running only ``invoice_verification``, with the double behind a ``UsageSink``."""
    sink = UsageSink([JOB_KIND], {ROUTE: PROVIDER}, prompt_version=PROMPT_VERSION)
    handler = invoice_verification_handler(
        {ROUTE: SeparatorReader()},
        [RouteOption(ROUTE, frozenset({REQUIRED_CAPABILITY}), estimated_cost_units=1)],
        call_timeout,
        max_provider_calls_per_attempt=cap,
        recorder=sink.recorder,
    )
    return Worker(
        registry=HandlerRegistry({JOB_KIND: handler}),
        settings=settings,
        connect=functools.partial(connect_supervisor, url, settings),
        usage=sink,
    )


def _refuse(message: str) -> int:
    print(json.dumps({"status": "error", "error": message}))
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        settings = WorkerSettings(_env_file=None)
    except ValidationError as exc:
        fields = sorted({".".join(map(str, error["loc"])) for error in exc.errors()})
        return _refuse(f"invalid ORQESTRA_WORKER_* settings: {', '.join(fields)}")
    try:
        check_provider_call_budget(
            settings,
            max_provider_calls_per_attempt=args.cap,
            provider_call_timeout=args.call_timeout,
        )
    except (TypeError, ValueError) as exc:
        return _refuse(f"refused: {exc}")
    url = os.environ.get(DATABASE_URL_ENV, "").strip()
    if not url:
        return _refuse(f"{DATABASE_URL_ENV} is not set")
    configure_logging("INFO")
    worker = build_worker(url, settings, cap=args.cap, call_timeout=args.call_timeout)
    run_until_signalled(worker)  # until a signal stops it, or the process ends
    return 0


if __name__ == "__main__":
    sys.exit(main())
