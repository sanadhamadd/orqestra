"""Deterministic provider faults for the poison-input drill (D4), and the worker that has them.

``FaultingReader`` answers exactly as ``SeparatorReader`` does, except for two
kinds of marked raw strings:

- **always**: every call raises ``ProviderTransientError(UNAVAILABLE)``, so the
  batch that holds it fails each attempt and ends ``failed`` once its attempts
  are used up;
- **once**: the first call, across every process, raises the same error, and
  every later call is answered. "First" is decided by PostgreSQL: the reader
  inserts the raw string into ``bench_provider_faults`` with
  ``ON CONFLICT DO NOTHING RETURNING`` and faults only when that created the
  row. A provider is built per process and request ids are pure functions of
  content, so no per-process state could say "first attempt only".

A marked string writes a total's digits one per group (``poison_total``):
``1_2_3_4`` for always and ``1 2 3 4`` for once. The generator always groups
digits by three, so it never produces either, and the reader still reads both
to the total's digits.

``python -m benchmarks.faults`` is the benchmark worker of ``benchmarks.worker``
with this reader in place of ``SeparatorReader``; nothing else differs. Its
fault table lives in the run schema its ``ORQESTRA_DATABASE_URL`` selects
(``create_fault_table``). Like the double, it never touches a network.
"""

import argparse
import functools
import json
import os
import sys
from collections.abc import Sequence
from datetime import timedelta
from enum import StrEnum
from typing import Any

import psycopg
from pydantic import ValidationError

from benchmarks.provider import SEPARATORS, SeparatorReader
from benchmarks.worker import (
    DATABASE_URL_ENV,
    DEFAULT_CALL_TIMEOUT,
    DEFAULT_CAP,
    PROMPT_VERSION,
    PROVIDER,
    ROUTE,
    parse_duration,
)
from orqestra.calls.provider import (
    ProviderRequest,
    ProviderResponse,
    ProviderTransientError,
    TransientCode,
)
from orqestra.calls.routing import RouteOption
from orqestra.logging import configure_logging
from orqestra.worker.handlers import HandlerRegistry
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.shutdown import run_until_signalled
from orqestra.worker.usage import UsageSink, check_provider_call_budget
from orqestra.workloads.invoice.handler import JOB_KIND, invoice_verification_handler
from orqestra.workloads.invoice.verify import REQUIRED_CAPABILITY

FAULT_TABLE = "bench_provider_faults"
_CREATE = (
    f"CREATE TABLE {FAULT_TABLE} "
    "(raw text PRIMARY KEY, faulted_at timestamptz NOT NULL DEFAULT clock_timestamp())"
)
_CLAIM_FAULT = f"INSERT INTO {FAULT_TABLE} (raw) VALUES (%s) ON CONFLICT DO NOTHING RETURNING raw"
_ALWAYS_SEPARATOR = "_"
_ONCE_SEPARATOR = " "
assert {_ALWAYS_SEPARATOR, _ONCE_SEPARATOR} <= SEPARATORS


class Fault(StrEnum):
    ALWAYS = "always"
    ONCE = "once"


def poison_total(total: int, fault: Fault) -> str:
    """``total`` written with one digit per group, marked for ``fault``; needs two digits."""
    digits = str(total)
    if isinstance(total, bool) or total < 10:
        msg = "a poison total needs at least two digits"
        raise ValueError(msg)
    separator = _ALWAYS_SEPARATOR if fault is Fault.ALWAYS else _ONCE_SEPARATOR
    return separator.join(digits)


def fault_of(raw: str) -> Fault | None:
    """Which fault ``raw`` is marked for, if any."""
    for fault, separator in ((Fault.ALWAYS, _ALWAYS_SEPARATOR), (Fault.ONCE, _ONCE_SEPARATOR)):
        parts = raw.split(separator)
        if len(parts) >= 2 and all(len(part) == 1 and part.isdigit() for part in parts):
            return fault
    return None


def create_fault_table(conn: psycopg.Connection[Any]) -> None:
    """Create the shared fault table in ``conn``'s current schema (a run schema)."""
    conn.execute(_CREATE)


class FaultingReader:
    """``SeparatorReader`` with the marked faults; ``url`` selects the fault table's schema."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._reader = SeparatorReader()
        self._conn: psycopg.Connection[Any] | None = None

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        fault = fault_of(request.raw)
        if fault is Fault.ALWAYS or (fault is Fault.ONCE and self._first(request.raw)):
            raise ProviderTransientError(TransientCode.UNAVAILABLE)
        return self._reader.interpret(request)

    def _first(self, raw: str) -> bool:
        """Whether this call is the first for ``raw`` in any process, decided by PostgreSQL."""
        if self._conn is None:  # one connection per process, used by the handler thread only
            self._conn = psycopg.connect(self._url, autocommit=True, connect_timeout=5)
        return self._conn.execute(_CLAIM_FAULT, (raw,)).fetchone() is not None


def build_worker(
    url: str, settings: WorkerSettings, *, cap: int, call_timeout: timedelta
) -> Worker:
    """``benchmarks.worker.build_worker`` with ``FaultingReader`` as the route's provider."""
    sink = UsageSink([JOB_KIND], {ROUTE: PROVIDER}, prompt_version=PROMPT_VERSION)
    handler = invoice_verification_handler(
        {ROUTE: FaultingReader(url)},
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
    parser = argparse.ArgumentParser(prog="python -m benchmarks.faults", description=__doc__)
    parser.add_argument("--cap", type=int, default=DEFAULT_CAP)
    parser.add_argument(
        "--call-timeout", type=parse_duration, default=parse_duration(DEFAULT_CALL_TIMEOUT)
    )
    args = parser.parse_args(argv)
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
    run_until_signalled(build_worker(url, settings, cap=args.cap, call_timeout=args.call_timeout))
    return 0


if __name__ == "__main__":
    sys.exit(main())
