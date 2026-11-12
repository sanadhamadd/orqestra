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

With ``--provider openrouter`` the route is answered by the OpenRouter adapter
instead (DEC-071, ``benchmarks.live``): only with ``--dry-run`` (canned responses,
no network, no cost) or ``--allow-live-calls`` (real, billable calls, which need the
owner's separate approval), always with ``--model``, ``--max-provider-calls`` and a
``--budget-file`` the harness has already created. Every call then reserves one of
the cell's calls before its intent is recorded; with none left the attempt fails
closed (``ProviderCallBudgetExhausted``). A dry run never reads the key; a live run
reads ``ORQESTRA_OPENROUTER_API_KEY`` from the environment, never from ``.env``.

Before it connects, it checks the provider flags, then with
``check_provider_call_budget`` that the call cap, each call's timeout and the
usage handoffs fit in the execution timeout, and exits 1 if they do not.
"""

import argparse
import functools
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import timedelta
from pathlib import Path

from pydantic import TypeAdapter, ValidationError

from benchmarks.live import (
    DOUBLE,
    BudgetedRecorder,
    CallBudget,
    OpenRouterWiring,
    ProviderChoice,
    ProviderName,
    canned_requests_line,
    openrouter_wiring,
)
from benchmarks.provider import SeparatorReader
from orqestra.calls.provider import Provider
from orqestra.calls.recording import ProviderCallRecorder
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
    parser.add_argument(
        "--provider",
        type=ProviderName,
        choices=list(ProviderName),
        default=ProviderName.DOUBLE,
        help="what answers the calls: the offline double (default) or openrouter (DEC-071)",
    )
    parser.add_argument("--model", help="openrouter: the model slug")
    parser.add_argument(
        "--max-provider-calls", type=int, help="openrouter: the most calls the whole cell makes"
    )
    parser.add_argument(
        "--budget-file",
        type=Path,
        help="openrouter: the cell's call counter, created by the harness",
    )
    parser.add_argument(
        "--allow-live-calls",
        action="store_true",
        help="openrouter: make real, billable calls (needs the owner's approval)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="openrouter: canned responses, no network, no cost"
    )
    return parser


def choice_of(args: argparse.Namespace) -> ProviderChoice:
    return ProviderChoice(
        name=args.provider,
        model=args.model,
        max_provider_calls=args.max_provider_calls,
        allow_live_calls=args.allow_live_calls,
        dry_run=args.dry_run,
    )


def provider_refusal(
    choice: ProviderChoice, budget_file: Path | None, environ: Mapping[str, str]
) -> str | None:
    """Why the provider flags may not run, or ``None``; checked before anything is built."""
    refusal = choice.refusal(environ)
    if refusal is not None:
        return refusal
    if choice.openrouter and (budget_file is None or not budget_file.is_file()):
        return "the call budget file must already exist; the harness creates it (--budget-file)"
    if not choice.openrouter and budget_file is not None:
        return "--budget-file applies only to --provider openrouter"
    return None


def build_worker(
    url: str,
    settings: WorkerSettings,
    *,
    cap: int,
    call_timeout: timedelta,
    choice: ProviderChoice = DOUBLE,
    budget_file: Path | None = None,
    environ: Mapping[str, str] = os.environ,
    wiring: list[OpenRouterWiring] | None = None,
) -> Worker:
    """A worker running only ``invoice_verification``, its provider behind a ``UsageSink``.

    The double by default. For OpenRouter the sink's recorder is wrapped in a
    ``BudgetedRecorder`` on ``budget_file``; the wiring is appended to ``wiring``
    when given, so the caller can read a dry run's request count.
    """
    provider: Provider
    recorder: ProviderCallRecorder
    if choice.openrouter:
        assert budget_file is not None
        assert choice.max_provider_calls is not None
        wired = openrouter_wiring(choice, ROUTE, environ)
        if wiring is not None:
            wiring.append(wired)
        provider = wired.provider
        sink = UsageSink(
            [JOB_KIND],
            {ROUTE: wired.provider_name},
            {ROUTE: wired.model},
            prompt_version=wired.prompt_version,
        )
        recorder = BudgetedRecorder(
            sink.recorder, CallBudget(budget_file, choice.max_provider_calls)
        )
    else:
        provider = SeparatorReader()
        sink = UsageSink([JOB_KIND], {ROUTE: PROVIDER}, prompt_version=PROMPT_VERSION)
        recorder = sink.recorder
    handler = invoice_verification_handler(
        {ROUTE: provider},
        [RouteOption(ROUTE, frozenset({REQUIRED_CAPABILITY}), estimated_cost_units=1)],
        call_timeout,
        max_provider_calls_per_attempt=cap,
        recorder=recorder,
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
    choice = choice_of(args)
    refusal = provider_refusal(choice, args.budget_file, os.environ)
    if refusal is not None:
        return _refuse(refusal)
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
    wiring: list[OpenRouterWiring] = []
    worker = build_worker(
        url,
        settings,
        cap=args.cap,
        call_timeout=args.call_timeout,
        choice=choice,
        budget_file=args.budget_file,
        wiring=wiring,
    )
    run_until_signalled(worker)  # until a signal stops it, or the process ends
    for wired in wiring:
        line = canned_requests_line(wired.canned)
        if line is not None:
            print(line, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
