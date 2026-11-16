"""The plain-script baseline (DEC-072): ``python -m benchmarks.baseline``.

It sends the same documents as the extraction workload to the same adapter, without
Orqestra: no database, no worker, no lease. ``--mode loop`` handles one document after
another; ``--mode threads`` hands them to a thread pool of ``--workers`` threads. The
result carries the extraction workload's figures (timing, call latency, provider time,
accuracy, tokens, cost, retries and errors by code), so the two compare cell for cell.

Each document gets up to ``MAX_ATTEMPTS`` calls: a transient error (``timeout``,
``rate_limited``, ``unavailable``) is retried after the benchmark worker's retry delay,
doubled each time up to its maximum; any other error ends the document. Before every
call one call is reserved from the run's counter (``benchmarks.live.CallBudget``,
DEC-071), so ``--max-provider-calls`` bounds what the run can send; once it is spent
the remaining documents are not called.

The extraction itself (documents, the adapter call, the strict parse and the oracle's
scoring) is the extraction workload's. ``Extraction`` is the shape this module needs
from it; ``connect`` binds it, and until that workload is part of the code base the
command refuses. Only counts and timings are written: never a key, prompt or reply.
"""

import argparse
import json
import os
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from benchmarks import metrics
from benchmarks.generate import DEFAULT_SEED
from benchmarks.live import (
    BUDGET_EXHAUSTED,
    BUDGET_FILE_NAME,
    OPENROUTER_USAGE_UNIT,
    CallBudget,
    ProviderCallBudgetExhausted,
    ProviderChoice,
    ProviderName,
    create_budget_file,
)
from benchmarks.result import RAW_DIR, DirtyTreeError, code_state, now
from benchmarks.run import BENCH_WORKER_SETTINGS, MAX_ATTEMPTS, refuse_dirty
from benchmarks.worker import parse_duration
from orqestra.calls.provider import ProviderTransientError, TransientCode
from orqestra.domain.errors import PermanentError

FORMAT = "baseline-1"
CALL_TIMEOUT = "PT30S"
PER_MILLION = Decimal(1_000_000)
RETRY_BASE = parse_duration(BENCH_WORKER_SETTINGS["ORQESTRA_WORKER_RETRY_BASE_DELAY"])
RETRY_MAX = parse_duration(BENCH_WORKER_SETTINGS["ORQESTRA_WORKER_RETRY_MAX_DELAY"])
FAILED = "failed"  # no answer: a permanent error, retries used up, or the counter spent
NEEDS_REVIEW = "needs_review"
STATUSES = ("verified", "rejected", NEEDS_REVIEW, "malformed", FAILED)
UNEXPECTED = "unexpected_error"
RETRIES_EXHAUSTED = "retries_exhausted"


class Mode(StrEnum):
    LOOP = "loop"
    THREADS = "threads"


# --- what the extraction workload provides --------------------------------------------------


class Document(Protocol):
    @property
    def text(self) -> str: ...

    @property
    def expected(self) -> object: ...


@dataclass(frozen=True, slots=True)
class Reply:
    """One answered call: what the adapter reported, never what the model wrote."""

    content: str | None
    input_tokens: int | None
    output_tokens: int | None
    latency_ms: int
    credits: Decimal | None


@dataclass(frozen=True, slots=True)
class Score:
    """A reply judged against the oracle."""

    status: str  # verified, rejected, needs_review or malformed
    reason: str | None  # a closed code for needs_review
    fields: Mapping[str, tuple[int, int]]  # per scored field: (matched, expected)
    exact: bool  # every field right, same line count
    status_matched: bool  # the status the oracle expects


class Extraction(Protocol):
    """The extraction workload's parts, as the baseline uses them."""

    @property
    def fields(self) -> Sequence[str]:
        """The scored field names."""
        ...

    def documents(self, count: int, seed: int) -> Sequence[Document]: ...

    def call(self, text: str) -> Reply: ...

    def score(self, expected: object, content: str | None) -> Score: ...


@dataclass(frozen=True, slots=True)
class Prices:
    """Per million tokens. ``canned`` marks a dry run's fixed prices."""

    input: Decimal
    output: Decimal
    canned: bool


class NotConnectedError(RuntimeError):
    """The extraction workload is not part of this code base yet."""


def connect(choice: ProviderChoice, call_timeout: str, environ: Mapping[str, str]) -> Extraction:
    """The extraction workload on ``choice``'s adapter."""
    msg = "the extraction workload is not connected to the baseline yet"
    raise NotConnectedError(msg)


# --- one document --------------------------------------------------------------------------


@dataclass(slots=True)
class Outcome:
    """What happened to one document."""

    status: str = FAILED
    reason: str | None = None
    score: Score | None = None
    latencies_ms: list[int] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    without_counts: int = 0  # answered calls that reported no token counts
    credits: Decimal = Decimal(0)
    transient: Counter[str] = field(default_factory=Counter)
    permanent: str | None = None
    seconds: float = 0.0


Sleep = Callable[[float], None]


def retry_delay(retry: int) -> float:
    """The wait before retry ``retry`` (1-based): the worker's base delay, doubled, capped."""
    seconds: float = RETRY_BASE.total_seconds() * 2.0 ** (retry - 1)
    return min(seconds, RETRY_MAX.total_seconds())


class Exhausted:
    """Set once the counter is spent, so no later document reserves a call."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def __bool__(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()


def handle(
    document: Document,
    extraction: Extraction,
    budget: CallBudget,
    exhausted: Exhausted,
    sleep: Sleep,
) -> Outcome:
    """Call the provider for ``document`` until it answers, fails for good or has had
    ``MAX_ATTEMPTS`` calls, then score the answer."""
    outcome = Outcome()
    began = time.perf_counter()
    answered = False
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if exhausted:
            outcome.permanent = outcome.permanent or BUDGET_EXHAUSTED
            break
        try:
            budget.reserve()
        except ProviderCallBudgetExhausted:
            exhausted.set()
            outcome.permanent = BUDGET_EXHAUSTED
            break
        try:
            reply = extraction.call(document.text)
        except ProviderTransientError as exc:
            outcome.transient[exc.code.value] += 1
            if attempt < MAX_ATTEMPTS:
                sleep(retry_delay(attempt))
            continue
        except PermanentError as exc:
            outcome.permanent = str(exc)
            break
        except Exception:  # an adapter defect ends this document, not the run
            outcome.permanent = UNEXPECTED
            break
        outcome.latencies_ms.append(reply.latency_ms)
        if reply.input_tokens is None or reply.output_tokens is None:
            outcome.without_counts += 1
        else:
            outcome.input_tokens += reply.input_tokens
            outcome.output_tokens += reply.output_tokens
        if reply.credits is not None:
            outcome.credits += reply.credits
        score = extraction.score(document.expected, reply.content)
        outcome.status, outcome.reason, outcome.score = score.status, score.reason, score
        answered = True
        break
    if not answered and outcome.permanent is None:
        outcome.permanent = RETRIES_EXHAUSTED
    outcome.seconds = time.perf_counter() - began
    return outcome


# --- one run -------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Run:
    outcomes: list[Outcome]
    total_s: float
    calls: int


def run_documents(
    documents: Sequence[Document],
    extraction: Extraction,
    *,
    mode: Mode,
    workers: int,
    budget: CallBudget,
    sleep: Sleep = time.sleep,
) -> Run:
    """Handle every document in order (``loop``) or on ``workers`` threads (``threads``)."""
    exhausted = Exhausted()

    def one(document: Document) -> Outcome:
        return handle(document, extraction, budget, exhausted, sleep)

    began = time.perf_counter()
    if mode is Mode.LOOP:
        outcomes = [one(document) for document in documents]
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="baseline") as pool:
            outcomes = list(pool.map(one, documents))
    total = time.perf_counter() - began
    calls = sum(len(outcome.latencies_ms) + sum(outcome.transient.values()) for outcome in outcomes)
    return Run(outcomes, total, calls)


def _share(part: float, whole: float) -> float | None:
    return part / whole if whole > 0 else None


def extraction_figures(run: Run, prices: Prices, fields: Sequence[str] = ()) -> dict[str, Any]:
    """The extraction block of the result, with the extraction workload's names.

    ``fields`` are the scored field names; a document without an answer counts each as
    expected and unmatched, as the workload does. The baseline has no attempts, so the
    workload's split by attempt outcome has no counterpart here.
    """
    outcomes = run.outcomes
    documents = len(outcomes)
    latencies = [latency for outcome in outcomes for latency in outcome.latencies_ms]
    provider_s = sum(latencies) / 1000
    handler_s = sum(outcome.seconds for outcome in outcomes)
    per_field: dict[str, list[int]] = {name: [0, 0] for name in fields}
    for outcome in outcomes:
        scored = outcome.score.fields if outcome.score is not None else {}
        for name in set(per_field) | set(scored):
            matched, expected = scored.get(name, (0, 1))
            tally = per_field.setdefault(name, [0, 0])
            tally[0] += matched
            tally[1] += expected
    matched = sum(tally[0] for tally in per_field.values())
    expected = sum(tally[1] for tally in per_field.values())
    input_tokens = sum(outcome.input_tokens for outcome in outcomes)
    output_tokens = sum(outcome.output_tokens for outcome in outcomes)
    total = (input_tokens * prices.input + output_tokens * prices.output) / PER_MILLION
    credits = sum((outcome.credits for outcome in outcomes), Decimal(0))
    transient: Counter[str] = Counter()
    for outcome in outcomes:
        transient.update(outcome.transient)
    permanent = Counter(outcome.permanent for outcome in outcomes if outcome.permanent)
    review = Counter(
        outcome.reason or "unspecified" for outcome in outcomes if outcome.status == NEEDS_REVIEW
    )
    statuses = Counter({name: 0 for name in STATUSES})
    statuses.update(outcome.status for outcome in outcomes)
    answered = len(latencies)
    return {
        "documents": documents,
        "calls": {
            "total": run.calls,
            "answered": answered,
            "failed": run.calls - answered,
            "unresolved": 0,
            "latency_ms": {"n": answered, **_ranks(latencies)},
        },
        "time_split": {
            "provider_s": provider_s,
            "handler_s": handler_s,
            "provider_share": provider_s / handler_s if handler_s > 0 else None,
        },
        "accuracy": {
            "matched": matched,
            "expected": expected,
            "overall": matched / expected if expected else None,
            "per_field": {
                name: {"matched": tally[0], "expected": tally[1]}
                for name, tally in sorted(per_field.items())
            },
            "exact_documents": sum(
                1 for outcome in outcomes if outcome.score is not None and outcome.score.exact
            ),
            "status_matched": sum(
                1
                for outcome in outcomes
                if outcome.score is not None and outcome.score.status_matched
            ),
        },
        "statuses": dict(sorted(statuses.items())),
        "needs_review": {"count": sum(review.values()), "by_reason": dict(sorted(review.items()))},
        "tokens": {
            "input": input_tokens,
            "output": output_tokens,
            "answered_without_counts": sum(outcome.without_counts for outcome in outcomes),
        },
        "cost": {
            "currency": "USD",
            "prices": {
                "in_per_mtok": str(prices.input),
                "out_per_mtok": str(prices.output),
                "canned": prices.canned,
            },
            "total": str(total),
            "per_1000_invoices": str(total * 1000 / documents) if documents else None,
            "reported": {OPENROUTER_USAGE_UNIT: str(credits)},
        },
        "retries": sum(transient.values()),
        "transient_errors": {code.value: transient[code.value] for code in TransientCode},
        "permanent_errors": dict(sorted(permanent.items())),
    }


def _ranks(values: Sequence[int]) -> dict[str, Any]:
    figures = metrics.percentiles(values)
    return {name: figures[name] for name in ("p50", "p95", "p99", "max")}


def baseline_record(
    run: Run,
    *,
    run_id: str,
    mode: Mode,
    workers: int,
    seed: int,
    choice: ProviderChoice,
    call_timeout: str,
    prices: Prices,
    fields: Sequence[str],
    note: str,
    code: Mapping[str, Any],
) -> dict[str, Any]:
    documents = len(run.outcomes)
    block = extraction_figures(run, prices, fields)
    failed = any(outcome.status == FAILED for outcome in run.outcomes)
    return {
        "format": FORMAT,
        "run_id": run_id,
        "status": "failed" if failed else "complete",
        "recorded_at": now(),
        "cell": {"mode": mode.value, "workers": workers, "invoices": documents, "seed": seed},
        "environment": {
            "git_sha": code["git_sha"],
            "git_dirty": code["git_dirty"],
            "python": sys.version.split()[0],
            "note": note,
        },
        "provider": {
            "name": choice.name.value,
            "model": choice.model,
            "dry_run": choice.dry_run,
            "max_provider_calls": choice.max_provider_calls,
            "call_timeout": call_timeout,
            "max_attempts": MAX_ATTEMPTS,
        },
        "timing": {
            "total_s": run.total_s,
            "per_invoice_s": run.total_s / documents if documents else None,
        },
        "throughput": {"makespan_items_per_s": metrics.rate(documents, run.total_s)},
        "extraction": block,
    }


def write_baseline(record: Mapping[str, Any], directory: Path) -> Path:
    cell = record["cell"]
    stamp = record["recorded_at"]
    sha = str(record["environment"]["git_sha"])[:7]
    name = f"{stamp}-{sha}-baseline-{cell['mode']}-{cell['invoices']}-w{cell['workers']}.json"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    with path.open("x", encoding="utf-8") as file:
        file.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return path


# --- the command ---------------------------------------------------------------------------


def _decimal(text: str) -> Decimal:
    try:
        return Decimal(text)
    except InvalidOperation:
        msg = f"not a decimal number: {text!r}"
        raise argparse.ArgumentTypeError(msg) from None


def refusal(args: argparse.Namespace, environ: Mapping[str, str]) -> str | None:
    choice = choice_of(args)
    reason = choice.refusal(environ)
    if reason is not None:
        return reason
    if args.mode is Mode.LOOP and args.workers != 1:
        return "--mode loop runs one document at a time: pass --workers 1"
    if args.invoices < 1 or args.workers < 1:
        return "--invoices and --workers must be positive"
    prices = (args.price_in_per_mtok, args.price_out_per_mtok)
    if args.dry_run and any(price is not None for price in prices):
        return "a dry run uses canned prices: drop --price-in-per-mtok and --price-out-per-mtok"
    if args.allow_live_calls and any(
        price is None or not price.is_finite() or price <= 0 for price in prices
    ):
        return "a live run needs --price-in-per-mtok and --price-out-per-mtok, each above 0"
    try:
        parse_duration(args.call_timeout)
    except argparse.ArgumentTypeError as exc:
        return f"--call-timeout: {exc}"
    return None


def choice_of(args: argparse.Namespace) -> ProviderChoice:
    return ProviderChoice(
        name=ProviderName.OPENROUTER,
        model=args.model,
        max_provider_calls=args.max_provider_calls,
        allow_live_calls=args.allow_live_calls,
        dry_run=args.dry_run,
    )


def main(
    argv: Sequence[str] | None = None,
    connector: Callable[[ProviderChoice, str, Mapping[str, str]], Extraction] = connect,
    canned_prices: Prices | None = None,
) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.baseline", description=__doc__)
    parser.add_argument("--mode", type=Mode, choices=list(Mode), required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--invoices", type=int, required=True)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--model", required=True)
    parser.add_argument("--max-provider-calls", type=int, required=True)
    live = parser.add_mutually_exclusive_group(required=True)
    live.add_argument("--allow-live-calls", action="store_true")
    live.add_argument("--dry-run", action="store_true")
    parser.add_argument("--price-in-per-mtok", type=_decimal)
    parser.add_argument("--price-out-per-mtok", type=_decimal)
    parser.add_argument("--call-timeout", default=CALL_TIMEOUT)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--note", default="")
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args(argv)
    reason = refusal(args, os.environ)
    if reason is not None:
        print(f"refused: {reason}", file=sys.stderr)
        return 1
    choice = choice_of(args)
    try:
        refuse_dirty(args.allow_dirty)
        extraction = connector(choice, args.call_timeout, os.environ)
    except (DirtyTreeError, NotConnectedError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    if args.dry_run:
        prices = canned_prices or Prices(Decimal(0), Decimal(0), canned=True)
    else:
        prices = Prices(args.price_in_per_mtok, args.price_out_per_mtok, canned=False)
    run_id = uuid.uuid4().hex[:12]
    raw_dir = RAW_DIR / run_id / "baseline"
    raw_dir.mkdir(parents=True)
    budget = CallBudget(create_budget_file(raw_dir / BUDGET_FILE_NAME), args.max_provider_calls)
    documents = extraction.documents(args.invoices, args.seed)
    run = run_documents(documents, extraction, mode=args.mode, workers=args.workers, budget=budget)
    record = baseline_record(
        run,
        run_id=run_id,
        mode=args.mode,
        workers=args.workers,
        seed=args.seed,
        choice=choice,
        call_timeout=args.call_timeout,
        prices=prices,
        fields=extraction.fields,
        note=args.note,
        code=code_state(),
    )
    path = write_baseline(record, args.out_dir)
    print(f"{record['status']} {path}")
    return 0 if record["status"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
