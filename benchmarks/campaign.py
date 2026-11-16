"""The paid benchmark campaign (DEC-072): ``python -m benchmarks.campaign``.

``run`` executes the approved plan's eight steps in order, one cell at a time. Every
cell is one subprocess of an existing command, ``benchmarks.run``,
``benchmarks.drills`` or ``benchmarks.baseline``, so the campaign never calls the
provider itself and every refusal those commands make still applies. The model comes
only from ``--model`` and the per-million-token prices only from
``--price-in-per-mtok`` and ``--price-out-per-mtok``; both are written into the
results, never into code.

**The key.** ``ORQESTRA_OPENROUTER_API_KEY`` is checked for presence before a live
campaign and reaches live children through their environment only. A dry run removes
it from every child environment, and its children run on the canned transport: no
network, no key, no cost.

**Gates.** After the pilot the campaign stops if field accuracy is below 0.90, if the
cost per call is above 3 x the $0.0006 estimate, or if any call failed permanently. A
figure the pilot's result does not carry fails its gate.

**Budget.** Before every cell, what the campaign has spent is recomputed from its own
result files (``charge_for``). A cell runs only if its call maximum times its per-call
ceiling fits what remains. For a cell that reads documents the ceiling is a hard
bound: the most one call can cost, from the system prompt, the cell's largest
document and the reply's token limit (``hard_call_cost``). The existing call counter
(DEC-071) enforces the call maximum before each request, so such a cell cannot spend
past what it was given. The mix and the drill have no reply limit in code; they are
charged at the cost gate's figure per call, and the $5 between the campaign budget
and the account's $50 cap is the reserve for them. A cell that does not fit is
skipped, except the large run, which is sized to fit.

Every decision is a line of ``campaign-log.jsonl`` in the campaign's folder.
``reconcile`` records the router's and the upstream provider's own figures afterwards.
"""

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import IntEnum, StrEnum
from functools import cache
from pathlib import Path
from typing import Any, Protocol

from benchmarks import extract
from benchmarks.extract import fixed_point_text
from benchmarks.generate import DEFAULT_SEED, expected_outcome, generate_invoices
from benchmarks.live import (
    API_KEY_ENV,
    BUDGET_FILE_NAME,
    OPENROUTER,
    OPENROUTER_USAGE_UNIT,
    ProviderChoice,
    ProviderName,
    canned_requests,
    read_budget_file,
)
from benchmarks.result import RAW_DIR, RESULTS_DIR, ROOT, DirtyTreeError, Status, code_state
from benchmarks.run import refuse_dirty
from benchmarks.worker import DEFAULT_CAP

CAMPAIGN_BUDGET = Decimal(45)
ESTIMATED_COST_PER_CALL = Decimal("0.0006")
COST_GATE = 3 * ESTIMATED_COST_PER_CALL
# A mix or drill call has no reply limit in code: it is planned and charged at this figure.
UNBOUNDED_CALL_COST = COST_GATE
ACCURACY_GATE = Decimal("0.90")
CALL_HEADROOM = Decimal("1.1")
# The request's JSON, roles and the user message's wrapper around the document, in bytes.
FRAMING_BYTES = 1_024
PER_MILLION = Decimal(1_000_000)
LARGE_MAX = 50_000
CALL_TIMEOUT = "PT30S"
CELL_STOP_MINUTES = 60
LARGE_STOP_MINUTES = 180
VICTIMS = 2
WORKER_COUNTS = (1, 4, 8, 16, 32)
REPETITIONS = 3
WORKER_MODULE = "benchmarks.worker"
LOG_NAME = "campaign-log.jsonl"
SUMMARY_NAME = "campaign.json"
BILLING_NAME = "billing-check.json"
LIVE_BASE = RESULTS_DIR / "campaigns"
DRY_RUN_BASE = RAW_DIR / "campaigns"


class Step(IntEnum):
    PILOT = 1
    SCALING = 2
    TASK_SIZE = 3
    RULES_FIRST = 4
    FAILURES = 5
    BILLING = 6
    BASELINE = 7
    LARGE = 8


class Tool(StrEnum):
    RUN = "benchmarks.run"
    DRILL = "benchmarks.drills"
    BASELINE = "benchmarks.baseline"


class Workload(StrEnum):
    EXTRACT = "extract"  # the model reads every document (``--profile extract``)
    MIX = "invoice"  # the existing rules-first mix (``--profile invoice``)


@dataclass(frozen=True, slots=True, kw_only=True)
class PlannedCell:
    """One command the campaign runs. ``batch`` is 1 for a baseline cell, which has no tasks."""

    step: Step
    tool: Tool
    workload: Workload
    size: int
    batch: int
    workers: int
    repetition: int = 1
    mode: str | None = None  # baseline: "loop" or "threads"

    @property
    def key(self) -> str:
        """A unique, readable name: ``s2-run-extract-2000-b25-w8-r1``."""
        tool = self.tool.value.removeprefix("benchmarks.")
        mode = f"-{self.mode}" if self.mode else ""
        return (
            f"s{self.step.value}-{tool}{mode}-{self.workload.value}-{self.size}"
            f"-b{self.batch}-w{self.workers}-r{self.repetition}"
        )

    @property
    def expected_calls(self) -> int:
        """Calls a clean run makes: one per document, or the mix oracle's calls."""
        if self.workload is Workload.EXTRACT:
            return self.size
        return mix_calls(self.size, self.batch)

    @property
    def call_maximum(self) -> int:
        """Expected calls plus 10 %, plus one task's calls per worker: a retry or a killed
        worker's task is called again whole. A mix task calls at most the per-attempt cap."""
        per_task = self.batch if self.workload is Workload.EXTRACT else min(self.batch, DEFAULT_CAP)
        return math.ceil(self.expected_calls * CALL_HEADROOM) + per_task * self.workers


@cache
def mix_calls(size: int, batch: int) -> int:
    """The calls the rules-first mix's oracle expects for ``size`` invoices."""
    return expected_outcome(generate_invoices(size, DEFAULT_SEED), batch, DEFAULT_CAP).calls


@dataclass(frozen=True, slots=True)
class CampaignPlan:
    """The sizes of every step; ``APPROVED_PLAN`` is the owner's (DEC-072)."""

    pilot: int = 100
    pilot_batch: int = 25
    pilot_workers: int = 4
    scaling: int = 2_000
    scaling_batch: int = 25
    worker_counts: tuple[int, ...] = WORKER_COUNTS
    repetitions: int = REPETITIONS
    task_size: int = 1_000
    task_size_batches: tuple[int, ...] = (5, 25, 100)
    task_size_workers: int = 8
    mix_same_size: int = 2_000  # the scaling cells' size, for a like-for-like comparison
    mix_same_size_batch: int = 25
    mix: int = 10_000
    mix_batch: int = 100
    mix_workers: int = 8
    failures: int = 2_000
    failure_batch: int = 25  # 80 tasks: every one of 32 workers has work
    rate_limit_batch: int = 5
    failure_workers: int = 32
    baseline: int = 2_000
    large_max: int = LARGE_MAX
    large_batch: int = 25

    def cells(self) -> list[PlannedCell]:
        """Every fixed cell of steps 1 to 7, in run order. The large run is sized later."""
        extract = partial_cell(Tool.RUN, Workload.EXTRACT)
        mix = partial_cell(Tool.RUN, Workload.MIX)
        cells = [extract(Step.PILOT, self.pilot, self.pilot_batch, self.pilot_workers)]
        for repetition in range(1, self.repetitions + 1):
            for workers in self.worker_counts:
                cell = extract(Step.SCALING, self.scaling, self.scaling_batch, workers)
                cells.append(replace(cell, repetition=repetition))
        for batch in self.task_size_batches:
            cells.append(extract(Step.TASK_SIZE, self.task_size, batch, self.task_size_workers))
        for repetition in range(1, self.repetitions + 1):
            cell = mix(
                Step.RULES_FIRST, self.mix_same_size, self.mix_same_size_batch, self.mix_workers
            )
            cells.append(replace(cell, repetition=repetition))
        cells.append(mix(Step.RULES_FIRST, self.mix, self.mix_batch, self.mix_workers))
        # The crash drill runs the extraction workload (``d1 --workload extract``).
        drill = partial_cell(Tool.DRILL, Workload.EXTRACT)
        cells.append(drill(Step.FAILURES, self.failures, self.failure_batch, self.failure_workers))
        cells.append(
            extract(Step.FAILURES, self.failures, self.rate_limit_batch, self.failure_workers)
        )
        baseline = partial_cell(Tool.BASELINE, Workload.EXTRACT)
        cells.append(replace(baseline(Step.BASELINE, self.baseline, 1, 1), mode="loop"))
        for workers in self.worker_counts:
            cells.append(
                replace(baseline(Step.BASELINE, self.baseline, 1, workers), mode="threads")
            )
        return cells

    def large(self, size: int, workers: int) -> PlannedCell:
        return PlannedCell(
            step=Step.LARGE,
            tool=Tool.RUN,
            workload=Workload.EXTRACT,
            size=size,
            batch=self.large_batch,
            workers=workers,
        )


def partial_cell(tool: Tool, workload: Workload) -> Callable[[Step, int, int, int], PlannedCell]:
    def make(step: Step, size: int, batch: int, workers: int) -> PlannedCell:
        return PlannedCell(
            step=step, tool=tool, workload=workload, size=size, batch=batch, workers=workers
        )

    return make


APPROVED_PLAN = CampaignPlan()


# --- the most one call can cost ------------------------------------------------------------


class ExtractLimits(Protocol):
    """What bounds one extraction call, from the extraction workload's own definitions."""

    @property
    def prompt_bytes(self) -> int:
        """The UTF-8 size of the fixed system prompt."""
        ...

    @property
    def max_output_tokens(self) -> int:
        """The ``max_tokens`` every extraction request sets."""
        ...

    def largest_document_bytes(self, size: int) -> int:
        """The UTF-8 size of the largest of the first ``size`` documents a cell sends."""
        ...


def connect_limits() -> ExtractLimits:
    """The extraction workload's limits, for the documents every campaign cell sends (the
    run command's default seed)."""
    return extract.Limits(DEFAULT_SEED)


@dataclass(frozen=True, slots=True)
class Prices:
    """USD per million tokens. A dry run plans with synthetic prices, marked as such."""

    input: Decimal
    output: Decimal
    synthetic: bool = False


# Arbitrary values of a plausible scale, unrelated to any real price.
DRY_RUN_PRICES = Prices(Decimal("0.37"), Decimal("1.73"), synthetic=True)


def hard_call_cost(input_bytes: int, max_output_tokens: int, prices: Prices) -> Decimal:
    """The most one call can cost. A token covers at least one byte, so the request's
    bytes bound its input tokens; the reply is bounded by ``max_tokens``."""
    tokens_in = Decimal(input_bytes + FRAMING_BYTES)
    return (tokens_in * prices.input + max_output_tokens * prices.output) / PER_MILLION


class Ceilings:
    """The per-call ceiling of each cell: a hard bound where the workload has one."""

    def __init__(self, limits: ExtractLimits, prices: Prices) -> None:
        self._limits = limits
        self._prices = prices
        self._largest: dict[int, int] = {}

    def of(self, cell: PlannedCell) -> Decimal:
        if cell.workload is not Workload.EXTRACT:
            return UNBOUNDED_CALL_COST
        input_bound, output_bound = self.token_bounds(cell)
        return hard_call_cost(input_bound - FRAMING_BYTES, output_bound, self._prices)

    def token_bounds(self, cell: PlannedCell) -> tuple[int, int]:
        """The most input and output tokens one call of ``cell`` may report."""
        if cell.size not in self._largest:
            self._largest[cell.size] = self._limits.largest_document_bytes(cell.size)
        request = self._limits.prompt_bytes + self._largest[cell.size] + FRAMING_BYTES
        return request, self._limits.max_output_tokens


# --- what a result says ------------------------------------------------------------------

# The extraction block: written by ``benchmarks.run --profile extract`` and by the baseline,
# with the same names for the figures read here.
EXTRACTION = "extraction"


@dataclass(frozen=True, slots=True)
class Figures:
    """What the campaign reads from one result. ``None`` means the result does not say."""

    status: str | None
    calls: int | None  # every ledger call, retried and repeated ones included
    token_cost: Decimal | None  # every answered call's tokens x the given prices
    credits: Decimal | None  # what the provider reported, in its credits
    accuracy: Decimal | None  # pooled field accuracy
    permanent_failures: int | None
    items_per_s: float | None
    answered: int | None = None  # calls that returned an answer, and so token counts
    max_input_tokens: int | None = None  # the largest any one answered call reported
    max_output_tokens: int | None = None


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() and number >= 0 else None


def _count(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _path(record: Mapping[str, Any], *keys: str) -> object:
    node: object = record
    for key in keys:
        if not isinstance(node, Mapping):
            return None
        node = node.get(key)
    return node


def step_figures(record: Mapping[str, Any]) -> Figures:
    """The figures of a run, drill or baseline result, wherever each record keeps them."""
    status = record.get("status")
    status = status if isinstance(status, str) else None
    items_per_s = _path(record, "throughput", "makespan_items_per_s")
    speed = float(items_per_s) if isinstance(items_per_s, int | float) else None
    block = record.get(EXTRACTION)
    if isinstance(block, Mapping):
        permanent = _path(block, "permanent_errors")
        failures = (
            sum(count for count in permanent.values() if isinstance(count, int))
            if isinstance(permanent, Mapping)
            else None
        )
        overall = _path(block, "accuracy", "overall")
        # ``cost.total`` prices only the answered calls that reported their tokens; with
        # any that did not, the token cost is unknown rather than low.
        uncounted = _count(_path(block, "tokens", "answered_without_counts"))
        return Figures(
            status=status,
            calls=_count(_path(block, "calls", "total")),
            answered=_count(_path(block, "calls", "answered")),
            token_cost=_decimal(_path(block, "cost", "total")) if uncounted == 0 else None,
            credits=_decimal(_path(block, "cost", "reported", OPENROUTER_USAGE_UNIT)),
            accuracy=_decimal(overall) if isinstance(overall, float | int) else None,
            permanent_failures=failures,
            items_per_s=speed,
            max_input_tokens=_count(_path(block, "tokens", "max_per_call", "input")),
            max_output_tokens=_count(_path(block, "tokens", "max_per_call", "output")),
        )
    calls = _count(_path(record, "usage", "calls"))
    if calls is None:
        calls = _count(_path(record, "provider", "calls_made"))
    credits = _decimal(_path(record, "usage", "reported_totals", OPENROUTER_USAGE_UNIT, "amount"))
    return Figures(
        status=status,
        calls=calls,
        token_cost=None,
        credits=credits,
        accuracy=None,
        permanent_failures=None,
        items_per_s=speed,
    )


# --- budget ------------------------------------------------------------------------------


class Basis(StrEnum):
    TOKENS = "tokens"  # the larger of tokens x prices and the credits
    CALLS = "calls"  # no token figures: calls x the ceiling, or the credits if larger
    RESERVATIONS = "reservations"  # no result: the counter's reservations x the ceiling
    CALL_MAXIMUM = "call_maximum"  # no result and no counter: the call maximum x the ceiling


@dataclass(frozen=True, slots=True)
class Charge:
    cost: Decimal
    calls: int | None
    basis: Basis


def charge_for(
    figures: Figures | None, *, ceiling: Decimal, call_maximum: int, reserved: int | None
) -> Charge:
    """What a cell is charged against the budget. Unknown spend is never zero.

    ``reserved`` is what the cell's call counters hold, read when it left no result.
    """
    if figures is None or figures.calls is None:
        if reserved is not None:
            return Charge(reserved * ceiling, reserved, Basis.RESERVATIONS)
        return Charge(call_maximum * ceiling, None, Basis.CALL_MAXIMUM)
    credits = figures.credits if figures.credits is not None else Decimal(0)
    if figures.token_cost is None:
        return Charge(max(figures.calls * ceiling, credits), figures.calls, Basis.CALLS)
    # The token cost prices answered calls only. A call that failed or never resolved (a
    # timeout, a worker killed mid-call) may still be billed, so each is charged at the
    # ceiling; a 429 is charged too, which errs on the safe side.
    answered = figures.answered
    if answered is None or answered > figures.calls:
        answered = 0
    unanswered = figures.calls - answered
    token_side = figures.token_cost + unanswered * ceiling
    return Charge(max(token_side, credits), figures.calls, Basis.TOKENS)


def observed_cost_per_call(charges: Sequence[Charge]) -> Decimal | None:
    """The mean cost of a token-priced call so far: an estimate for the log, never a bound."""
    priced = [charge for charge in charges if charge.basis is Basis.TOKENS and charge.calls]
    calls = sum(charge.calls or 0 for charge in priced)
    if not calls:
        return None
    return sum((charge.cost for charge in priced), Decimal(0)) / calls


def fits(cell: PlannedCell, remaining: Decimal, ceiling: Decimal) -> bool:
    return cell.call_maximum * ceiling <= remaining


def size_large(
    plan: CampaignPlan, workers: int, remaining: Decimal, ceiling: Decimal
) -> PlannedCell | None:
    """The largest large run the remaining budget pays for, in whole tasks, or ``None``
    when it would not give every worker a task. ``ceiling`` must bound a call of any
    size up to ``plan.large_max``."""
    affordable = math.floor(remaining / ceiling) - plan.large_batch * workers
    size = min(plan.large_max, math.floor(affordable / CALL_HEADROOM))
    size -= size % plan.large_batch
    while size > 0 and not fits(plan.large(size, workers), remaining, ceiling):
        size -= plan.large_batch
    if size < plan.large_batch * workers:
        return None
    return plan.large(size, workers)


def best_workers(scaling: Sequence[tuple[int, Figures]]) -> int | None:
    """The worker count with the highest median throughput over complete scaling cells;
    a tie goes to fewer workers."""
    speeds: dict[int, list[float]] = {}
    for workers, figures in scaling:
        if figures.status == Status.COMPLETE and figures.items_per_s is not None:
            speeds.setdefault(workers, []).append(figures.items_per_s)
    if not speeds:
        return None
    medians = {workers: statistics.median(values) for workers, values in speeds.items()}
    return min(medians, key=lambda workers: (-medians[workers], workers))


def reservations_between(start: float, end: float | None) -> int | None:
    """The calls reserved by every call counter last written in ``[start, end)``.

    Cells run one at a time and no worker outlives its cell (``Campaign`` checks), so the
    counters written while a cell ran are that cell's. ``None`` when there are none.
    """
    if not RAW_DIR.is_dir():
        return None
    total: int | None = None
    for path in sorted(RAW_DIR.glob(f"*/**/{BUDGET_FILE_NAME}")):
        if path.is_relative_to(DRY_RUN_BASE):
            continue
        written = path.stat().st_mtime
        if written >= start and (end is None or written < end):
            total = (total or 0) + read_budget_file(path)
    return total


# --- gates -------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Gate:
    name: str
    value: str | None
    threshold: str
    passed: bool
    cell: str | None = None

    def event(self) -> dict[str, Any]:
        return {
            "event": "gate",
            "cell": self.cell,
            "gate": self.name,
            "value": self.value,
            "threshold": self.threshold,
            "passed": self.passed,
        }


def pilot_gates(figures: Figures | None, charge: Charge) -> list[Gate]:
    """The three pilot gates. A figure the result does not carry fails its gate.

    The cost per call is what an answered call cost: the larger of the token cost and the
    credits over the answered calls. The budget's surcharge for unanswered calls is a
    safety margin, not a price, so it is left out here.
    """
    accuracy = None if figures is None else figures.accuracy
    per_call: Decimal | None = None
    if (
        charge.basis is Basis.TOKENS
        and figures is not None
        and figures.token_cost is not None
        and figures.answered
    ):
        credits = figures.credits if figures.credits is not None else Decimal(0)
        per_call = max(figures.token_cost, credits) / figures.answered
    permanent = None if figures is None else figures.permanent_failures
    complete = figures is not None and figures.status == Status.COMPLETE
    return [
        Gate(
            "field_accuracy",
            None if accuracy is None else fixed_point_text(accuracy),
            f">= {ACCURACY_GATE}",
            accuracy is not None and accuracy >= ACCURACY_GATE,
        ),
        Gate(
            "cost_per_call",
            None if per_call is None else fixed_point_text(per_call),
            f"<= {COST_GATE}",
            per_call is not None and per_call <= COST_GATE,
        ),
        Gate(
            "no_permanent_failure",
            None if figures is None else f"{permanent} permanent, status {figures.status}",
            "0 permanent and complete",
            permanent == 0 and complete,
        ),
    ]


def bound_gates(cell: str, figures: Figures | None, bounds: tuple[int, int]) -> list[Gate]:
    """Whether every call of an extract cell stayed inside the per-call bound its budget
    assumed. A cell whose result does not say fails both: its spend is then unproven."""
    input_bound, output_bound = bounds
    largest_in = None if figures is None else figures.max_input_tokens
    largest_out = None if figures is None else figures.max_output_tokens
    return [
        Gate(
            "input_tokens_bound",
            None if largest_in is None else str(largest_in),
            f"<= {input_bound}",
            largest_in is not None and largest_in <= input_bound,
            cell,
        ),
        Gate(
            "output_tokens_bound",
            None if largest_out is None else str(largest_out),
            f"<= {output_bound}",
            largest_out is not None and largest_out <= output_bound,
            cell,
        ),
    ]


# --- commands ----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Settings:
    """What every child command is given besides its cell."""

    campaign_id: str
    model: str
    live: bool
    price_in: Decimal | None
    price_out: Decimal | None
    out_dir: Path

    @property
    def prices(self) -> Prices:
        """The prices the budget plans with: the given ones, or a dry run's synthetic ones."""
        if self.price_in is None or self.price_out is None:
            return DRY_RUN_PRICES
        return Prices(self.price_in, self.price_out)

    def provider_arguments(self, call_maximum: int) -> list[str]:
        return [
            "--model",
            self.model,
            "--max-provider-calls",
            str(call_maximum),
            "--call-timeout",
            CALL_TIMEOUT,
            "--allow-live-calls" if self.live else "--dry-run",
        ]

    def price_arguments(self) -> list[str]:
        if not self.live:
            return []
        assert self.price_in is not None
        assert self.price_out is not None
        return [
            "--price-in-per-mtok",
            fixed_point_text(self.price_in),
            "--price-out-per-mtok",
            fixed_point_text(self.price_out),
        ]

    def note(self, cell: PlannedCell) -> str:
        return f"campaign {self.campaign_id} step {cell.step.value} {cell.key}"


def command_for(cell: PlannedCell, settings: Settings, call_maximum: int) -> list[str]:
    """The argv of ``cell``. It never holds the key: that travels in the environment."""
    common = ["--out-dir", str(settings.out_dir), "--note", settings.note(cell)]
    provider = settings.provider_arguments(call_maximum)
    head = [sys.executable, "-m", cell.tool.value]
    if cell.tool is Tool.BASELINE:
        assert cell.mode is not None
        return [
            *head,
            "--mode",
            cell.mode,
            "--workers",
            str(cell.workers),
            "--invoices",
            str(cell.size),
            *provider,
            *settings.price_arguments(),
            *common,
        ]
    provider = ["--provider", OPENROUTER, *provider]
    if cell.tool is Tool.DRILL:
        return [
            *head,
            "d1",
            "--invoices",
            str(cell.size),
            "--batch",
            str(cell.batch),
            "--workers",
            str(cell.workers),
            "--victims",
            str(VICTIMS),
            "--stop-after",
            str(CELL_STOP_MINUTES * 60),
            "--workload",
            "extract" if cell.workload is Workload.EXTRACT else "mix",
            *provider,
            *(settings.price_arguments() if cell.workload is Workload.EXTRACT else []),
            *common,
        ]
    stop = LARGE_STOP_MINUTES if cell.step is Step.LARGE else CELL_STOP_MINUTES
    prices = settings.price_arguments() if cell.workload is Workload.EXTRACT else []
    return [
        *head,
        "--profile",
        cell.workload.value,
        "--size",
        str(cell.size),
        "--batch",
        str(cell.batch),
        "--workers",
        str(cell.workers),
        "--repetition",
        str(cell.repetition),
        "--stop-after-minutes",
        str(stop),
        *provider,
        *prices,
        *common,
    ]


def child_environment(environ: Mapping[str, str], *, live: bool) -> dict[str, str]:
    """The environment of every child: a dry run's never holds the key."""
    env = dict(environ)
    if not live:
        env.pop(API_KEY_ENV, None)
    return env


@dataclass(frozen=True, slots=True)
class Finished:
    exit_code: int
    stdout: str
    stderr: str


Runner = Callable[[Sequence[str], Mapping[str, str]], Finished]
Alive = Callable[[], list[int]]


def run_command(argv: Sequence[str], env: Mapping[str, str]) -> Finished:
    completed = subprocess.run(  # an argv list, never a shell
        list(argv), env=dict(env), cwd=ROOT, capture_output=True, text=True, check=False
    )
    return Finished(completed.returncode, completed.stdout, completed.stderr)


def workers_alive() -> list[int]:
    """The PIDs of every running benchmark worker process on this machine."""
    listing = subprocess.run(  # a fixed command
        ["ps", "-Ao", "pid=,args="], capture_output=True, text=True, check=True, timeout=30
    ).stdout
    alive = []
    for line in listing.splitlines():
        pid, _, args = line.strip().partition(" ")
        if f"-m {WORKER_MODULE}" in args and pid.isdigit():
            alive.append(int(pid))
    return alive


def result_path(stdout: str) -> Path | None:
    """The result file a command named on its last line (``<status> [drill] <path>``)."""
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        return None
    candidate = Path(lines[-1].split()[-1])
    return candidate if candidate.suffix == ".json" and candidate.is_file() else None


def read_record(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return record if isinstance(record, dict) else None


# --- the log -----------------------------------------------------------------------------


def now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class CampaignLog:
    """``campaign-log.jsonl``: one event per line, written and synced as it happens."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def write(self, event: Mapping[str, Any]) -> None:
        line = json.dumps({"at": now(), **event}, sort_keys=True, default=str)
        with self.path.open("a", encoding="utf-8") as file:
            file.write(line + "\n")
            file.flush()
            os.fsync(file.fileno())

    def events(self) -> Iterator[dict[str, Any]]:
        if not self.path.exists():
            return
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                yield json.loads(line)


# --- the campaign ------------------------------------------------------------------------


@dataclass(slots=True)
class CellRun:
    """A started cell: what it was allowed and what it is charged."""

    cell_key: str
    step: int
    workers: int
    call_maximum: int
    ceiling: Decimal
    result: Path | None
    figures: Figures | None
    charge: Charge


@dataclass(slots=True)
class Campaign:
    settings: Settings
    budget: Decimal
    plan: CampaignPlan
    runner: Runner
    environ: Mapping[str, str]
    log: CampaignLog
    limits: ExtractLimits
    alive: Alive = workers_alive
    clock: Callable[[], float] = time.time
    runs: dict[str, CellRun] = field(default_factory=dict)
    skipped: set[str] = field(default_factory=set)
    pilot_passed: bool = False
    stopped: str | None = None
    started_at: str | None = None
    ceilings: Ceilings = field(init=False)

    def __post_init__(self) -> None:
        self.ceilings = Ceilings(self.limits, self.settings.prices)

    @property
    def spent(self) -> Decimal:
        return sum((run.charge.cost for run in self.runs.values()), Decimal(0))

    @property
    def remaining(self) -> Decimal:
        return self.budget - self.spent

    @property
    def halted(self) -> bool:
        return self.stopped is not None

    def resume(self) -> None:
        """Rebuild the state from the log. A started cell is never run again, and one that
        left no result stays charged for what its counters reserved."""
        started: dict[str, dict[str, Any]] = {}
        finished: dict[str, Path | None] = {}
        order: list[str] = []
        for event in self.log.events():
            kind = event.get("event")
            if kind == "campaign_started" and self.started_at is None:
                self.started_at = str(event["at"])
            elif kind == "cell_started":
                started[event["cell"]] = event
                order.append(event["cell"])
            elif kind == "cell_finished":
                finished[event["cell"]] = Path(event["result"]) if event["result"] else None
            elif kind == "cell_skipped":
                self.skipped.add(event["cell"])
            elif kind == "pilot_passed":
                self.pilot_passed = True
            elif kind == "campaign_stopped":
                self.stopped = str(event["reason"])
        for index, key in enumerate(order):
            event = started[key]
            following = started[order[index + 1]] if index + 1 < len(order) else None
            path = finished.get(key)
            record = read_record(path)
            figures = None if record is None else step_figures(record)
            ceiling = Decimal(event["ceiling"])
            maximum = int(event["call_maximum"])
            reserved = None
            if figures is None or figures.calls is None:
                end = None if following is None else float(following["started_epoch"])
                reserved = reservations_between(float(event["started_epoch"]), end)
            charge = charge_for(figures, ceiling=ceiling, call_maximum=maximum, reserved=reserved)
            self.runs[key] = CellRun(
                key,
                int(event["step"]),
                int(event["workers"]),
                maximum,
                ceiling,
                path,
                figures,
                charge,
            )

    def _workers_quiet(self) -> bool:
        """No cell starts while a benchmark worker from an earlier one is still running."""
        alive = self.alive()
        if alive:
            self.stop(f"benchmark workers still running: {', '.join(map(str, alive))}")
            return False
        return True

    def run_cell(self, cell: PlannedCell, *, sized: bool = False) -> CellRun | None:
        """Run ``cell`` if it fits the remaining budget; ``None`` if it was skipped or the
        campaign stopped."""
        if cell.key in self.runs:
            return self.runs[cell.key]
        if self.stopped is not None:
            return None
        ceiling = self.ceilings.of(cell)
        remaining = self.remaining
        observed = observed_cost_per_call([run.charge for run in self.runs.values()])
        budget = {
            "spent": fixed_point_text(self.spent),
            "remaining": fixed_point_text(remaining),
            "ceiling": fixed_point_text(ceiling),
            "call_maximum": cell.call_maximum,
            "projected": fixed_point_text(cell.call_maximum * ceiling),
            "estimate": None
            if observed is None
            else fixed_point_text(cell.expected_calls * observed),
        }
        if cell.key in self.skipped or not fits(cell, remaining, ceiling):
            if cell.key not in self.skipped:
                self.log.write(
                    {"event": "cell_skipped", "cell": cell.key, "reason": "budget", **budget}
                )
                self.skipped.add(cell.key)
            return None
        if not self._workers_quiet():
            return None
        argv = command_for(cell, self.settings, cell.call_maximum)
        began = self.clock()
        self.log.write(
            {
                "event": "cell_started",
                "cell": cell.key,
                "step": cell.step.value,
                "workers": cell.workers,
                "sized": sized,
                "argv": argv,
                "started_epoch": began,
                **budget,
            }
        )
        env = child_environment(self.environ, live=self.settings.live)
        finished = self.runner(argv, env)
        path = result_path(finished.stdout)
        self._keep_output(cell, finished)
        record = read_record(path)
        figures = None if record is None else step_figures(record)
        reserved = None
        if figures is None or figures.calls is None:
            reserved = reservations_between(began, None)
        charge = charge_for(
            figures, ceiling=ceiling, call_maximum=cell.call_maximum, reserved=reserved
        )
        self.log.write(
            {
                "event": "cell_finished",
                "cell": cell.key,
                "exit_code": finished.exit_code,
                "result": None if path is None else str(path),
                "status": None if figures is None else figures.status,
                "charge": fixed_point_text(charge.cost),
                "charge_basis": charge.basis.value,
                "calls": charge.calls,
            }
        )
        run = CellRun(
            cell.key,
            cell.step.value,
            cell.workers,
            cell.call_maximum,
            ceiling,
            path,
            figures,
            charge,
        )
        self.runs[cell.key] = run
        if cell.workload is Workload.EXTRACT:
            gates = bound_gates(cell.key, figures, self.ceilings.token_bounds(cell))
            for gate in gates:
                self.log.write(gate.event())
            failed = [gate.name for gate in gates if not gate.passed]
            if failed:
                self.stop(f"per-call bound not verified for {cell.key}: {', '.join(failed)}")
        return run

    def _keep_output(self, cell: PlannedCell, finished: Finished) -> None:
        """The child's output, for diagnosis: counts, statuses and paths only."""
        logs = self.settings.out_dir / "logs"
        logs.mkdir(exist_ok=True)
        (logs / f"{cell.key}.out").write_text(finished.stdout, encoding="utf-8")
        (logs / f"{cell.key}.err").write_text(finished.stderr, encoding="utf-8")

    def stop(self, reason: str) -> None:
        self.stopped = reason
        self.log.write({"event": "campaign_stopped", "reason": reason})

    def run_pilot(self, cell: PlannedCell) -> bool:
        if self.pilot_passed:
            return True
        if cell.key in self.runs:
            self.stop("pilot already ran without passing its gates")
            return False
        run = self.run_cell(cell)
        if run is None:
            if not self.halted:
                self.stop("the pilot does not fit the budget")
            return False
        gates = pilot_gates(run.figures, run.charge)
        for gate in gates:
            self.log.write(gate.event())
        if self.halted:  # the per-call bound already failed
            return False
        if not all(gate.passed for gate in gates):
            failed = ", ".join(gate.name for gate in gates if not gate.passed)
            self.stop(f"pilot gate failed: {failed}")
            return False
        self.pilot_passed = True
        self.log.write({"event": "pilot_passed"})
        return True

    def run_large(self) -> None:
        if any(run.step == Step.LARGE for run in self.runs.values()):
            return
        scaling = [
            (run.workers, run.figures)
            for run in self.runs.values()
            if run.step == Step.SCALING and run.figures is not None
        ]
        workers = best_workers(scaling)
        if workers is None:
            self.log.write({"event": "large_skipped", "reason": "no complete scaling cell"})
            return
        # The ceiling of the largest set any size could send bounds every smaller one.
        ceiling = self.ceilings.of(self.plan.large(self.plan.large_max, workers))
        cell = size_large(self.plan, workers, self.remaining, ceiling)
        self.log.write(
            {
                "event": "large_sized",
                "workers": workers,
                "remaining": fixed_point_text(self.remaining),
                "ceiling": fixed_point_text(ceiling),
                "size": None if cell is None else cell.size,
            }
        )
        if cell is not None:
            self.run_cell(cell, sized=True)

    def billing_check(self, *, final: bool) -> dict[str, Any]:
        """The ledger beside the counters, per cell and in total; ``final`` closes the window."""
        rows = [billing_row(run, live=self.settings.live) for run in self.runs.values()]
        # Each total sums the cells that report the figure; ``unknown_cells`` says how many
        # do not, so a partial sum is never read as the whole.
        totals: dict[str, Any] = {}
        for name in ("calls", "counted_calls", "token_cost", "credits", "charged"):
            known = [row[name] for row in rows if row[name] is not None]
            total = sum((Decimal(value) for value in known), Decimal(0))
            totals[name] = int(total) if name.endswith("calls") else fixed_point_text(total)
            totals[f"{name}_unknown_cells"] = len(rows) - len(known)
        totals["token_cost_minus_credits"] = _difference(totals["token_cost"], totals["credits"])
        check = {
            "campaign_id": self.settings.campaign_id,
            "model": self.settings.model,
            "live": self.settings.live,
            "window_utc": {"start": self.started_at, "end": now() if final else None},
            "final": final,
            "recorded_at": now(),
            "cells": rows,
            "totals": totals,
            "router_reported_usd": None,
            "upstream_reported_usd": None,
        }
        path = self.settings.out_dir / BILLING_NAME
        path.write_text(json.dumps(check, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.log.write(
            {"event": "billing_check", "final": final, "path": str(path), "totals": totals}
        )
        return check

    def summary(self) -> dict[str, Any]:
        prices = self.settings.prices
        return {
            "campaign_id": self.settings.campaign_id,
            "model": self.settings.model,
            "live": self.settings.live,
            "prices_per_mtok": {
                "input": fixed_point_text(prices.input),
                "output": fixed_point_text(prices.output),
                "synthetic": prices.synthetic,
            },
            "budget": fixed_point_text(self.budget),
            "spent": fixed_point_text(self.spent),
            "remaining": fixed_point_text(self.remaining),
            "pilot_passed": self.pilot_passed,
            "stopped": self.stopped,
            "cells": {
                key: {
                    "result": None if run.result is None else str(run.result),
                    "status": None if run.figures is None else run.figures.status,
                    "charge": fixed_point_text(run.charge.cost),
                    "charge_basis": run.charge.basis.value,
                    "call_maximum": run.call_maximum,
                    "ceiling": fixed_point_text(run.ceiling),
                }
                for key, run in self.runs.items()
            },
            "skipped": sorted(self.skipped),
        }

    def execute(self) -> int:
        """Steps 1 to 8 in order; 0 when every step ran, 1 when the campaign stopped."""
        if self.stopped is not None:
            print(f"refused: campaign stopped earlier: {self.stopped}", file=sys.stderr)
            return 1
        if self.started_at is None:
            self.started_at = now()
        self.log.write(
            {
                "event": "campaign_started",
                "campaign_id": self.settings.campaign_id,
                "model": self.settings.model,
                "live": self.settings.live,
                "budget": fixed_point_text(self.budget),
                "prices_synthetic": self.settings.prices.synthetic,
                "resumed_cells": sorted(self.runs),
            }
        )
        cells = self.plan.cells()
        if not self.run_pilot(cells[0]):
            self._write_summary()
            return 1
        billed = self._billing_done()
        for cell in cells[1:]:
            if cell.step is Step.BASELINE and not billed:
                self.billing_check(final=False)  # step 6: steps 1 to 5
                billed = True
            self.run_cell(cell)
            if self.halted:
                self._write_summary()
                return 1
        if not billed:
            self.billing_check(final=False)
        self.run_large()
        if self.halted:
            self._write_summary()
            return 1
        self.billing_check(final=True)
        self.log.write({"event": "campaign_finished", "spent": fixed_point_text(self.spent)})
        self._write_summary()
        return 0

    def _billing_done(self) -> bool:
        return any(event.get("event") == "billing_check" for event in self.log.events())

    def _write_summary(self) -> None:
        path = self.settings.out_dir / SUMMARY_NAME
        text = json.dumps(self.summary(), indent=2, sort_keys=True) + "\n"
        path.write_text(text, encoding="utf-8")


def _difference(left: str | None, right: str | None) -> str | None:
    if left is None or right is None:
        return None
    return fixed_point_text(Decimal(left) - Decimal(right))


def counted_calls(record_run_id: str | None) -> int | None:
    """The reservations every call counter of one command's run holds (DEC-071)."""
    if record_run_id is None:
        return None
    files = sorted((RAW_DIR / record_run_id).glob(f"**/{BUDGET_FILE_NAME}"))
    if not files:
        return None
    return sum(read_budget_file(path) for path in files)


def transport_requests(record_run_id: str | None) -> int | None:
    """A dry run's requests the canned transport answered, from the workers' logs."""
    if record_run_id is None:
        return None
    directory = RAW_DIR / record_run_id
    if not directory.is_dir():
        return None
    return sum(canned_requests(path) for path in sorted(directory.iterdir()) if path.is_dir())


def billing_row(run: CellRun, *, live: bool) -> dict[str, Any]:
    record = read_record(run.result)
    run_id = None if record is None else record.get("run_id")
    run_id = run_id if isinstance(run_id, str) else None
    figures = run.figures
    token_cost = (
        None
        if figures is None or figures.token_cost is None
        else fixed_point_text(figures.token_cost)
    )
    credits = (
        None if figures is None or figures.credits is None else fixed_point_text(figures.credits)
    )
    calls = None if figures is None else figures.calls
    counted = counted_calls(run_id)
    row: dict[str, Any] = {
        "cell": run.cell_key,
        "result": None if run.result is None else str(run.result),
        "calls": calls,
        "counted_calls": counted,
        "counted_minus_calls": None if counted is None or calls is None else counted - calls,
        "token_cost": token_cost,
        "credits": credits,
        "token_cost_minus_credits": _difference(token_cost, credits),
        "charged": fixed_point_text(run.charge.cost),
        "charge_basis": run.charge.basis.value,
    }
    if not live:
        row["transport_requests"] = transport_requests(run_id)
    return row


# --- the command -------------------------------------------------------------------------


def campaign_id(stamp: datetime | None = None) -> str:
    moment = stamp or datetime.now(UTC)
    return f"{moment.strftime('%Y%m%dT%H%M%SZ')}-{code_state()['git_sha'][:7]}"


def refusal(args: argparse.Namespace, environ: Mapping[str, str]) -> str | None:
    """Why ``run`` may not start, or ``None``. Checked before any directory or child."""
    choice = ProviderChoice(
        name=ProviderName.OPENROUTER,
        model=args.model,
        max_provider_calls=1,
        allow_live_calls=args.allow_live_calls,
        dry_run=args.dry_run,
    )
    reason = choice.refusal(environ)  # the slug, the mode and, for a live run, the key
    if reason is not None:
        return reason
    prices = (args.price_in_per_mtok, args.price_out_per_mtok)
    if args.dry_run and any(price is not None for price in prices):
        return "a dry run uses canned prices: drop --price-in-per-mtok and --price-out-per-mtok"
    if args.allow_live_calls:
        for price in prices:
            if price is None or not price.is_finite() or price <= 0:
                return (
                    "a live campaign needs --price-in-per-mtok and --price-out-per-mtok, "
                    "the published prices per million tokens, each above 0"
                )
    budget = args.budget_usd
    if not budget.is_finite() or not Decimal(0) < budget <= CAMPAIGN_BUDGET:
        return f"--budget-usd must be above 0 and at most {CAMPAIGN_BUDGET}"
    return None


def _price(text: str) -> Decimal:
    try:
        return Decimal(text)
    except InvalidOperation:
        msg = f"not a decimal number: {text!r}"
        raise argparse.ArgumentTypeError(msg) from None


def campaign_folder(identifier: str) -> Path | None:
    for base in (LIVE_BASE, DRY_RUN_BASE):
        folder = base / identifier
        if (folder / LOG_NAME).is_file():
            return folder
    return None


def start(
    args: argparse.Namespace,
    environ: Mapping[str, str],
    runner: Runner,
    limits_factory: Callable[[], ExtractLimits],
    alive: Alive,
) -> int:
    reason = refusal(args, environ)
    if reason is not None:
        print(f"refused: {reason}", file=sys.stderr)
        return 1
    try:
        refuse_dirty(False)
        limits = limits_factory()
    except DirtyTreeError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    live = bool(args.allow_live_calls)
    base = LIVE_BASE if live else DRY_RUN_BASE
    if args.resume:
        identifier = args.resume
        out_dir = base / identifier
        if not (out_dir / LOG_NAME).is_file():
            print(f"refused: no campaign {identifier!r} to resume", file=sys.stderr)
            return 1
    else:
        identifier = campaign_id()
        out_dir = base / identifier
        out_dir.mkdir(parents=True, exist_ok=False)
    settings = Settings(
        campaign_id=identifier,
        model=args.model,
        live=live,
        price_in=args.price_in_per_mtok,
        price_out=args.price_out_per_mtok,
        out_dir=out_dir,
    )
    campaign = Campaign(
        settings,
        args.budget_usd,
        APPROVED_PLAN,
        runner,
        environ,
        CampaignLog(out_dir / LOG_NAME),
        limits,
        alive=alive,
    )
    if args.resume:
        campaign.resume()
    code = campaign.execute()
    print(f"campaign {identifier}: spent {campaign.spent} of {campaign.budget}; {out_dir}")
    return code


def reconcile(identifier: str, router: Decimal | None, upstream: Decimal | None) -> int:
    """Record the router's and the upstream provider's own figures beside the ledger's."""
    folder = campaign_folder(identifier)
    path = None if folder is None else folder / BILLING_NAME
    check = read_record(path)
    if path is None or folder is None or check is None or not check.get("final"):
        print(f"refused: campaign {identifier!r} has no final billing check", file=sys.stderr)
        return 1
    figures = {"router": router, "upstream": upstream}
    if all(value is None for value in figures.values()):
        print(
            "refused: pass --router-reported-usd, --upstream-reported-usd or both",
            file=sys.stderr,
        )
        return 1
    if any(
        value is not None and (not value.is_finite() or value < 0) for value in figures.values()
    ):
        print("refused: a reported figure must be 0 or more", file=sys.stderr)
        return 1
    totals = check["totals"]
    for name, value in figures.items():
        if value is None:
            continue
        check[f"{name}_reported_usd"] = fixed_point_text(value)
        for total in ("token_cost", "credits", "charged"):
            check[f"{name}_minus_{total}"] = _difference(fixed_point_text(value), totals[total])
    path.write_text(json.dumps(check, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    CampaignLog(folder / LOG_NAME).write(
        {
            "event": "reconciled",
            "router_reported_usd": None if router is None else fixed_point_text(router),
            "upstream_reported_usd": None if upstream is None else fixed_point_text(upstream),
        }
    )
    print(f"reconciled {path}")
    return 0


def main(
    argv: Sequence[str] | None = None,
    runner: Runner = run_command,
    limits_factory: Callable[[], ExtractLimits] = connect_limits,
    alive: Alive = workers_alive,
) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.campaign", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the campaign's steps in order")
    run.add_argument("--model", required=True, help="the model slug the owner selected")
    mode = run.add_mutually_exclusive_group(required=True)
    mode.add_argument("--allow-live-calls", action="store_true", help="real, billable calls")
    mode.add_argument("--dry-run", action="store_true", help="canned transport, no key, no cost")
    run.add_argument("--price-in-per-mtok", type=_price, help="live: USD per million input tokens")
    run.add_argument(
        "--price-out-per-mtok", type=_price, help="live: USD per million output tokens"
    )
    run.add_argument("--budget-usd", type=_price, default=CAMPAIGN_BUDGET)
    run.add_argument("--resume", help="continue the campaign with this id")
    check = commands.add_parser("reconcile", help="record the provider's own usage figures")
    check.add_argument("campaign_id")
    check.add_argument("--router-reported-usd", type=_price, help="the router's activity total")
    check.add_argument(
        "--upstream-reported-usd", type=_price, help="the upstream provider console's total"
    )
    args = parser.parse_args(argv)
    if args.command == "reconcile":
        return reconcile(args.campaign_id, args.router_reported_usd, args.upstream_reported_usd)
    return start(args, os.environ, runner, limits_factory, alive)


if __name__ == "__main__":
    sys.exit(main())
