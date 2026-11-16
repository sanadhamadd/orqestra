"""The paid benchmark campaign (DEC-072): ``python -m benchmarks.campaign``.

``run`` executes the approved plan's eight steps in order. Every cell is one
subprocess of an existing command, ``benchmarks.run``, ``benchmarks.drills`` or
``benchmarks.baseline``, so the campaign never calls the provider itself and every
refusal those commands make still applies. The model comes only from ``--model``
and the per-million-token prices only from ``--price-in-per-mtok`` and
``--price-out-per-mtok``; both are written into the results, never into code.

**The key.** ``ORQESTRA_OPENROUTER_API_KEY`` is checked for presence before a live
campaign and reaches live children through their environment only. A dry run
removes it from every child environment, and its children run on the canned
transport: no network, no key, no cost.

**Gates.** After the pilot the campaign stops if field accuracy is below 0.90, if
the cost per call is above 3 x the $0.0006 estimate, or if any call failed
permanently. A figure the pilot's result does not carry fails its gate.

**Budget.** Before every cell, what the campaign has spent is recomputed from its
own result files (``charge_for``), and the cell gets a call maximum its budget can
pay for at the per-call ceiling. The existing call counter (DEC-071) enforces that
maximum before each request, so no cell spends past what it was given. A cell
that does not fit is skipped, except the large run, which is sized to fit.

Every decision is a line of ``campaign-log.jsonl`` in the campaign's folder.
``reconcile`` records the provider's own usage figure after the campaign.
"""

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import IntEnum, StrEnum
from functools import cache
from pathlib import Path
from typing import Any

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
ACCURACY_GATE = Decimal("0.90")
CEILING_MARGIN = Decimal("1.25")
CALL_HEADROOM = Decimal("1.1")
LARGE_MAX = 50_000
CALL_TIMEOUT = "PT30S"
CELL_STOP_MINUTES = 60
LARGE_STOP_MINUTES = 180
VICTIMS = 2
WORKER_COUNTS = (1, 4, 8, 16, 32)
REPETITIONS = 3
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
        cells = [extract(Step.PILOT, self.pilot, self.pilot_batch, self.pilot_workers)]
        for repetition in range(1, self.repetitions + 1):
            for workers in self.worker_counts:
                cell = extract(Step.SCALING, self.scaling, self.scaling_batch, workers)
                cells.append(replace(cell, repetition=repetition))
        for batch in self.task_size_batches:
            cells.append(extract(Step.TASK_SIZE, self.task_size, batch, self.task_size_workers))
        cells.append(
            PlannedCell(
                step=Step.RULES_FIRST,
                tool=Tool.RUN,
                workload=Workload.MIX,
                size=self.mix,
                batch=self.mix_batch,
                workers=self.mix_workers,
            )
        )
        # The drill runs the mix until its extract workload exists.
        cells.append(
            PlannedCell(
                step=Step.FAILURES,
                tool=Tool.DRILL,
                workload=Workload.MIX,
                size=self.failures,
                batch=self.failure_batch,
                workers=self.failure_workers,
            )
        )
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


# --- what a result says ------------------------------------------------------------------

# The extraction block: written by ``benchmarks.run --profile extract`` and by the baseline,
# with the same names for the figures read here.
EXTRACTION = "extraction"


@dataclass(frozen=True, slots=True)
class Figures:
    """What the campaign reads from one result. ``None`` means the result does not say."""

    status: str | None
    calls: int | None  # every ledger call, retried and repeated ones included
    token_cost: Decimal | None  # ledger tokens x the given prices
    credits: Decimal | None  # what the provider reported, in its credits
    matched: int | None
    expected: int | None
    permanent_failures: int | None
    items_per_s: float | None


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
        # ``cost.total`` prices only the answered calls that reported their tokens; with
        # any that did not, the token cost is unknown rather than low.
        uncounted = _count(_path(block, "tokens", "answered_without_counts"))
        return Figures(
            status=status,
            calls=_count(_path(block, "calls", "total")),
            token_cost=_decimal(_path(block, "cost", "total")) if uncounted == 0 else None,
            credits=_decimal(_path(block, "cost", "reported", OPENROUTER_USAGE_UNIT)),
            matched=_count(_path(block, "accuracy", "matched")),
            expected=_count(_path(block, "accuracy", "expected")),
            permanent_failures=failures,
            items_per_s=speed,
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
        matched=None,
        expected=None,
        permanent_failures=None,
        items_per_s=speed,
    )


# --- budget ------------------------------------------------------------------------------


class Basis(StrEnum):
    TOKENS = "tokens"  # the larger of tokens x prices and the credits
    CALLS = "calls"  # no token figures: calls x the ceiling, or the credits if larger
    CALL_MAXIMUM = "call_maximum"  # no readable result: the whole call maximum x the ceiling


@dataclass(frozen=True, slots=True)
class Charge:
    cost: Decimal
    calls: int | None
    basis: Basis


def charge_for(figures: Figures | None, call_maximum: int, ceiling: Decimal) -> Charge:
    """What a cell is charged against the budget. Unknown spend is never zero."""
    if figures is None or figures.calls is None:
        return Charge(call_maximum * ceiling, None, Basis.CALL_MAXIMUM)
    credits = figures.credits if figures.credits is not None else Decimal(0)
    if figures.token_cost is None:
        return Charge(max(figures.calls * ceiling, credits), figures.calls, Basis.CALLS)
    return Charge(max(figures.token_cost, credits), figures.calls, Basis.TOKENS)


def per_call_ceiling(pilot_passed: bool, charges: Sequence[Charge]) -> Decimal:
    """The cost per call the budget plans with: the gate's limit until the pilot passes,
    then the observed cost per token-priced call (at least the estimate) with a margin."""
    if not pilot_passed:
        return COST_GATE
    priced = [charge for charge in charges if charge.basis is Basis.TOKENS and charge.calls]
    calls = sum(charge.calls or 0 for charge in priced)
    observed = sum((charge.cost for charge in priced), Decimal(0)) / calls if calls else Decimal(0)
    return max(observed, ESTIMATED_COST_PER_CALL) * CEILING_MARGIN


def fits(cell: PlannedCell, remaining: Decimal, ceiling: Decimal) -> bool:
    return cell.call_maximum * ceiling <= remaining


def size_large(
    plan: CampaignPlan, workers: int, remaining: Decimal, ceiling: Decimal
) -> PlannedCell | None:
    """The largest large run the remaining budget pays for, in whole tasks, or ``None``
    when it would not give every worker a task."""
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


# --- gates -------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Gate:
    name: str
    value: str | None
    threshold: str
    passed: bool

    def event(self) -> dict[str, Any]:
        return {
            "event": "gate",
            "gate": self.name,
            "value": self.value,
            "threshold": self.threshold,
            "passed": self.passed,
        }


def pilot_gates(figures: Figures | None, charge: Charge) -> list[Gate]:
    """The three pilot gates. A figure the result does not carry fails its gate."""
    accuracy: Decimal | None = None
    if figures is not None and figures.matched is not None and figures.expected:
        accuracy = Decimal(figures.matched) / Decimal(figures.expected)
    per_call: Decimal | None = None
    if charge.basis is Basis.TOKENS and charge.calls:
        per_call = charge.cost / charge.calls
    permanent = None if figures is None else figures.permanent_failures
    complete = figures is not None and figures.status == Status.COMPLETE
    return [
        Gate(
            "field_accuracy",
            None if accuracy is None else str(accuracy),
            f">= {ACCURACY_GATE}",
            accuracy is not None and accuracy >= ACCURACY_GATE,
        ),
        Gate(
            "cost_per_call",
            None if per_call is None else str(per_call),
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
            str(self.price_in),
            "--price-out-per-mtok",
            str(self.price_out),
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
            *provider,
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


def run_command(argv: Sequence[str], env: Mapping[str, str]) -> Finished:
    completed = subprocess.run(  # an argv list, never a shell
        list(argv), env=dict(env), cwd=ROOT, capture_output=True, text=True, check=False
    )
    return Finished(completed.returncode, completed.stdout, completed.stderr)


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
    runs: dict[str, CellRun] = field(default_factory=dict)
    skipped: set[str] = field(default_factory=set)
    pilot_passed: bool = False
    stopped: str | None = None

    @property
    def spent(self) -> Decimal:
        return sum((run.charge.cost for run in self.runs.values()), Decimal(0))

    @property
    def remaining(self) -> Decimal:
        return self.budget - self.spent

    def ceiling(self) -> Decimal:
        return per_call_ceiling(self.pilot_passed, [run.charge for run in self.runs.values()])

    def resume(self) -> None:
        """Rebuild the state from the log: a started cell is never run again."""
        started: dict[str, dict[str, Any]] = {}
        finished: dict[str, Path | None] = {}
        for event in self.log.events():
            kind = event.get("event")
            if kind == "cell_started":
                started[event["cell"]] = event
            elif kind == "cell_finished":
                finished[event["cell"]] = Path(event["result"]) if event["result"] else None
            elif kind == "cell_skipped":
                self.skipped.add(event["cell"])
            elif kind == "pilot_passed":
                self.pilot_passed = True
            elif kind == "campaign_stopped":
                self.stopped = str(event["reason"])
        for key, event in started.items():
            path = finished.get(key)
            record = read_record(path)
            figures = None if record is None else step_figures(record)
            ceiling = Decimal(event["ceiling"])
            self.runs[key] = CellRun(
                key,
                int(event["step"]),
                int(event["workers"]),
                int(event["call_maximum"]),
                ceiling,
                path,
                figures,
                charge_for(figures, int(event["call_maximum"]), ceiling),
            )

    def run_cell(self, cell: PlannedCell, *, sized: bool = False) -> CellRun | None:
        """Run ``cell`` if it fits the remaining budget; ``None`` if it was skipped."""
        if cell.key in self.runs:
            return self.runs[cell.key]
        ceiling = self.ceiling()
        remaining = self.remaining
        budget = {
            "spent": str(self.spent),
            "remaining": str(remaining),
            "ceiling": str(ceiling),
            "call_maximum": cell.call_maximum,
            "projected": str(cell.call_maximum * ceiling),
        }
        if cell.key in self.skipped or not fits(cell, remaining, ceiling):
            if cell.key not in self.skipped:
                self.log.write(
                    {"event": "cell_skipped", "cell": cell.key, "reason": "budget", **budget}
                )
                self.skipped.add(cell.key)
            return None
        argv = command_for(cell, self.settings, cell.call_maximum)
        self.log.write(
            {
                "event": "cell_started",
                "cell": cell.key,
                "step": cell.step.value,
                "workers": cell.workers,
                "sized": sized,
                "argv": argv,
                **budget,
            }
        )
        env = child_environment(self.environ, live=self.settings.live)
        finished = self.runner(argv, env)
        path = result_path(finished.stdout)
        self._keep_output(cell, finished)
        record = read_record(path)
        figures = None if record is None else step_figures(record)
        charge = charge_for(figures, cell.call_maximum, ceiling)
        self.log.write(
            {
                "event": "cell_finished",
                "cell": cell.key,
                "exit_code": finished.exit_code,
                "result": None if path is None else str(path),
                "status": None if figures is None else figures.status,
                "charge": str(charge.cost),
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
            self.stop("the pilot does not fit the budget")
            return False
        gates = pilot_gates(run.figures, run.charge)
        for gate in gates:
            self.log.write(gate.event())
        if not all(gate.passed for gate in gates):
            failed = ", ".join(gate.name for gate in gates if not gate.passed)
            self.stop(f"pilot gate failed: {failed}")
            return False
        self.pilot_passed = True
        self.log.write({"event": "pilot_passed"})
        return True

    def run_large(self) -> None:
        scaling = [
            (run.workers, run.figures)
            for run in self.runs.values()
            if run.step == Step.SCALING and run.figures is not None
        ]
        workers = best_workers(scaling)
        if workers is None:
            self.log.write({"event": "large_skipped", "reason": "no complete scaling cell"})
            return
        done = [run for run in self.runs.values() if run.step == Step.LARGE]
        if done:
            return
        ceiling = self.ceiling()
        cell = size_large(self.plan, workers, self.remaining, ceiling)
        decision: dict[str, Any] = {
            "event": "large_sized",
            "workers": workers,
            "remaining": str(self.remaining),
            "ceiling": str(ceiling),
            "size": None if cell is None else cell.size,
        }
        self.log.write(decision)
        if cell is not None:
            self.run_cell(cell, sized=True)

    def billing_check(self) -> dict[str, Any]:
        rows = [billing_row(run, live=self.settings.live) for run in self.runs.values()]
        # Each total sums the cells that report the figure; ``unknown_cells`` says how many
        # do not, so a partial sum is never read as the whole.
        totals: dict[str, Any] = {}
        for name in ("calls", "counted_calls", "token_cost", "credits", "charged"):
            known = [row[name] for row in rows if row[name] is not None]
            total = sum((Decimal(value) for value in known), Decimal(0))
            totals[name] = int(total) if name.endswith("calls") else str(total)
            totals[f"{name}_unknown_cells"] = len(rows) - len(known)
        totals["token_cost_minus_credits"] = _difference(totals["token_cost"], totals["credits"])
        check = {
            "campaign_id": self.settings.campaign_id,
            "recorded_at": now(),
            "cells": rows,
            "totals": totals,
            "provider_reported_usd": None,
        }
        path = self.settings.out_dir / BILLING_NAME
        path.write_text(json.dumps(check, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        self.log.write({"event": "billing_check", "path": str(path), "totals": totals})
        return check

    def summary(self) -> dict[str, Any]:
        return {
            "campaign_id": self.settings.campaign_id,
            "model": self.settings.model,
            "live": self.settings.live,
            "prices_per_mtok": {
                "input": None if self.settings.price_in is None else str(self.settings.price_in),
                "output": None if self.settings.price_out is None else str(self.settings.price_out),
            },
            "budget": str(self.budget),
            "spent": str(self.spent),
            "remaining": str(self.remaining),
            "pilot_passed": self.pilot_passed,
            "stopped": self.stopped,
            "cells": {
                key: {
                    "result": None if run.result is None else str(run.result),
                    "status": None if run.figures is None else run.figures.status,
                    "charge": str(run.charge.cost),
                    "charge_basis": run.charge.basis.value,
                    "call_maximum": run.call_maximum,
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
        self.log.write(
            {
                "event": "campaign_started",
                "campaign_id": self.settings.campaign_id,
                "model": self.settings.model,
                "live": self.settings.live,
                "budget": str(self.budget),
                "resumed_cells": sorted(self.runs),
            }
        )
        cells = self.plan.cells()
        if not self.run_pilot(cells[0]):
            self._write_summary()
            return 1
        for cell in cells[1:]:
            if cell.step is Step.BASELINE and not self._billing_done():
                self.billing_check()
            self.run_cell(cell)
        if not self._billing_done():
            self.billing_check()
        self.run_large()
        self.log.write({"event": "campaign_finished", "spent": str(self.spent)})
        self._write_summary()
        return 0

    def _billing_done(self) -> bool:
        return (self.settings.out_dir / BILLING_NAME).exists()

    def _write_summary(self) -> None:
        path = self.settings.out_dir / SUMMARY_NAME
        path.write_text(
            json.dumps(self.summary(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )


def _difference(left: str | None, right: str | None) -> str | None:
    if left is None or right is None:
        return None
    return str(Decimal(left) - Decimal(right))


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
    token_cost = None if figures is None or figures.token_cost is None else str(figures.token_cost)
    credits = None if figures is None or figures.credits is None else str(figures.credits)
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
        "charged": str(run.charge.cost),
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


def start(args: argparse.Namespace, environ: Mapping[str, str], runner: Runner) -> int:
    reason = refusal(args, environ)
    if reason is not None:
        print(f"refused: {reason}", file=sys.stderr)
        return 1
    try:
        refuse_dirty(False)
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
        settings, args.budget_usd, APPROVED_PLAN, runner, environ, CampaignLog(out_dir / LOG_NAME)
    )
    if args.resume:
        campaign.resume()
    code = campaign.execute()
    print(f"campaign {identifier}: spent {campaign.spent} of {campaign.budget}; {out_dir}")
    return code


def reconcile(identifier: str, provider_usd: Decimal) -> int:
    """Record the provider's own figure beside the ledger's in the billing check."""
    folder = campaign_folder(identifier)
    if folder is None or not (folder / BILLING_NAME).is_file():
        print(f"refused: campaign {identifier!r} has no billing check", file=sys.stderr)
        return 1
    if not provider_usd.is_finite() or provider_usd < 0:
        print("refused: --provider-reported-usd must be 0 or more", file=sys.stderr)
        return 1
    path = folder / BILLING_NAME
    check = json.loads(path.read_text(encoding="utf-8"))
    totals = check["totals"]
    check["provider_reported_usd"] = str(provider_usd)
    check["provider_minus_token_cost"] = _difference(str(provider_usd), totals["token_cost"])
    check["provider_minus_credits"] = _difference(str(provider_usd), totals["credits"])
    check["provider_minus_charged"] = _difference(str(provider_usd), totals["charged"])
    path.write_text(json.dumps(check, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    CampaignLog(folder / LOG_NAME).write(
        {"event": "reconciled", "provider_reported_usd": str(provider_usd)}
    )
    print(f"reconciled {path}")
    return 0


def main(argv: Sequence[str] | None = None, runner: Runner = run_command) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.campaign", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the campaign's steps in order")
    run.add_argument("--model", required=True, help="the model slug the owner selected")
    mode = run.add_mutually_exclusive_group(required=True)
    mode.add_argument("--allow-live-calls", action="store_true", help="real, billable calls")
    mode.add_argument("--dry-run", action="store_true", help="canned transport, no key, no cost")
    run.add_argument(
        "--price-in-per-mtok", type=_price, help="live: price per million input tokens"
    )
    run.add_argument(
        "--price-out-per-mtok", type=_price, help="live: price per million output tokens"
    )
    run.add_argument("--budget-usd", type=_price, default=CAMPAIGN_BUDGET)
    run.add_argument("--resume", help="continue the campaign with this id")
    check = commands.add_parser("reconcile", help="record the provider's own usage figure")
    check.add_argument("campaign_id")
    check.add_argument("--provider-reported-usd", type=_price, required=True)
    args = parser.parse_args(argv)
    if args.command == "reconcile":
        return reconcile(args.campaign_id, args.provider_reported_usd)
    return start(args, os.environ, runner)


if __name__ == "__main__":
    sys.exit(main())
