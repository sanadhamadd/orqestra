"""The paid campaign's runner (DEC-072): plan, budget, gates, commands, log and resume.

No child command runs: a fake runner writes the result a command would have written
and prints its last line. The key is a sentinel string and sockets are blocked.
"""

import ast
import inspect
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from benchmarks import campaign
from benchmarks.campaign import (
    APPROVED_PLAN,
    BILLING_NAME,
    COST_GATE,
    ESTIMATED_COST_PER_CALL,
    LOG_NAME,
    SUMMARY_NAME,
    Basis,
    Campaign,
    CampaignLog,
    CampaignPlan,
    Charge,
    Figures,
    Finished,
    PlannedCell,
    Settings,
    Step,
    Tool,
    Workload,
    best_workers,
    charge_for,
    child_environment,
    command_for,
    per_call_ceiling,
    pilot_gates,
    result_path,
    size_large,
    step_figures,
)
from benchmarks.live import API_KEY_ENV, BUDGET_FILE_NAME
from benchmarks.result import Status
from benchmarks.worker import DEFAULT_CAP

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
PRICE_IN = Decimal("0.10")
PRICE_OUT = Decimal("0.50")


def figures(**overrides: Any) -> Figures:
    values: dict[str, Any] = {
        "status": Status.COMPLETE.value,
        "calls": 100,
        "token_cost": Decimal("0.05"),
        "credits": Decimal("0.01"),
        "matched": 950,
        "expected": 1000,
        "permanent_failures": 0,
        "items_per_s": 10.0,
    }
    values.update(overrides)
    return Figures(**values)


def extraction_record(
    *,
    run_id: str = "run1",
    status: str = "complete",
    calls: int = 100,
    token_priced: str = "0.05",
    credits: str = "0.01",
    matched: int = 950,
    expected: int = 1000,
    permanent: Mapping[str, int] | None = None,
    speed: float = 10.0,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "status": status,
        "throughput": {"makespan_items_per_s": speed},
        "extraction": {
            "calls": {"total": calls, "answered": calls, "failed": 0, "unresolved": 0},
            "tokens": {"input": calls * 900, "output": calls * 150, "answered_without_counts": 0},
            "cost": {
                "currency": "USD",
                "total": token_priced,
                "reported": {"openrouter_credits": credits},
            },
            "accuracy": {
                "matched": matched,
                "expected": expected,
                "overall": matched / expected if expected else None,
            },
            "permanent_errors": {"rejected": 0, "unsupported_request": 0, **(permanent or {})},
        },
    }


def mix_record(*, calls: int = 500, credits: str = "0.02") -> dict[str, Any]:
    return {
        "run_id": "mix1",
        "status": "complete",
        "throughput": {"makespan_items_per_s": 50.0},
        "usage": {
            "calls": calls,
            "reported_totals": {"openrouter_credits": {"amount": credits, "calls": calls}},
        },
        "provider": {"calls_made": calls},
    }


# --- the plan ----------------------------------------------------------------------------


def test_the_approved_plan_runs_its_cells_in_order() -> None:
    cells = APPROVED_PLAN.cells()
    assert [cell.step for cell in cells] == sorted(cell.step for cell in cells)
    assert Step.BILLING not in {cell.step for cell in cells}
    assert Step.LARGE not in {cell.step for cell in cells}
    assert len({cell.key for cell in cells}) == len(cells)
    pilot = cells[0]
    assert (pilot.step, pilot.size, pilot.workload) == (Step.PILOT, 100, Workload.EXTRACT)
    scaling = [cell for cell in cells if cell.step is Step.SCALING]
    assert [(cell.workers, cell.repetition) for cell in scaling[:6]] == [
        (1, 1),
        (4, 1),
        (8, 1),
        (16, 1),
        (32, 1),
        (1, 2),
    ]
    assert len(scaling) == 15
    assert {cell.size for cell in scaling} == {2_000}
    assert [cell.batch for cell in cells if cell.step is Step.TASK_SIZE] == [5, 25, 100]
    (mix,) = [cell for cell in cells if cell.step is Step.RULES_FIRST]
    assert (mix.workload, mix.size, mix.workers) == (Workload.MIX, 10_000, 8)
    failures = [cell for cell in cells if cell.step is Step.FAILURES]
    assert [cell.tool for cell in failures] == [Tool.DRILL, Tool.RUN]
    assert {cell.workers for cell in failures} == {32}
    baseline = [cell for cell in cells if cell.step is Step.BASELINE]
    assert [(cell.mode, cell.workers) for cell in baseline] == [
        ("loop", 1),
        ("threads", 1),
        ("threads", 4),
        ("threads", 8),
        ("threads", 16),
        ("threads", 32),
    ]


def test_every_32_worker_cell_has_a_task_for_every_worker() -> None:
    for cell in APPROVED_PLAN.cells():
        if cell.tool is not Tool.BASELINE:
            assert -(-cell.size // cell.batch) >= cell.workers, cell.key


def test_a_cells_call_maximum_covers_one_repeated_task_per_worker() -> None:
    extract = PlannedCell(
        step=Step.SCALING, tool=Tool.RUN, workload=Workload.EXTRACT, size=2_000, batch=25, workers=8
    )
    assert extract.expected_calls == 2_000
    assert extract.call_maximum == 2_200 + 25 * 8
    mix = PlannedCell(
        step=Step.RULES_FIRST,
        tool=Tool.RUN,
        workload=Workload.MIX,
        size=10_000,
        batch=100,
        workers=8,
    )
    assert 0 < mix.expected_calls < 10_000  # the oracle's calls, about 1 in 20
    assert mix.call_maximum == -(-mix.expected_calls * 11 // 10) + DEFAULT_CAP * 8


# --- what a result says ------------------------------------------------------------------


def test_an_extraction_results_figures() -> None:
    read = step_figures(extraction_record(permanent={"rejected": 2}))
    assert read == Figures(
        status="complete",
        calls=100,
        token_cost=Decimal("0.05"),
        credits=Decimal("0.01"),
        matched=950,
        expected=1000,
        permanent_failures=2,
        items_per_s=10.0,
    )


def test_a_mix_results_figures_have_no_token_cost() -> None:
    read = step_figures(mix_record())
    assert (read.calls, read.credits, read.token_cost) == (500, Decimal("0.02"), None)
    assert read.matched is None
    assert read.permanent_failures is None


@pytest.mark.parametrize(
    ("broken", "unknown"),
    [
        ({"calls": {"total": True}}, "calls"),
        ({"calls": {"total": -1}}, "calls"),
        ({"calls": "100"}, "calls"),
        ({"cost": {"total": "NaN"}}, "token_cost"),
        ({"cost": {"total": "-1"}}, "token_cost"),
        ({"cost": {"total": "a lot"}}, "token_cost"),
        ({"tokens": {"answered_without_counts": 1}}, "token_cost"),
        ({"tokens": {}}, "token_cost"),
        ({"accuracy": {"matched": 1.5, "expected": 2}}, "matched"),
    ],
)
def test_a_figure_that_is_not_a_clean_number_is_unknown(
    broken: dict[str, Any], unknown: str
) -> None:
    record = extraction_record()
    record["extraction"] |= broken
    assert getattr(step_figures(record), unknown) is None


# --- budget ------------------------------------------------------------------------------


def test_a_priced_cell_is_charged_the_larger_of_tokens_and_credits() -> None:
    ceiling = Decimal("0.001")
    assert charge_for(figures(), 500, ceiling) == Charge(Decimal("0.05"), 100, Basis.TOKENS)
    more_credits = figures(credits=Decimal("0.07"))
    assert charge_for(more_credits, 500, ceiling).cost == Decimal("0.07")
    no_credits = figures(credits=None)
    assert charge_for(no_credits, 500, ceiling).cost == Decimal("0.05")


def test_a_cell_without_token_figures_is_charged_its_calls_at_the_ceiling() -> None:
    ceiling = Decimal("0.001")
    read = figures(token_cost=None, credits=Decimal("0.0001"))
    assert charge_for(read, 500, ceiling) == Charge(Decimal("0.100"), 100, Basis.CALLS)
    rich = figures(token_cost=None, credits=Decimal("0.5"))
    assert charge_for(rich, 500, ceiling).cost == Decimal("0.5")


@pytest.mark.parametrize("read", [None, figures(calls=None)])
def test_a_cell_without_a_readable_result_is_charged_its_whole_maximum(
    read: Figures | None,
) -> None:
    charge = charge_for(read, 500, Decimal("0.001"))
    assert charge == Charge(Decimal("0.500"), None, Basis.CALL_MAXIMUM)


def test_the_ceiling_is_the_gate_until_the_pilot_passes_then_observed_with_a_margin() -> None:
    cheap = [Charge(Decimal("0.04"), 100, Basis.TOKENS)]  # $0.0004 a call
    assert per_call_ceiling(False, cheap) == COST_GATE
    assert per_call_ceiling(True, []) == ESTIMATED_COST_PER_CALL * Decimal("1.25")
    assert per_call_ceiling(True, cheap) == ESTIMATED_COST_PER_CALL * Decimal("1.25")
    dear = [
        Charge(Decimal("0.10"), 100, Basis.TOKENS),
        Charge(Decimal("0.02"), 100, Basis.TOKENS),
        Charge(Decimal("9"), None, Basis.CALL_MAXIMUM),  # not an observation
        Charge(Decimal("9"), 10, Basis.CALLS),  # its cost is the ceiling itself
    ]
    assert per_call_ceiling(True, dear) == Decimal("0.0006") * Decimal("1.25")
    dearer = [Charge(Decimal("0.20"), 100, Basis.TOKENS)]
    assert per_call_ceiling(True, dearer) == Decimal("0.002") * Decimal("1.25")


def test_the_large_run_is_the_largest_whole_task_count_the_budget_pays_for() -> None:
    ceiling = Decimal("0.00075")
    cell = size_large(APPROVED_PLAN, 8, Decimal("10"), ceiling)
    assert cell is not None
    assert cell.size % APPROVED_PLAN.large_batch == 0
    assert cell.call_maximum * ceiling <= Decimal("10")
    bigger = APPROVED_PLAN.large(cell.size + APPROVED_PLAN.large_batch, 8)
    assert bigger.call_maximum * ceiling > Decimal("10")
    assert (cell.step, cell.workers, cell.workload) == (Step.LARGE, 8, Workload.EXTRACT)


def test_the_large_run_stops_at_fifty_thousand_and_needs_a_task_per_worker() -> None:
    huge = size_large(APPROVED_PLAN, 8, Decimal(1_000), Decimal("0.00075"))
    assert huge is not None
    assert huge.size == 50_000
    assert size_large(APPROVED_PLAN, 32, Decimal("0.5"), Decimal("0.00075")) is None


def test_the_best_worker_count_has_the_highest_median_and_ties_go_to_fewer() -> None:
    def cell(workers: int, speed: float, status: str = "complete") -> tuple[int, Figures]:
        return workers, figures(items_per_s=speed, status=status)

    scaling = [cell(1, 5), cell(8, 30), cell(8, 10), cell(8, 31), cell(16, 30), cell(16, 30)]
    assert best_workers(scaling) == 8  # median 30 at both; fewer wins
    assert best_workers([*scaling, cell(32, 99, "incomplete")]) == 8
    assert best_workers([cell(4, 1, "failed_correctness")]) is None
    assert best_workers([]) is None


# --- gates -------------------------------------------------------------------------------


def priced(cost: str, calls: int = 1000) -> Charge:
    return Charge(Decimal(cost), calls, Basis.TOKENS)


def test_a_pilot_inside_every_gate_passes_even_at_the_edges() -> None:
    gates = pilot_gates(figures(matched=900, expected=1000), priced("1.8"))
    assert [gate.name for gate in gates] == [
        "field_accuracy",
        "cost_per_call",
        "no_permanent_failure",
    ]
    assert all(gate.passed for gate in gates)


@pytest.mark.parametrize(
    ("read", "charge", "failed"),
    [
        (figures(matched=899, expected=1000), priced("1"), "field_accuracy"),
        (figures(matched=None), priced("1"), "field_accuracy"),
        (figures(expected=0, matched=0), priced("1"), "field_accuracy"),
        (figures(), priced("1.81"), "cost_per_call"),
        (figures(), Charge(Decimal("0.1"), 1000, Basis.CALLS), "cost_per_call"),
        (figures(), Charge(Decimal("0.1"), None, Basis.CALL_MAXIMUM), "cost_per_call"),
        (figures(permanent_failures=1), priced("1"), "no_permanent_failure"),
        (figures(permanent_failures=None), priced("1"), "no_permanent_failure"),
        (figures(status="incomplete"), priced("1"), "no_permanent_failure"),
    ],
)
def test_a_pilot_outside_a_gate_or_without_its_figure_fails_it(
    read: Figures, charge: Charge, failed: str
) -> None:
    gates = {gate.name: gate for gate in pilot_gates(read, charge)}
    assert not gates[failed].passed
    assert all(gate.passed for name, gate in gates.items() if name != failed)


def test_a_pilot_with_no_result_fails_every_gate() -> None:
    gates = pilot_gates(None, Charge(Decimal(1), None, Basis.CALL_MAXIMUM))
    assert not any(gate.passed for gate in gates)


# --- commands ----------------------------------------------------------------------------


def settings(tmp_path: Path, *, live: bool) -> Settings:
    return Settings(
        campaign_id="c1",
        model=MODEL,
        live=live,
        price_in=PRICE_IN if live else None,
        price_out=PRICE_OUT if live else None,
        out_dir=tmp_path,
    )


def cell_of(step: Step, tool: Tool | None = None) -> PlannedCell:
    return next(
        cell
        for cell in APPROVED_PLAN.cells()
        if cell.step is step and (tool is None or cell.tool is tool)
    )


def test_a_dry_run_command_passes_dry_run_and_no_prices(tmp_path: Path) -> None:
    for cell in APPROVED_PLAN.cells():
        argv = command_for(cell, settings(tmp_path, live=False), 123)
        assert argv[:3] == [sys.executable, "-m", cell.tool.value]
        assert "--dry-run" in argv
        assert "--allow-live-calls" not in argv
        assert "--price-in-per-mtok" not in argv
        assert argv[argv.index("--model") + 1] == MODEL
        assert argv[argv.index("--max-provider-calls") + 1] == "123"
        assert argv[argv.index("--call-timeout") + 1] == "PT30S"
        assert argv[argv.index("--out-dir") + 1] == str(tmp_path)
        assert "--keep-schema" not in argv
        assert "--allow-dirty" not in argv


def test_a_live_command_passes_prices_only_where_the_model_reads_every_document(
    tmp_path: Path,
) -> None:
    live = settings(tmp_path, live=True)
    pilot = command_for(cell_of(Step.PILOT), live, 10)
    assert "--allow-live-calls" in pilot
    assert pilot[pilot.index("--profile") + 1] == "extract"
    assert pilot[pilot.index("--price-in-per-mtok") + 1] == "0.10"
    assert pilot[pilot.index("--price-out-per-mtok") + 1] == "0.50"
    assert pilot[pilot.index("--provider") + 1] == "openrouter"
    baseline = command_for(cell_of(Step.BASELINE), live, 10)
    assert "--price-in-per-mtok" in baseline
    assert "--provider" not in baseline
    mix = command_for(cell_of(Step.RULES_FIRST), live, 10)
    assert mix[mix.index("--profile") + 1] == "invoice"
    assert "--price-in-per-mtok" not in mix
    drill = command_for(cell_of(Step.FAILURES, Tool.DRILL), live, 10)
    assert drill[3:4] == ["d1"]
    assert drill[drill.index("--victims") + 1] == "2"
    assert drill[drill.index("--workers") + 1] == "32"
    assert "--price-in-per-mtok" not in drill


def test_a_dry_runs_children_never_see_the_key() -> None:
    environ = {API_KEY_ENV: SENTINEL, "PATH": "/bin"}
    assert child_environment(environ, live=False) == {"PATH": "/bin"}
    assert child_environment(environ, live=True) == environ


def test_the_result_path_is_the_last_word_of_the_last_line(tmp_path: Path) -> None:
    path = tmp_path / "r.json"
    path.write_text("{}")
    assert result_path(f"dry run: a line\ncomplete {path}\n") == path
    assert result_path(f"complete d1 {path}\n\n") == path
    assert result_path(f"complete {tmp_path / 'missing.json'}") is None
    assert result_path(f"complete {tmp_path}") is None
    assert result_path("") is None


# --- the campaign ------------------------------------------------------------------------

RecordFor = Callable[[str, Sequence[str]], dict[str, Any] | None]


class FakeRunner:
    """Writes the result each command would have written and prints its last line."""

    def __init__(self, record_for: RecordFor, *, fail_at: int | None = None) -> None:
        self.record_for = record_for
        self.fail_at = fail_at
        self.calls: list[tuple[list[str], dict[str, str]]] = []

    def __call__(self, argv: Sequence[str], env: Mapping[str, str]) -> Finished:
        if self.fail_at is not None and len(self.calls) == self.fail_at:
            msg = "the machine went down"
            raise RuntimeError(msg)
        self.calls.append((list(argv), dict(env)))
        key = argv[argv.index("--note") + 1].split()[-1]
        record = self.record_for(key, argv)
        if record is None:
            return Finished(1, "", "refused: not connected\n")
        path = Path(argv[argv.index("--out-dir") + 1]) / f"{key}.json"
        path.write_text(json.dumps(record))
        return Finished(0, f"{record['status']} {path}\n", "")

    @property
    def keys(self) -> list[str]:
        return [argv[argv.index("--note") + 1].split()[-1] for argv, _ in self.calls]


def good_records(key: str, argv: Sequence[str]) -> dict[str, Any] | None:
    """Every cell succeeds at $0.0005 a call; more workers are faster up to 16."""
    if "-invoice-" in key:
        return mix_record()
    size = int(argv[argv.index("--invoices" if "--invoices" in argv else "--size") + 1])
    workers = int(argv[argv.index("--workers") + 1])
    return extraction_record(
        calls=size,
        token_priced=str(Decimal("0.0005") * size),
        credits="0",
        matched=size * 9,
        expected=size * 10,
        speed=float(min(workers, 16)),
    )


def new_campaign(
    tmp_path: Path,
    runner: FakeRunner,
    *,
    live: bool = False,
    budget: Decimal = Decimal(45),
    plan: CampaignPlan = APPROVED_PLAN,
) -> Campaign:
    environ = {API_KEY_ENV: SENTINEL, "PATH": "/bin"}
    return Campaign(
        settings(tmp_path, live=live),
        budget,
        plan,
        runner,
        environ,
        CampaignLog(tmp_path / LOG_NAME),
    )


def events(tmp_path: Path, kind: str | None = None) -> list[dict[str, Any]]:
    found = list(CampaignLog(tmp_path / LOG_NAME).events())
    return [event for event in found if kind is None or event["event"] == kind]


@pytest.fixture(autouse=True)
def raw_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    raw = tmp_path / "raw"
    raw.mkdir()
    monkeypatch.setattr(campaign, "RAW_DIR", raw)
    return raw


def test_a_dry_campaign_runs_every_step_in_order(tmp_path: Path) -> None:
    runner = FakeRunner(good_records)
    run = new_campaign(tmp_path, runner)
    assert run.execute() == 0
    planned = [cell.key for cell in APPROVED_PLAN.cells()]
    assert runner.keys[: len(planned)] == planned
    (large,) = runner.keys[len(planned) :]
    assert large.startswith("s8-run-extract-")
    assert "-w16-" in large  # the fastest median; 32 is no faster
    for argv, env in runner.calls:
        assert API_KEY_ENV not in env
        assert "--dry-run" in argv
        assert SENTINEL not in " ".join(argv)
    kinds = [event["event"] for event in events(tmp_path)]
    assert kinds[0] == "campaign_started"
    assert kinds.index("pilot_passed") < kinds.index("billing_check")
    started = [event["cell"] for event in events(tmp_path, "cell_started")]
    billing = kinds.index("billing_check")
    first_baseline = next(
        index
        for index, event in enumerate(events(tmp_path))
        if event["event"] == "cell_started" and event["cell"].startswith("s7-")
    )
    assert billing < first_baseline
    assert len(started) == len(planned) + 1
    assert kinds[-1] == "campaign_finished"
    summary = json.loads((tmp_path / SUMMARY_NAME).read_text())
    assert summary["model"] == MODEL
    assert summary["stopped"] is None
    assert Decimal(summary["spent"]) + Decimal(summary["remaining"]) == Decimal(45)
    assert (tmp_path / "logs" / f"{planned[0]}.out").read_text().startswith("complete ")


def test_a_live_campaign_passes_the_key_in_the_environment_only(tmp_path: Path) -> None:
    runner = FakeRunner(good_records)
    assert new_campaign(tmp_path, runner, live=True).execute() == 0
    for argv, env in runner.calls:
        assert env[API_KEY_ENV] == SENTINEL
        assert SENTINEL not in " ".join(argv)
        assert "--allow-live-calls" in argv
    assert SENTINEL not in (tmp_path / LOG_NAME).read_text()
    assert SENTINEL not in (tmp_path / SUMMARY_NAME).read_text()


@pytest.mark.parametrize(
    ("pilot", "gate"),
    [
        (extraction_record(matched=899, expected=1000, token_priced="0.01"), "field_accuracy"),
        (extraction_record(calls=100, token_priced="0.181"), "cost_per_call"),
        (extraction_record(permanent={"unsupported_request": 1}), "no_permanent_failure"),
        (None, "field_accuracy"),
    ],
)
def test_a_failed_pilot_gate_stops_the_campaign(
    tmp_path: Path, pilot: dict[str, Any] | None, gate: str
) -> None:
    runner = FakeRunner(lambda key, argv: pilot)
    run = new_campaign(tmp_path, runner)
    assert run.execute() == 1
    assert len(runner.calls) == 1
    gates = {event["gate"]: event for event in events(tmp_path, "gate")}
    assert not gates[gate]["passed"]
    assert gates[gate]["threshold"]
    (stopped,) = events(tmp_path, "campaign_stopped")
    assert gate in stopped["reason"]
    assert json.loads((tmp_path / SUMMARY_NAME).read_text())["stopped"] == stopped["reason"]
    assert not (tmp_path / BILLING_NAME).exists()


def test_cells_that_do_not_fit_are_skipped_and_spending_never_passes_the_budget(
    tmp_path: Path,
) -> None:
    runner = FakeRunner(good_records)
    run = new_campaign(tmp_path, runner, budget=Decimal(12))
    assert run.execute() == 0
    skipped = events(tmp_path, "cell_skipped")
    assert skipped
    assert all(event["reason"] == "budget" for event in skipped)
    for event in skipped:
        assert Decimal(event["projected"]) > Decimal(event["remaining"])
    for event in events(tmp_path, "cell_started"):
        assert Decimal(event["projected"]) <= Decimal(event["remaining"])
    assert run.spent <= Decimal(12)


def test_a_cell_that_leaves_no_result_is_charged_its_whole_maximum(tmp_path: Path) -> None:
    def baseline_not_connected(key: str, argv: Sequence[str]) -> dict[str, Any] | None:
        return None if key.startswith("s7-") else good_records(key, argv)

    run = new_campaign(tmp_path, FakeRunner(baseline_not_connected))
    assert run.execute() == 0
    finished = {event["cell"]: event for event in events(tmp_path, "cell_finished")}
    started = {event["cell"]: event for event in events(tmp_path, "cell_started")}
    for key, event in finished.items():
        if key.startswith("s7-"):
            assert event["exit_code"] == 1
            assert event["result"] is None
            assert event["charge_basis"] == "call_maximum"
            expected = Decimal(started[key]["call_maximum"]) * Decimal(started[key]["ceiling"])
            assert Decimal(event["charge"]) == expected


def test_a_too_small_budget_stops_before_the_pilot(tmp_path: Path) -> None:
    runner = FakeRunner(good_records)
    assert new_campaign(tmp_path, runner, budget=Decimal("0.1")).execute() == 1
    assert runner.calls == []
    (stopped,) = events(tmp_path, "campaign_stopped")
    assert "fit" in stopped["reason"]


def test_the_billing_check_sets_the_ledger_beside_the_counters(
    tmp_path: Path, raw_dir: Path
) -> None:
    counter = raw_dir / "run1" / "cell" / BUDGET_FILE_NAME
    counter.parent.mkdir(parents=True)
    counter.write_text("103\n")
    (counter.parent / "worker-0.log").write_text(
        json.dumps({"event": "canned_transport_requests", "requests": 101}) + "\n"
    )
    assert new_campaign(tmp_path, FakeRunner(good_records)).execute() == 0
    check = json.loads((tmp_path / BILLING_NAME).read_text())
    pilot = check["cells"][0]
    assert pilot["calls"] == 100
    assert pilot["counted_calls"] == 103
    assert pilot["counted_minus_calls"] == 3
    assert pilot["transport_requests"] == 101
    assert Decimal(pilot["token_cost_minus_credits"]) == Decimal(pilot["token_cost"])
    assert check["provider_reported_usd"] is None
    assert Decimal(check["totals"]["charged"]) > 0


def test_a_resumed_campaign_never_runs_a_started_cell_again(tmp_path: Path) -> None:
    first = FakeRunner(good_records, fail_at=5)
    with pytest.raises(RuntimeError):
        new_campaign(tmp_path, first).execute()
    second = FakeRunner(good_records)
    resumed = new_campaign(tmp_path, second)
    resumed.resume()
    assert resumed.pilot_passed
    crashed = APPROVED_PLAN.cells()[5].key
    assert resumed.runs[crashed].charge.basis is Basis.CALL_MAXIMUM
    assert resumed.execute() == 0
    assert not set(first.keys) & set(second.keys)
    assert crashed not in second.keys
    assert second.keys[0] == APPROVED_PLAN.cells()[6].key


def test_a_stopped_campaign_is_not_resumed(tmp_path: Path) -> None:
    new_campaign(tmp_path, FakeRunner(lambda key, argv: None)).execute()
    again = FakeRunner(good_records)
    resumed = new_campaign(tmp_path, again)
    resumed.resume()
    assert resumed.execute() == 1
    assert again.calls == []


# --- the command line --------------------------------------------------------------------


@pytest.fixture
def bases(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(campaign, "LIVE_BASE", tmp_path / "live")
    monkeypatch.setattr(campaign, "DRY_RUN_BASE", tmp_path / "dry")
    monkeypatch.setattr(campaign, "refuse_dirty", lambda allow_dirty: None)
    monkeypatch.setattr(
        campaign, "code_state", lambda: {"git_sha": "abcdef0123", "git_dirty": False}
    )
    return tmp_path


DRY = ["run", "--model", MODEL, "--dry-run"]
LIVE = ["run", "--model", MODEL, "--allow-live-calls"]
PRICES = ["--price-in-per-mtok", "0.10", "--price-out-per-mtok", "0.50"]


@pytest.mark.parametrize(
    ("argv", "environ", "words"),
    [
        (LIVE + PRICES, {}, "is not set"),
        (LIVE, {API_KEY_ENV: SENTINEL}, "--price-in-per-mtok"),
        ([*LIVE, "--price-in-per-mtok", "0.10"], {API_KEY_ENV: SENTINEL}, "--price-out"),
        (
            [*LIVE, "--price-in-per-mtok", "0", "--price-out-per-mtok", "1"],
            {API_KEY_ENV: "k"},
            "above 0",
        ),
        (DRY + PRICES, {}, "canned prices"),
        ([*DRY, "--budget-usd", "45.01"], {}, "at most 45"),
        ([*DRY, "--budget-usd", "0"], {}, "above 0"),
        (["run", "--model", "-bad slug", "--dry-run"], {}, "--model"),
    ],
)
def test_the_command_refuses_before_any_folder_or_child(
    bases: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    environ: dict[str, str],
    words: str,
) -> None:
    monkeypatch.setattr(os, "environ", environ)
    runner = FakeRunner(good_records)
    assert campaign.main(argv, runner) == 1
    error = capsys.readouterr().err
    assert error.startswith("refused: ")
    assert words in error
    assert SENTINEL not in error
    assert runner.calls == []
    assert not (bases / "live").exists()
    assert not (bases / "dry").exists()


def test_live_and_dry_run_are_exclusive_and_one_is_required(bases: Path) -> None:
    for argv in (["run", "--model", MODEL], [*DRY, "--allow-live-calls"]):
        with pytest.raises(SystemExit):
            campaign.main(argv, FakeRunner(good_records))


def test_the_command_runs_a_dry_campaign_into_its_own_folder_and_reconciles(
    bases: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(os, "environ", {API_KEY_ENV: SENTINEL})
    runner = FakeRunner(good_records)
    assert campaign.main(DRY, runner) == 0
    (folder,) = (bases / "dry").iterdir()
    assert folder.name.endswith("-abcdef0")
    assert (folder / LOG_NAME).is_file()
    assert not (bases / "live").exists()
    assert all(API_KEY_ENV not in env for _, env in runner.calls)
    capsys.readouterr()
    assert campaign.main(["reconcile", folder.name, "--provider-reported-usd", "12.5"]) == 0
    check = json.loads((folder / BILLING_NAME).read_text())
    assert check["provider_reported_usd"] == "12.5"
    expected = Decimal("12.5") - Decimal(check["totals"]["token_cost"])
    assert Decimal(check["provider_minus_token_cost"]) == expected
    assert events(folder, "reconciled")[0]["provider_reported_usd"] == "12.5"
    assert campaign.main(["reconcile", "nope", "--provider-reported-usd", "1"]) == 1


def test_a_resume_names_an_existing_campaign(
    bases: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert campaign.main([*DRY, "--resume", "missing"], FakeRunner(good_records)) == 1
    assert "no campaign" in capsys.readouterr().err


# --- scope -------------------------------------------------------------------------------


def test_the_campaign_imports_no_network_module_and_names_no_model() -> None:
    source = inspect.getsource(campaign)
    tree = ast.parse(source)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    network = {"socket", "http", "http.client", "urllib", "urllib.request", "ssl"}
    assert not imported & network
    assert not any("stdlib_transport" in name for name in imported)
    slug = re.compile(r"[A-Za-z0-9][\w.:-]*/[\w.:-]+")
    constants = [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    assert not [text for text in constants if slug.fullmatch(text)]
