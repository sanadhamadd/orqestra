"""The paid campaign's runner (DEC-072): plan, budget, gates, commands, log and resume.

No child command runs: a fake runner writes the result a command would have written
and prints its last line. Fake limits stand in for the extraction workload's. The key
is a sentinel string and sockets are blocked.
"""

import ast
import inspect
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from benchmarks import campaign
from benchmarks.campaign import (
    APPROVED_PLAN,
    BILLING_NAME,
    COST_GATE,
    DRY_RUN_PRICES,
    FRAMING_BYTES,
    LOG_NAME,
    SUMMARY_NAME,
    UNBOUNDED_CALL_COST,
    Basis,
    Campaign,
    CampaignLog,
    Ceilings,
    Charge,
    Figures,
    Finished,
    PlannedCell,
    Prices,
    Settings,
    Step,
    Tool,
    Workload,
    best_workers,
    charge_for,
    child_environment,
    command_for,
    hard_call_cost,
    observed_cost_per_call,
    pilot_gates,
    reservations_between,
    result_path,
    size_large,
    step_figures,
)
from benchmarks.live import API_KEY_ENV, BUDGET_FILE_NAME
from benchmarks.result import Status
from benchmarks.run import extract_cap
from benchmarks.worker import DEFAULT_CAP

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
PRICE_IN = Decimal("0.31")  # arbitrary test prices, per million tokens
PRICE_OUT = Decimal("1.17")
LIVE_PRICES = Prices(PRICE_IN, PRICE_OUT)


@dataclass(frozen=True)
class FakeLimits:
    """A 2,000-byte prompt, a 1,024-token reply, and documents that grow with the set."""

    prompt_bytes: int = 2_000
    max_output_tokens: int = 1_024

    def largest_document_bytes(self, size: int) -> int:
        return 1_000 + size // 100


def hard(size: int, prices: Prices = LIVE_PRICES) -> Decimal:
    limits = FakeLimits()
    return hard_call_cost(
        limits.prompt_bytes + limits.largest_document_bytes(size), limits.max_output_tokens, prices
    )


def figures(**overrides: Any) -> Figures:
    values: dict[str, Any] = {
        "status": Status.COMPLETE.value,
        "calls": 100,
        "answered": 100,
        "token_cost": Decimal("0.05"),
        "credits": Decimal("0.01"),
        "accuracy": Decimal("0.95"),
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
    accuracy: float = 0.95,
    permanent: Mapping[str, int] | None = None,
    speed: float = 10.0,
    largest: tuple[int | None, int | None] = (3_000, 500),
    answered: int | None = None,
) -> dict[str, Any]:
    answered = calls if answered is None else answered
    return {
        "run_id": run_id,
        "status": status,
        "throughput": {"makespan_items_per_s": speed},
        "extraction": {
            "documents": calls,
            "calls": {
                "total": calls,
                "answered": answered,
                "failed": calls - answered,
                "unresolved": 0,
            },
            "tokens": {
                "input": calls * 900,
                "output": calls * 150,
                "answered_without_counts": 0,
                "max_per_call": {"input": largest[0], "output": largest[1]},
            },
            "cost": {
                "currency": "USD",
                "total": token_priced,
                "reported": {"openrouter_credits": credits},
            },
            "accuracy": {"matched": 0, "expected": 0, "overall": accuracy},
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
    # One worker reads 2,000 documents in about 85 minutes, past the 60-minute limit.
    assert {(cell.workers, cell.size, cell.batch) for cell in scaling} == {
        (1, 1_000, 25),
        (4, 2_000, 25),
        (8, 2_000, 25),
        (16, 2_000, 25),
        (32, 2_000, 25),
    }
    assert [cell.batch for cell in cells if cell.step is Step.TASK_SIZE] == [5, 25, 100]
    rules_first = [cell for cell in cells if cell.step is Step.RULES_FIRST]
    assert [(c.workload, c.size, c.batch, c.workers, c.repetition) for c in rules_first] == [
        (Workload.MIX, 2_000, 25, 8, 1),
        (Workload.MIX, 2_000, 25, 8, 2),
        (Workload.MIX, 2_000, 25, 8, 3),
        (Workload.MIX, 10_000, 100, 8, 1),
    ]
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
    # an attempt may call up to extract_cap(25) = 30: a throttled document is retried in it
    assert extract.call_maximum == 2_200 + 30 * 8
    drill = replace(extract, step=Step.FAILURES, tool=Tool.DRILL, batch=25, workers=32)
    assert drill.call_maximum == 2_200 + 25 * 32  # the drill's cap is its batch
    baseline = replace(extract, step=Step.BASELINE, tool=Tool.BASELINE, batch=1, workers=4)
    assert baseline.call_maximum == 2_200 + 1 * 4
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


# --- the most one call can cost ------------------------------------------------------------


def test_the_hard_call_cost_counts_every_request_byte_as_a_token_plus_the_reply_limit() -> None:
    cost = hard_call_cost(3_000, 1_024, LIVE_PRICES)
    assert cost == (Decimal(3_000 + FRAMING_BYTES) * PRICE_IN + 1_024 * PRICE_OUT) / 1_000_000


def test_an_extract_cells_ceiling_comes_from_its_own_largest_document() -> None:
    ceilings = Ceilings(FakeLimits(), LIVE_PRICES)
    small = PlannedCell(
        step=Step.PILOT, tool=Tool.RUN, workload=Workload.EXTRACT, size=100, batch=25, workers=4
    )
    large = APPROVED_PLAN.large(50_000, 8)
    assert ceilings.of(small) == hard(100)
    assert ceilings.of(large) == hard(50_000) > hard(100)
    baseline = APPROVED_PLAN.cells()[-1]
    assert baseline.tool is Tool.BASELINE
    assert ceilings.of(baseline) == hard(2_000)
    mix = PlannedCell(
        step=Step.RULES_FIRST, tool=Tool.RUN, workload=Workload.MIX, size=100, batch=25, workers=4
    )
    assert ceilings.of(mix) == UNBOUNDED_CALL_COST == COST_GATE


def test_the_observed_mean_is_only_over_token_priced_calls() -> None:
    assert observed_cost_per_call([]) is None
    charges = [
        Charge(Decimal("0.10"), 100, Basis.TOKENS),
        Charge(Decimal("0.02"), 100, Basis.TOKENS),
        Charge(Decimal(9), 10, Basis.CALLS),
        Charge(Decimal(9), 10, Basis.RESERVATIONS),
        Charge(Decimal(9), None, Basis.CALL_MAXIMUM),
    ]
    assert observed_cost_per_call(charges) == Decimal("0.0006")


# --- what a result says ------------------------------------------------------------------


def test_an_extraction_results_figures() -> None:
    read = step_figures(extraction_record(permanent={"rejected": 2}))
    assert read == Figures(
        status="complete",
        calls=100,
        token_cost=Decimal("0.05"),
        credits=Decimal("0.01"),
        accuracy=Decimal("0.95"),
        permanent_failures=2,
        items_per_s=10.0,
        answered=100,
        max_input_tokens=3_000,
        max_output_tokens=500,
    )


def test_a_mix_results_figures_have_no_token_cost() -> None:
    read = step_figures(mix_record())
    assert (read.calls, read.credits, read.token_cost) == (500, Decimal("0.02"), None)
    assert read.accuracy is None
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
        ({"accuracy": {"overall": None}}, "accuracy"),
        ({"accuracy": {"overall": "0.95"}}, "accuracy"),
        ({"accuracy": {"overall": True}}, "accuracy"),
    ],
)
def test_a_figure_that_is_not_a_clean_number_is_unknown(
    broken: dict[str, Any], unknown: str
) -> None:
    record = extraction_record()
    record["extraction"] |= broken
    assert getattr(step_figures(record), unknown) is None


# --- budget ------------------------------------------------------------------------------


def charge(read: Figures | None, reserved: int | None = None) -> Charge:
    return charge_for(read, ceiling=Decimal("0.001"), call_maximum=500, reserved=reserved)


def test_a_priced_cell_is_charged_the_larger_of_tokens_and_credits() -> None:
    assert charge(figures()) == Charge(Decimal("0.05"), 100, Basis.TOKENS)
    assert charge(figures(credits=Decimal("0.07"))).cost == Decimal("0.07")
    assert charge(figures(credits=None)).cost == Decimal("0.05")


@pytest.mark.parametrize(
    ("answered", "cost"),
    [
        (90, Decimal("0.060")),  # 10 failed or unresolved calls at the 0.001 ceiling
        (0, Decimal("0.150")),
        (None, Decimal("0.150")),  # not reported: every call may be unpriced
        (101, Decimal("0.150")),  # more answered than made: not believed
    ],
)
def test_calls_without_an_answer_are_charged_at_the_ceiling(
    answered: int | None, cost: Decimal
) -> None:
    assert charge(figures(answered=answered)) == Charge(cost, 100, Basis.TOKENS)
    rich = figures(answered=answered, credits=Decimal(1))
    assert charge(rich).cost == Decimal(1)


def test_a_record_with_failed_and_unresolved_calls_is_charged_for_them() -> None:
    record = extraction_record(calls=100, answered=93, token_priced="0.05", credits="0")
    record["extraction"]["calls"] |= {"failed": 4, "unresolved": 3}
    read = step_figures(record)
    assert (read.calls, read.answered) == (100, 93)
    assert charge(read).cost == Decimal("0.05") + 7 * Decimal("0.001")


def test_a_cell_without_token_figures_is_charged_its_calls_at_the_ceiling() -> None:
    read = figures(token_cost=None, credits=Decimal("0.0001"))
    assert charge(read) == Charge(Decimal("0.100"), 100, Basis.CALLS)
    assert charge(figures(token_cost=None, credits=Decimal("0.5"))).cost == Decimal("0.5")


@pytest.mark.parametrize("read", [None, figures(calls=None)])
def test_a_cell_without_a_result_is_charged_its_reservations_or_its_maximum(
    read: Figures | None,
) -> None:
    assert charge(read, reserved=40) == Charge(Decimal("0.040"), 40, Basis.RESERVATIONS)
    assert charge(read, reserved=0) == Charge(Decimal("0.000"), 0, Basis.RESERVATIONS)
    assert charge(read) == Charge(Decimal("0.500"), None, Basis.CALL_MAXIMUM)


def counter(raw: Path, run_id: str, count: int, written: float) -> Path:
    path = raw / run_id / "cell" / BUDGET_FILE_NAME
    path.parent.mkdir(parents=True)
    path.write_text(f"{count}\n")
    os.utime(path, (written, written))
    return path


def test_reservations_are_read_from_the_counters_written_in_the_window(raw_dir: Path) -> None:
    counter(raw_dir, "before", 7, 1_000.0)
    counter(raw_dir, "inside", 11, 2_000.0)
    counter(raw_dir, "insidetoo", 13, 2_500.0)
    counter(raw_dir, "after", 17, 3_000.0)
    assert reservations_between(1_500.0, 3_000.0) == 24
    assert reservations_between(1_500.0, None) == 41
    assert reservations_between(4_000.0, None) is None


def test_the_large_run_is_the_largest_whole_task_count_the_budget_pays_for() -> None:
    ceiling = Decimal("0.00075")
    cell = size_large(APPROVED_PLAN, 8, Decimal(10), ceiling)
    assert cell is not None
    assert cell.size % APPROVED_PLAN.large_batch == 0
    assert cell.call_maximum * ceiling <= Decimal(10)
    bigger = APPROVED_PLAN.large(cell.size + APPROVED_PLAN.large_batch, 8)
    assert bigger.call_maximum * ceiling > Decimal(10)
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


PRICED = Charge(Decimal(1), 100, Basis.TOKENS)


def answered_at(cost: str, **overrides: Any) -> Figures:
    """A pilot of 100 answered calls whose tokens cost ``cost`` in all."""
    return figures(token_cost=Decimal(cost), credits=Decimal(0), **overrides)


def test_a_pilot_inside_every_gate_passes_even_at_the_edges() -> None:
    gates = pilot_gates(answered_at("0.18", accuracy=Decimal("0.9")), PRICED)
    assert [gate.name for gate in gates] == [
        "field_accuracy",
        "cost_per_call",
        "no_permanent_failure",
    ]
    assert all(gate.passed for gate in gates)


@pytest.mark.parametrize(
    ("read", "spend", "failed"),
    [
        (figures(accuracy=Decimal("0.899")), PRICED, "field_accuracy"),
        (figures(accuracy=None), PRICED, "field_accuracy"),
        (answered_at("0.181"), PRICED, "cost_per_call"),
        (figures(credits=Decimal("0.181")), PRICED, "cost_per_call"),  # credits are larger
        (figures(answered=None), PRICED, "cost_per_call"),
        (figures(answered=0), PRICED, "cost_per_call"),
        (figures(token_cost=None), PRICED, "cost_per_call"),
        (figures(), Charge(Decimal("0.1"), 1000, Basis.CALLS), "cost_per_call"),
        (figures(), Charge(Decimal("0.1"), 10, Basis.RESERVATIONS), "cost_per_call"),
        (figures(), Charge(Decimal("0.1"), None, Basis.CALL_MAXIMUM), "cost_per_call"),
        (figures(permanent_failures=1), PRICED, "no_permanent_failure"),
        (figures(permanent_failures=None), PRICED, "no_permanent_failure"),
        (figures(status="incomplete"), PRICED, "no_permanent_failure"),
    ],
)
def test_a_pilot_outside_a_gate_or_without_its_figure_fails_it(
    read: Figures, spend: Charge, failed: str
) -> None:
    gates = {gate.name: gate for gate in pilot_gates(read, spend)}
    assert not gates[failed].passed
    assert all(gate.passed for name, gate in gates.items() if name != failed)


def test_a_pilot_with_no_result_fails_every_gate() -> None:
    gates = pilot_gates(None, Charge(Decimal(1), None, Basis.CALL_MAXIMUM))
    assert not any(gate.passed for gate in gates)


def test_the_token_bounds_are_the_ones_the_ceiling_assumes() -> None:
    ceilings = Ceilings(FakeLimits(), LIVE_PRICES)
    pilot = APPROVED_PLAN.cells()[0]
    assert ceilings.token_bounds(pilot) == (2_000 + 1_001 + FRAMING_BYTES, 1_024)
    assert ceilings.of(pilot) == hard(100)


@pytest.mark.parametrize(
    ("largest", "failed"),
    [
        ((4_025, 1_024), set()),  # both exactly at their bound
        ((4_026, 1_024), {"input_tokens_bound"}),
        ((4_025, 1_025), {"output_tokens_bound"}),
        ((None, 10), {"input_tokens_bound"}),
        ((10, None), {"output_tokens_bound"}),
    ],
)
def test_the_bound_gate_compares_each_calls_largest_tokens_with_the_bound(
    largest: tuple[int | None, int | None], failed: set[str]
) -> None:
    read = figures(max_input_tokens=largest[0], max_output_tokens=largest[1])
    gates = campaign.bound_gates("c", read, (4_025, 1_024))
    assert {gate.name for gate in gates if not gate.passed} == failed
    assert {gate.threshold for gate in gates} == {"<= 4025", "<= 1024"}
    assert {gate.cell for gate in gates} == {"c"}
    assert not any(gate.passed for gate in campaign.bound_gates("c", None, (4_025, 1_024)))


@pytest.mark.parametrize(
    ("largest", "gate"),
    [((4_045, 500), "input_tokens_bound"), ((3_000, 1_025), "output_tokens_bound")],
)
def test_an_extract_cell_past_its_per_call_bound_stops_the_campaign(
    tmp_path: Path, largest: tuple[int, int], gate: str
) -> None:
    breaking = "s2-run-extract-2000-b25-w8-r1"  # bounds (2,000 + 1,020 + 1,024, 1,024)

    def one_cell_breaks(key: str, argv: Sequence[str]) -> dict[str, Any] | None:
        record = good_records(key, argv)
        if key == breaking and record is not None:
            record["extraction"]["tokens"]["max_per_call"] = {
                "input": largest[0],
                "output": largest[1],
            }
        return record

    runner = FakeRunner(one_cell_breaks)
    assert new_campaign(tmp_path, runner, live=True).execute() == 1
    assert runner.keys[-1] == breaking
    verdicts = [event for event in events(tmp_path, "gate") if event["cell"] == breaking]
    failed = {event["gate"]: event for event in verdicts if not event["passed"]}
    assert set(failed) == {gate}
    assert failed[gate]["value"] == str(largest[0] if gate == "input_tokens_bound" else largest[1])
    (stopped,) = events(tmp_path, "campaign_stopped")
    assert breaking in stopped["reason"]
    assert gate in stopped["reason"]


def test_every_extract_cell_and_no_mix_cell_is_bound_checked(tmp_path: Path) -> None:
    assert new_campaign(tmp_path, FakeRunner(good_records)).execute() == 0
    checked = {event["cell"] for event in events(tmp_path, "gate") if event["cell"]}
    extract = {
        event["cell"] for event in events(tmp_path, "cell_started") if "-extract-" in event["cell"]
    }
    assert checked == extract
    assert all(event["passed"] for event in events(tmp_path, "gate") if event["cell"] is not None)


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


def test_a_dry_run_plans_with_synthetic_prices_and_a_live_run_with_the_given_ones(
    tmp_path: Path,
) -> None:
    assert settings(tmp_path, live=False).prices == DRY_RUN_PRICES
    assert DRY_RUN_PRICES.synthetic
    assert settings(tmp_path, live=True).prices == LIVE_PRICES


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
    assert pilot[pilot.index("--price-in-per-mtok") + 1] == "0.31"
    assert pilot[pilot.index("--price-out-per-mtok") + 1] == "1.17"
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
    # The crash drill reads every document too, so it is priced like the pilot.
    assert drill[drill.index("--workload") + 1] == "extract"
    assert drill[drill.index("--price-in-per-mtok") + 1] == "0.31"
    assert drill[drill.index("--price-out-per-mtok") + 1] == "1.17"


def test_the_throttle_probe_is_unpaced_and_gets_a_call_per_invoice_for_throttled_tries() -> None:
    (probe,) = [cell for cell in APPROVED_PLAN.cells() if cell.throttle_probe]
    assert (probe.step, probe.tool, probe.size, probe.batch, probe.workers) == (
        Step.FAILURES,
        Tool.RUN,
        2_000,
        5,
        32,
    )
    assert not probe.paced
    assert probe.call_maximum == 2_200 + extract_cap(5) * 32 + 2_000
    assert probe.headroom_reason is not None
    paced = {cell.key for cell in APPROVED_PLAN.cells() if cell.paced}
    assert paced == {
        cell.key
        for cell in APPROVED_PLAN.cells()
        if cell.tool is Tool.RUN and cell.workload is Workload.EXTRACT and cell.key != probe.key
    }
    assert APPROVED_PLAN.large(1_000, 16).paced


def test_only_live_paced_cells_pass_the_rate_limit(tmp_path: Path) -> None:
    for live in (True, False):
        for cell in APPROVED_PLAN.cells():
            argv = command_for(cell, settings(tmp_path, live=live), 10)
            if live and cell.paced:
                assert argv[argv.index("--rate-limit-per-s") + 1] == "10"
            else:
                assert "--rate-limit-per-s" not in argv, cell.key


def test_the_probes_budget_event_names_its_extra_headroom(tmp_path: Path) -> None:
    assert new_campaign(tmp_path, FakeRunner(good_records), live=True).execute() == 0
    started = {event["cell"]: event for event in events(tmp_path, "cell_started")}
    (probe,) = [cell for cell in APPROVED_PLAN.cells() if cell.throttle_probe]
    assert started[probe.key]["call_headroom"].startswith("throttle probe")
    assert started[probe.key]["call_maximum"] == probe.call_maximum
    others = [event for key, event in started.items() if key != probe.key]
    assert all(event["call_headroom"] is None for event in others)


def test_a_dry_runs_children_never_see_the_key() -> None:
    environ = {API_KEY_ENV: SENTINEL, "PATH": "/bin"}
    assert child_environment(environ, live=False) == {"PATH": "/bin"}
    assert child_environment(environ, live=True) == environ


def test_the_result_path_is_the_rest_of_the_last_line(tmp_path: Path) -> None:
    path = tmp_path / "r.json"
    path.write_text("{}")
    assert result_path(f"dry run: a line\ncomplete {path}\n", Tool.RUN) == path
    assert result_path(f"complete d1 {path}\n\n", Tool.DRILL) == path
    assert result_path(f"complete {path}", Tool.BASELINE) == path
    assert result_path(f"complete {tmp_path / 'missing.json'}", Tool.RUN) is None
    assert result_path(f"complete {tmp_path}", Tool.RUN) is None
    assert result_path(f"complete d1 {path}", Tool.RUN) is None  # a run line has no drill
    for tool in Tool:
        assert result_path("complete", tool) is None
        assert result_path("complete ", tool) is None
        assert result_path("", tool) is None
    assert result_path(f"complete {path}", Tool.DRILL) is None  # too few fields


@pytest.mark.parametrize(
    ("line", "tool"),
    [
        ("complete {path}", Tool.RUN),
        ("failed_correctness {path}", Tool.RUN),
        ("complete d1 {path}", Tool.DRILL),
        ("complete {path}", Tool.BASELINE),
        ("failed {path}", Tool.BASELINE),
    ],
)
@pytest.mark.parametrize(
    "folder", ["Orqestra Project/bench workspace", "two  spaces", "trailing ", " d1 x"]
)
def test_a_result_path_with_spaces_is_read_whole(
    tmp_path: Path, line: str, tool: Tool, folder: str
) -> None:
    path = tmp_path / folder / "20261116T000000Z-abc1234 cell.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    assert result_path(f"dry run: a line\n{line.format(path=path)}\n", tool) == path


def test_a_drill_path_containing_a_drill_name_is_read_whole(tmp_path: Path) -> None:
    path = tmp_path / "x d1 y" / "r.json"
    path.parent.mkdir()
    path.write_text("{}")
    assert result_path(f"complete d1 {path}", Tool.DRILL) == path


def test_a_run_path_under_a_folder_named_like_a_drill_is_read_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "d1 out").mkdir()
    (tmp_path / "d1 out" / "r.json").write_text("{}")
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "r.json").write_text("{}")  # a decoy a probing parser would find
    assert result_path("complete d1 out/r.json", Tool.RUN) == Path("d1 out/r.json")
    assert result_path("complete d1 out/r.json", Tool.DRILL) == Path("out/r.json")


def test_a_campaign_under_a_folder_with_spaces_reads_every_result(tmp_path: Path) -> None:
    out_dir = tmp_path / "Orqestra Project" / "bench workspace"
    out_dir.mkdir(parents=True)
    runner = FakeRunner(good_records)
    assert new_campaign(out_dir, runner).execute() == 0
    finished = events(out_dir, "cell_finished")
    assert len(finished) == len(runner.calls)
    assert all(event["result"] is not None for event in finished)
    assert all(" " in event["result"] for event in finished)
    assert all(event["status"] == "complete" for event in finished)


def test_running_workers_are_found_by_their_module(monkeypatch: pytest.MonkeyPatch) -> None:
    listing = (
        "  101 /usr/bin/python -m benchmarks.worker --route route-a\n"
        "  102 /usr/bin/python -m benchmarks.campaign run --dry-run\n"
        "  103 /bin/zsh\n"
    )

    class Listed:
        stdout = listing

    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: Listed())
    assert campaign.workers_alive() == [101]


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
        drill = " d1" if argv[2] == Tool.DRILL.value else ""  # as each command prints it
        return Finished(0, f"{record['status']}{drill} {path}\n", "")

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
        accuracy=0.9,
        speed=float(min(workers, 16)),
    )


def new_campaign(
    tmp_path: Path,
    runner: FakeRunner,
    *,
    live: bool = False,
    budget: Decimal = Decimal(45),
    alive: Callable[[], list[int]] = list,
    clock: Callable[[], float] | None = None,
) -> Campaign:
    environ = {API_KEY_ENV: SENTINEL, "PATH": "/bin"}
    run = Campaign(
        settings(tmp_path, live=live),
        budget,
        APPROVED_PLAN,
        runner,
        environ,
        CampaignLog(tmp_path / LOG_NAME),
        FakeLimits(),
        alive=alive,
    )
    if clock is not None:
        run.clock = clock
    return run


def events(tmp_path: Path, kind: str | None = None) -> list[dict[str, Any]]:
    found = list(CampaignLog(tmp_path / LOG_NAME).events())
    return [event for event in found if kind is None or event["event"] == kind]


@pytest.fixture(autouse=True)
def raw_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    raw = tmp_path / "raw"
    raw.mkdir()
    monkeypatch.setattr(campaign, "RAW_DIR", raw)
    monkeypatch.setattr(campaign, "DRY_RUN_BASE", raw / "campaigns")
    return raw


def test_a_live_campaign_runs_every_step_in_order_within_its_hard_bounds(
    tmp_path: Path,
) -> None:
    runner = FakeRunner(good_records)
    run = new_campaign(tmp_path, runner, live=True)
    assert run.execute() == 0
    planned = [cell.key for cell in APPROVED_PLAN.cells()]
    assert runner.keys[: len(planned)] == planned
    (large,) = runner.keys[len(planned) :]
    assert large.startswith("s8-run-extract-")
    assert "-w16-" in large  # the fastest median; 32 is no faster
    for argv, env in runner.calls:
        assert env[API_KEY_ENV] == SENTINEL
        assert SENTINEL not in " ".join(argv)
        assert "--allow-live-calls" in argv
    for event in events(tmp_path, "cell_started"):
        assert Decimal(event["projected"]) <= Decimal(event["remaining"])
        if "-extract-" in event["cell"]:
            size = int(event["cell"].split("-extract-")[1].split("-")[0])
            assert Decimal(event["ceiling"]) == hard(size)  # the cell's own documents
        else:
            assert Decimal(event["ceiling"]) == UNBOUNDED_CALL_COST
    large_started = events(tmp_path, "cell_started")[-1]
    (sized,) = events(tmp_path, "large_sized")
    assert Decimal(sized["ceiling"]) == hard(50_000)
    assert large_started["sized"] is True
    kinds = [event["event"] for event in events(tmp_path)]
    assert kinds[0] == "campaign_started"
    assert kinds[-1] == "campaign_finished"
    first_baseline = next(
        index
        for index, event in enumerate(events(tmp_path))
        if event["event"] == "cell_started" and event["cell"].startswith("s7-")
    )
    checks = [i for i, kind in enumerate(kinds) if kind == "billing_check"]
    assert len(checks) == 2
    assert kinds.index("pilot_passed") < checks[0] < first_baseline < checks[1]
    summary = json.loads((tmp_path / SUMMARY_NAME).read_text())
    assert summary["model"] == MODEL
    assert summary["stopped"] is None
    assert summary["prices_per_mtok"] == {"input": "0.31", "output": "1.17", "synthetic": False}
    assert Decimal(summary["spent"]) + Decimal(summary["remaining"]) == Decimal(45)
    assert Decimal(summary["spent"]) <= Decimal(45)
    assert (tmp_path / "logs" / f"{planned[0]}.out").read_text().startswith("complete ")
    assert SENTINEL not in (tmp_path / LOG_NAME).read_text()
    assert SENTINEL not in (tmp_path / SUMMARY_NAME).read_text()


def test_a_dry_campaign_passes_dry_run_and_no_key(tmp_path: Path) -> None:
    runner = FakeRunner(good_records)
    assert new_campaign(tmp_path, runner).execute() == 0
    for argv, env in runner.calls:
        assert API_KEY_ENV not in env
        assert "--dry-run" in argv
        assert "--price-in-per-mtok" not in argv
    (started,) = events(tmp_path, "campaign_started")
    assert started["prices_synthetic"] is True


@pytest.mark.parametrize(
    ("pilot", "gate"),
    [
        (extraction_record(accuracy=0.899, token_priced="0.01"), "field_accuracy"),
        (extraction_record(calls=100, token_priced="0.181"), "cost_per_call"),
        (extraction_record(permanent={"rejected": 1}), "no_permanent_failure"),
        (None, "field_accuracy"),
    ],
)
def test_a_failed_pilot_gate_stops_the_campaign(
    tmp_path: Path, pilot: dict[str, Any] | None, gate: str
) -> None:
    runner = FakeRunner(lambda key, argv: pilot)
    assert new_campaign(tmp_path, runner).execute() == 1
    assert len(runner.calls) == 1
    gates = {event["gate"]: event for event in events(tmp_path, "gate")}
    assert not gates[gate]["passed"]
    assert gates[gate]["threshold"]
    (stopped,) = events(tmp_path, "campaign_stopped")
    if pilot is None:  # no result: the per-call bound is unproven, which stops it first
        assert "per-call bound" in stopped["reason"]
    else:
        assert gate in stopped["reason"]
    assert json.loads((tmp_path / SUMMARY_NAME).read_text())["stopped"] == stopped["reason"]
    assert not (tmp_path / BILLING_NAME).exists()


def test_cells_that_do_not_fit_are_skipped_and_spending_never_passes_the_budget(
    tmp_path: Path,
) -> None:
    run = new_campaign(tmp_path, FakeRunner(good_records), live=True, budget=Decimal(12))
    assert run.execute() == 0
    skipped = events(tmp_path, "cell_skipped")
    assert skipped
    assert all(event["reason"] == "budget" for event in skipped)
    for event in skipped:
        assert Decimal(event["projected"]) > Decimal(event["remaining"])
    for event in events(tmp_path, "cell_started"):
        assert Decimal(event["projected"]) <= Decimal(event["remaining"])
    assert run.spent <= Decimal(12)


def test_a_cell_that_leaves_no_result_is_charged_what_its_counters_reserved(
    tmp_path: Path, raw_dir: Path
) -> None:
    loop = "s7-baseline-loop-extract-2000-b1-w1-r1"

    def crash_after_reserving(key: str, argv: Sequence[str]) -> dict[str, Any] | None:
        if key == loop:
            counter(raw_dir, "crashedloop", 37, 4e9)  # written after the cell started
            return None
        return good_records(key, argv)

    clock = iter(float(second) for second in range(1_000, 9_000, 10))
    run = new_campaign(
        tmp_path, FakeRunner(crash_after_reserving), live=True, clock=lambda: next(clock)
    )
    assert run.execute() == 1
    finished = {event["cell"]: event for event in events(tmp_path, "cell_finished")}
    started = {event["cell"]: event for event in events(tmp_path, "cell_started")}
    assert finished[loop]["charge_basis"] == "reservations"
    assert finished[loop]["calls"] == 37
    assert Decimal(finished[loop]["charge"]) == 37 * Decimal(started[loop]["ceiling"])
    assert run.runs[loop].charge.cost == 37 * run.runs[loop].ceiling
    (stopped,) = events(tmp_path, "campaign_stopped")
    assert stopped["reason"].startswith(f"per-call bound not verified for {loop}")


def test_a_cell_without_a_result_or_a_counter_is_charged_its_whole_maximum(
    tmp_path: Path,
) -> None:
    loop = "s7-baseline-loop-extract-2000-b1-w1-r1"

    def refuse_baseline(key: str, argv: Sequence[str]) -> dict[str, Any] | None:
        return None if key.startswith("s7-") else good_records(key, argv)

    assert new_campaign(tmp_path, FakeRunner(refuse_baseline), live=True).execute() == 1
    finished = {event["cell"]: event for event in events(tmp_path, "cell_finished")}
    started = {event["cell"]: event for event in events(tmp_path, "cell_started")}
    assert finished[loop]["charge_basis"] == "call_maximum"
    expected = Decimal(started[loop]["call_maximum"]) * Decimal(started[loop]["ceiling"])
    assert Decimal(finished[loop]["charge"]) == expected


def test_a_too_small_budget_stops_before_the_pilot(tmp_path: Path) -> None:
    runner = FakeRunner(good_records)
    assert new_campaign(tmp_path, runner, budget=Decimal("0.01")).execute() == 1
    assert runner.calls == []
    (stopped,) = events(tmp_path, "campaign_stopped")
    assert "fit" in stopped["reason"]


def test_no_cell_starts_while_a_benchmark_worker_is_alive(tmp_path: Path) -> None:
    runner = FakeRunner(good_records)
    answers: list[list[int]] = [[], [], [4242]]  # quiet for two cells, then not

    def alive() -> list[int]:
        return answers.pop(0) if len(answers) > 1 else answers[0]

    assert new_campaign(tmp_path, runner, alive=alive).execute() == 1
    assert len(runner.calls) == 2
    (stopped,) = events(tmp_path, "campaign_stopped")
    assert "4242" in stopped["reason"]
    assert json.loads((tmp_path / SUMMARY_NAME).read_text())["stopped"] == stopped["reason"]


def test_the_billing_check_sets_the_ledger_beside_the_counters_in_a_utc_window(
    tmp_path: Path, raw_dir: Path
) -> None:
    run1 = raw_dir / "run1" / "cell"
    run1.mkdir(parents=True)
    (run1 / BUDGET_FILE_NAME).write_text("103\n")
    (run1 / "worker-0.log").write_text(
        json.dumps({"event": "canned_transport_requests", "requests": 101}) + "\n"
    )
    assert new_campaign(tmp_path, FakeRunner(good_records)).execute() == 0
    check = json.loads((tmp_path / BILLING_NAME).read_text())
    assert check["model"] == MODEL
    assert check["final"] is True
    window = check["window_utc"]
    assert window["start"] < window["end"]
    assert window["start"].endswith("Z")
    pilot = check["cells"][0]
    assert pilot["calls"] == 100
    assert pilot["counted_calls"] == 103
    assert pilot["counted_minus_calls"] == 3
    assert pilot["transport_requests"] == 101
    assert Decimal(pilot["token_cost_minus_credits"]) == Decimal(pilot["token_cost"])
    assert (check["router_reported_usd"], check["upstream_reported_usd"]) == (None, None)
    assert len(check["cells"]) == len(APPROVED_PLAN.cells()) + 1
    assert Decimal(check["totals"]["charged"]) > 0
    assert check["totals"]["token_cost_unknown_cells"] == 4  # the four mix cells
    partial = events(tmp_path, "billing_check")[0]
    assert partial["final"] is False


def test_a_resumed_campaign_never_reruns_a_started_cell_and_keeps_it_charged(
    tmp_path: Path, raw_dir: Path
) -> None:
    times = iter(float(second) for second in range(1_000, 9_000, 10))
    first = FakeRunner(good_records, fail_at=5)
    with pytest.raises(RuntimeError):
        new_campaign(tmp_path, first, clock=lambda: next(times)).execute()
    crashed = APPROVED_PLAN.cells()[5].key
    (crashed_start,) = [e for e in events(tmp_path, "cell_started") if e["cell"] == crashed]
    counter(raw_dir, "crashed", 29, float(crashed_start["started_epoch"]) + 1)
    second = FakeRunner(good_records)
    resumed = new_campaign(tmp_path, second, clock=lambda: next(times))
    resumed.resume()
    assert resumed.pilot_passed
    assert resumed.runs[crashed].charge.basis is Basis.RESERVATIONS
    assert resumed.runs[crashed].charge.calls == 29
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


def cli(argv: list[str], runner: FakeRunner) -> int:
    return campaign.main(argv, runner, FakeLimits, list)


DRY = ["run", "--model", MODEL, "--dry-run"]
LIVE = ["run", "--model", MODEL, "--allow-live-calls"]
PRICES = ["--price-in-per-mtok", "0.31", "--price-out-per-mtok", "1.17"]


@pytest.mark.parametrize(
    ("argv", "environ", "words"),
    [
        (LIVE + PRICES, {}, "is not set"),
        (LIVE, {API_KEY_ENV: SENTINEL}, "--price-in-per-mtok"),
        ([*LIVE, "--price-in-per-mtok", "0.31"], {API_KEY_ENV: SENTINEL}, "--price-out"),
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
    assert cli(argv, runner) == 1
    error = capsys.readouterr().err
    assert error.startswith("refused: ")
    assert words in error
    assert SENTINEL not in error
    assert runner.calls == []
    assert not (bases / "live").exists()
    assert not (bases / "dry").exists()


def test_the_limits_are_the_extraction_workloads_own() -> None:
    from benchmarks import extract
    from benchmarks.documents import generate_documents
    from benchmarks.generate import DEFAULT_SEED
    from orqestra.providers.openrouter import EXTRACT_MAX_TOKENS, EXTRACT_SYSTEM_PROMPT

    limits = campaign.connect_limits()
    assert limits.prompt_bytes == len(EXTRACT_SYSTEM_PROMPT.encode("utf-8"))
    assert limits.max_output_tokens == EXTRACT_MAX_TOKENS
    sent = [extract.request_text(d.text) for d in generate_documents(300, DEFAULT_SEED)]
    assert limits.largest_document_bytes(300) == max(len(text.encode()) for text in sent)
    # Escapes count: the request text is longer than the raw document.
    largest_raw = max(len(d.text.encode()) for d in generate_documents(300, DEFAULT_SEED))
    assert limits.largest_document_bytes(300) > largest_raw


def test_live_and_dry_run_are_exclusive_and_one_is_required(bases: Path) -> None:
    for argv in (["run", "--model", MODEL], [*DRY, "--allow-live-calls"]):
        with pytest.raises(SystemExit):
            cli(argv, FakeRunner(good_records))


def test_the_command_runs_a_dry_campaign_into_its_own_folder_and_reconciles(
    bases: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(os, "environ", {API_KEY_ENV: SENTINEL})
    runner = FakeRunner(good_records)
    assert cli(DRY, runner) == 0
    (folder,) = (bases / "dry").iterdir()
    assert folder.name.endswith("-abcdef0")
    assert (folder / LOG_NAME).is_file()
    assert not (bases / "live").exists()
    assert all(API_KEY_ENV not in env for _, env in runner.calls)
    capsys.readouterr()
    assert cli(["reconcile", folder.name], runner) == 1
    assert "both" in capsys.readouterr().err
    reconcile = ["reconcile", folder.name, "--router-reported-usd", "12.5"]
    assert cli([*reconcile, "--upstream-reported-usd", "11"], runner) == 0
    check = json.loads((folder / BILLING_NAME).read_text())
    assert (check["router_reported_usd"], check["upstream_reported_usd"]) == ("12.5", "11")
    totals = check["totals"]
    assert Decimal(check["router_minus_charged"]) == Decimal("12.5") - Decimal(totals["charged"])
    assert Decimal(check["upstream_minus_token_cost"]) == Decimal(11) - Decimal(
        totals["token_cost"]
    )
    assert Decimal(check["upstream_minus_credits"]) == Decimal(11) - Decimal(totals["credits"])
    (reconciled,) = events(folder, "reconciled")
    assert (reconciled["router_reported_usd"], reconciled["upstream_reported_usd"]) == (
        "12.5",
        "11",
    )
    assert cli(["reconcile", "nope", "--router-reported-usd", "1"], runner) == 1


def test_a_resume_names_an_existing_campaign(
    bases: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli([*DRY, "--resume", "missing"], FakeRunner(good_records)) == 1
    assert "no campaign" in capsys.readouterr().err


# --- scope -------------------------------------------------------------------------------


def test_the_campaign_imports_no_network_module_and_names_no_model() -> None:
    tree = ast.parse(inspect.getsource(campaign))
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


# --- every argv of the campaign plan is accepted by its tool ----------------------------


class _NoDocuments:
    """A baseline extraction with no documents: the command's parser and refusals run,
    nothing is called."""

    fields: tuple[str, ...] = ()

    def documents(self, count: int, seed: int) -> list[Any]:
        return []

    def call(self, text: str) -> Any:
        raise AssertionError

    def score(self, expected: object, content: str | None) -> Any:
        raise AssertionError


@pytest.mark.parametrize("live", [False, True], ids=["dry", "live"])
def test_every_command_of_the_approved_plan_passes_its_tools_own_refusals(
    live: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from benchmarks import baseline as baseline_module
    from benchmarks import drills as drills_module
    from benchmarks import run as run_module

    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    monkeypatch.setenv("ORQESTRA_BENCH_DATABASE_URL", "postgresql://b@localhost/orqestra_bench")
    for module in (run_module, drills_module, baseline_module):
        monkeypatch.setattr(module, "refuse_dirty", lambda allow_dirty: None)
    monkeypatch.setattr(baseline_module, "RAW_DIR", tmp_path / "raw")
    # Below the refusals, nothing runs: no database, no worker, no call.
    record = {"status": "complete", "provider": {"calls_made": 0}}
    monkeypatch.setattr(
        run_module, "run_in_bench_database", lambda *a, **k: (record, tmp_path / "r.json")
    )
    monkeypatch.setattr(run_module, "dry_run_summary", lambda *a: [])
    monkeypatch.setattr(drills_module, "run_named", lambda *a: [])
    plan = APPROVED_PLAN
    cells = [*plan.cells(), plan.large(plan.large_max, 8)]
    chosen = settings(tmp_path, live=live)
    for cell in cells:
        argv = command_for(cell, chosen, cell.call_maximum)
        arguments = argv[3:]
        if cell.tool is Tool.RUN:
            code = run_module.main(arguments)
        elif cell.tool is Tool.DRILL:
            code = drills_module.main(arguments)
        else:
            code = baseline_module.main(arguments, connector=lambda *a: _NoDocuments())
        error = capsys.readouterr().err
        assert (code, error) == (0, ""), (cell.key, error)
        assert SENTINEL not in json.dumps(argv)


# --- an unreadable counter never aborts the campaign -----------------------------------


def empty_counter(raw: Path, run_id: str, written: float) -> Path:
    """A counter left empty, as an in-place update torn by a kill used to leave it."""
    path = raw / run_id / "cell" / BUDGET_FILE_NAME
    path.parent.mkdir(parents=True)
    path.write_text("")
    os.utime(path, (written, written))
    return path


def test_an_unreadable_counter_is_not_measured_in_the_billing_check(raw_dir: Path) -> None:
    counter(raw_dir, "fine", 12, 1_000.0)
    assert campaign.counted_calls("fine") == 12
    empty_counter(raw_dir, "torn", 1_000.0)
    assert campaign.counted_calls("torn") is None  # never read as zero, never raised


def test_a_crashed_cell_with_an_unreadable_counter_is_charged_its_maximum_and_logged(
    tmp_path: Path, raw_dir: Path
) -> None:
    mix = "s4-run-invoice-2000-b25-w8-r1"

    def crash_with_a_torn_counter(key: str, argv: Sequence[str]) -> dict[str, Any] | None:
        if key == mix:
            empty_counter(raw_dir, "tornmix", 4e9)  # written after the cell started
            return None
        return good_records(key, argv)

    clock = iter(float(second) for second in range(1_000, 9_000, 10))
    run = new_campaign(
        tmp_path, FakeRunner(crash_with_a_torn_counter), live=True, clock=lambda: next(clock)
    )
    assert run.execute() == 0  # the campaign goes on and finishes
    (unreadable,) = events(tmp_path, "counter_unreadable")
    assert unreadable["cell"] == mix
    assert unreadable["charged"] == "call_maximum"
    assert unreadable["path"].endswith(BUDGET_FILE_NAME)
    finished = {event["cell"]: event for event in events(tmp_path, "cell_finished")}
    started = {event["cell"]: event for event in events(tmp_path, "cell_started")}
    assert finished[mix]["charge_basis"] == "call_maximum"
    expected = Decimal(started[mix]["call_maximum"]) * Decimal(started[mix]["ceiling"])
    assert Decimal(finished[mix]["charge"]) == expected
    assert events(tmp_path, "campaign_finished")
    assert events(tmp_path, "campaign_stopped") == []

    # Resumed, the counter is found in the cell's own window (from its start to the next
    # cell's), still unreadable: the cell stays charged its maximum and it is logged again.
    torn = raw_dir / "tornmix" / "cell" / BUDGET_FILE_NAME
    inside = float(started[mix]["started_epoch"]) + 1
    os.utime(torn, (inside, inside))
    resumed = new_campaign(tmp_path, FakeRunner(good_records), live=True)
    resumed.resume()
    assert resumed.runs[mix].charge.basis is Basis.CALL_MAXIMUM
    assert resumed.runs[mix].charge.cost == expected
    assert len(events(tmp_path, "counter_unreadable")) == 2
