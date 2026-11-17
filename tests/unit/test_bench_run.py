"""A cell's live settings, its call cap, its pacing flags and its supervised fleet.

No database, worker or network: the fleet tests start plain Python processes that exit
with a chosen code, in place of benchmark workers.
"""

import importlib
import subprocess
import sys
import time
import types
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from benchmarks import run
from benchmarks.live import ProviderChoice, ProviderName
from benchmarks.ratelimit import LIMITER_MAX_WAIT
from benchmarks.run import (
    BENCH_WORKER_SETTINGS,
    LIVE_MAX_ATTEMPTS,
    LIVE_WORKER_SETTINGS,
    MAX_ATTEMPTS,
    RATE_LIMIT_FILE_NAME,
    Cell,
    Fleet,
    Profile,
    WorkerHandle,
    bench_settings,
    build_workload,
    cell_cap,
    cell_worker_settings,
    extract_cap,
    extract_worker_extra,
    max_attempts_for,
    rate_limit_refusal,
    replacement_limit,
    worker_exits_check,
)
from benchmarks.worker import DEFAULT_CAP
from orqestra.domain.retry import RetryPolicy, retry_delay
from orqestra.worker.fence import ExitCode

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

MODEL = "vendor/model-a"
DOUBLE = ProviderChoice()
DRY = ProviderChoice(
    name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=500, dry_run=True
)
LIVE = ProviderChoice(
    name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=500, allow_live_calls=True
)
EXTRACT = Cell(Profile.EXTRACT, size=100, batch=25, workers=4)
INVOICE = Cell(Profile.INVOICE, size=100, batch=25, workers=4)


# --- live settings -------------------------------------------------------------------------


def test_the_live_settings_pass_the_workers_own_validation_and_dec_070() -> None:
    settings = bench_settings(LIVE_WORKER_SETTINGS)
    assert settings.db_operation_timeout == timedelta(seconds=3)
    assert settings.lock_timeout == timedelta(seconds=1.5)
    assert settings.heartbeat_interval == timedelta(seconds=6)
    assert settings.lease == timedelta(seconds=20)
    # DEC-070: room for one record or sweep step between heartbeats of full database time
    assert settings.heartbeat_interval >= 2 * settings.db_operation_timeout
    assert settings.lease >= 3 * settings.db_operation_timeout + settings.fence_margin
    assert (
        settings.heartbeat_interval + settings.db_operation_timeout + settings.fence_margin
        < settings.lease
    )


def test_the_live_settings_change_only_the_database_bounds_and_the_retry_budget() -> None:
    changed = {
        key
        for key in LIVE_WORKER_SETTINGS
        if LIVE_WORKER_SETTINGS[key] != BENCH_WORKER_SETTINGS[key]
    }
    assert set(LIVE_WORKER_SETTINGS) == set(BENCH_WORKER_SETTINGS)
    assert changed == {
        "ORQESTRA_WORKER_LEASE",
        "ORQESTRA_WORKER_HEARTBEAT_INTERVAL",
        "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT",
        "ORQESTRA_WORKER_LOCK_TIMEOUT",
        "ORQESTRA_WORKER_RETRY_BASE_DELAY",
        "ORQESTRA_WORKER_RETRY_MAX_DELAY",
    }


def test_the_live_retry_budget_outlasts_a_throttling_episode() -> None:
    settings = bench_settings(LIVE_WORKER_SETTINGS)
    policy = RetryPolicy(settings.retry_base_delay, settings.retry_max_delay)
    assert LIVE_MAX_ATTEMPTS == 8
    for task in (uuid.UUID(int=0), uuid.UUID(int=1), uuid.uuid5(uuid.NAMESPACE_URL, "x")):
        waited = sum(
            (retry_delay(policy, task, attempt) for attempt in range(1, LIVE_MAX_ATTEMPTS)),
            timedelta(),
        )
        # at least half of 2 + 4 + 8 + 16 + 32 + 60 + 60 s; the episodes lasted about 45 s
        assert waited >= timedelta(seconds=91)


def test_openrouter_cells_run_on_the_live_settings_and_the_double_on_the_benchs() -> None:
    assert cell_worker_settings(INVOICE, DOUBLE, "PT0.1S") == dict(BENCH_WORKER_SETTINGS)
    for choice in (DRY, LIVE):
        settings = cell_worker_settings(EXTRACT, choice, "PT30S")
        unchanged = {k: v for k, v in settings.items() if k != "ORQESTRA_WORKER_EXECUTION_TIMEOUT"}
        assert unchanged == {
            k: v
            for k, v in LIVE_WORKER_SETTINGS.items()
            if k != "ORQESTRA_WORKER_EXECUTION_TIMEOUT"
        }
        # 30 calls (extract_cap(25)) x (30 s + 2 x 3 x 3 s) + 2 s
        assert settings["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] == "PT1442S"
    assert max_attempts_for(DOUBLE) == MAX_ATTEMPTS == 3
    assert max_attempts_for(DRY) == max_attempts_for(LIVE) == LIVE_MAX_ATTEMPTS


def test_the_task_file_carries_the_cells_max_attempts() -> None:
    assert {task["max_attempts"] for task in build_workload(EXTRACT, 8).tasks} == {8}
    assert {task["max_attempts"] for task in build_workload(INVOICE).tasks} == {MAX_ATTEMPTS}


# --- the extract cap and the pacing flags ---------------------------------------------------


@pytest.mark.parametrize(("batch", "cap"), [(1, 6), (5, 10), (25, 30), (100, 120), (250, 300)])
def test_an_extract_attempt_may_call_a_little_more_than_its_batch(batch: int, cap: int) -> None:
    assert extract_cap(batch) == cap
    assert cell_cap(Cell(Profile.EXTRACT, size=500, batch=batch, workers=1)) == cap


def test_the_invoice_cap_is_unchanged() -> None:
    assert cell_cap(INVOICE) == DEFAULT_CAP


def test_an_extract_worker_gets_the_pacing_flags_and_the_cells_limiter_file(
    tmp_path: Path,
) -> None:
    flags = extract_worker_extra(EXTRACT, LIVE, tmp_path, Decimal(10))
    assert flags == (
        "--rate-limit-per-s",
        "10",
        "--rate-limit-file",
        str(tmp_path / RATE_LIMIT_FILE_NAME),
    )
    assert not (tmp_path / RATE_LIMIT_FILE_NAME).exists()  # the workers create it
    dry = extract_worker_extra(EXTRACT, DRY, tmp_path, Decimal("2.5"))
    assert dry[:4] == ("--documents", "100", "--seed", str(EXTRACT.seed))
    assert dry[4:6] == ("--rate-limit-per-s", "2.5")
    assert extract_worker_extra(EXTRACT, LIVE, tmp_path, None) == ()
    assert extract_worker_extra(INVOICE, LIVE, tmp_path, Decimal(10)) == ()


@pytest.mark.parametrize(
    ("cell", "rate", "words"),
    [
        (INVOICE, Decimal(10), "only to the extract profile"),
        (EXTRACT, Decimal(0), "above 0"),
        (EXTRACT, Decimal(-1), "above 0"),
        (EXTRACT, Decimal("NaN"), "above 0"),
    ],
)
def test_the_rate_limit_is_refused_off_extract_and_below_one_call(
    cell: Cell, rate: Decimal, words: str
) -> None:
    refusal = rate_limit_refusal(cell, rate)
    assert refusal is not None
    assert words in refusal
    assert rate_limit_refusal(EXTRACT, Decimal(10)) is None
    assert rate_limit_refusal(INVOICE, None) is None


def test_the_command_refuses_the_rate_limit_before_any_database(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ORQESTRA_BENCH_DATABASE_URL", raising=False)
    argv = ["--profile", "invoice", "--size", "10", "--batch", "5", "--workers", "1"]
    assert run.main([*argv, "--rate-limit-per-s", "5"]) == 1
    assert "only to the extract profile" in capsys.readouterr().err


# --- the worker_exits check -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("stopped", "died", "passed"),
    [
        ([0, 0], [], True),
        ([0, 0], [ExitCode.STALLED], True),
        ([0, 0], [ExitCode.STALLED, ExitCode.STALLED], True),
        ([0, 0], [ExitCode.FENCED], False),
        ([0, 0], [ExitCode.STALLED, ExitCode.DEFECT], False),
        ([0, 0], [-9], False),  # killed
        ([0, 4], [], False),  # a code 4 at the end is still a bad stop
        ([0, -15], [], False),
    ],
)
def test_a_worker_that_stalled_mid_cell_does_not_void_it_but_any_other_exit_does(
    stopped: list[int], died: list[int], passed: bool
) -> None:
    check = worker_exits_check(stopped, None, died)
    assert check["name"] == "worker_exits"
    assert check["passed"] is passed
    assert check["detail"]["exit_codes"] == stopped
    assert check["detail"].get("exits_while_running", []) == died


def test_the_drills_expected_codes_still_rule_the_stopped_workers() -> None:
    assert worker_exits_check([-9, 0], [-9, 0], [])["passed"]
    assert not worker_exits_check([0, 0], [-9, 0], [])["passed"]


# --- the supervised fleet -------------------------------------------------------------------


class ExitingFleet(Fleet):
    """A ``Fleet`` whose workers are Python processes that exit with the next code given;
    one with ``None`` keeps running (a healthy worker) until it is signalled."""

    codes: list[int | None]

    def start(self) -> WorkerHandle:
        self.started += 1
        code = self.codes.pop(0) if self.codes else None
        script = "import time; time.sleep(60)" if code is None else f"raise SystemExit({code})"
        path = self.log_dir / f"worker-{self.started}.log"
        log = path.open("wb")
        process = subprocess.Popen(  # a fixed command
            [sys.executable, "-c", script], stdout=log, stderr=log
        )
        handle = WorkerHandle(process, path, log, self.started)
        self.workers.append(handle)
        return handle


def fleet_with(tmp_path: Path, codes: list[int | None]) -> ExitingFleet:
    fleet = ExitingFleet(Profile.INVOICE, {}, tmp_path)
    fleet.codes = codes
    return fleet


def wait_exited(fleet: Fleet, count: int) -> None:
    deadline = time.monotonic() + 30
    while sum(1 for handle in fleet.workers if not handle.running()) < count:
        assert time.monotonic() < deadline
        time.sleep(0.02)


def test_a_worker_that_exits_is_recorded_and_replaced(tmp_path: Path) -> None:
    fleet = fleet_with(tmp_path, [4, None, None])
    fleet.start()
    fleet.start()
    wait_exited(fleet, 1)
    found = fleet.supervise(1.25, limit=2)
    assert found == [
        {
            "worker": 1,
            "pid": fleet.workers[0].process.pid,
            "exit_code": 4,
            "at_s": 1.25,
            "replaced": True,
        }
    ]
    assert fleet.replaced == 1
    assert len(fleet.workers) == 3
    assert fleet.alive() == 2
    assert fleet.supervise(1.5, limit=2) == []  # each exit is recorded once
    stopped = fleet.stop(timeout=10)
    assert len(stopped) == 3
    assert [handle.died for handle in fleet.workers] == [True, False, False]


def test_replacements_stop_at_the_limit_and_the_fleet_runs_down(tmp_path: Path) -> None:
    fleet = fleet_with(tmp_path, [4, 4, 4, 4])
    fleet.start()
    found: list[dict[str, object]] = []
    for _ in range(3):  # the first worker and its two replacements
        wait_exited(fleet, len(found) + 1)
        found += fleet.supervise(0.0, limit=replacement_limit(1))
    assert replacement_limit(1) == 2
    assert [item["replaced"] for item in found] == [True, True, False]
    assert fleet.replaced == 2
    assert fleet.alive() == 0
    fleet.stop(timeout=10)


def test_the_replacement_limit_is_at_least_two_and_one_per_worker() -> None:
    assert [replacement_limit(workers) for workers in (1, 2, 4, 32)] == [2, 2, 4, 32]


def test_settle_waits_only_for_running_workers_that_have_not_started(tmp_path: Path) -> None:
    fleet = fleet_with(tmp_path, [None, 0])
    fleet.start()
    fleet.start()
    wait_exited(fleet, 1)
    began = time.monotonic()
    fleet.settle(timeout=0.3)  # the running one never logs worker_started: bounded, no raise
    assert 0.3 <= time.monotonic() - began < 5
    fleet.workers[0].log_path.write_text('{"event": "worker_started"}\n')
    began = time.monotonic()
    fleet.settle(timeout=5)
    assert time.monotonic() - began < 1
    fleet.stop(timeout=10)


# --- a paced cell's execution timeout -------------------------------------------------------


def test_a_paced_cell_gives_every_call_of_its_cap_the_limiters_longest_wait() -> None:
    unpaced = cell_worker_settings(EXTRACT, DRY, "PT30S")
    paced = cell_worker_settings(
        EXTRACT, DRY, "PT30S", Decimal(10), pacing_wait=timedelta(seconds=5)
    )
    assert unpaced["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] == "PT1442S"  # 30 x (30 + 18) + 2
    assert paced["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] == "PT1592S"  # 30 x (30 + 18 + 5) + 2
    assert {k: v for k, v in paced.items() if k != "ORQESTRA_WORKER_EXECUTION_TIMEOUT"} == {
        k: v for k, v in unpaced.items() if k != "ORQESTRA_WORKER_EXECUTION_TIMEOUT"
    }
    # the pacing wait is the extract workers' alone, and the double has no limiter
    assert cell_worker_settings(INVOICE, DRY, "PT30S", None) == cell_worker_settings(
        INVOICE, DRY, "PT30S"
    )
    assert cell_worker_settings(INVOICE, DOUBLE, "PT0.1S") == dict(BENCH_WORKER_SETTINGS)


@pytest.mark.parametrize("wait", [timedelta(seconds=5), 5, 5.0])
def test_the_pacing_wait_is_read_from_the_workers_limiter(
    monkeypatch: pytest.MonkeyPatch, wait: object
) -> None:
    monkeypatch.setitem(
        sys.modules, "benchmarks.ratelimit", types.SimpleNamespace(LIMITER_MAX_WAIT=wait)
    )
    assert run.limiter_max_wait() == timedelta(seconds=5)
    settings = cell_worker_settings(EXTRACT, LIVE, "PT30S", Decimal(10))
    assert settings["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] == "PT1592S"


def test_without_the_workers_limiter_a_paced_cell_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(name: str) -> object:
        raise ImportError(name)

    monkeypatch.setattr(importlib, "import_module", missing)
    with pytest.raises(ValueError, match="rate limiter"):
        cell_worker_settings(EXTRACT, LIVE, "PT30S", Decimal(10))
    assert cell_worker_settings(EXTRACT, LIVE, "PT30S")  # unpaced needs no limiter


def test_a_paced_cell_covers_the_limiters_own_minimum_timeout() -> None:
    # The workers' limiter asks for batch x LIMITER_MAX_WAIT on top of the calls; every
    # call of the cap may wait for a token, so the cell gives cap x LIMITER_MAX_WAIT.
    for batch in (5, 25, 100):
        cell = Cell(Profile.EXTRACT, size=1_000, batch=batch, workers=8)
        settings = cell_worker_settings(cell, LIVE, "PT30S", Decimal(10))
        seconds = bench_settings(settings).execution_timeout.total_seconds()
        handoff = 2 * 3 * 3  # two handoff bounds of three live database bounds
        minimum = extract_cap(batch) * (30 + handoff) + 2 + batch * LIMITER_MAX_WAIT
        assert seconds >= minimum
        assert seconds == extract_cap(batch) * (30 + handoff + LIMITER_MAX_WAIT) + 2
