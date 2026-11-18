"""A cell's live settings, its call cap, its pacing flags and its supervised fleet.

No database, worker or network: the fleet tests start plain Python processes that exit
with a chosen code, in place of benchmark workers.
"""

import contextlib
import math
import subprocess
import sys
import time
import types
import uuid
from collections.abc import Iterator, Mapping, Sequence
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

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
    STALL_REPLACEMENT_DELAY,
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
from benchmarks.worker import DEFAULT_CAP, parse_duration
from orqestra.domain.retry import RetryPolicy, retry_delay
from orqestra.worker.fence import ExitCode
from tests.integration.test_bench_smoke_pg import DRILL_SETTINGS
from tests.integration.test_worker_process import ENVIRONMENT

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
        (EXTRACT, Decimal("Infinity"), "at most 1000"),
        (EXTRACT, Decimal("1000.5"), "at most 1000"),  # the limiter's own MAX_PER_S
    ],
)
def test_the_rate_limit_is_refused_off_extract_and_outside_the_limiters_range(
    cell: Cell, rate: Decimal, words: str
) -> None:
    refusal = rate_limit_refusal(cell, rate)
    assert refusal is not None
    assert words in refusal
    for allowed in (Decimal(10), Decimal("0.5"), Decimal(1000)):
        assert rate_limit_refusal(EXTRACT, allowed) is None
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


def test_a_stalled_worker_is_replaced_only_after_the_delay(tmp_path: Path) -> None:
    fleet = fleet_with(tmp_path, [4, None, None])
    fleet.start()
    fleet.start()
    wait_exited(fleet, 1)
    found = fleet.supervise(10.0, limit=8)
    assert found == [
        {
            "worker": 1,
            "pid": fleet.workers[0].process.pid,
            "exit_code": 4,
            "at_s": 10.0,
            "replaced": False,
            "replacement_at_s": None,
        }
    ]
    assert len(fleet.workers) == 2  # not yet: the slowdown may still be on
    assert fleet.pending
    assert fleet.supervise(10.5, limit=8) == []
    assert len(fleet.workers) == 2
    assert fleet.supervise(10.0 + STALL_REPLACEMENT_DELAY, limit=8) == []  # each exit once
    assert len(fleet.workers) == 3
    assert found[0]["replaced"] is True
    assert found[0]["replacement_at_s"] == 11.0
    assert (fleet.replaced, fleet.scheduled, fleet.pending) == (1, 1, [])
    assert fleet.alive() == 2
    fleet.stop(timeout=10)
    assert [handle.died for handle in fleet.workers] == [True, False, False]


@pytest.mark.parametrize("code", [ExitCode.FENCED, ExitCode.DEFECT, 1, 0])
def test_any_other_exit_is_replaced_at_once(tmp_path: Path, code: int) -> None:
    fleet = fleet_with(tmp_path, [code, None])
    fleet.start()
    wait_exited(fleet, 1)
    (found,) = fleet.supervise(2.0, limit=4)
    assert (found["exit_code"], found["replaced"], found["replacement_at_s"]) == (code, True, 2.0)
    assert len(fleet.workers) == 2
    assert fleet.pending == []
    fleet.stop(timeout=10)


def test_replacements_stop_at_the_limit_and_count_the_pending_ones(tmp_path: Path) -> None:
    fleet = fleet_with(tmp_path, [4, 4, 4, 4, 4])
    fleet.start()
    found: list[dict[str, Any]] = []
    elapsed = 0.0
    limit = replacement_limit(1)
    for exits in range(1, limit + 2):  # the first worker and its four replacements
        wait_exited(fleet, exits)
        found += fleet.supervise(elapsed, limit=limit)
        assert fleet.scheduled == min(exits, limit)  # granted when scheduled
        elapsed += STALL_REPLACEMENT_DELAY
        fleet.supervise(elapsed, limit=limit)  # starts the one that is due
    assert limit == 4
    assert [item["replaced"] for item in found] == [True, True, True, True, False]
    assert found[-1]["replacement_at_s"] is None
    assert fleet.replaced == 4
    assert fleet.pending == []
    assert fleet.alive() == 0
    fleet.stop(timeout=10)


def test_the_replacement_limit_is_four_per_worker() -> None:
    assert [replacement_limit(workers) for workers in (1, 2, 8, 32)] == [4, 8, 32, 128]
    assert STALL_REPLACEMENT_DELAY == 1.0


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
    fleet.settle(timeout=30)
    assert time.monotonic() - began < 10  # it returned as soon as it could, not at the timeout
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


def test_the_pacing_wait_is_the_workers_limiters_own() -> None:
    assert run.limiter_max_wait() == timedelta(seconds=LIMITER_MAX_WAIT)
    settings = cell_worker_settings(EXTRACT, LIVE, "PT30S", Decimal(10))
    seconds = 30 * (30 + 18 + LIMITER_MAX_WAIT) + 2
    assert settings["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] == f"PT{math.ceil(seconds)}S"


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


# --- checkpoints during a cell ------------------------------------------------------------


def sample(at: float, timed: float, requested: float, write: float, sync: float) -> Any:
    return (at, timed, requested, write, sync)


def test_each_reading_whose_count_rose_is_one_event_of_ended_checkpoints() -> None:
    samples = [
        sample(0.0, 10, 3, 1_000.0, 50.0),
        sample(1.0, 10, 3, 1_000.0, 50.0),
        sample(2.0, 11, 3, 59_500.0, 70.0),  # a timed checkpoint of 58.5 s write ended
        sample(3.0, 11, 3, 59_500.0, 70.0),
        sample(4.0, 11, 5, 59_600.0, 75.0),  # two requested ones within one interval
    ]
    summary = run.summarize_checkpoints("pg_stat_bgwriter", samples, lost=1)
    assert summary == {
        "source": "pg_stat_bgwriter",
        "count": 3,
        "timed": 1,
        "requested": 2,
        "write_ms": 58_600.0,
        "sync_ms": 25.0,
        "longest_ms": 58_520.0,  # only an event of exactly one checkpoint
        "events": [
            {"ended_by_s": 2.0, "checkpoints": 1, "write_ms": 58_500.0, "sync_ms": 20.0},
            {"ended_by_s": 4.0, "checkpoints": 2, "write_ms": 100.0, "sync_ms": 5.0},
        ],
        "samples": 5,
        "lost_samples": 1,
    }


def test_no_checkpoint_and_a_statistics_reset_are_counted_right() -> None:
    quiet = [sample(0.0, 4, 1, 10.0, 1.0), sample(1.0, 4, 1, 10.0, 1.0)]
    summary = run.summarize_checkpoints("pg_stat_checkpointer", quiet)
    assert isinstance(summary, dict)
    assert (summary["count"], summary["events"], summary["longest_ms"]) == (0, [], None)
    reset = [
        sample(0.0, 40, 9, 9_000.0, 90.0),
        sample(1.0, 0, 0, 0.0, 0.0),  # pg_stat_reset_shared: a new baseline, no event
        sample(2.0, 1, 0, 300.0, 4.0),
    ]
    after = run.summarize_checkpoints("pg_stat_checkpointer", reset)
    assert isinstance(after, dict)
    assert (after["count"], after["write_ms"], after["sync_ms"]) == (1, 300.0, 4.0)
    assert [event["ended_by_s"] for event in after["events"]] == [2.0]


@pytest.mark.parametrize(
    ("source", "samples"),
    [(None, [sample(0.0, 1, 1, 1.0, 1.0)] * 3), ("pg_stat_bgwriter", [sample(0.0, 1, 1, 1, 1)])],
)
def test_without_a_view_or_two_readings_checkpoints_are_not_measured(
    source: str | None, samples: list[Any]
) -> None:
    assert run.summarize_checkpoints(source, samples) == "not_measured"


class FakeStats:
    """A connection whose catalog has the views in ``views``; every reading of a counter
    view returns the next row of ``rows``, or raises when the row is an exception."""

    def __init__(self, views: set[str], rows: list[Any]) -> None:
        self.views = views
        self.rows = rows
        self.queries: list[str] = []
        self.transactions = 0

    @contextlib.contextmanager
    def transaction(self, force_rollback: bool = False) -> Iterator[None]:
        assert force_rollback
        self.transactions += 1
        yield

    def execute(self, query: str, params: Sequence[object] = ()) -> Any:
        self.queries.append(query)
        if "to_regclass" in query:
            found = str(params[0]).removeprefix("pg_catalog.") in self.views
            return types.SimpleNamespace(fetchone=lambda: (found,))
        row = self.rows.pop(0)
        if isinstance(row, Exception):
            raise row
        return types.SimpleNamespace(fetchone=lambda: row)


@pytest.mark.parametrize(
    ("views", "source", "columns"),
    [
        ({"pg_stat_checkpointer", "pg_stat_bgwriter"}, "pg_stat_checkpointer", "num_timed"),
        ({"pg_stat_bgwriter"}, "pg_stat_bgwriter", "checkpoints_timed"),
    ],
)
def test_the_watch_reads_the_view_the_server_has_and_never_a_missing_one(
    views: set[str], source: str, columns: str
) -> None:
    conn = FakeStats(views, [(1, 0, 10.0, 1.0), (2, 0, 30.0, 2.0)])
    watch = run.CheckpointWatch(conn)  # type: ignore[arg-type]
    watch.sample(0.0)
    watch.sample(1.0)
    readings = [query for query in conn.queries if "to_regclass" not in query]
    assert all(f"FROM pg_catalog.{source}" in query and columns in query for query in readings)
    summary = watch.summary()
    assert isinstance(summary, dict)
    assert (summary["source"], summary["count"], summary["write_ms"]) == (source, 1, 20.0)
    assert conn.transactions == 1 + 2  # the choice, then one transaction per reading


def test_a_failed_reading_is_counted_and_never_raised() -> None:
    conn = FakeStats(
        {"pg_stat_bgwriter"}, [(1, 0, 1.0, 1.0), RuntimeError("gone"), (1, 0, 1.0, 1.0)]
    )
    watch = run.CheckpointWatch(conn)  # type: ignore[arg-type]
    for at in (0.0, 1.0, 2.0):
        watch.sample(at)
    summary = watch.summary()
    assert isinstance(summary, dict)
    assert (summary["samples"], summary["lost_samples"], summary["count"]) == (2, 1, 0)


def test_a_server_without_either_view_is_not_measured_and_sends_no_reading() -> None:
    conn = FakeStats(set(), [])
    watch = run.CheckpointWatch(conn)  # type: ignore[arg-type]
    watch.sample(0.0)
    watch.sample(1.0)
    assert watch.summary() == "not_measured"
    assert all("to_regclass" in query for query in conn.queries)


def test_the_watch_reads_at_most_once_an_interval() -> None:
    watch = run.CheckpointWatch(FakeStats(set(), []))  # type: ignore[arg-type]
    assert watch.due(0.0)
    watch.sample(0.0)
    assert not watch.due(0.5)
    assert watch.due(run.CHECKPOINT_SAMPLE_INTERVAL)


# --- settings read once, through bench_settings ---------------------------------------------


def seconds(settings: Mapping[str, str], name: str) -> float:
    """A value as the harness parsed it field by field before: the string, read alone."""
    return parse_duration(settings[f"ORQESTRA_WORKER_{name}"]).total_seconds()


SETTINGS_IN_USE = [
    pytest.param(BENCH_WORKER_SETTINGS, id="bench"),
    pytest.param(LIVE_WORKER_SETTINGS, id="live"),
    pytest.param(cell_worker_settings(EXTRACT, DRY, "PT30S"), id="extract-cell"),
    pytest.param(cell_worker_settings(INVOICE, DRY, "PT30S"), id="invoice-cell"),
    pytest.param(ENVIRONMENT, id="test-profile"),
    pytest.param(DRILL_SETTINGS, id="drill"),
]


@pytest.mark.parametrize("settings", SETTINGS_IN_USE)
def test_every_derived_value_is_todays_for_the_settings_the_harness_uses(
    settings: Mapping[str, str],
) -> None:
    validated = bench_settings(settings)
    for workers in (1, 4, 32):
        poll, sweep = seconds(settings, "POLL_INTERVAL"), seconds(settings, "RECOVERY_INTERVAL")
        assert run.implied_idle_rate(workers, validated) == workers * (1 / poll + 1 / sweep)
    assert validated.shutdown_timeout.total_seconds() == seconds(settings, "SHUTDOWN_TIMEOUT")
    operation = validated.db_operation_timeout.total_seconds()
    assert operation == seconds(settings, "DB_OPERATION_TIMEOUT")
    assert validated.lease.total_seconds() == seconds(settings, "LEASE")
    assert validated.retry_policy == RetryPolicy(
        timedelta(seconds=seconds(settings, "RETRY_BASE_DELAY")),
        timedelta(seconds=seconds(settings, "RETRY_MAX_DELAY")),
    )


def test_a_cell_refuses_settings_a_worker_would_refuse_before_any_worker_starts(
    tmp_path: Path,
) -> None:
    broken = {**BENCH_WORKER_SETTINGS, "ORQESTRA_WORKER_LEASE": "PT1S"}  # shorter than heartbeat
    with pytest.raises(ValueError, match="lease"):
        run.run_cell(
            "postgresql:///unused",
            INVOICE,
            worker_settings=broken,
            raw_dir=tmp_path,
            stop_after=1.0,
        )
    assert list(tmp_path.iterdir()) == []  # refused before the raw directory or a worker
