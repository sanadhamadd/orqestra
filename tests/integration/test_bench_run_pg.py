"""A cell's supervised fleet and its live-cell settings, end to end on real workers and
PostgreSQL. Offline: the double, or OpenRouter as a dry run on the canned transport.
"""

import os
import signal
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks import extract
from benchmarks.live import ProviderChoice, ProviderName
from benchmarks.result import Status, WaitEnded
from benchmarks.run import (
    LIVE_MAX_ATTEMPTS,
    RATE_LIMIT_FILE_NAME,
    Cell,
    Profile,
    Running,
    extract_cap,
    run_cell,
)
from orqestra.worker.fence import ExitCode
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

STOP = 180.0
MODEL = "vendor/model-a"
# 20 tasks of 100 on the double: the job outlasts several polls.
CELL = Cell(Profile.INVOICE, size=2_000, batch=100, workers=2)
EXTRACT_CELL = Cell(Profile.EXTRACT, size=40, batch=10, workers=2)
DRY = ProviderChoice(
    name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=400, dry_run=True
)


def test_a_worker_killed_mid_cell_is_recorded_and_replaced_and_the_job_finishes(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    killed: list[int] = []

    def kill_the_first_worker_once(running: Running) -> None:
        handle = running.fleet.workers[0]
        if killed or handle.exited is not None:
            return
        os.kill(handle.process.pid, signal.SIGKILL)
        killed.append(handle.process.pid)

    record = run_cell(
        schema_url,
        CELL,
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        stop_after=STOP,
        idle_window=0.5,
        hook=kill_the_first_worker_once,
    )

    assert killed
    # The fleet was refilled, so the job finished: its lost attempt came back through
    # lease expiry and recovery.
    assert record["timing"]["wait_ended"] == WaitEnded.FINISHED
    failures = record["failures"]
    (died,) = failures["exits_while_running"]
    assert (died["worker"], died["pid"], died["replaced"]) == (1, killed[0], True)
    assert died["exit_code"] not in (0, ExitCode.STALLED)
    assert failures["replacements"] == 1
    assert len(failures["worker_exit_codes"]) == CELL.workers + 1
    gate = {check["name"]: check for check in record["gate"]}
    # A kill is not the watchdog's designed fail-fast, so it still fails the cell; the
    # stopped workers, the replacement included, all exited 0.
    assert not gate["worker_exits"]["passed"]
    assert gate["worker_exits"]["detail"] == {
        "exit_codes": [0, 0],
        "exits_while_running": [died["exit_code"]],
    }
    assert [name for name, check in gate.items() if not check["passed"]] == ["worker_exits"]
    assert record["status"] == Status.FAILED_CORRECTNESS
    assert (tmp_path / "raw" / "worker-3.log").is_file()


def test_an_openrouter_cell_writes_its_max_attempts_into_the_task_file(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    record = run_cell(
        schema_url,
        EXTRACT_CELL,
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        stop_after=STOP,
        idle_window=0.5,
        provider=DRY,
        call_timeout="PT1S",
        prices=extract.CANNED_PRICES,
    )
    assert record["status"] == Status.COMPLETE
    attempts = db.execute("SELECT DISTINCT max_attempts FROM tasks").fetchall()
    assert attempts == [(LIVE_MAX_ATTEMPTS,)]
    provider: dict[str, Any] = record["provider"]
    assert (provider["cap"], provider["max_attempts"]) == (extract_cap(10), LIVE_MAX_ATTEMPTS)
    assert provider["rate_limit_per_s"] is None
    assert record["failures"]["exits_while_running"] == []
    assert record["failures"]["replacements"] == 0


def test_the_pacing_flags_reach_every_worker_of_a_dry_extract_cell(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    pytest.importorskip("benchmarks.ratelimit", reason="the workers' limiter comes with F1")
    raw_dir = tmp_path / "raw"
    record = run_cell(
        schema_url,
        EXTRACT_CELL,
        worker_settings=ENVIRONMENT,
        raw_dir=raw_dir,
        stop_after=STOP,
        idle_window=0.5,
        provider=DRY,
        call_timeout="PT1S",
        prices=extract.CANNED_PRICES,
        rate_limit_per_s=Decimal(200),
    )
    assert record["status"] == Status.COMPLETE
    assert record["provider"]["rate_limit_per_s"] == "200"
    assert (raw_dir / RATE_LIMIT_FILE_NAME).is_file()  # created by the workers
