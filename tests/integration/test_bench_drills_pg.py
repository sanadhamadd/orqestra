"""The failure drills at fleet size: D1 with four workers, D2 and D4 (plan 2.9 and 2.10).

Marked ``integration`` and ``performance``, so no default lane runs them: they
are run by hand, and their output recorded. Each runs 1,000 invoices at batch
100 on the short test profile of the process-level tests (lease ``PT4S``).
Only outcomes and the order of stored timestamps are asserted, never a
duration: ``STOP`` bounds a hung run.
"""

from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks.drills import (
    FAULTS_COMMAND,
    FREEZE_EXTRA,
    FreezeDrill,
    KillDrill,
    PayloadDrill,
    RetryDrill,
    payload_workload,
    retry_policy,
    retry_workload,
    run_drill,
    standard_workload,
)
from benchmarks.faults import create_fault_table
from benchmarks.generate import generate_invoices
from benchmarks.result import Status
from benchmarks.run import Cell, Profile
from benchmarks.worker import parse_duration
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = [pytest.mark.integration, pytest.mark.performance]

STOP = 600.0
CELL = Cell(Profile.INVOICE, size=1_000, batch=100, workers=4)
INVOICES = generate_invoices(CELL.size)


def _passes(record: dict[str, Any], drill: str) -> dict[str, dict[str, Any]]:
    checks = {check["name"]: check for check in record["gate"]}
    failed = [check for check in record["gate"] if not check["passed"]]
    assert failed == []
    assert record["status"] == Status.COMPLETE
    assert record["drill"]["name"] == drill
    assert record["drill"]["injected"] is True
    return checks


def test_d1_kills_two_of_four_workers_holding_attempts(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    record = run_drill(
        schema_url,
        CELL,
        KillDrill(victims=2),
        standard_workload(INVOICES, CELL.batch or 0),
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path,
        stop_after=STOP,
    )

    _passes(record, "d1")
    assert len(record["drill"]["injections"]) == 2
    assert record["drill"]["measured"]["recovery_delay_s"]["n"] == 2


def test_d2_a_frozen_worker_never_records_a_stale_completion(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    lease = parse_duration(ENVIRONMENT["ORQESTRA_WORKER_LEASE"]).total_seconds()
    record = run_drill(
        schema_url,
        CELL,
        FreezeDrill(lease + FREEZE_EXTRA),
        standard_workload(INVOICES, CELL.batch or 0),
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path,
        stop_after=STOP,
    )

    checks = _passes(record, "d2")
    [injection] = record["drill"]["injections"]
    assert injection["thawed_by"] == "drill"  # the freeze ran its planned time
    assert injection["frozen_s"] >= lease + FREEZE_EXTRA
    assert checks["stale_completion_not_recorded"]["detail"]["state"] in {"revoked", "succeeded"}


def test_d4_payload_job_fails_only_the_extra_task(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    create_fault_table(db)
    workload = payload_workload(INVOICES, CELL.batch or 0)
    record = run_drill(
        schema_url,
        CELL,
        PayloadDrill(workload.expected),
        workload,
        command=FAULTS_COMMAND,
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path,
        stop_after=STOP,
    )

    checks = _passes(record, "d4-payload")
    assert checks["terminal"]["detail"]["job_status"] == "failed"
    assert checks["failed_tasks"]["detail"]["failed"] == ["poison-0"]


def test_d4_retry_job_fails_the_always_batch_and_retries_the_once_batch(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    create_fault_table(db)
    workload, poison = retry_workload(INVOICES, CELL.batch or 0)
    record = run_drill(
        schema_url,
        CELL,
        RetryDrill(workload.expected, poison, retry_policy(ENVIRONMENT)),
        workload,
        command=FAULTS_COMMAND,
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path,
        stop_after=STOP,
    )

    checks = _passes(record, "d4-retry")
    assert checks["failed_tasks"]["detail"]["failed"] == [poison.always_key]
    measured = record["drill"]["measured"]
    assert measured[poison.always_key]["attempts"] == 3
    assert measured[poison.once_key]["attempts"] == 2
