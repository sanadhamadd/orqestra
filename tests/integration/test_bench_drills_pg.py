"""The failure drills at fleet size: D1 with four workers, D2 and D4 (plan 2.9 and 2.10).

Marked ``integration`` and ``performance``, so no default lane runs them: they
are run by hand, and their output recorded. Each runs 1,000 invoices at batch
100 on the short test profile of the process-level tests (lease ``PT4S``).
Only outcomes and the order of stored timestamps are asserted, never a
duration: ``STOP`` bounds a hung run.
"""

import json
import subprocess
from collections.abc import Sequence
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
    expected_calls_by_item,
    payload_workload,
    retry_policy,
    retry_workload,
    run_drill,
    standard_workload,
)
from benchmarks.faults import create_fault_table
from benchmarks.generate import generate_invoices
from benchmarks.live import (
    API_KEY_ENV,
    BUDGET_FILE_NAME,
    ProviderChoice,
    ProviderName,
    read_budget_file,
)
from benchmarks.result import Status, write_result
from benchmarks.run import Cell, Profile
from benchmarks.sysload import METRICS
from benchmarks.worker import DEFAULT_CAP, parse_duration
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


# --- d1 on the OpenRouter adapter, as a dry run (DEC-071, T2) ---------------------------------

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"


def test_d1_on_a_dry_run_provider_loses_nothing_and_leaks_nothing(
    db: psycopg.Connection[TupleRow],
    schema_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real workers, real PostgreSQL and the real adapter, answered by CannedTransport.

    A sentinel key is set in this process's environment, as for a live run; a dry run
    must pass it to no worker and write it nowhere.
    """
    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    started: list[tuple[list[str], dict[str, str]]] = []
    real = subprocess.Popen

    class Recording(real):  # type: ignore[valid-type,misc]
        def __init__(self, args: Sequence[str], *rest: Any, **options: Any) -> None:
            started.append(([str(part) for part in args], dict(options.get("env") or {})))
            super().__init__(args, *rest, **options)

    monkeypatch.setattr(subprocess, "Popen", Recording)
    workload = standard_workload(INVOICES, CELL.batch or 0)
    choice = ProviderChoice(
        name=ProviderName.OPENROUTER,
        model="vendor/model-a",
        max_provider_calls=3 * workload.expected.calls,  # room for the killed attempts' repeats
        dry_run=True,
    )
    raw_dir = tmp_path / "raw"
    record = run_drill(
        schema_url,
        CELL,
        KillDrill(
            victims=2,
            expected_calls=expected_calls_by_item(CELL.size, CELL.batch or 0, DEFAULT_CAP),
        ),
        workload,
        worker_settings=ENVIRONMENT,
        raw_dir=raw_dir,
        stop_after=STOP,
        provider=choice,
    )

    checks = _passes(record, "d1")
    assert checks["no_lost_tasks"]["detail"] == {"lost_tasks": 0}
    assert checks["no_duplicate_calls"]["detail"] == {"duplicate_calls": 0}
    measured = record["drill"]["measured"]
    assert measured["recovery_from_kill_s"]["n"] == 2
    assert measured["lost_tasks"] == 0
    assert measured["duplicate_calls"] == 0
    assert measured["repeated_calls_after_recovery"] >= 0
    assert record["provider"]["name"] == "openrouter"
    assert record["provider"]["dry_run"] is True
    providers = db.execute("SELECT DISTINCT provider FROM provider_calls").fetchall()
    assert providers == [("openrouter",)]
    calls = db.execute("SELECT count(*) FROM provider_calls").fetchone()
    assert calls is not None
    assert record["provider"]["calls_made"] == calls[0]
    assert read_budget_file(raw_dir / BUDGET_FILE_NAME) >= calls[0]  # a reservation per call
    sysload = record["resources"]["sysload"]
    assert set(sysload["metrics"]) == set(METRICS)
    assert sysload["metrics"]["worker_rss_bytes"]["n"] > 0

    stored = write_result(record, tmp_path / "results", allow_dirty=True)
    texts = {"result file": stored.read_text(encoding="utf-8")}
    for path in sorted(raw_dir.rglob("*")):
        if path.is_file():
            texts[str(path)] = path.read_bytes().decode("utf-8", errors="replace")
    harness = [(argv, env) for argv, env in started if "ORQESTRA_DATABASE_URL" in env]
    workers = [argv for argv, _ in harness if "benchmarks.worker" in argv]
    assert len(workers) == CELL.workers + record["drill"]["restarts"]  # replacements too
    for argv, env in harness:
        assert API_KEY_ENV not in env  # a dry run never passes the key
        texts[f"env {argv[:3]}"] = json.dumps(env)
    for argv in workers:
        assert "--dry-run" in argv
        assert "--allow-live-calls" not in argv
    for argv, _ in started:
        texts[f"argv {argv[:3]}"] = json.dumps(argv)
    assert [name for name, text in texts.items() if SENTINEL in text] == []
