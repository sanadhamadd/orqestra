"""The crash drill (d1) on the OpenRouter adapter, as a dry run (DEC-071, T2).

In the default integration lane, so every submit and CI run it: real worker
processes, real PostgreSQL and the real adapter, answered by ``CannedTransport``,
so no network and no cost. A sentinel key is set in this process's environment,
as it would be for a live run, and must reach no worker, no log, no raw file and
no result. Only outcomes are asserted, never a duration: ``STOP`` bounds a hung run.
"""

import json
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks import extract
from benchmarks.drills import (
    KillDrill,
    expected_calls_by_item,
    extract_workload,
    run_drill,
    standard_workload,
)
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
from benchmarks.worker import DEFAULT_CAP
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

STOP = 300.0
# Small enough for every submit, with batches for two workers to hold when the kill comes.
CELL = Cell(Profile.INVOICE, size=600, batch=50, workers=2)
INVOICES = generate_invoices(CELL.size)


def _passes(record: dict[str, Any]) -> dict[str, dict[str, Any]]:
    failed = [check for check in record["gate"] if not check["passed"]]
    assert failed == []
    assert record["status"] == Status.COMPLETE
    assert record["drill"]["name"] == "d1"
    assert record["drill"]["injected"] is True
    return {check["name"]: check for check in record["gate"]}


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
        max_provider_calls=3 * workload.oracle.calls,  # room for the killed attempts' repeats
        dry_run=True,
    )
    raw_dir = tmp_path / "raw"
    record = run_drill(
        schema_url,
        CELL,
        KillDrill(
            victims=1,
            expected_calls=expected_calls_by_item(CELL.size, CELL.batch or 0, DEFAULT_CAP),
        ),
        workload,
        worker_settings=ENVIRONMENT,
        raw_dir=raw_dir,
        stop_after=STOP,
        provider=choice,
    )

    checks = _passes(record)
    assert checks["no_lost_tasks"]["detail"] == {"lost_tasks": 0}
    assert checks["no_duplicate_calls"]["detail"] == {"duplicate_calls": 0}
    measured = record["drill"]["measured"]
    assert measured["recovery_from_kill_s"]["n"] == 1
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


# The extraction workload (T4): every document is one call, so a batch of 25 is 25 calls.
EXTRACT_CELL = Cell(Profile.EXTRACT, size=300, batch=25, workers=2)


def test_d1_on_the_extract_workload_loses_nothing_and_records_its_figures(
    db: psycopg.Connection[TupleRow],
    schema_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    started: list[list[str]] = []
    real = subprocess.Popen

    class Recording(real):  # type: ignore[valid-type,misc]
        def __init__(self, args: Sequence[str], *rest: Any, **options: Any) -> None:
            started.append([str(part) for part in args])
            super().__init__(args, *rest, **options)

    monkeypatch.setattr(subprocess, "Popen", Recording)
    workload = extract_workload(EXTRACT_CELL)
    choice = ProviderChoice(
        name=ProviderName.OPENROUTER,
        model="vendor/model-a",
        max_provider_calls=3 * EXTRACT_CELL.size,
        dry_run=True,
    )
    raw_dir = tmp_path / "raw"
    record = run_drill(
        schema_url,
        EXTRACT_CELL,
        KillDrill(victims=1, expected_calls=dict.fromkeys(range(EXTRACT_CELL.size), 1)),
        workload,
        worker_settings=ENVIRONMENT,
        raw_dir=raw_dir,
        stop_after=STOP,
        provider=choice,
        call_timeout="PT1S",
        prices=extract.CANNED_PRICES,
    )

    checks = _passes(record)
    assert {
        "extraction_complete",
        "extraction_usage",
        "no_lost_tasks",
        "no_duplicate_calls",
    } <= set(checks)
    assert checks["no_duplicate_calls"]["detail"] == {"duplicate_calls": 0}
    block = record["extraction"]
    assert block["documents"] == EXTRACT_CELL.size
    assert block["accuracy"]["overall"] == 1.0
    assert block["tokens"]["max_per_call"]["input"] is not None
    assert record["provider"]["cap"] == EXTRACT_CELL.batch
    assert record["provider"]["calls_made"] == block["calls"]["total"]
    assert record["usage"] is None  # the invoice usage report reads invoice jobs only
    assert record["resources"]["sysload"]["samples"] >= 1
    workers = [argv for argv in started if "benchmarks.worker" in argv]
    assert workers
    for argv in workers:
        assert argv[argv.index("--workload") + 1] == "extract"
        assert argv[argv.index("--cap") + 1] == str(EXTRACT_CELL.batch)
        assert "--dry-run" in argv
    stored = write_result(record, tmp_path / "results", allow_dirty=True)
    assert SENTINEL not in stored.read_text(encoding="utf-8")
