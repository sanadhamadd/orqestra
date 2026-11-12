"""The OpenRouter opt-in end to end, as a dry run (DEC-071): real worker processes, real
PostgreSQL, the real adapter, and ``CannedTransport`` in place of the network.

No live call is made: every worker gets ``--dry-run``. A sentinel key is set in this
process's environment, as it would be for a live run, and must reach no worker, no
log, no raw file and no result. The second cell sets the call maximum below what the
oracle needs and checks the run stops there and fails closed.
"""

import json
import subprocess
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks.live import (
    API_KEY_ENV,
    BUDGET_FILE_NAME,
    CANNED_COST,
    ProviderChoice,
    ProviderName,
    canned_requests,
    read_budget_file,
)
from benchmarks.result import Status, write_result
from benchmarks.run import Cell, Profile, build_workload, run_cell
from orqestra.providers.openrouter import PROMPT_VERSION, SYSTEM_PROMPT, USAGE_UNIT
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

STOP = 180.0
SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
CELL = Cell(Profile.INVOICE, size=100, batch=25, workers=2)
INVOICE_CHECKS = [
    "terminal",
    "one_succeeded_attempt",
    "no_double_running",
    "nothing_running",
    "failed_tasks",
    "worker_exits",
    "summary",
    "usage",
]


def dry_run(maximum: int) -> ProviderChoice:
    return ProviderChoice(
        name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=maximum, dry_run=True
    )


@pytest.fixture
def started(monkeypatch: pytest.MonkeyPatch) -> list[tuple[list[str], dict[str, str]]]:
    """Every process this test starts, with its argv and environment; the key is set."""
    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    seen: list[tuple[list[str], dict[str, str]]] = []
    real = subprocess.Popen

    class Recording(real):  # type: ignore[valid-type,misc]
        def __init__(self, args: Sequence[str], *rest: Any, **options: Any) -> None:
            seen.append(([str(part) for part in args], dict(options.get("env") or {})))
            super().__init__(args, *rest, **options)

    monkeypatch.setattr(subprocess, "Popen", Recording)
    return seen


def _run(schema_url: str, raw_dir: Path, choice: ProviderChoice) -> dict[str, Any]:
    return run_cell(
        schema_url,
        CELL,
        worker_settings=ENVIRONMENT,
        raw_dir=raw_dir,
        stop_after=STOP,
        idle_window=0.5,
        provider=choice,
    )


def _assert_nothing_leaked(
    record: dict[str, Any],
    stored: Path,
    raw_dir: Path,
    started: list[tuple[list[str], dict[str, str]]],
) -> None:
    secrets = (SENTINEL, SYSTEM_PROMPT)
    texts = {"record": json.dumps(record), "result file": stored.read_text(encoding="utf-8")}
    for path in sorted(raw_dir.rglob("*")):
        if path.is_file():
            texts[str(path)] = path.read_bytes().decode("utf-8", errors="replace")
    # The processes the harness starts with its own environment (process_environment):
    # the workers and ``orqestra job submit``. Helpers such as git or ``file`` (run by
    # platform.platform()) inherit this process's environment, run no Orqestra code
    # and record nothing, so only their argv is scanned.
    harness = [(argv, env) for argv, env in started if "ORQESTRA_DATABASE_URL" in env]
    workers = [(argv, env) for argv, env in harness if "benchmarks.worker" in argv]
    assert len(workers) == CELL.workers
    assert len(harness) == CELL.workers + 1  # and the submission
    for argv, env in harness:
        assert API_KEY_ENV not in env  # a dry run never passes the key
        texts[f"env {argv[:3]}"] = json.dumps(env)
    for argv, _ in started:
        texts[f"argv {argv[:3]}"] = json.dumps(argv)
    for argv, _ in workers:
        assert "--dry-run" in argv
        assert "--allow-live-calls" not in argv
    leaks = [name for name, text in texts.items() if any(secret in text for secret in secrets)]
    assert leaks == []


def _ledger(db: psycopg.Connection[TupleRow], job_id: str) -> dict[str, Any]:
    intents = db.execute(
        "SELECT provider, model, prompt_version, count(*) FROM provider_calls "
        "WHERE job_id = %s GROUP BY 1, 2, 3",
        (job_id,),
    ).fetchall()
    results = db.execute(
        "SELECT r.outcome, r.reported_unit, count(*), sum(r.reported_amount) "
        "FROM provider_call_results r JOIN provider_calls c USING (attempt_id, call_seq) "
        "WHERE c.job_id = %s GROUP BY 1, 2",
        (job_id,),
    ).fetchall()
    closures = db.execute(
        "SELECT u.capture_tier, count(*) FROM attempt_usage_closures u "
        "JOIN attempts a ON a.id = u.attempt_id JOIN tasks t ON t.id = a.task_id "
        "WHERE t.job_id = %s GROUP BY 1",
        (job_id,),
    ).fetchall()
    attempts = db.execute(
        "SELECT count(*) FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s",
        (job_id,),
    ).fetchone()
    assert attempts is not None
    return {"intents": intents, "results": results, "closures": closures, "attempts": attempts[0]}


def _job_id(db: psycopg.Connection[TupleRow]) -> str:
    row = db.execute("SELECT id::text FROM jobs").fetchone()
    assert row is not None
    return str(row[0])


def test_a_dry_run_cell_passes_its_gate_and_leaks_nothing(
    db: psycopg.Connection[TupleRow],
    schema_url: str,
    tmp_path: Path,
    started: list[tuple[list[str], dict[str, str]]],
) -> None:
    expected = build_workload(CELL).expected
    assert expected is not None
    assert expected.calls > 0
    raw_dir = tmp_path / "raw"
    record = _run(schema_url, raw_dir, dry_run(expected.calls))

    assert [check["name"] for check in record["gate"]] == INVOICE_CHECKS
    assert [check for check in record["gate"] if not check["passed"]] == []
    assert record["status"] == Status.COMPLETE
    usage = {check["name"]: check for check in record["gate"]}["usage"]["detail"]
    assert usage["unit"] == USAGE_UNIT
    assert usage["calls"] == expected.calls
    assert Decimal(usage["amount"]) == CANNED_COST * expected.calls
    assert record["provider"] == {
        "name": "openrouter",
        "model": MODEL,
        "max_provider_calls": expected.calls,
        "dry_run": True,
        "call_timeout": "PT0.1S",
        "cap": 16,
        "route": "route-a",
        "calls_made": expected.calls,
        "usage_unit": USAGE_UNIT,
    }

    ledger = _ledger(db, _job_id(db))
    assert ledger["intents"] == [("openrouter", MODEL, PROMPT_VERSION, expected.calls)]
    assert ledger["results"] == [
        ("answered", USAGE_UNIT, expected.calls, CANNED_COST * expected.calls)
    ]
    assert ledger["closures"] == [("t3", ledger["attempts"])]
    assert read_budget_file(raw_dir / BUDGET_FILE_NAME) == expected.calls
    assert canned_requests(raw_dir) == expected.calls

    stored = write_result(record, tmp_path / "results", allow_dirty=True)
    _assert_nothing_leaked(record, stored, raw_dir, started)


def test_the_call_maximum_stops_the_cell_and_fails_closed(
    db: psycopg.Connection[TupleRow],
    schema_url: str,
    tmp_path: Path,
    started: list[tuple[list[str], dict[str, str]]],
) -> None:
    expected = build_workload(CELL).expected
    assert expected is not None
    maximum = expected.calls // 2
    assert 0 < maximum < expected.calls
    raw_dir = tmp_path / "raw"
    record = _run(schema_url, raw_dir, dry_run(maximum))

    assert record["status"] == Status.FAILED_CORRECTNESS
    gate = {check["name"]: check for check in record["gate"]}
    assert not gate["failed_tasks"]["passed"]
    assert gate["worker_exits"]["passed"]  # the workers carry on; only tasks fail

    job_id = _job_id(db)
    ledger = _ledger(db, job_id)
    assert ledger["intents"] == [("openrouter", MODEL, PROMPT_VERSION, maximum)]
    assert ledger["results"] == [("answered", USAGE_UNIT, maximum, CANNED_COST * maximum)]
    assert read_budget_file(raw_dir / BUDGET_FILE_NAME) == maximum
    assert canned_requests(raw_dir) == maximum
    assert record["provider"]["calls_made"] == maximum

    failed = db.execute(
        "SELECT a.error_type, a.failure_kind, a.error_message, count(*) FROM attempts a "
        "JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s AND a.state = 'failed' "
        "GROUP BY 1, 2, 3",
        (job_id,),
    ).fetchall()
    assert len(failed) == 1
    error_type, failure_kind, message, count = failed[0]
    assert (error_type, failure_kind, message) == (
        "benchmarks.live.ProviderCallBudgetExhausted",
        "permanent",
        "provider_call_budget_exhausted",
    )
    tasks = db.execute(
        "SELECT count(*) FROM tasks WHERE job_id = %s AND state = 'failed'", (job_id,)
    ).fetchone()
    assert tasks is not None
    assert tasks[0] == count  # one attempt each: a permanent failure is not retried

    stored = write_result(record, tmp_path / "results", allow_dirty=True)
    _assert_nothing_leaked(record, stored, raw_dir, started)
