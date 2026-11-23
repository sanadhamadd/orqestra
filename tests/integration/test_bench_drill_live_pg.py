"""The crash drill (d1) on the OpenRouter adapter, as a dry run (DEC-071, T2).

In the default integration lane, so every submit and CI run it: real worker
processes, real PostgreSQL and the real adapter, answered by ``CannedTransport``,
so no network and no cost. A sentinel key is set in this process's environment,
as it would be for a live run, and must reach no worker, no log, no raw file and
no result. The workers use the smoke drills' ``DRILL_SETTINGS`` (the live cells'
3 s database bound), so a slow call on a loaded machine does not end a worker.
Only outcomes are asserted, never a duration: ``STOP`` bounds a hung run, except
in the last test, which bounds how long the drill's freeze keeps workers stopped
(DEC-076).
"""

import contextlib
import json
import subprocess
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks import extract
from benchmarks.drills import (
    Drill,
    KillDrill,
    Poll,
    _process_state,
    expected_calls_by_item,
    extract_workload,
    freeze_victims,
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
from benchmarks.run import Cell, Profile, bench_settings
from benchmarks.sysload import METRICS
from benchmarks.worker import DEFAULT_CAP
from tests.integration.test_bench_smoke_pg import DRILL_SETTINGS

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
        worker_settings=DRILL_SETTINGS,
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
        worker_settings=DRILL_SETTINGS,
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


# --- the freeze never outlasts a worker's operation bound (DEC-076) ------------------------

BOUND = 3.0  # DRILL_SETTINGS' db_operation_timeout
WINDOW = BOUND / 2  # benchmarks.drills.freeze_window
SLACK = 0.5  # the stop and quiet checks poll; a bound on scheduling, not an expectation
BUSY = 2 * BOUND  # how long another worker-named connection keeps the database busy

_SLOW_COMPLETION = """
    CREATE TABLE test_slow_completion (armed boolean NOT NULL);
    INSERT INTO test_slow_completion VALUES (false);
    CREATE FUNCTION test_slow_completion() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF (SELECT armed FROM test_slow_completion) THEN
            PERFORM pg_sleep(1.0);  -- a third of the bound: alone, never a stall
        END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER test_slow_completion BEFORE UPDATE ON tasks FOR EACH ROW
        WHEN (NEW.state = 'succeeded' AND OLD.state <> NEW.state)
        EXECUTE FUNCTION test_slow_completion();
"""
_NO_SLOW_COMPLETION = """
    DROP TRIGGER IF EXISTS test_slow_completion ON tasks;
    DROP FUNCTION IF EXISTS test_slow_completion();
    DROP TABLE IF EXISTS test_slow_completion;
"""
_WORKER_ACTIVE = (
    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
    "AND state = 'active' AND application_name = 'orqestra-worker' AND wait_event = 'PgSleep' "
    "AND query {} 'SELECT pg_sleep%%'"
)


def _await_active(conn: psycopg.Connection[Any], query: str) -> None:
    deadline = time.monotonic() + 60.0  # bounds a hung run only
    while conn.execute(query).fetchone() == (0,):
        assert time.monotonic() < deadline, query
        time.sleep(0.005)


class ActiveFreeze(Drill):
    """Freeze every worker while one is inside its completion and the database stays busy.

    Once, at the trigger: completions are made to sleep 1 s in a test trigger, and the
    drill waits until a worker's completion is active on the server. A separate
    connection named like a worker then runs ``pg_sleep(BUSY)``, so the database is
    not quiet for twice the bound, and ``freeze_victims`` is called for no victim.
    Without the freeze the completing worker's call takes about 1 s; frozen until
    the database is quiet, it would be stopped past its bound and exit 4.
    """

    name = "forced-freeze"

    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url
        self.sleeper: threading.Thread | None = None
        self.busy: psycopg.Connection[Any] | None = None
        self.took: float | None = None
        self.states: list[str] = []

    def _busy(self) -> None:
        with psycopg.connect(self.url, application_name="orqestra-worker", autocommit=True) as busy:
            self.busy = busy
            with contextlib.suppress(psycopg.errors.QueryCanceled):  # cancelled by close()
                busy.execute("SELECT pg_sleep(%s)", (BUSY,))

    def step(self, poll: Poll) -> None:
        if self.injected or not poll.reached():
            return
        conn = poll.conn
        conn.execute("UPDATE test_slow_completion SET armed = true")
        try:
            _await_active(conn, _WORKER_ACTIVE.format("NOT LIKE"))  # a completion, mid-call
            self.sleeper = threading.Thread(target=self._busy, daemon=True)
            self.sleeper.start()
            _await_active(conn, _WORKER_ACTIVE.format("LIKE"))
            began = time.monotonic()
            # No victim: only the freeze itself is measured.
            freeze_victims(poll, 0, set(), freezes=self.freezes)
            self.took = time.monotonic() - began
            self.states = [_process_state(h.process.pid) for h in poll.fleet.workers]
        finally:
            conn.execute("UPDATE test_slow_completion SET armed = false")
        self.injected = True
        self.injections.append({"freeze_s": self.took})

    def close(self) -> None:
        if self.busy is not None:
            with contextlib.suppress(psycopg.Error):  # already closed once its sleep ended
                self.busy.cancel_safe()
        if self.sleeper is not None:
            self.sleeper.join(timeout=BUSY + 10.0)


def test_a_freeze_on_a_busy_database_continues_a_worker_inside_its_bound(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    settings = bench_settings(DRILL_SETTINGS)  # validated as the harness reads them
    assert settings.db_operation_timeout.total_seconds() == BOUND
    db.execute(_SLOW_COMPLETION)
    drill = ActiveFreeze(schema_url)
    try:
        record = run_drill(
            schema_url,
            CELL,
            drill,
            standard_workload(INVOICES, CELL.batch or 0),
            worker_settings=DRILL_SETTINGS,
            raw_dir=tmp_path / "raw",
            stop_after=STOP,
        )
    finally:
        drill.close()  # run_drill closes it too; again here if it raised before that
        db.execute(_NO_SLOW_COMPLETION)

    # No worker stopped itself: the completing worker was continued inside its bound.
    exits = [check for check in record["gate"] if check["name"] == "worker_exits"]
    assert [check["passed"] for check in exits] == [True], exits
    assert drill.took is not None
    # The database stayed busy for 2 x the bound, yet everyone was continued within the window.
    assert drill.took < WINDOW + SLACK < BOUND
    assert [state for state in drill.states if state.startswith("T")] == []
    # The freeze took the not-quiet path: the busy statement was what it waited on.
    [freeze] = drill.freezes.log
    assert freeze["quiet"] is False
    assert "Timeout:PgSleep" in freeze["waits"]
    assert [check for check in record["gate"] if not check["passed"]] == []
    assert record["status"] == Status.COMPLETE
