"""The benchmark harness end to end, on a throwaway schema of the integration database.

Each test runs one cell through ``benchmarks.run.run_cell``: real worker
processes (the benchmark worker for ``invoice``, the installed ``orqestra worker
run`` for ``echo``), a job submitted through ``orqestra job submit``, and the
profile's correctness gate (plan 2.6). Only the gate is asserted, never a
timing: ``STOP`` is a bound on a hung run, not an expectation. The workers use
the short test profile of the process-level tests (lease ``PT4S``).
"""

import json
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from typing import Any, NoReturn

import psycopg
import pytest
from psycopg.rows import TupleRow

import benchmarks.drills
from benchmarks.drills import (
    DrillFleet,
    KillDrill,
    OutageDrill,
    Poll,
    RestartDrill,
    _await_started,
    _process_state,
    freeze_victims,
    run_drill,
    standard_workload,
)
from benchmarks.faults import Fault, FaultingReader, create_fault_table, poison_total
from benchmarks.generate import generate_invoices
from benchmarks.result import ROOT, Status, validate_result, write_result
from benchmarks.run import Cell, Profile, process_environment, run_cell, worker_command
from orqestra.calls.provider import FieldKind, ProviderTransientError, make_request
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

STOP = 180.0
INVOICE_CHECKS = (
    "terminal",
    "one_succeeded_attempt",
    "no_double_running",
    "nothing_running",
    "failed_tasks",
    "worker_exits",
    "summary",
    "usage",
)
ECHO_CHECKS = (*INVOICE_CHECKS[:6], "results_exact")
# The drills run 2,000 invoices at batch 100, so a job outlasts several polls.
DRILL_CELL = Cell(Profile.INVOICE, size=2_000, batch=100, workers=1)


def _passes_gate(record: dict[str, Any], names: Sequence[str]) -> None:
    assert [check["name"] for check in record["gate"]] == list(names)
    failed = [check for check in record["gate"] if not check["passed"]]
    assert failed == []
    assert record["status"] == Status.COMPLETE


def _written(record: dict[str, Any], directory: Path) -> dict[str, Any]:
    path = write_result(record, directory, allow_dirty=True)
    stored: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    validate_result(stored)
    return stored


def test_an_invoice_cell_passes_its_gate(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    cell = Cell(Profile.INVOICE, size=200, batch=20, workers=2)
    record = run_cell(
        schema_url,
        cell,
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        stop_after=STOP,
        idle_window=0.5,
    )

    _passes_gate(record, INVOICE_CHECKS)
    usage = {check["name"]: check for check in record["gate"]}["usage"]["detail"]
    assert usage["finality"] == "closed"
    assert usage["budget_verdict"] == "within"
    assert record["provider"] == {"call_timeout": "PT0.1S", "cap": 16, "route": "route-a"}
    assert len(record["resources"]["workers"]) == 2
    stored = _written(record, tmp_path / "results")
    assert stored["cell"] == {
        "profile": "invoice",
        "size": 200,
        "batch": 20,
        "workers": 2,
        "repetition": 1,
        "seed": cell.seed,
    }


def test_an_echo_cell_passes_its_gate(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    record = run_cell(
        schema_url,
        Cell(Profile.ECHO, size=50, batch=None, workers=2),
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        stop_after=STOP,
        idle_window=0.5,
    )

    _passes_gate(record, ECHO_CHECKS)
    assert record["usage"] is None
    assert record["provider"] is None
    _written(record, tmp_path / "results")


# --- drills (plan 2.10: D1 with one worker, and D3a) --------------------------------------


def _drill_passes(record: dict[str, Any], drill: str) -> None:
    failed = [check for check in record["gate"] if not check["passed"]]
    assert failed == []
    assert record["status"] == Status.COMPLETE
    assert record["drill"]["name"] == drill
    assert record["drill"]["injected"] is True
    assert record["throughput"] is None


def test_d1_kill_with_one_worker_recovers_and_passes_the_gate(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    record = run_drill(
        schema_url,
        DRILL_CELL,
        KillDrill(victims=1),
        standard_workload(generate_invoices(DRILL_CELL.size), 100),
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        stop_after=STOP,
    )

    _drill_passes(record, "d1")
    names = {check["name"] for check in record["gate"]}
    assert {"victims_revoked_lease_expired", "victims_closed_by_recovery_t3"} <= names
    assert {"victims_capture", "victims_ledger_kept"} <= names
    assert [exit_["exit_code"] for exit_ in record["drill"]["exits_while_running"]] == [-9]
    assert record["drill"]["restarts"] >= 1
    _written(record, tmp_path / "results")


def test_d3a_connection_outage_passes_the_gate(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    cell = Cell(Profile.INVOICE, size=2_000, batch=100, workers=2)
    record = run_drill(
        schema_url,
        cell,
        OutageDrill(schema_url),
        standard_workload(generate_invoices(cell.size), 100),
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        stop_after=STOP,
    )

    _drill_passes(record, "d3a")
    assert len(record["drill"]["injections"]) == 1
    _written(record, tmp_path / "results")


def test_d3b_counts_no_injection_when_the_server_did_not_restart(
    db: psycopg.Connection[TupleRow],
    schema_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A restart command that exits 0 but restarts nothing: the record is not complete.

    The command is a real process that does nothing; ``RESTART_COMMAND`` and Docker
    are never run, so the real D3b still never runs in tests.
    """
    run = subprocess.run

    def no_docker(command: Sequence[str], *args: Any, **kwargs: Any) -> Any:
        if list(command)[:1] == ["docker"]:
            pytest.fail("the test ran a docker command")
        return run(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", no_docker)
    record = run_drill(
        schema_url,
        DRILL_CELL,
        RestartDrill(command=(sys.executable, "-c", "pass")),
        standard_workload(generate_invoices(DRILL_CELL.size), 100),
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        stop_after=STOP,
    )

    assert record["status"] == Status.FAILED_CORRECTNESS
    assert record["drill"]["injected"] is False
    [injection] = record["drill"]["injections"]
    assert injection["restarted"] is False
    assert injection["server_started_before"] == injection["server_started_after"]
    failed = [check["name"] for check in record["gate"] if not check["passed"]]
    assert failed == ["injected"]
    _written(record, tmp_path / "results")


# --- D3b's refusal, and the shared fail-once fault --------------------------------------


@pytest.mark.parametrize(
    ("arguments", "project"),
    [
        (["d3b"], None),
        (["d3b"], "orqestra-bench"),
        (["d3b", "--allow-db-restart"], None),
        (["d3b", "--allow-db-restart"], "orqestra"),
    ],
)
def test_d3b_is_refused_without_the_flag_and_the_bench_project(
    arguments: list[str],
    project: str | None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def no_command(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail("d3b connected or ran a command although it was refused")

    monkeypatch.setattr(subprocess, "run", no_command)
    monkeypatch.setattr(benchmarks.drills, "run_named", no_command)
    monkeypatch.setattr(benchmarks.drills, "connect_verified", no_command)
    monkeypatch.setattr(benchmarks.drills, "check_restart_target", no_command)
    monkeypatch.setattr(psycopg, "connect", no_command)
    if project is None:
        monkeypatch.delenv("COMPOSE_PROJECT_NAME", raising=False)
    else:
        monkeypatch.setenv("COMPOSE_PROJECT_NAME", project)

    assert benchmarks.drills.main(arguments) == 1
    assert "refused: d3b" in capsys.readouterr().err


_INTERPRET = """
import sys
from datetime import timedelta
from benchmarks.faults import FaultingReader
from orqestra.calls.provider import FieldKind, ProviderTransientError, make_request
request = make_request(capability="c", route_id="route-a", field_kind=FieldKind.TOTAL,
                       raw=sys.argv[2], timeout=timedelta(seconds=1))
sys.stdin.read()  # released together, after every process has started
try:
    print(FaultingReader(sys.argv[1]).interpret(request).value)
except ProviderTransientError:
    print("fault")
"""


def test_the_once_fault_happens_once_across_processes(
    db: psycopg.Connection[TupleRow], schema_url: str
) -> None:
    create_fault_table(db)
    raw = poison_total(123456, Fault.ONCE)
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", _INTERPRET, schema_url, raw],
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    for process in processes:
        assert process.stdin is not None
        process.stdin.close()  # release every call at once
    answers = []
    for process in processes:
        assert process.stdout is not None
        answers.append(process.stdout.read().strip())
        assert process.wait(timeout=60) == 0
    answers.sort()

    assert answers == ["123456", "123456", "123456", "fault"]
    rows = db.execute("SELECT raw FROM bench_provider_faults").fetchall()
    assert rows == [(raw,)]


def test_the_always_fault_never_answers_and_other_strings_are_read(schema_url: str) -> None:
    reader = FaultingReader(schema_url)  # neither case touches the database

    def ask(raw: str) -> str:
        request = make_request(
            capability="c",
            route_id="route-a",
            field_kind=FieldKind.TOTAL,
            raw=raw,
            timeout=timedelta(seconds=1),
        )
        return reader.interpret(request).value

    for _ in range(3):
        with pytest.raises(ProviderTransientError):
            ask(poison_total(98765, Fault.ALWAYS))
    assert ask("98_765") == "98765"
    assert ask("1 234 567") == "1234567"


# --- the drill harness's own cleanup -----------------------------------------------------


def test_a_worker_stopped_once_it_has_started_exits_0(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    env = process_environment(schema_url, ENVIRONMENT)
    fleet = DrillFleet(Profile.INVOICE, env, tmp_path, command=worker_command(Profile.INVOICE))
    fleet.start()  # stopped at once, as a replacement started at a job's end would be
    _await_started(fleet)
    assert [worker["exit_code"] for worker in fleet.stop(timeout=30)] == [0]


class _FailingHarness:
    """A harness whose database reads fail: ``freeze_victims`` must still continue everyone."""

    @property
    def conn(self) -> NoReturn:
        raise psycopg.OperationalError("the harness lost its connection")


def test_freezing_continues_every_worker_when_reading_fails(tmp_path: Path) -> None:
    fleet = DrillFleet(
        Profile.INVOICE, {}, tmp_path, command=[sys.executable, "-c", "import time; time.sleep(60)"]
    )
    fleet.start()
    fleet.start()
    poll = Poll(_FailingHarness(), uuid.uuid4(), fleet, 10, 10, quiet_bound=1.0)  # type: ignore[arg-type]
    try:
        with pytest.raises(psycopg.OperationalError):
            freeze_victims(poll, 1, set())
        for _ in range(1000):  # a bound on signal delivery, not an expectation
            states = [_process_state(handle.process.pid) for handle in fleet.workers]
            if not [state for state in states if state.startswith("T")]:
                break
            time.sleep(0.01)
        assert not [state for state in states if state.startswith("T")]
    finally:
        fleet.stop(timeout=10)
