"""The benchmark harness end to end, on a throwaway schema of the integration database.

Each test runs one cell through ``benchmarks.run.run_cell``: real worker
processes (the benchmark worker for ``invoice``, the installed ``orqestra worker
run`` for ``echo``), a job submitted through ``orqestra job submit``, and the
profile's correctness gate (plan 2.6). Only the gate is asserted, never a
timing: ``STOP`` is a bound on a hung run, not an expectation. The workers use
the short test profile of the process-level tests (lease ``PT4S``).
"""

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks.result import Status, validate_result, write_result
from benchmarks.run import Cell, Profile, run_cell
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
