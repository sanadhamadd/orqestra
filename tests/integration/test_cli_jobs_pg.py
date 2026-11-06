"""`orqestra job submit` and `orqestra job status` against real PostgreSQL (DEC-064).

The commands run in-process through ``main`` with ``ORQESTRA_DATABASE_URL`` pointing at a
test schema, so these tests see exactly what an operator would: one JSON line and an exit
code, with the rows the command wrote checked directly.
"""

import json
import uuid
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.cli import main
from orqestra.domain.jobs import AttemptSucceeded
from orqestra.domain.leases import LeasePolicy
from orqestra.domain.retry import RetryPolicy
from orqestra.domain.states import TaskState
from orqestra.persistence.jobs import list_tasks
from orqestra.persistence.lifecycle import claim_next_attempt, complete_attempt

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

KIND = "cli.test"
# Test values only (DEC-034), never recommendations.
POLICY = LeasePolicy(lease=timedelta(minutes=5), execution_timeout=timedelta(hours=1))
RETRY = RetryPolicy(base_delay=timedelta(hours=1), max_delay=timedelta(hours=8))
TASKS: list[dict[str, Any]] = [
    {"task_key": "b", "payload": {"invoice_id": "INV-2", "lines": [1, 2.5, None]}},
    {"task_key": "a", "payload": "plain text", "max_attempts": 2},
]


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, schema_url: str, db: Conn) -> Conn:
    """The CLI's database is the migrated test schema ``db`` is connected to."""
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", schema_url)
    return db


@pytest.fixture
def write_tasks(tmp_path: Path) -> Callable[[object], str]:
    def write(content: object) -> str:
        path = tmp_path / f"{uuid.uuid4().hex}.json"
        path.write_text(json.dumps(content), "utf-8")
        return str(path)

    return write


def run(capsys: pytest.CaptureFixture[str], *argv: str, code: int = 0) -> dict[str, Any]:
    assert main(list(argv)) == code
    payload: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert payload["status"] == ("ok" if code == 0 else "error")
    return payload


def submit(
    capsys: pytest.CaptureFixture[str], path: str, *options: str, code: int = 0
) -> dict[str, Any]:
    return run(capsys, "job", "submit", "--kind", KIND, *options, path, code=code)


def job_count(db: Conn) -> int:
    row = db.execute("SELECT count(*) FROM jobs").fetchone()
    return int(row[0]) if row else 0


def test_submit_stores_the_file_and_status_reads_it_back(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    out = submit(capsys, write_tasks(TASKS), "--max-attempts", "3", "--idempotency-key", "k-1")
    assert (out["kind"], out["idempotency_key"], out["created"]) == (KIND, "k-1", True)
    job_id = uuid.UUID(out["job_id"])
    stored = [(t.task_key, t.payload, t.max_attempts, t.state) for t in list_tasks(cli_env, job_id)]
    assert stored == [
        ("a", "plain text", 2, TaskState.QUEUED),
        ("b", {"invoice_id": "INV-2", "lines": [1, 2.5, None]}, 3, TaskState.QUEUED),
    ]
    status = run(capsys, "job", "status", str(job_id))
    assert status == {
        "status": "ok",
        "job_id": str(job_id),
        "kind": KIND,
        "idempotency_key": "k-1",
        "created_at": out["created_at"],
        "job_status": "queued",
        "task_counts": {"queued": 2, "running": 0, "succeeded": 0, "failed": 0, "cancelled": 0},
    }


def test_a_resubmission_with_the_same_key_returns_the_original_job(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    first = submit(capsys, write_tasks(TASKS), "--max-attempts", "3", "--idempotency-key", "k")
    # The same tasks in another order are the same submission (DEC-025).
    again = submit(
        capsys, write_tasks(TASKS[::-1]), "--max-attempts", "3", "--idempotency-key", "k"
    )
    assert (again["job_id"], again["created"]) == (first["job_id"], False)
    assert again["created_at"] == first["created_at"]
    assert job_count(cli_env) == 1


def test_the_same_key_with_different_content_is_refused_and_stores_nothing(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    first = submit(capsys, write_tasks(TASKS), "--max-attempts", "3", "--idempotency-key", "k")
    refused = submit(
        capsys, write_tasks(TASKS), "--max-attempts", "4", "--idempotency-key", "k", code=1
    )
    assert refused["error"] == (
        f"idempotency key is already used by job {first['job_id']} with different content"
    )
    assert job_count(cli_env) == 1


def test_submissions_without_a_key_always_create_a_job(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    path = write_tasks(TASKS)
    first = submit(capsys, path, "--max-attempts", "3")
    second = submit(capsys, path, "--max-attempts", "3")
    assert first["created"]
    assert second["created"]
    assert first["job_id"] != second["job_id"]
    assert (first["idempotency_key"], second["idempotency_key"]) == (None, None)
    assert job_count(cli_env) == 2


def test_status_follows_the_tasks_through_claim_and_completion(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    out = submit(capsys, write_tasks(TASKS[1:]))
    attempt = claim_next_attempt(cli_env, "worker-1", kinds=[KIND], policy=POLICY)
    assert attempt is not None
    running = run(capsys, "job", "status", out["job_id"])
    assert running["job_status"] == "running"
    assert running["task_counts"]["running"] == 1
    complete_attempt(
        cli_env,
        attempt.id,
        generation=attempt.attempt_number,
        outcome=AttemptSucceeded({"ok": True}),
        retry_policy=RETRY,
    )
    done = run(capsys, "job", "status", out["job_id"])
    assert (done["job_status"], done["task_counts"]["succeeded"]) == ("succeeded", 1)


def test_status_of_an_unknown_job_is_refused(
    cli_env: Conn, capsys: pytest.CaptureFixture[str]
) -> None:
    job_id = uuid.uuid4()
    assert run(capsys, "job", "status", str(job_id), code=1)["error"] == f"job {job_id} not found"


def test_a_retry_limit_out_of_range_names_the_constraint_and_stores_nothing(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    refused = submit(capsys, write_tasks(TASKS[:1]), "--max-attempts", "101", code=1)
    assert refused["error"] == (
        "the database request failed "
        "(CheckViolation, SQLSTATE 23514, constraint tasks_max_attempts_range)"
    )
    assert job_count(cli_env) == 0


def test_a_payload_the_database_rejects_is_reported_without_its_value(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    tasks = [{"task_key": "a", "payload": {"note": "secret-value\u0000"}, "max_attempts": 1}]
    assert main(["job", "submit", "--kind", KIND, write_tasks(tasks)]) == 1
    captured = capsys.readouterr()
    assert "secret-value" not in captured.out + captured.err
    assert json.loads(captured.out)["error"] == (
        "the database request failed (UntranslatableCharacter, SQLSTATE 22P05)"
    )
    assert job_count(cli_env) == 0


def test_a_kind_no_worker_runs_is_accepted_and_stays_queued(
    cli_env: Conn, write_tasks: Callable[[object], str], capsys: pytest.CaptureFixture[str]
) -> None:
    out = run(capsys, "job", "submit", "--kind", "unregistered.kind", write_tasks(TASKS[1:]))
    assert run(capsys, "job", "status", out["job_id"])["job_status"] == "queued"


def test_a_schema_without_migrations_is_refused_before_any_write(
    monkeypatch: pytest.MonkeyPatch,
    schema_factory: Callable[[], str],
    write_tasks: Callable[[object], str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    url = schema_factory()
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", url)
    refused = submit(capsys, write_tasks(TASKS[1:]), code=1)
    assert refused["error"].startswith("the database schema is not current: ")
    with psycopg.connect(url, autocommit=True) as conn:
        row = conn.execute("SELECT count(*) FROM pg_tables WHERE schemaname = current_schema()")
        assert row.fetchone() == (0,)
