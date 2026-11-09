import dataclasses
import io
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import MappingProxyType
from typing import Any, NoReturn

import psycopg
import pytest
from psycopg import errors
from pydantic import SecretStr

import orqestra
from orqestra import cli
from orqestra.aggregation.budget import CallBudgetCheck, check_call_budget
from orqestra.aggregation.invoice import WrongJobKindError
from orqestra.aggregation.usage import (
    AttemptCapture,
    JobUsageReport,
    ProvenanceUsage,
    UnitTotal,
    UsageAnomaly,
    UsageAnomalyCode,
    UsageFinality,
)
from orqestra.calls.recording import MAX_CALL_SEQ
from orqestra.cli import main
from orqestra.config import Settings
from orqestra.domain.jobs import Job, JobProgress, JobStatus, JobSubmission
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence.jobs import IdempotencyConflictError, SubmissionResult
from orqestra.persistence.migrate import MigrationError
from orqestra.worker.runtime import connect_supervisor
from orqestra.workloads.invoice.summary import Anomaly, AnomalyCode, InvoiceJobSummary

pytestmark = pytest.mark.unit


def test_version_command(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == orqestra.__version__


def test_health_command_reports_ok(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["health"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["version"] == orqestra.__version__
    assert payload["settings"]["worker_concurrency"] == 4


def test_health_never_prints_secrets(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", "postgresql://u:s3cret@db/orq")
    assert main(["health"]) == 0
    out = capsys.readouterr().out
    assert "s3cret" not in out
    assert json.loads(out)["settings"]["database_url"] == "***"


def test_health_never_prints_the_openrouter_api_key(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ORQESTRA_OPENROUTER_API_KEY", "sk-or-v1-synthetic-s3cret")
    assert main(["health"]) == 0
    out = capsys.readouterr().out
    assert "s3cret" not in out
    assert json.loads(out)["settings"]["openrouter_api_key"] == "***"


def test_health_reports_no_openrouter_api_key_by_default(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["health"]) == 0
    assert json.loads(capsys.readouterr().out)["settings"]["openrouter_api_key"] is None


def test_health_reports_invalid_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ORQESTRA_WORKER_CONCURRENCY", "0")
    assert main(["health"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "error"
    assert payload["errors"]


def test_migrate_requires_database_url(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["migrate"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"status": "error", "error": "ORQESTRA_DATABASE_URL is not set"}


BLANK = ["", "   ", "\t\n"]


@pytest.mark.parametrize("blank", BLANK)
def test_migrate_treats_a_blank_database_url_as_unset(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], blank: str
) -> None:
    def no_migration(dsn: str) -> NoReturn:
        pytest.fail("migrate reached a database although the URL was blank")

    monkeypatch.setattr(cli, "migrate", no_migration)
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", blank)
    assert main(["migrate"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"status": "error", "error": "ORQESTRA_DATABASE_URL is not set"}


@pytest.mark.parametrize("blank", BLANK)
def test_health_reports_a_blank_secret_as_unset(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], blank: str
) -> None:
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", blank)
    monkeypatch.setenv("ORQESTRA_OPENROUTER_API_KEY", blank)
    assert main(["health"]) == 0
    settings = json.loads(capsys.readouterr().out)["settings"]
    assert settings["database_url"] is None
    assert settings["openrouter_api_key"] is None


def test_the_connection_string_refuses_a_blank_url_that_reaches_it() -> None:
    # A backstop for settings built without validation.
    settings = Settings.model_construct(database_url=SecretStr("  "))
    with pytest.raises(cli._Refused, match="ORQESTRA_DATABASE_URL is not set"):
        cli._connection_string(settings)


def test_migrate_reports_invalid_configuration(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ORQESTRA_LOG_LEVEL", "LOUD")
    assert main(["migrate"]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "error"


def test_migrate_never_echoes_a_malformed_url(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # libpq's own parse error would quote this token, password included.
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", "postgresql://u:s3%zzcret@127.0.0.1:1/orq")
    assert main(["migrate"]) == 1
    out = capsys.readouterr().out
    assert "s3" not in out
    assert json.loads(out)["error"] == (
        "ORQESTRA_DATABASE_URL is not a valid PostgreSQL connection string"
    )


def test_migrate_reports_unreachable_database_without_secrets(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", "postgresql://u:s3cret@127.0.0.1:1/orq")
    assert main(["migrate"]) == 1
    out = capsys.readouterr().out
    assert "s3cret" not in out
    assert json.loads(out) == {"status": "error", "error": MIGRATE_DRIVER_ERROR}


MIGRATE_DRIVER_ERROR = "the database could not be reached or migrated (OperationalError)"


@pytest.mark.parametrize("command", ["health", "migrate"])
def test_health_and_migrate_never_echo_an_invalid_setting(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    # A .env line missing its newline: the log level swallows the next assignment.
    monkeypatch.setenv(
        "ORQESTRA_LOG_LEVEL", f"INFO ORQESTRA_DATABASE_URL=postgresql://u:{SECRET}@db/orq"
    )
    assert main([command]) == 1
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    errors = json.loads(captured.out)["errors"]
    assert [tuple(error["loc"]) for error in errors] == [("log_level",)]
    assert all(set(error) == {"type", "loc", "msg"} for error in errors)


@pytest.mark.parametrize(
    ("dsn", "driver_message"),
    [
        # libpq ends the user info at the first '@': host 'ss@127.0.0.1', password 'pa'.
        ("postgresql://u:pa@ss@127.0.0.1:1/orq", "failed to resolve host 'ss@127.0.0.1'"),
        # ... and the authority at the first '/': host 'app', port 'ab'.
        (
            "postgresql://app:ab/cd@127.0.0.1:5432/orq",
            'invalid integer value "ab" for connection option "port"',
        ),
    ],
)
def test_migrate_reports_a_driver_error_by_type_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    dsn: str,
    driver_message: str,
) -> None:
    def failing(url: str) -> NoReturn:
        raise psycopg.OperationalError(driver_message)

    monkeypatch.setattr(cli, "migrate", failing)
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", dsn)
    assert main(["migrate"]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"status": "error", "error": MIGRATE_DRIVER_ERROR}
    assert captured.err == ""


def test_migrate_reports_a_migration_error_in_its_own_words(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def failing(url: str) -> NoReturn:
        msg = "migration 0002_attempt_leases was edited after it was applied"
        raise MigrationError(msg)

    monkeypatch.setattr(cli, "migrate", failing)
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", URL)
    assert main(["migrate"]) == 1
    assert json.loads(capsys.readouterr().out)["error"] == (
        "migration 0002_attempt_leases was edited after it was applied"
    )


def test_unknown_command_exits_with_usage_error() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["frobnicate"])
    assert exc.value.code == 2


# --- worker run (DEC-051) -----------------------------------------------------------------

# Test values only (DEC-034), never recommendations.
WORKER_ENV = {
    "ORQESTRA_WORKER_LEASE": "PT30S",
    "ORQESTRA_WORKER_EXECUTION_TIMEOUT": "PT10M",
    "ORQESTRA_WORKER_HEARTBEAT_INTERVAL": "PT5S",
    "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT": "PT2S",
    "ORQESTRA_WORKER_LOCK_TIMEOUT": "PT1S",
    "ORQESTRA_WORKER_FENCE_MARGIN": "PT3S",
    "ORQESTRA_WORKER_POLL_INTERVAL": "PT1S",
    "ORQESTRA_WORKER_RECOVERY_INTERVAL": "PT10S",
    "ORQESTRA_WORKER_RECOVERY_BATCH_LIMIT": "3",
    "ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT30S",
    "ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT1S",
    "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT1M",
}
SECRET = "s3cret-pw"
URL = f"postgresql://u:{SECRET}@127.0.0.1:1/orq"


class Started:
    """Stands in for the runtime: records what would have run, or fails if anything did."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, allowed: bool) -> None:
        self.workers: list[Any] = []
        self.allowed = allowed
        monkeypatch.setattr(cli, "Worker", self._worker)
        monkeypatch.setattr(cli, "run_until_signalled", self._run)

    def _worker(self, **kwargs: Any) -> Any:
        assert self.allowed, "a worker was created after a refused startup"
        self.workers.append(kwargs)
        return kwargs

    def _run(self, worker: Any) -> None:
        assert self.allowed, "a worker ran after a refused startup"


@pytest.fixture
def worker_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name, value in WORKER_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", URL)
    return monkeypatch


def _no_connection(*args: object, **kwargs: object) -> NoReturn:
    pytest.fail("a database was reached although the URL was refused")


def refused(capsys: pytest.CaptureFixture[str], *argv: str) -> dict[str, Any]:
    assert main(["worker", "run", *argv]) == 1
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert "127.0.0.1:1" not in captured.out + captured.err  # no part of the DSN
    payload: dict[str, Any] = json.loads(captured.out)
    assert payload["status"] == "error"
    return payload


def test_worker_run_requires_a_kind() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["worker", "run"])
    assert exc.value.code == 2
    with pytest.raises(SystemExit) as exc:
        main(["worker"])
    assert exc.value.code == 2


def test_worker_run_needs_every_worker_setting(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(monkeypatch, allowed=False)
    payload = refused(capsys, "--kind", "diagnostic.echo")
    missing = {tuple(error["loc"]) for error in payload["errors"]}
    assert missing == {(name.removeprefix("ORQESTRA_WORKER_").lower(),) for name in WORKER_ENV}
    assert all(set(error) == {"type", "loc", "msg"} for error in payload["errors"])


def test_worker_run_never_echoes_an_invalid_setting(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(worker_env, allowed=False)
    worker_env.setenv("ORQESTRA_WORKER_LEASE", f"P{SECRET}")
    payload = refused(capsys, "--kind", "diagnostic.echo")
    assert [tuple(error["loc"]) for error in payload["errors"]] == [("lease",)]
    assert "input" not in payload["errors"][0]


def test_worker_run_refuses_settings_that_break_their_relationships(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(worker_env, allowed=False)
    worker_env.setenv("ORQESTRA_WORKER_DB_OPERATION_TIMEOUT", "PT5S")  # = heartbeat interval
    payload = refused(capsys, "--kind", "diagnostic.echo")
    assert "heartbeat_interval" in payload["errors"][0]["msg"]


@pytest.mark.parametrize("lock", ["PT2S", "PT1.9995S", "PT3S"])
def test_worker_run_refuses_a_lock_wait_that_is_not_shorter_than_a_database_call(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], lock: str
) -> None:
    Started(worker_env, allowed=False)
    worker_env.setenv("ORQESTRA_WORKER_LOCK_TIMEOUT", lock)  # 2 s database bound (DEC-050)
    (error,) = refused(capsys, "--kind", "diagnostic.echo")["errors"]
    assert "must be shorter than db_operation_timeout" in error["msg"]


@pytest.mark.parametrize(
    ("kinds", "reason"),
    [
        (["invoice.parse"], "unsupported job kinds ['invoice.parse']"),
        (["diagnostic.echo", "Diagnostic.Echo"], "unsupported job kinds ['Diagnostic.Echo']"),
        ([" "], "invalid --kind: a kind must not be blank"),
    ],
)
def test_worker_run_refuses_kinds_it_cannot_run(
    worker_env: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    kinds: list[str],
    reason: str,
) -> None:
    Started(worker_env, allowed=False)
    argv = [arg for kind in kinds for arg in ("--kind", kind)]
    assert reason in refused(capsys, *argv)["error"]


def test_worker_run_refuses_diagnostic_handlers_in_production(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(worker_env, allowed=False)
    worker_env.setenv("ORQESTRA_ENVIRONMENT", "production")
    payload = refused(capsys, "--kind", "diagnostic.echo", "--kind", "diagnostic.wait")
    assert payload["error"] == (
        "diagnostic handlers are refused in production: ['diagnostic.echo', 'diagnostic.wait']"
    )


def test_worker_run_requires_a_database_url(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(worker_env, allowed=False)
    worker_env.delenv("ORQESTRA_DATABASE_URL")
    assert refused(capsys, "--kind", "diagnostic.echo")["error"] == (
        "ORQESTRA_DATABASE_URL is not set"
    )


@pytest.mark.parametrize("blank", BLANK)
def test_worker_run_treats_a_blank_database_url_as_unset(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], blank: str
) -> None:
    Started(worker_env, allowed=False)
    worker_env.setattr(psycopg, "connect", _no_connection)
    worker_env.setenv("ORQESTRA_DATABASE_URL", blank)
    assert refused(capsys, "--kind", "diagnostic.echo")["error"] == (
        "ORQESTRA_DATABASE_URL is not set"
    )


def test_worker_run_never_echoes_a_malformed_url(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(worker_env, allowed=False)
    worker_env.setenv("ORQESTRA_DATABASE_URL", f"postgresql://u:{SECRET}%zz@127.0.0.1:1/orq")
    assert refused(capsys, "--kind", "diagnostic.echo")["error"] == (
        "ORQESTRA_DATABASE_URL is not a valid PostgreSQL connection string"
    )


def test_worker_run_refuses_an_unreachable_database_without_secrets(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(worker_env, allowed=False)
    payload = refused(capsys, "--kind", "diagnostic.echo")  # nothing listens on port 1
    assert payload["error"] == "the database could not be reached or read (OperationalError)"


def test_worker_run_refuses_a_schema_that_is_not_current(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Started(worker_env, allowed=False)

    class Conn:
        def __enter__(self) -> "Conn":
            return self

        def __exit__(self, *exc: object) -> None:
            pass

    def behind(conn: object) -> None:
        msg = "schema 'public' is behind; pending migrations: 0003_cancellation_grace"
        raise MigrationError(msg)

    worker_env.setattr(cli, "connect_supervisor", lambda dsn, settings: Conn())
    worker_env.setattr(cli, "verify_current", behind)
    assert refused(capsys, "--kind", "diagnostic.echo")["error"] == (
        "the database schema is not current: "
        "schema 'public' is behind; pending migrations: 0003_cancellation_grace"
    )


def test_worker_run_starts_the_requested_kinds_after_a_current_preflight(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    started = Started(worker_env, allowed=True)
    checked: list[object] = []

    class Conn:
        def __enter__(self) -> "Conn":
            return self

        def __exit__(self, *exc: object) -> None:
            pass

    worker_env.setattr(cli, "connect_supervisor", lambda dsn, settings: Conn())
    worker_env.setattr(cli, "verify_current", checked.append)
    assert main(["worker", "run", "--kind", "diagnostic.wait", "--kind", "diagnostic.wait"]) == 0
    assert len(checked) == 1  # the preflight ran first
    ((worker,),) = [tuple(started.workers)]
    assert worker["registry"].kinds == ("diagnostic.wait",)  # exact, deduplicated
    assert worker["settings"].lease == timedelta(seconds=30)
    # The worker connects as the preflight did (stood in for here), with the same settings.
    assert worker["connect"].func is vars(cli)["connect_supervisor"]
    assert worker["connect"].args == (URL, worker["settings"])
    assert capsys.readouterr().out == ""


def test_the_cli_connects_with_the_supervisors_bounds() -> None:
    # Unpatched, that is the runtime's own: statement, lock and idle bounds (DEC-050).
    assert vars(cli)["connect_supervisor"] is connect_supervisor


# --- job submit / job status (DEC-064) ------------------------------------------------------

JOB_ID = uuid.UUID("0b9f3c52-6a4e-4d4b-9a51-2f1f0e9c7d10")
CREATED_AT = datetime(2026, 11, 6, 16, 0, tzinfo=UTC)
NOT_FINITE = (
    "TASKS_FILE is not valid JSON: "
    "a number that is not finite (NaN, Infinity or out of range) is not valid JSON"
)
TASKS: list[dict[str, Any]] = [
    {"task_key": "invoice-1", "payload": {"invoice_id": "INV-1"}, "max_attempts": 3},
    {"task_key": "invoice-2", "payload": {"invoice_id": "INV-2"}},
]


class Database:
    """Stands in for PostgreSQL: records what the job commands did, or fails if they connected."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, allowed: bool = True) -> None:
        self.allowed = allowed
        self.submissions: list[JobSubmission] = []
        self.verified = 0
        self.result = SubmissionResult(Job(JOB_ID, "invoice_verification", "k-1", CREATED_AT), True)
        self.progress: JobProgress | None = None
        monkeypatch.setattr(psycopg, "connect", self._connect)
        monkeypatch.setattr(cli, "verify_current", self._verify)
        monkeypatch.setattr(cli, "submit_job", self._submit)
        monkeypatch.setattr(cli, "get_job", self._get_job)
        monkeypatch.setattr(cli, "get_job_progress", lambda conn, job_id: self.progress)

    def _connect(self, dsn: str, *, autocommit: bool) -> "Database":
        assert self.allowed, "the database was reached after a refusal"
        assert (dsn, autocommit) == (URL, True)
        return self

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def _verify(self, conn: object) -> None:
        self.verified += 1

    def _submit(self, conn: object, submission: JobSubmission) -> SubmissionResult:
        assert self.verified == 1, "submitted before the schema check"
        self.submissions.append(submission)
        return self.result

    def _get_job(self, conn: object, job_id: uuid.UUID) -> Job | None:
        assert self.verified == 1, "read before the schema check"
        return self.result.job if self.progress is not None else None


@pytest.fixture
def job_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", URL)
    return monkeypatch


ALL_JOB_COMMANDS = [
    ["submit"],
    ["status", str(JOB_ID)],
    ["summary", str(JOB_ID)],
    ["usage", str(JOB_ID)],
    ["usage", str(JOB_ID), "--max-provider-calls-per-attempt", "4"],
]


def tasks_file(tmp_path: Path, content: object) -> str:
    path = tmp_path / "tasks.json"
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content if isinstance(content, str) else json.dumps(content), "utf-8")
    return str(path)


def job_output(capsys: pytest.CaptureFixture[str], argv: list[str], code: int) -> dict[str, Any]:
    assert main(argv) == code
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert "127.0.0.1:1" not in captured.out + captured.err  # no part of the DSN
    payload: dict[str, Any] = json.loads(captured.out)
    assert payload["status"] == ("ok" if code == 0 else "error")
    return payload


def job_argv(tmp_path: Path, command: list[str]) -> list[str]:
    """``job submit`` with a valid tasks file and retry limit, or ``job status``."""
    if command[0] == "submit":
        return ["job", "submit", "--kind", "k", "--max-attempts", "1", tasks_file(tmp_path, TASKS)]
    return ["job", *command]


def submit_refused(
    capsys: pytest.CaptureFixture[str], path: str, *options: str, kind: str = "invoice_verification"
) -> str:
    error: str = job_output(capsys, ["job", "submit", "--kind", kind, *options, path], 1)["error"]
    return error


@pytest.mark.parametrize(
    "argv",
    [
        ["job"],
        ["job", "submit", "tasks.json"],
        ["job", "submit", "--kind", "k"],
        ["job", "submit", "--kind", "k", "--max-attempts", "3.5", "tasks.json"],
        ["job", "status"],
        ["job", "status", "not-a-uuid"],
        ["job", "summary"],
        ["job", "summary", "not-a-uuid"],
        ["job", "usage"],
        ["job", "usage", str(JOB_ID), "--max-provider-calls-per-attempt", "1.5"],
        ["job", "usage", str(JOB_ID), "--max-provider-calls-per-attempt"],
    ],
)
def test_job_commands_report_usage_errors_through_argparse(argv: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(argv)
    assert exc.value.code == 2


def test_job_submit_submits_the_file_through_submit_job(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = Database(job_env)
    argv = ["job", "submit", "--kind", "invoice_verification", "--idempotency-key", "k-1"]
    payload = job_output(capsys, [*argv, "--max-attempts", "5", tasks_file(tmp_path, TASKS)], 0)
    assert payload == {
        "status": "ok",
        "job_id": str(JOB_ID),
        "kind": "invoice_verification",
        "idempotency_key": "k-1",
        "created": True,
        "created_at": "2026-11-06T16:00:00+00:00",
    }
    ((submission,),) = [tuple(db.submissions)]
    assert submission.kind == "invoice_verification"
    assert submission.idempotency_key == "k-1"
    # A task's own retry limit wins over --max-attempts.
    assert [(t.task_key, t.payload, t.max_attempts) for t in submission.tasks] == [
        ("invoice-1", {"invoice_id": "INV-1"}, 3),
        ("invoice-2", {"invoice_id": "INV-2"}, 5),
    ]


def test_job_submit_reports_a_replayed_submission(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = Database(job_env)
    db.result = SubmissionResult(db.result.job, False)
    argv = ["job", "submit", "--kind", "invoice_verification", "--idempotency-key", "k-1"]
    payload = job_output(capsys, [*argv, "--max-attempts", "3", tasks_file(tmp_path, TASKS)], 0)
    assert (payload["job_id"], payload["created"]) == (str(JOB_ID), False)


def test_job_submit_reads_standard_input_for_a_dash(
    job_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = Database(job_env)
    stdin = io.TextIOWrapper(io.BytesIO(json.dumps(TASKS[:1]).encode()), encoding="utf-8")
    job_env.setattr(sys, "stdin", stdin)
    job_output(capsys, ["job", "submit", "--kind", "k", "-"], 0)
    assert [t.task_key for t in db.submissions[0].tasks] == ["invoice-1"]
    assert db.submissions[0].idempotency_key is None


@pytest.mark.parametrize(
    ("content", "error"),
    [
        (b"\xff[]", "TASKS_FILE is not UTF-8"),
        ("[{", "TASKS_FILE is not valid JSON: Expecting property name enclosed in double quotes"),
        ('[{"task_key": "a", "payload": NaN, "max_attempts": 1}]', NOT_FINITE),
        ('[{"task_key": "a", "payload": Infinity, "max_attempts": 1}]', NOT_FINITE),
        ('[{"task_key": "a", "payload": -Infinity, "max_attempts": 1}]', NOT_FINITE),
        # Overflowing literals parse to infinity, which PostgreSQL would refuse.
        ('[{"task_key": "a", "payload": [1, 1e400], "max_attempts": 1}]', NOT_FINITE),
        ('[{"task_key": "a", "payload": {"x": -1e400}, "max_attempts": 1}]', NOT_FINITE),
        ('[{"task_key": "a", "payload": 1.5E+309, "max_attempts": 1}]', NOT_FINITE),
        # A lone surrogate is valid JSON but has no UTF-8 encoding for the database.
        (
            '[{"task_key": "\\ud800", "payload": 1, "max_attempts": 1}]',
            "task 0: task_key cannot be encoded as UTF-8",
        ),
        (
            '[{"task_key": "a", "payload": {"x": [{"y": "ok \\udfff"}]}, "max_attempts": 1}]',
            "task 0: payload holds a string or key that cannot be encoded as UTF-8",
        ),
        (
            '[{"task_key": "a", "payload": {"x": {"\\udc80": 1}}, "max_attempts": 1}]',
            "task 0: payload holds a string or key that cannot be encoded as UTF-8",
        ),
        (
            '[{"task_key": "a", "payload": "\\ud800", "max_attempts": 1}]',
            "task 0: payload holds a string or key that cannot be encoded as UTF-8",
        ),
        (
            '[{"task_key": "a", "payload": {"x": 1, "x": 2}, "max_attempts": 1}]',
            "TASKS_FILE is not valid JSON: an object repeats a key",
        ),
        ("[" * 100_000 + "]" * 100_000, "TASKS_FILE is nested too deeply"),
        ({"tasks": TASKS}, "TASKS_FILE must hold a JSON array of tasks"),
        ([], "a job submission needs at least one task"),
        (["a"], "task 0 must be a JSON object"),
        (
            [TASKS[0], {**TASKS[0], "task_key": "b", "maxattempts": 2}],
            "task 1 has unknown fields ['maxattempts']; "
            "allowed: ['max_attempts', 'payload', 'task_key']",
        ),
        ([{"payload": {}, "max_attempts": 1}], "task 0 is missing ['task_key']"),
        ([{"task_key": "a"}], "task 0 is missing ['payload']"),
        ([{"task_key": 1, "payload": {}, "max_attempts": 1}], "task 0: task_key must be a string"),
        (
            [{"task_key": "a", "payload": {}, "max_attempts": True}],
            "max_attempts must be an integer",
        ),
        (
            [{"task_key": "a", "payload": {}, "max_attempts": 3.0}],
            "max_attempts must be an integer",
        ),
        (
            [{"task_key": "a", "payload": {}, "max_attempts": "3"}],
            "max_attempts must be an integer",
        ),
        (
            [{"task_key": "a", "payload": {}}],
            "task 0 sets no max_attempts and --max-attempts was not given",
        ),
        (
            [TASKS[0], TASKS[0]],
            "task keys must be unique within a job; repeated: ['invoice-1']",
        ),
    ],
)
def test_job_submit_refuses_a_bad_file_before_reaching_the_database(
    job_env: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    content: object,
    error: str,
) -> None:
    Database(job_env, allowed=False)
    assert error in submit_refused(capsys, tasks_file(tmp_path, content))


def test_job_submit_never_echoes_the_file_content(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env, allowed=False)
    content = '[{"task_key": "a", "payload": {"card": "4111-secret"}, "max_attempts": 1} oops'
    error = submit_refused(capsys, tasks_file(tmp_path, content))
    assert error.startswith("TASKS_FILE is not valid JSON")
    assert "4111" not in error


def test_job_submit_refuses_a_missing_file(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env, allowed=False)
    error = submit_refused(capsys, str(tmp_path / "absent.json"))
    assert error == "cannot read TASKS_FILE: No such file or directory"


@pytest.mark.parametrize(("kind", "error"), [("  ", "a kind must not be blank"), ("a\x00", "NUL")])
def test_job_submit_refuses_an_invalid_kind(
    job_env: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    kind: str,
    error: str,
) -> None:
    Database(job_env, allowed=False)
    message = submit_refused(capsys, tasks_file(tmp_path, TASKS), "--max-attempts", "1", kind=kind)
    assert message.startswith("invalid --kind: ")
    assert error in message


@pytest.mark.parametrize("command", ALL_JOB_COMMANDS)
def test_job_commands_refuse_missing_or_invalid_settings(
    job_env: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: list[str],
) -> None:
    Database(job_env, allowed=False)
    argv = job_argv(tmp_path, command)
    job_env.setenv("ORQESTRA_LOG_LEVEL", "LOUD")
    assert job_output(capsys, argv, 1)["errors"]
    job_env.delenv("ORQESTRA_LOG_LEVEL")
    job_env.delenv("ORQESTRA_DATABASE_URL")
    assert job_output(capsys, argv, 1)["error"] == "ORQESTRA_DATABASE_URL is not set"
    job_env.setenv("ORQESTRA_DATABASE_URL", f"postgresql://u:{SECRET}%zz@127.0.0.1:1/orq")
    assert job_output(capsys, argv, 1)["error"] == (
        "ORQESTRA_DATABASE_URL is not a valid PostgreSQL connection string"
    )


@pytest.mark.parametrize("command", ALL_JOB_COMMANDS)
def test_job_commands_report_an_unreachable_database_without_secrets(
    job_env: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: list[str],
) -> None:
    argv = job_argv(tmp_path, command)
    # Nothing listens on port 1; the driver's own message would name the host.
    payload = job_output(capsys, argv, 1)
    assert payload["error"] == "the database request failed (OperationalError)"


def test_job_commands_refuse_a_schema_that_is_not_current(
    job_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env)

    def behind(conn: object) -> None:
        msg = "schema 'public' is behind; pending migrations: 0005_provider_usage"
        raise MigrationError(msg)

    job_env.setattr(cli, "verify_current", behind)
    assert job_output(capsys, ["job", "status", str(JOB_ID)], 1)["error"] == (
        "the database schema is not current: "
        "schema 'public' is behind; pending migrations: 0005_provider_usage"
    )


def test_job_submit_reports_an_idempotency_conflict(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env)

    def conflict(conn: object, submission: JobSubmission) -> SubmissionResult:
        raise IdempotencyConflictError("k-1", JOB_ID)

    job_env.setattr(cli, "submit_job", conflict)
    error = submit_refused(capsys, tasks_file(tmp_path, TASKS), "--max-attempts", "1")
    assert error == f"idempotency key is already used by job {JOB_ID} with different content"


def test_job_submit_reports_a_constraint_by_name_and_never_its_values(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env)

    class Violation(errors.CheckViolation):
        sqlstate = "23514"

        @property
        def diag(self) -> Any:
            return type("Diag", (), {"constraint_name": "tasks_max_attempts_range"})()

    def violate(conn: object, submission: JobSubmission) -> SubmissionResult:
        msg = "new row violates check constraint; Failing row contains (secret-value)"
        raise Violation(msg)

    job_env.setattr(cli, "submit_job", violate)
    error = submit_refused(capsys, tasks_file(tmp_path, TASKS), "--max-attempts", "101")
    assert error == (
        "the database request failed "
        "(Violation, SQLSTATE 23514, constraint tasks_max_attempts_range)"
    )


def test_job_submit_reports_other_database_errors_by_type_and_sqlstate(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env)

    def untranslatable(conn: object, submission: JobSubmission) -> SubmissionResult:
        msg = "unsupported Unicode escape sequence near secret-value"
        raise errors.UntranslatableCharacter(msg)

    job_env.setattr(cli, "submit_job", untranslatable)
    error = submit_refused(capsys, tasks_file(tmp_path, TASKS), "--max-attempts", "1")
    assert error == "the database request failed (UntranslatableCharacter, SQLSTATE 22P05)"
    assert "secret-value" not in error


def test_job_status_prints_the_derived_status_and_every_count(
    job_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = Database(job_env)
    counts = dict.fromkeys(TaskState, 0) | {TaskState.QUEUED: 1, TaskState.RUNNING: 2}
    db.progress = JobProgress(JOB_ID, JobStatus.RUNNING, MappingProxyType(counts))
    assert job_output(capsys, ["job", "status", str(JOB_ID)], 0) == {
        "status": "ok",
        "job_id": str(JOB_ID),
        "kind": "invoice_verification",
        "idempotency_key": "k-1",
        "created_at": "2026-11-06T16:00:00+00:00",
        "job_status": "running",
        "task_counts": {"queued": 1, "running": 2, "succeeded": 0, "failed": 0, "cancelled": 0},
    }
    assert db.submissions == []


def test_job_submit_accepts_valid_surrogate_pairs_and_large_finite_numbers(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = Database(job_env)
    # A surrogate pair is one character; only a lone surrogate is refused.
    content = (
        '[{"task_key": "\\ud83d\\ude00", "payload": {"\\u00e9": [1.7e308, -1e-400, 10]},'
        ' "max_attempts": 1}]'
    )
    job_output(capsys, ["job", "submit", "--kind", "k", tasks_file(tmp_path, content)], 0)
    (task,) = db.submissions[0].tasks
    assert task.task_key == "\N{GRINNING FACE}"
    assert task.payload == {"\u00e9": [1.7e308, -0.0, 10]}


def test_job_submit_checks_a_deeply_nested_payload_without_recursion(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env, allowed=False)
    depth = 500
    payload = "[" * depth + '"\\ud800"' + "]" * depth
    content = f'[{{"task_key": "a", "payload": {payload}, "max_attempts": 1}}]'
    assert submit_refused(capsys, tasks_file(tmp_path, content)) == (
        "task 0: payload holds a string or key that cannot be encoded as UTF-8"
    )


def test_job_submit_reports_a_payload_too_deep_to_encode_in_one_line(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # psycopg re-encodes the payload on a deeper stack than the parser used.
    Database(job_env)

    def too_deep(conn: object, submission: JobSubmission) -> SubmissionResult:
        msg = "maximum recursion depth exceeded while encoding a JSON object"
        raise RecursionError(msg)

    job_env.setattr(cli, "submit_job", too_deep)
    argv = ["job", "submit", "--kind", "k", "--max-attempts", "1", tasks_file(tmp_path, TASKS)]
    assert main(argv) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "status": "error",
        "error": "a value is nested too deeply to send to the database",
    }
    assert captured.err == ""


@pytest.mark.parametrize("option", ["--kind", "--idempotency-key"])
def test_job_submit_refuses_an_argument_that_is_not_utf8(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str], option: str
) -> None:
    Database(job_env, allowed=False)
    # A byte that is not UTF-8 on the command line, such as $'\xff', arrives as a surrogate.
    argv = ["job", "submit", "--kind", "k", "--max-attempts", "1", tasks_file(tmp_path, TASKS)]
    if option == "--kind":
        argv[3] = "k\udcff"
    else:
        argv[2:2] = [option, "key-\udcff"]
    payload = job_output(capsys, argv, 1)
    assert payload["error"] == f"{option} cannot be encoded as UTF-8"


def test_job_submit_maps_an_encoding_failure_at_the_database_to_an_error_line(
    job_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env)

    def unencodable(conn: object, submission: JobSubmission) -> SubmissionResult:
        raise UnicodeEncodeError("utf-8", "\ud800", 0, 1, "surrogates not allowed")

    job_env.setattr(cli, "submit_job", unencodable)
    error = submit_refused(capsys, tasks_file(tmp_path, TASKS), "--max-attempts", "1")
    assert error == "a value cannot be encoded as UTF-8 for the database"


@pytest.mark.parametrize(
    "argv",
    [["job", "status", str(JOB_ID)], ["migrate"], ["worker", "run", "--kind", "diagnostic.echo"]],
)
def test_a_database_url_that_is_not_utf8_is_reported_as_invalid(
    worker_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], argv: list[str]
) -> None:
    Started(worker_env, allowed=False)
    Database(worker_env, allowed=False)
    # A byte that is not UTF-8 in the environment arrives as a lone surrogate.
    worker_env.setenv("ORQESTRA_DATABASE_URL", f"postgresql://u:{SECRET}\udcff@127.0.0.1:1/orq")
    assert main(argv) == 1
    out = capsys.readouterr().out
    assert SECRET not in out
    assert json.loads(out) == {
        "status": "error",
        "error": "ORQESTRA_DATABASE_URL is not a valid PostgreSQL connection string",
    }


@pytest.mark.parametrize("command", ALL_JOB_COMMANDS)
@pytest.mark.parametrize("blank", BLANK)
def test_job_commands_treat_a_blank_database_url_as_unset(
    job_env: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: list[str],
    blank: str,
) -> None:
    Database(job_env, allowed=False)
    job_env.setenv("ORQESTRA_DATABASE_URL", blank)
    argv = job_argv(tmp_path, command)
    assert job_output(capsys, argv, 1)["error"] == "ORQESTRA_DATABASE_URL is not set"


def test_job_status_refuses_a_job_without_tasks(
    job_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = Database(job_env)
    db.progress = JobProgress(JOB_ID, JobStatus.QUEUED, MappingProxyType({}))  # the job exists

    def no_tasks(conn: object, job_id: uuid.UUID) -> NoReturn:
        msg = "a job has at least one task"  # as derive_job_status raises it
        raise ValueError(msg)

    job_env.setattr(cli, "get_job_progress", no_tasks)
    assert main(["job", "status", str(JOB_ID)]) == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    [line] = captured.out.splitlines()
    assert json.loads(line) == {"status": "error", "error": f"job {JOB_ID} has no tasks"}


def test_job_status_refuses_an_unknown_job(
    job_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    Database(job_env)
    assert job_output(capsys, ["job", "status", str(JOB_ID)], 1)["error"] == (
        f"job {JOB_ID} not found"
    )


# --- job summary / job usage (DEC-066) ------------------------------------------------------

INVOICE_JOB = Job(JOB_ID, "invoice_verification", "k-1", CREATED_AT)
OBSERVED_AT = datetime(2026, 11, 7, 9, 30, tzinfo=UTC)
TASK_COUNTS = dict.fromkeys(TaskState, 0) | {TaskState.SUCCEEDED: 2, TaskState.FAILED: 1}
SUMMARY = InvoiceJobSummary(
    job=INVOICE_JOB,
    observed_at=OBSERVED_AT,
    job_status=JobStatus.FAILED,
    task_counts=MappingProxyType(TASK_COUNTS),
    expected_items=9,
    verified=3,
    rejected=1,
    needs_review=1,
    malformed=0,
    provider_assisted_items=2,
    unprocessed_items=3,
    missing_items=0,
    invalid_items=1,
    anomalies=MappingProxyType(dict.fromkeys(AnomalyCode, 0) | {AnomalyCode.ITEM_INVALID: 1}),
    anomaly_samples=(Anomaly(AnomalyCode.ITEM_INVALID, "invoices-0002", 7),),
)
NO_UNITS: MappingProxyType[str, UnitTotal] = MappingProxyType({})
TOTALS = MappingProxyType(
    {"credits": UnitTotal(Decimal("1.75"), 2), "tiny": UnitTotal(Decimal("1E-7"), 1)}
)


def provenance(attempts: int = 0, calls: int = 0, **totals: UnitTotal) -> ProvenanceUsage:
    return ProvenanceUsage(attempts, calls, calls, 0, 0, MappingProxyType(totals))


REPORT = JobUsageReport(
    job=INVOICE_JOB,
    observed_at=OBSERVED_AT,
    job_status=JobStatus.SUCCEEDED,
    finality=UsageFinality.LOWER_BOUND,
    attempts_by_capture=MappingProxyType(
        dict.fromkeys(AttemptCapture, 0) | {AttemptCapture.CLOSED: 2, AttemptCapture.LOWER_BOUND: 1}
    ),
    unclosed_attempts=1,
    calls=4,
    answered=3,
    failed=0,
    unresolved=1,
    settled_without_reported_usage=0,
    reported_totals=TOTALS,
    by_attempt_state=MappingProxyType(
        {state: provenance() for state in AttemptState}
        | {AttemptState.SUCCEEDED: provenance(3, 3, **TOTALS)}
    ),
    late_intents=0,
    late_results=1,
    estimated_exposure_units=5,
    unresolved_without_estimate=0,
    assisted_items=2,
    assisted_checked_tasks=1,
    assisted_unchecked_tasks=1,
    anomalies=MappingProxyType(
        dict.fromkeys(UsageAnomalyCode, 0) | {UsageAnomalyCode.CLOSURE_CONTRADICTED: 1}
    ),
    anomaly_samples=(UsageAnomaly(UsageAnomalyCode.CLOSURE_CONTRADICTED, "invoices-0001", 2),),
)
EXPECTED_SUMMARY = {
    "status": "ok",
    "job_id": str(JOB_ID),
    "kind": "invoice_verification",
    "observed_at": "2026-11-07T09:30:00+00:00",
    "job_status": "failed",
    "task_counts": {"queued": 0, "running": 0, "succeeded": 2, "failed": 1, "cancelled": 0},
    "expected_items": 9,
    "processed_items": 5,
    "verified": 3,
    "rejected": 1,
    "needs_review": 1,
    "malformed": 0,
    "provider_assisted_items": 2,
    "unprocessed_items": 3,
    "missing_items": 0,
    "invalid_items": 1,
    "finished": True,
    "integrity_ok": False,
    "complete": False,
    "anomalies": {code.value: int(code is AnomalyCode.ITEM_INVALID) for code in AnomalyCode},
    "anomaly_samples": [{"code": "item_invalid", "task_key": "invoices-0002", "index": 7}],
}
EMPTY_STATE = {
    "attempts": 0,
    "calls": 0,
    "answered": 0,
    "failed": 0,
    "unresolved": 0,
    "reported_totals": {},
}
EXPECTED_TOTALS = {
    "credits": {"amount": "1.75", "calls": 2},
    "tiny": {"amount": "0.0000001", "calls": 1},
}
EXPECTED_USAGE = {
    "status": "ok",
    "job_id": str(JOB_ID),
    "kind": "invoice_verification",
    "observed_at": "2026-11-07T09:30:00+00:00",
    "job_status": "succeeded",
    "finished": True,
    "finality": "lower_bound",
    "attempts_by_capture": {"open": 0, "closed": 2, "lower_bound": 1, "not_captured": 0},
    "unclosed_attempts": 1,
    "calls": 4,
    "answered": 3,
    "failed": 0,
    "unresolved": 1,
    "settled_without_reported_usage": 0,
    "reported_totals": EXPECTED_TOTALS,
    "by_attempt_state": {state.value: EMPTY_STATE for state in AttemptState}
    | {
        "succeeded": EMPTY_STATE
        | {"attempts": 3, "calls": 3, "answered": 3, "reported_totals": EXPECTED_TOTALS}
    },
    "late_intents": 0,
    "late_results": 1,
    "estimated_exposure_units": 5,
    "unresolved_without_estimate": 0,
    "assisted_items": 2,
    "assisted_checked_tasks": 1,
    "assisted_unchecked_tasks": 1,
    "anomalies": {"closure_contradicted": 1, "assisted_exceeds_answered": 0},
    "anomaly_samples": [
        {"code": "closure_contradicted", "task_key": "invoices-0001", "attempt_number": 2}
    ],
}


class Reports:
    """Stands in for the summary, usage and budget reads; each must follow the schema check."""

    def __init__(self, db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
        self.db = db
        self.found = True
        self.calls: list[tuple[str, uuid.UUID, int | None]] = []
        monkeypatch.setattr(cli, "summarize_invoice_job", self._summary)
        monkeypatch.setattr(cli, "report_job_usage", self._usage)
        monkeypatch.setattr(cli, "read_job_call_budget", self._budget)

    def _read(self, name: str, job_id: uuid.UUID, cap: int | None = None) -> None:
        assert self.db.verified == 1, "read before the schema check"
        self.calls.append((name, job_id, cap))

    def _summary(self, conn: object, job_id: uuid.UUID) -> InvoiceJobSummary | None:
        self._read("summary", job_id)
        return SUMMARY if self.found else None

    def _usage(self, conn: object, job_id: uuid.UUID) -> JobUsageReport | None:
        self._read("usage", job_id)
        return REPORT if self.found else None

    def _budget(
        self, conn: object, job_id: uuid.UUID, *, cap: int
    ) -> tuple[JobUsageReport, CallBudgetCheck] | None:
        self._read("budget", job_id, cap)
        if not self.found:
            return None
        return REPORT, check_call_budget(REPORT, max_attempts_total=3, cap=cap)


@pytest.fixture
def reports(job_env: pytest.MonkeyPatch) -> Reports:
    return Reports(Database(job_env), job_env)


def test_job_summary_prints_every_count_of_summarize_invoice_job(
    reports: Reports, capsys: pytest.CaptureFixture[str]
) -> None:
    assert job_output(capsys, ["job", "summary", str(JOB_ID)], 0) == EXPECTED_SUMMARY
    assert reports.calls == [("summary", JOB_ID, None)]


def test_the_summary_document_holds_every_field_and_property() -> None:
    fields = {field.name for field in dataclasses.fields(InvoiceJobSummary)}
    properties = {"processed_items", "finished", "integrity_ok", "complete"}
    expected = (fields - {"job"}) | properties | {"job_id", "kind"}
    assert set(cli.summary_document(SUMMARY)) == expected


def test_job_usage_prints_every_figure_of_report_job_usage(
    reports: Reports, capsys: pytest.CaptureFixture[str]
) -> None:
    assert job_output(capsys, ["job", "usage", str(JOB_ID)], 0) == EXPECTED_USAGE
    assert reports.calls == [("usage", JOB_ID, None)]


def test_the_usage_document_holds_every_field() -> None:
    fields = {field.name for field in dataclasses.fields(JobUsageReport)}
    expected = (fields - {"job"}) | {"finished", "job_id", "kind"}
    assert set(cli.usage_document(REPORT)) == expected
    provenance_fields = {field.name for field in dataclasses.fields(ProvenanceUsage)}
    for state in cli.usage_document(REPORT)["by_attempt_state"].values():  # type: ignore[attr-defined]
        assert set(state) == provenance_fields


@pytest.mark.parametrize(
    ("cap", "budget"),
    [
        pytest.param(
            "1",
            {"bound": 3, "verdict": "exceeded", "unknown_reason": None},
            id="exceeded",
        ),
        pytest.param(
            "2",
            {"bound": 6, "verdict": "unknown", "unknown_reason": "lower_bound"},
            id="unknown",
        ),
    ],
)
def test_job_usage_adds_the_call_budget_check_from_one_snapshot(
    reports: Reports, capsys: pytest.CaptureFixture[str], cap: str, budget: dict[str, Any]
) -> None:
    argv = ["job", "usage", str(JOB_ID), "--max-provider-calls-per-attempt", cap]
    out = job_output(capsys, argv, 0)
    assert out == EXPECTED_USAGE | {
        "budget": {
            "cap": int(cap),
            "cap_source": "operator",
            "max_attempts_total": 3,
            "calls": 4,
            "finality": "lower_bound",
        }
        | budget
    }
    # The report and the check come from read_job_call_budget alone, never a second read.
    assert reports.calls == [("budget", JOB_ID, int(cap))]


def test_a_within_verdict_has_no_unknown_reason() -> None:
    report = dataclasses.replace(REPORT, finality=UsageFinality.CLOSED)
    check = check_call_budget(report, max_attempts_total=3, cap=2)
    assert cli.budget_document(check) == {
        "cap": 2,
        "cap_source": "operator",
        "max_attempts_total": 3,
        "bound": 6,
        "calls": 4,
        "finality": "closed",
        "verdict": "within",
        "unknown_reason": None,
    }


@pytest.mark.parametrize(
    ("cap", "error"),
    [
        ("-7", "--max-provider-calls-per-attempt must be between 0 and"),
        (str(MAX_CALL_SEQ + 1), "--max-provider-calls-per-attempt must be between 0 and"),
    ],
)
def test_job_usage_refuses_an_invalid_cap_before_reaching_the_database(
    job_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], cap: str, error: str
) -> None:
    Database(job_env, allowed=False)
    argv = ["job", "usage", str(JOB_ID), "--max-provider-calls-per-attempt", cap]
    message = job_output(capsys, argv, 1)["error"]
    assert message.startswith(error)
    assert cap not in message


@pytest.mark.parametrize("cap", [0, MAX_CALL_SEQ])
def test_job_usage_accepts_a_cap_of_zero_and_the_largest_cap(
    reports: Reports, capsys: pytest.CaptureFixture[str], cap: int
) -> None:
    argv = ["job", "usage", str(JOB_ID), "--max-provider-calls-per-attempt", str(cap)]
    assert job_output(capsys, argv, 0)["budget"]["cap"] == cap
    assert reports.calls == [("budget", JOB_ID, cap)]


READ_COMMANDS = ALL_JOB_COMMANDS[2:]


@pytest.mark.parametrize("command", READ_COMMANDS)
def test_summary_and_usage_refuse_an_unknown_job(
    reports: Reports, capsys: pytest.CaptureFixture[str], command: list[str]
) -> None:
    reports.found = False
    assert job_output(capsys, ["job", *command], 1)["error"] == f"job {JOB_ID} not found"


@pytest.mark.parametrize("command", READ_COMMANDS)
def test_summary_and_usage_refuse_another_kind_without_naming_it(
    reports: Reports,
    job_env: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: list[str],
) -> None:
    def wrong_kind(conn: object, job_id: uuid.UUID, **kwargs: object) -> None:
        raise WrongJobKindError(job_id, "secret-kind")

    for name in ("summarize_invoice_job", "report_job_usage", "read_job_call_budget"):
        job_env.setattr(cli, name, wrong_kind)
    out = job_output(capsys, ["job", *command], 1)
    assert out["error"] == f"job {JOB_ID} is not an invoice_verification job"
    assert "secret-kind" not in json.dumps(out)


@pytest.mark.parametrize("command", READ_COMMANDS)
def test_summary_and_usage_refuse_a_job_without_tasks(
    reports: Reports,
    job_env: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: list[str],
) -> None:
    def no_tasks(conn: object, job_id: uuid.UUID, **kwargs: object) -> None:
        msg = f"job {job_id} has no tasks to summarize"
        raise ValueError(msg)

    for name in ("summarize_invoice_job", "report_job_usage", "read_job_call_budget"):
        job_env.setattr(cli, name, no_tasks)
    assert job_output(capsys, ["job", *command], 1)["error"] == f"job {JOB_ID} has no tasks"


@pytest.mark.parametrize("command", READ_COMMANDS)
def test_summary_and_usage_refuse_a_schema_that_is_not_current(
    reports: Reports,
    job_env: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: list[str],
) -> None:
    def behind(conn: object) -> None:
        msg = "schema 'public' is behind; pending migrations: 0005_provider_usage"
        raise MigrationError(msg)

    job_env.setattr(cli, "verify_current", behind)
    assert job_output(capsys, ["job", *command], 1)["error"] == (
        "the database schema is not current: "
        "schema 'public' is behind; pending migrations: 0005_provider_usage"
    )
    assert reports.calls == []


@pytest.mark.parametrize("command", READ_COMMANDS)
def test_summary_and_usage_report_database_errors_by_type_and_sqlstate(
    reports: Reports,
    job_env: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: list[str],
) -> None:
    def broken(conn: object, job_id: uuid.UUID, **kwargs: object) -> None:
        msg = 'relation "usage_closures" does not exist near secret-value'
        raise errors.UndefinedTable(msg)

    for name in ("summarize_invoice_job", "report_job_usage", "read_job_call_budget"):
        job_env.setattr(cli, name, broken)
    error = job_output(capsys, ["job", *command], 1)["error"]
    assert error == "the database request failed (UndefinedTable, SQLSTATE 42P01)"
