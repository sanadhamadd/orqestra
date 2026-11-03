import json
from datetime import timedelta
from typing import Any

import pytest

import orqestra
from orqestra import cli
from orqestra.cli import main
from orqestra.persistence.migrate import MigrationError
from orqestra.worker.runtime import connect_supervisor

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
    payload = json.loads(out)
    assert payload["status"] == "error"
    assert "connection" in payload["error"]


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
