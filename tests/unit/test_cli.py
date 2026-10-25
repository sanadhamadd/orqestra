import json

import pytest

import orqestra
from orqestra.cli import main

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
