"""Every timestamp the CLI prints is in UTC, whatever the session's TimeZone.

The CLI runs in-process through ``main`` against a test schema, its database URL set to a
session TimeZone of Europe/Paris (an hour or two off UTC all year), so a timestamp that
followed the session would show a non-zero offset.
"""

import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import TupleRow

from orqestra.cli import main
from tests.integration.test_invoice_summary_pg import submit_items
from tests.unit.test_invoice_verify import known

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
ZONE = "Europe/Paris"


def in_zone(schema_url: str, zone: str) -> str:
    """``schema_url`` (with its search_path option) with the session TimeZone ``zone``."""
    existing = conninfo_to_dict(schema_url).get("options")
    zone_option = f"-c TimeZone={zone}"
    return make_conninfo(
        schema_url, options=f"{existing} {zone_option}" if existing else zone_option
    )


@pytest.fixture
def paris(monkeypatch: pytest.MonkeyPatch, schema_url: str, db: Conn) -> Conn:
    """The CLI's database is the migrated test schema, in a Paris session."""
    url = in_zone(schema_url, ZONE)
    with psycopg.connect(url) as session:
        zone = session.execute("SHOW TimeZone").fetchone()
        now = session.execute("SELECT now()").fetchone()
    assert zone == (ZONE,)
    assert now is not None
    # The session really prints a non-UTC offset, so a passing test means the CLI converts.
    assert now[0].utcoffset() in {timedelta(hours=1), timedelta(hours=2)}
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", url)
    return db


def run(capsys: pytest.CaptureFixture[str], *argv: str) -> dict[str, Any]:
    assert main(list(argv)) == 0
    payload: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    return payload


def stored_created_at(db: Conn, job_id: uuid.UUID) -> datetime:
    row = db.execute("SELECT created_at FROM jobs WHERE id = %s", (job_id,)).fetchone()
    assert row is not None
    created: datetime = row[0]
    return created


def test_submit_and_status_print_the_same_utc_created_at_in_a_paris_session(
    paris: Conn, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "tasks.json"
    path.write_text(json.dumps([{"task_key": "a", "payload": "x"}]), "utf-8")
    submitted = run(capsys, "job", "submit", "--kind", "cli.zone", "--max-attempts", "3", str(path))
    job_id = uuid.UUID(submitted["job_id"])
    status = run(capsys, "job", "status", str(job_id))

    assert status["created_at"] == submitted["created_at"]
    assert submitted["created_at"].endswith("+00:00")
    expected = stored_created_at(paris, job_id).astimezone(UTC).isoformat()
    assert submitted["created_at"] == expected


@pytest.mark.parametrize("command", ["summary", "usage"])
def test_summary_and_usage_print_observed_at_in_utc_in_a_paris_session(
    paris: Conn, capsys: pytest.CaptureFixture[str], command: str
) -> None:
    job = submit_items(paris, [known()], batch_size=1)
    before = datetime.now(UTC)
    out = run(capsys, "job", command, str(job.id))
    after = datetime.now(UTC)

    observed = datetime.fromisoformat(out["observed_at"])
    assert out["observed_at"].endswith("+00:00")
    assert observed.utcoffset() == timedelta(0)
    # The same instant as the clock around the call, not shifted by the session's offset.
    assert before - timedelta(minutes=5) <= observed <= after + timedelta(minutes=5)


def test_a_utc_session_prints_the_same_strings(
    db: Conn,
    schema_url: str,
    monkeypatch: pytest.MonkeyPatch,
    write_tasks: Callable[[object], str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", in_zone(schema_url, "UTC"))
    submitted = run(
        capsys,
        "job",
        "submit",
        "--kind",
        "cli.zone",
        "--max-attempts",
        "3",
        write_tasks([{"task_key": "a", "payload": 1}]),
    )
    status = run(capsys, "job", "status", submitted["job_id"])
    assert status["created_at"] == submitted["created_at"]
    assert submitted["created_at"].endswith("+00:00")


@pytest.fixture
def write_tasks(tmp_path: Path) -> Callable[[object], str]:
    def write(content: object) -> str:
        path = tmp_path / f"{uuid.uuid4().hex}.json"
        path.write_text(json.dumps(content), "utf-8")
        return str(path)

    return write
