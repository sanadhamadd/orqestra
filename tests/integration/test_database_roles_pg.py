"""The least-privilege roles of docs/database-roles.md work, and refuse what they should.

The SQL is read from the document itself, so the page and the test cannot drift apart.
Both roles get unique names, and so does their schema: roles are global to the server,
which other test runs share. Everything is dropped in ``finally``, whatever step failed.

As the owner role, ``orqestra migrate`` applies the schema. As the application role, the
real CLI submits a job, a real ``orqestra worker run`` completes it, and ``job status``
reads it back. The application role is then refused every change to the schema, the
safety triggers and the server's files.

Unlike the other integration tests, this one skips when the connecting role cannot create
roles: creating roles is a server-wide right a test database need not grant.
"""

import json
import os
import re
import secrets
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import errors
from psycopg.conninfo import make_conninfo

from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
DOC = ROOT / "docs" / "database-roles.md"
WAIT = 60.0


def sql_block(name: str) -> str:
    """The SQL between the document's ``BEGIN SQL: name`` and ``END SQL: name`` markers."""
    text = DOC.read_text(encoding="utf-8")
    match = re.search(
        rf"<!-- BEGIN SQL: {re.escape(name)} -->\s*```sql\n(.*?)```\s*"
        rf"<!-- END SQL: {re.escape(name)} -->",
        text,
        re.DOTALL,
    )
    assert match is not None, f"docs/database-roles.md has no SQL block {name!r}"
    return match.group(1)


def renamed(sql: str, owner: str, app: str, schema: str) -> str:
    """The document's SQL with its role and schema names replaced, whole words only."""
    sql = re.sub(r"\borqestra_owner\b", owner, sql)
    sql = re.sub(r"\borqestra_app\b", app, sql)
    return re.sub(r"\borqestra\b", schema, sql)


def as_role(database_url: str, role: str) -> str:
    """``database_url`` connecting as ``role``, with the role's own search_path."""
    return make_conninfo(database_url, user=role, options="")


def cli(url: str, *args: str, stdin: str | None = None) -> dict[str, Any]:
    """Run ``orqestra`` once as the role in ``url``; its JSON answer (exit 0 required)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("ORQESTRA_")}
    env["ORQESTRA_DATABASE_URL"] = url
    done = subprocess.run(  # a fixed command
        [sys.executable, "-m", "orqestra.cli", *args],
        cwd=ROOT,
        env=env,
        input=stdin,
        capture_output=True,
        text=True,
        timeout=WAIT,
        check=False,
    )
    assert done.returncode == 0, (done.returncode, done.stdout[-500:], done.stderr[-2000:])
    answer: dict[str, Any] = json.loads(done.stdout.strip().splitlines()[-1])
    return answer


@pytest.fixture
def roles(database_url: str) -> Iterator[tuple[str, str, str]]:
    """Unique owner and application roles and a schema, created from the document's first
    block; everything is dropped afterwards, even if a step failed partway."""
    with psycopg.connect(database_url, autocommit=True) as admin:
        row = admin.execute(
            "SELECT rolsuper OR rolcreaterole FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        if not (row and row[0]):
            pytest.skip("the test database role cannot create roles (CREATEROLE)")
        suffix = secrets.token_hex(4)
        owner, app, schema = f"orq_owner_{suffix}", f"orq_app_{suffix}", f"orq_roles_{suffix}"
        try:
            admin.execute(renamed(sql_block("before migrate"), owner, app, schema))
            yield owner, app, schema
        finally:
            for role in (app, owner):
                admin.execute(
                    "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity "
                    "WHERE usename = %s",
                    (role,),
                )
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            for role in (app, owner):
                exists = admin.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)
                ).fetchone()
                if exists:
                    admin.execute(f'DROP OWNED BY "{role}"')
                    admin.execute(f'DROP ROLE "{role}"')


def refused(conn: psycopg.Connection[Any], statement: str) -> None:
    with pytest.raises(errors.InsufficientPrivilege):
        conn.execute(statement)


def test_the_documented_roles_run_orqestra_and_refuse_everything_else(
    database_url: str, roles: tuple[str, str, str], tmp_path: Path
) -> None:
    owner, app, schema = roles
    owner_url, app_url = as_role(database_url, owner), as_role(database_url, app)

    # The owner migrates, then grants exactly what the document grants.
    assert cli(owner_url, "migrate")["status"] == "ok"
    with psycopg.connect(owner_url, autocommit=True) as conn:
        conn.execute(renamed(sql_block("after migrate"), owner, app, schema))

    # The application role submits a job, runs a worker to completion and reads it back.
    tasks = tmp_path / "tasks.json"
    tasks.write_text(
        json.dumps([{"task_key": f"t{i}", "payload": {"n": i}, "max_attempts": 2} for i in (1, 2)]),
        encoding="utf-8",
    )
    job_id = cli(app_url, "job", "submit", "--kind", "diagnostic.echo", str(tasks))["job_id"]
    env = {k: v for k, v in os.environ.items() if not k.startswith("ORQESTRA_")}
    env |= ENVIRONMENT | {"ORQESTRA_DATABASE_URL": app_url}
    worker = subprocess.Popen(  # a fixed command
        [sys.executable, "-m", "orqestra.cli", "worker", "run", "--kind", "diagnostic.echo"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + WAIT
        status: dict[str, Any] = {}
        while time.monotonic() < deadline:
            status = cli(app_url, "job", "status", job_id)
            if status.get("job_status") == "succeeded":
                break
            assert worker.poll() is None, worker.communicate(timeout=5)
            time.sleep(0.2)
        assert status.get("job_status") == "succeeded", status
    finally:
        if worker.poll() is None:
            worker.send_signal(signal.SIGTERM)
        _, stderr = worker.communicate(timeout=WAIT)
    assert worker.returncode == 0, stderr[-2000:]

    # And it is refused every change to the schema, the safety triggers and the server.
    with psycopg.connect(app_url, autocommit=True) as conn:
        flags = conn.execute(
            "SELECT rolsuper, rolcreaterole, rolcreatedb, "
            "pg_has_role(current_user, 'pg_read_server_files', 'member'), "
            "pg_has_role(current_user, 'pg_signal_backend', 'member') "
            "FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        assert flags == (False, False, False, False, False)
        refused(conn, "ALTER TABLE tasks ADD COLUMN extra integer")
        refused(conn, "ALTER TABLE tasks DISABLE TRIGGER tasks_attempt_count_fencing")
        refused(conn, "DROP TABLE tasks")
        refused(conn, "CREATE TABLE extra (i integer)")
        refused(conn, "DELETE FROM tasks")
        refused(conn, "SELECT pg_read_file('postgresql.conf')")
        refused(conn, "COPY tasks FROM '/etc/hosts'")
