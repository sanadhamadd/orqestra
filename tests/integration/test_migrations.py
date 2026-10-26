import json
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.cli import main
from orqestra.persistence.migrate import (
    LOCK_KEY,
    Migration,
    MigrationError,
    load_migrations,
    migrate,
)
from tests.integration.postgres import connect_verified, with_search_path

pytestmark = pytest.mark.integration


def _write(directory: Path, files: dict[str, str]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (directory / name).write_text(content, encoding="utf-8")
    return directory


def _history(url: str) -> list[tuple[int, str, str]]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            "SELECT version, name, checksum FROM schema_migrations ORDER BY version"
        ).fetchall()
    return [(int(v), str(n), str(c)) for v, n, c in rows]


def _tables(url: str) -> set[str]:
    with psycopg.connect(url) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()"
        ).fetchall()
    return {str(name) for (name,) in rows}


def test_applies_packaged_schema_to_an_empty_schema(schema_url: str) -> None:
    assert _tables(schema_url) == set()
    applied = migrate(schema_url)
    assert [m.label for m in applied] == [
        "0001_initial_schema",
        "0002_attempt_leases",
        "0003_cancellation_grace",
    ]
    assert _tables(schema_url) == {"schema_migrations", "jobs", "tasks", "attempts"}
    initial, leases, grace = load_migrations()
    assert _history(schema_url) == [
        (1, "initial_schema", initial.checksum),
        (2, "attempt_leases", leases.checksum),
        (3, "cancellation_grace", grace.checksum),
    ]


def test_refuses_to_run_without_a_current_schema(database_url: str) -> None:
    missing = with_search_path(database_url, "orq_schema_that_does_not_exist")
    with pytest.raises(MigrationError, match="no schema is selected"):
        migrate(missing)


def test_rerunning_is_a_no_op(schema_url: str) -> None:
    migrate(schema_url)
    with psycopg.connect(schema_url) as conn:
        before = conn.execute("SELECT version, applied_at FROM schema_migrations").fetchall()
    assert migrate(schema_url) == ()
    with psycopg.connect(schema_url) as conn:
        after = conn.execute("SELECT version, applied_at FROM schema_migrations").fetchall()
    assert before == after


def test_applies_only_pending_migrations(schema_url: str, tmp_path: Path) -> None:
    first = _write(tmp_path / "v1", {"0001_a.sql": "CREATE TABLE a (id int);"})
    second = _write(
        tmp_path / "v2",
        {"0001_a.sql": "CREATE TABLE a (id int);", "0002_b.sql": "CREATE TABLE b (id int);"},
    )
    assert [m.label for m in migrate(schema_url, load_migrations(first))] == ["0001_a"]
    assert [m.label for m in migrate(schema_url, load_migrations(second))] == ["0002_b"]
    assert _tables(schema_url) == {"schema_migrations", "a", "b"}


def test_rejects_a_modified_applied_migration(schema_url: str, tmp_path: Path) -> None:
    original = _write(tmp_path / "v1", {"0001_a.sql": "CREATE TABLE a (id int);"})
    edited = _write(
        tmp_path / "v2",
        {
            "0001_a.sql": "CREATE TABLE a (id bigint);",
            "0002_b.sql": "CREATE TABLE b (id int);",
        },
    )
    migrate(schema_url, load_migrations(original))
    with pytest.raises(MigrationError, match="0001_a was modified after it was applied"):
        migrate(schema_url, load_migrations(edited))
    assert _tables(schema_url) == {"schema_migrations", "a"}
    assert [v for v, _, _ in _history(schema_url)] == [1]


def test_rejects_migrations_unknown_to_this_code(schema_url: str, tmp_path: Path) -> None:
    newer = _write(
        tmp_path / "newer",
        {"0001_a.sql": "CREATE TABLE a (id int);", "0002_b.sql": "CREATE TABLE b (id int);"},
    )
    older = _write(tmp_path / "older", {"0001_a.sql": "CREATE TABLE a (id int);"})
    migrate(schema_url, load_migrations(newer))
    with pytest.raises(MigrationError, match="does not know: 0002_b"):
        migrate(schema_url, load_migrations(older))


def test_rejects_a_renamed_applied_migration(schema_url: str, tmp_path: Path) -> None:
    migrate(schema_url, load_migrations(_write(tmp_path / "a", {"0001_a.sql": "SELECT 1;"})))
    renamed = load_migrations(_write(tmp_path / "b", {"0001_renamed.sql": "SELECT 1;"}))
    with pytest.raises(MigrationError, match="history mismatch"):
        migrate(schema_url, renamed)


def test_a_failed_migration_rolls_back_completely_and_can_be_fixed(
    schema_url: str, tmp_path: Path
) -> None:
    good = "CREATE TABLE a (id int);"
    broken = _write(
        tmp_path / "broken",
        {"0001_a.sql": good, "0002_b.sql": "CREATE TABLE b (id int);\nSELECT * FROM missing;"},
    )
    with pytest.raises(MigrationError, match=r"0002_b failed and was rolled back: \[42P01\]") as e:
        migrate(schema_url, load_migrations(broken))
    assert e.value.sqlstate == "42P01"
    # 0001 committed in its own transaction; every effect of 0002 rolled back.
    assert _tables(schema_url) == {"schema_migrations", "a"}
    assert [v for v, _, _ in _history(schema_url)] == [1]

    fixed = _write(
        tmp_path / "fixed", {"0001_a.sql": good, "0002_b.sql": "CREATE TABLE b (id int);"}
    )
    assert [m.label for m in migrate(schema_url, load_migrations(fixed))] == ["0002_b"]
    assert _tables(schema_url) == {"schema_migrations", "a", "b"}


TRANSACTION_CONTROL = [
    ("COMMIT", "0A000"),
    ("rollback", "0A000"),
    ("/* hidden */ begin", "0A000"),
    ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "0A000"),
    ("END", "0A000"),
    ("SAVEPOINT inner_savepoint", "0A000"),
    ("PREPARE TRANSACTION 'orqestra'", "0A000"),
    ("DO $$ BEGIN COMMIT; END $$", "2D000"),
    (
        "CREATE PROCEDURE commit_inside() LANGUAGE plpgsql AS $$ BEGIN COMMIT; END $$;\n"
        "CALL commit_inside()",
        "2D000",
    ),
]


@pytest.mark.parametrize(("statement", "sqlstate"), TRANSACTION_CONTROL)
def test_transaction_control_cannot_break_atomicity(
    schema_url: str, tmp_path: Path, statement: str, sqlstate: str
) -> None:
    malicious = (
        f"CREATE TABLE before_control (id int);\n{statement};\nCREATE TABLE after_control (id int);"
    )
    directory = _write(
        tmp_path, {"0001_a.sql": "CREATE TABLE a (id int);", "0002_control.sql": malicious}
    )
    with pytest.raises(MigrationError) as e:
        migrate(schema_url, load_migrations(directory))
    assert e.value.sqlstate == sqlstate
    # Nothing from the offending migration survived, before or after the
    # control statement, and its history row was never written.
    assert _tables(schema_url) == {"schema_migrations", "a"}
    assert [v for v, _, _ in _history(schema_url)] == [1]


@pytest.mark.parametrize(
    "statement",
    ["CREATE INDEX CONCURRENTLY t_id_idx ON t (id)", "VACUUM t"],
)
def test_non_transactional_statements_are_rejected(
    schema_url: str, tmp_path: Path, statement: str
) -> None:
    directory = _write(tmp_path, {"0001_t.sql": f"CREATE TABLE t (id int);\n{statement};"})
    with pytest.raises(MigrationError) as e:
        migrate(schema_url, load_migrations(directory))
    assert e.value.sqlstate == "25001"
    # The first migration failed, so even the history table created in the same
    # transaction was rolled back: the schema is left exactly as it was.
    assert _tables(schema_url) == set()


def test_plpgsql_blocks_and_dependent_statements_are_accepted(
    schema_url: str, tmp_path: Path
) -> None:
    sql = """
    CREATE TABLE t (id int);
    CREATE INDEX t_id_idx ON t (id);
    INSERT INTO t VALUES (1);
    CREATE FUNCTION one() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$;
    DO $$ BEGIN IF one() <> 1 THEN RAISE EXCEPTION 'unexpected'; END IF; END $$;
    """
    migrate(schema_url, load_migrations(_write(tmp_path, {"0001_t.sql": sql})))
    with psycopg.connect(schema_url) as conn:
        assert conn.execute("SELECT count(*), one() FROM t").fetchone() == (1, 1)


def _advisory_waiters(monitor: psycopg.Connection[TupleRow]) -> int:
    row = monitor.execute(
        "SELECT count(*) FROM pg_locks "
        "WHERE locktype = 'advisory' AND NOT granted "
        "AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
        "AND classid = %s AND objid = %s AND objsubid = 1",
        ((LOCK_KEY >> 32) & 0xFFFFFFFF, LOCK_KEY & 0xFFFFFFFF),
    ).fetchone()
    return int(row[0]) if row else 0


def test_concurrent_runners_serialise_on_the_advisory_lock(
    schema_url: str, database_url: str, tmp_path: Path
) -> None:
    directory = _write(
        tmp_path,
        {
            "0001_slow.sql": "CREATE TABLE slow (id int);\nSELECT pg_sleep(0.5);",
            "0002_second.sql": "CREATE TABLE second (id int);",
            "0003_third.sql": "CREATE TABLE third (id int);",
        },
    )
    migrations = load_migrations(directory)
    runners = 4
    barrier = threading.Barrier(runners)

    def run() -> list[str]:
        barrier.wait()
        return [m.label for m in migrate(schema_url, migrations)]

    max_waiters = 0
    with connect_verified(database_url) as monitor, ThreadPoolExecutor(runners) as pool:
        futures = [pool.submit(run) for _ in range(runners)]
        while not all(future.done() for future in futures):
            max_waiters = max(max_waiters, _advisory_waiters(monitor))
            time.sleep(0.01)
        results = [future.result() for future in futures]

    # The runners really contended: while the slow migration held the lock,
    # the others were observed waiting for exactly this advisory lock.
    assert max_waiters >= 1
    applied = sorted(label for result in results for label in result)
    assert applied == ["0001_slow", "0002_second", "0003_third"]
    assert [v for v, _, _ in _history(schema_url)] == [1, 2, 3]


def _race(url: str, migrations: tuple[Migration, ...], runners: int) -> list[list[str]]:
    """Start ``runners`` migrate calls at the same instant; return what each applied."""
    barrier = threading.Barrier(runners)

    def run() -> list[str]:
        barrier.wait()
        return [m.label for m in migrate(url, migrations)]

    with ThreadPoolExecutor(runners) as pool:
        futures = [pool.submit(run) for _ in range(runners)]
        return [future.result() for future in futures]


def test_many_concurrent_runners_apply_each_migration_exactly_once(
    schema_factory: Callable[[], str],
) -> None:
    migrations = load_migrations()
    for _ in range(10):
        url = schema_factory()
        results = _race(url, migrations, runners=8)
        assert sorted(label for result in results for label in result) == [
            "0001_initial_schema",
            "0002_attempt_leases",
            "0003_cancellation_grace",
        ]
        assert len(_history(url)) == 3


def test_cli_migrate_applies_the_schema(
    schema_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", schema_url)
    assert main(["migrate"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "ok",
        "applied": ["0001_initial_schema", "0002_attempt_leases", "0003_cancellation_grace"],
    }
    assert main(["migrate"]) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "ok", "applied": []}


def test_cli_migrate_reports_connection_failures_without_the_password(
    schema_url: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    secret = "s3cret-not-the-real-one"
    monkeypatch.setenv(
        "ORQESTRA_DATABASE_URL", f"{schema_url} password={secret} user=orqestra_no_such_role"
    )
    assert main(["migrate"]) == 1
    out = capsys.readouterr().out
    assert secret not in out
    assert json.loads(out)["status"] == "error"
