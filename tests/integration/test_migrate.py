"""The migration lock under a session default isolation other than READ COMMITTED.

Under REPEATABLE READ or SERIALIZABLE a transaction's snapshot is taken by its
first statement that needs one. The runner's history read must still see every
migration committed while it waited for its turn, so its first lock statement
must take no snapshot. Every wait here is observed in ``pg_locks`` (runners are
named by ``application_name``); nothing sleeps for a fixed time.
"""

import secrets
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import TupleRow

from orqestra.persistence.migrate import Migration, MigrationError, load_migrations, migrate

pytestmark = pytest.mark.integration

ISOLATIONS = pytest.mark.parametrize(
    "isolation", ["repeatable\\ read", "serializable"], ids=["repeatable-read", "serializable"]
)


def _write(directory: Path, files: dict[str, str]) -> tuple[Migration, ...]:
    directory.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        (directory / name).write_text(content, encoding="utf-8")
    return load_migrations(directory)


def _runner(url: str, name: str, isolation: str | None = None) -> str:
    """``url`` for a runner named ``name``, with a session default isolation if given."""
    options = conninfo_to_dict(url)["options"]
    if isolation is not None:
        options = f"{options} -c default_transaction_isolation={isolation}"
    return make_conninfo(url, options=options, application_name=name)


def _await_waiting(monitor: psycopg.Connection[TupleRow], name: str) -> None:
    """Wait until the backend named ``name`` is blocked on some lock."""
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        row = monitor.execute(
            "SELECT EXISTS (SELECT FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid "
            "WHERE a.application_name = %s AND NOT l.granted)",
            (name,),
        ).fetchone()
        if row is not None and row[0]:
            return
        time.sleep(0.01)
    pytest.fail(f"{name} never waited for a lock")


def _versions(url: str) -> list[int]:
    with psycopg.connect(url) as conn:
        rows = conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    return [int(version) for (version,) in rows]


def _labels(applied: tuple[Migration, ...]) -> list[str]:
    return [migration.label for migration in applied]


@ISOLATIONS
def test_a_runner_that_waited_sees_the_history_committed_meanwhile(
    schema_url: str, tmp_path: Path, isolation: str
) -> None:
    pause = secrets.randbits(62)  # runner A stops inside 0002 until the test releases this
    migrations = _write(
        tmp_path,
        {
            "0001_first.sql": "CREATE TABLE first (id int);",
            "0002_second.sql": (
                f"SELECT pg_advisory_xact_lock({pause});\nCREATE TABLE second (id int);"
            ),
        },
    )
    migrate(schema_url, migrations[:1])
    with (
        psycopg.connect(schema_url, autocommit=True) as monitor,
        ThreadPoolExecutor(2) as pool,
    ):
        monitor.execute("SELECT pg_advisory_lock(%s)", (pause,))
        try:
            first = pool.submit(migrate, _runner(schema_url, "runner-a"), migrations)
            _await_waiting(monitor, "runner-a")  # A holds the migration locks, inside 0002
            second = pool.submit(migrate, _runner(schema_url, "runner-b", isolation), migrations)
            _await_waiting(monitor, "runner-b")  # B has begun and waits for its turn
        finally:
            monitor.execute("SELECT pg_advisory_unlock(%s)", (pause,))
        assert _labels(first.result(timeout=30)) == ["0002_second"]
        assert second.result(timeout=30) == ()  # it saw 0002, committed while it waited
    assert _versions(schema_url) == [1, 2]


@ISOLATIONS
def test_the_history_read_sees_a_commit_made_while_the_runner_waited_for_the_table_lock(
    schema_url: str, tmp_path: Path, isolation: str
) -> None:
    migrations = _write(
        tmp_path,
        {
            "0001_first.sql": "CREATE TABLE first (id int);",
            "0002_second.sql": "CREATE TABLE second (id int);",
        },
    )
    migrate(schema_url, migrations[:1])
    second = migrations[1]
    with psycopg.connect(schema_url) as holder, ThreadPoolExecutor(1) as pool:
        # Another runner, mid-way: it holds the history table's lock and has applied 0002.
        holder.execute("LOCK TABLE schema_migrations IN SHARE ROW EXCLUSIVE MODE")
        holder.execute("CREATE TABLE second (id int)")
        holder.execute(
            "INSERT INTO schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
            (second.version, second.name, second.checksum),
        )
        with psycopg.connect(schema_url, autocommit=True) as monitor:
            waiter = pool.submit(migrate, _runner(schema_url, "runner-b", isolation), migrations)
            _await_waiting(monitor, "runner-b")
        holder.commit()
        assert waiter.result(timeout=30) == ()
    assert _versions(schema_url) == [1, 2]


def test_a_history_conflict_is_a_migration_error(schema_url: str, tmp_path: Path) -> None:
    migrations = _write(
        tmp_path,
        {
            "0001_self_recorded.sql": (
                "CREATE TABLE self_recorded (id int);\n"
                "INSERT INTO schema_migrations (version, name, checksum) VALUES (1, 'x', 'x');"
            )
        },
    )
    with pytest.raises(MigrationError) as info:
        migrate(schema_url, migrations)
    assert info.value.sqlstate == "23505"
    with psycopg.connect(schema_url) as conn:  # rolled back with its migration: nothing is left
        row = conn.execute(
            "SELECT to_regclass('self_recorded') IS NULL, to_regclass('schema_migrations') IS NULL"
        ).fetchone()
    assert row == (True, True)


def test_concurrent_first_runs_apply_each_migration_once_at_repeatable_read(
    schema_factory: Callable[[], str], tmp_path: Path
) -> None:
    # Test migrations: the packaged 0002 refuses any isolation but READ COMMITTED (DEC-039).
    migrations = _write(
        tmp_path,
        {f"{n:04d}_table_{n}.sql": f"CREATE TABLE table_{n} (id int);" for n in range(1, 4)},
    )
    for _ in range(3):
        url = schema_factory()
        runners = 8
        barrier = threading.Barrier(runners)

        def run(name: str, url: str = url, barrier: threading.Barrier = barrier) -> list[str]:
            barrier.wait()
            return _labels(migrate(_runner(url, name, "repeatable\\ read"), migrations))

        with ThreadPoolExecutor(runners) as pool:
            results = list(pool.map(run, [f"runner-{n}" for n in range(runners)]))
        assert sorted(label for result in results for label in result) == _labels(migrations)
        assert _versions(url) == [m.version for m in migrations]
