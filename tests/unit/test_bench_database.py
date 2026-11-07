"""The benchmark database guard and run-schema naming, without a database."""

from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from psycopg import sql

from benchmarks.database import (
    BENCH_DATABASE_NAME,
    BENCH_DATABASE_URL_ENV,
    UnsafeBenchDatabaseError,
    check_bench_target,
    check_run_schema,
    create_run_schema,
    drop_run_schema,
    require_bench_database_url,
    run_schema_name,
    schema_url,
    verify_connection,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


@dataclass
class Info:
    host: str
    hostaddr: str
    port: int = 5433


@dataclass
class Result:
    row: tuple[object, ...] | None

    def fetchone(self) -> tuple[object, ...] | None:
        return self.row


@dataclass
class FakeConnection:
    """Answers ``current_database()`` and records every other statement."""

    database: str
    info: Info
    statements: list[object] = field(default_factory=list)

    def execute(self, query: object) -> Result:
        if query == "SELECT current_database()":
            return Result((self.database,))
        self.statements.append(query)
        return Result(None)


def fake(database: str = BENCH_DATABASE_NAME, host: str = "/tmp", hostaddr: str = "") -> Any:
    return FakeConnection(database, Info(host, hostaddr))


@pytest.mark.parametrize(
    ("host", "hostaddr"),
    [
        pytest.param("/tmp", "", id="unix-socket"),
        pytest.param("/var/run/postgresql", "", id="other-socket-directory"),
        pytest.param("localhost", "127.0.0.1", id="localhost-ipv4"),
        pytest.param("localhost", "::1", id="localhost-ipv6"),
        pytest.param("127.0.0.1", "127.0.0.1", id="loopback-address"),
        pytest.param("::1", "::1", id="loopback-ipv6-address"),
    ],
)
def test_a_local_benchmark_database_is_accepted(host: str, hostaddr: str) -> None:
    check_bench_target(BENCH_DATABASE_NAME, host, hostaddr)


@pytest.mark.parametrize(
    ("database", "host", "hostaddr"),
    [
        pytest.param("orqestra_test", "/tmp", "", id="the-test-database"),
        pytest.param("orqestra", "/tmp", "", id="the-application-database"),
        pytest.param("", "/tmp", "", id="no-name"),
        pytest.param(BENCH_DATABASE_NAME, "db.example.com", "203.0.113.5", id="remote-host"),
        pytest.param(BENCH_DATABASE_NAME, "localhost", "203.0.113.5", id="local-name-remote-addr"),
        pytest.param(BENCH_DATABASE_NAME, "10.0.0.5", "10.0.0.5", id="private-address"),
        pytest.param(BENCH_DATABASE_NAME, "/tmp", "203.0.113.5", id="socket-with-hostaddr"),
        pytest.param(BENCH_DATABASE_NAME, "localhost", "", id="name-without-address"),
    ],
)
def test_anything_else_is_refused(database: str, host: str, hostaddr: str) -> None:
    with pytest.raises(UnsafeBenchDatabaseError):
        check_bench_target(database, host, hostaddr)


def test_the_url_is_required() -> None:
    with pytest.raises(UnsafeBenchDatabaseError, match=BENCH_DATABASE_URL_ENV):
        require_bench_database_url({})
    with pytest.raises(UnsafeBenchDatabaseError):
        require_bench_database_url({BENCH_DATABASE_URL_ENV: "   "})
    url = "postgresql:///orqestra_bench"
    assert require_bench_database_url({BENCH_DATABASE_URL_ENV: f" {url} "}) == url


def test_the_connection_is_checked_against_the_server_answer() -> None:
    verify_connection(fake())
    with pytest.raises(UnsafeBenchDatabaseError):
        verify_connection(fake(database="orqestra_test"))
    with pytest.raises(UnsafeBenchDatabaseError):
        verify_connection(fake(host="db.example.com", hostaddr="203.0.113.5"))


def test_run_schema_names() -> None:
    assert run_schema_name("abc123") == "orq_bench_abc123"
    assert run_schema_name("abc123", "w4r1") == "orq_bench_abc123_w4r1"
    for bad in ["", "ABC", "a-b", "a_b", "x" * 25, "a b"]:
        with pytest.raises(ValueError, match="identifiers"):
            run_schema_name(bad)
        with pytest.raises(ValueError, match="identifiers"):
            run_schema_name("run", bad)


@pytest.mark.parametrize(
    "schema",
    ["public", "orq_test_abc", "orq_bench_", "orq_bench_a_b_c", "orq_bench_ABC", "x; DROP"],
)
def test_only_run_schemas_are_created_or_dropped(schema: str) -> None:
    conn = fake()
    with pytest.raises(ValueError, match="run schema"):
        check_run_schema(schema)
    with pytest.raises(ValueError, match="run schema"):
        create_run_schema(conn, schema)
    with pytest.raises(ValueError, match="run schema"):
        drop_run_schema(conn, schema)
    assert conn.statements == []


def test_a_run_schema_is_dropped_only_on_the_benchmark_database() -> None:
    elsewhere = fake(database="orqestra_test")
    with pytest.raises(UnsafeBenchDatabaseError):
        drop_run_schema(elsewhere, "orq_bench_run1")
    assert elsewhere.statements == []

    conn = fake()
    create_run_schema(conn, "orq_bench_run1_c1")
    drop_run_schema(conn, "orq_bench_run1_c1")
    rendered = [cast(sql.Composed, statement).as_string(None) for statement in conn.statements]
    assert rendered == [
        'CREATE SCHEMA "orq_bench_run1_c1"',
        'DROP SCHEMA IF EXISTS "orq_bench_run1_c1" CASCADE',
    ]


def test_the_schema_url_is_pinned_to_the_verified_server() -> None:
    conn = fake(host="localhost", hostaddr="127.0.0.1")
    url = schema_url(conn, "postgresql://u@localhost,otherhost:5433/orqestra_bench", "orq_bench_r1")
    assert "hostaddr=127.0.0.1" in url
    assert "host=localhost" in url
    assert "otherhost" not in url
    assert "search_path=orq_bench_r1" in url
    with pytest.raises(ValueError, match="run schema"):
        schema_url(conn, "postgresql:///orqestra_bench", "public")
