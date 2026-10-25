import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from tests.conftest import TEST_DATABASE_URL_ENV
from tests.integration.postgres import (
    SCHEMA_PREFIX,
    TEST_DATABASE_NAME,
    UnsafeTestDatabaseError,
    check_test_target,
    connect_verified,
    drop_session_schemas,
    new_session_prefix,
    pin_to_server,
    require_test_database_url,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("environ", [{}, {TEST_DATABASE_URL_ENV: ""}, {TEST_DATABASE_URL_ENV: " "}])
def test_missing_url_fails_instead_of_skipping(environ: dict[str, str]) -> None:
    with pytest.raises(UnsafeTestDatabaseError, match="is not set"):
        require_test_database_url(environ)


@pytest.mark.parametrize(
    ("host", "hostaddr"),
    [
        ("/tmp", ""),
        ("/var/run/postgresql", ""),
        ("localhost", "127.0.0.1"),
        ("localhost", "::1"),
        ("127.0.0.1", "127.0.0.1"),
        ("::1", "::1"),
    ],
)
def test_local_test_database_is_accepted(host: str, hostaddr: str) -> None:
    check_test_target(TEST_DATABASE_NAME, host, hostaddr)


@pytest.mark.parametrize(
    ("database", "host", "hostaddr"),
    [
        ("postgres", "/tmp", ""),
        ("orqestra", "localhost", "127.0.0.1"),
        ("orqestra_test_copy", "localhost", "127.0.0.1"),
        ("ORQESTRA_TEST", "localhost", "127.0.0.1"),
        ("orqestra_test", "db.example.supabase.co", "203.0.113.7"),
        ("orqestra_test", "10.0.0.5", "10.0.0.5"),
        ("orqestra_test", "", ""),
        # A local host name does not make the connection local: with hostaddr set,
        # libpq connects to that address and still reports the host name.
        ("orqestra_test", "localhost", "203.0.113.7"),
        ("orqestra_test", "localhost", "10.0.0.5"),
        ("orqestra_test", "127.0.0.1", "192.168.1.20"),
        ("orqestra_test", "::1", "2001:db8::1"),
        ("orqestra_test", "localhost", ""),
        ("orqestra_test", "localhost", "not-an-address"),
        ("orqestra_test", "/tmp", "203.0.113.7"),
        # A loopback address behind a remote host name is refused too (fails closed).
        ("orqestra_test", "db.example.supabase.co", "127.0.0.1"),
    ],
)
def test_other_databases_and_remote_hosts_are_refused(
    database: str, host: str, hostaddr: str
) -> None:
    with pytest.raises(UnsafeTestDatabaseError, match="refusing"):
        check_test_target(database, host, hostaddr)


def test_configured_url_reaches_the_test_database(database_url: str) -> None:
    with connect_verified(database_url) as conn:
        row = conn.execute("SELECT current_database()").fetchone()
    assert row == (TEST_DATABASE_NAME,)


def test_connection_to_another_database_is_refused(database_url: str) -> None:
    other = make_conninfo(database_url, dbname="postgres")
    with pytest.raises(UnsafeTestDatabaseError, match="'postgres'"):
        connect_verified(other)


def test_connection_is_checked_by_the_address_it_reached(
    database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Simulates libpq reporting a remote server address behind the configured host
    # name; no remote server is contacted.
    monkeypatch.setattr(psycopg.ConnectionInfo, "hostaddr", property(lambda _: "203.0.113.7"))
    with pytest.raises(UnsafeTestDatabaseError, match=r"address '203\.0\.113\.7'"):
        connect_verified(database_url)


def test_url_is_pinned_to_the_verified_endpoint(database_url: str) -> None:
    candidates = make_conninfo(
        database_url,
        host="db.example.supabase.co,db2.example.supabase.co",
        hostaddr="203.0.113.7,203.0.113.8",
        port="5432,5433",
        load_balance_hosts="random",
    )
    with connect_verified(database_url) as conn:
        pinned = conninfo_to_dict(pin_to_server(candidates, conn))
        expected = (conn.info.host, conn.info.hostaddr, str(conn.info.port))
    assert (pinned["host"], pinned["hostaddr"], pinned["port"]) == expected
    assert pinned["dbname"] == TEST_DATABASE_NAME


def test_suite_url_names_a_single_verified_endpoint(database_url: str) -> None:
    params = conninfo_to_dict(database_url)
    with connect_verified(database_url) as conn:
        assert params["host"] == conn.info.host
        assert params["hostaddr"] == conn.info.hostaddr


def test_session_prefixes_are_unique_and_do_not_overlap() -> None:
    first, second = new_session_prefix(), new_session_prefix()
    assert first.startswith(SCHEMA_PREFIX)
    assert first != second
    assert not first.startswith(second)
    assert not second.startswith(first)


def test_cleanup_only_drops_schemas_of_its_own_session(database_url: str) -> None:
    mine, theirs = new_session_prefix(), new_session_prefix()
    own = [f"{mine}a", f"{mine}b"]
    # Another session's live schema, a stale schema from before session ownership,
    # and a schema the suite never created: all must survive.
    survivors = [f"{theirs}a", f"{SCHEMA_PREFIX}stale_fixture_safety", "orq_keep_fixture_safety"]
    with connect_verified(database_url) as conn:
        for name in own + survivors:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        try:
            dropped = drop_session_schemas(conn, mine)
            remaining = {
                str(name)
                for (name,) in conn.execute(
                    "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)",
                    (own + survivors,),
                )
            }
        finally:
            for name in own + survivors:
                conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {}").format(sql.Identifier(name)))
    assert dropped == own
    assert remaining == set(survivors)


@pytest.mark.parametrize(
    "prefix", ["", SCHEMA_PREFIX, f"{SCHEMA_PREFIX}abc_", f"{SCHEMA_PREFIX}%", "orq_"]
)
def test_cleanup_refuses_anything_but_a_session_prefix(database_url: str, prefix: str) -> None:
    with (
        connect_verified(database_url) as conn,
        pytest.raises(ValueError, match="not a session prefix"),
    ):
        drop_session_schemas(conn, prefix)


def test_cleanup_refuses_an_unverified_connection(database_url: str) -> None:
    other = make_conninfo(database_url, dbname="postgres")
    with (
        psycopg.connect(other, autocommit=True) as conn,
        pytest.raises(UnsafeTestDatabaseError),
    ):
        drop_session_schemas(conn, new_session_prefix())
