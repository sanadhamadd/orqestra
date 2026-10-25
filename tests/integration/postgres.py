"""Safety guard and helpers for the PostgreSQL integration suite.

Integration tests may only touch a dedicated, disposable, local database named
``orqestra_test``. Every connection the suite makes for setup or cleanup is
verified against the server's own answer for ``current_database()`` and the
address the connection actually reached, and cleanup only drops schemas the
current test session created.
"""

import ipaddress
import re
import uuid
from collections.abc import Mapping

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow

from tests.conftest import TEST_DATABASE_URL_ENV

TEST_DATABASE_NAME = "orqestra_test"
SCHEMA_PREFIX = "orq_test_"
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_SESSION_PREFIX = re.compile(rf"{SCHEMA_PREFIX}[0-9a-f]{{12}}_")


class UnsafeTestDatabaseError(RuntimeError):
    """The configured database is missing or is not the dedicated local test database."""


def require_test_database_url(environ: Mapping[str, str]) -> str:
    """Return the configured test database URL or raise; never silently skip."""
    url = environ.get(TEST_DATABASE_URL_ENV, "").strip()
    if not url:
        msg = (
            f"{TEST_DATABASE_URL_ENV} is not set: integration tests need a local PostgreSQL "
            f"database named {TEST_DATABASE_NAME!r} (see docs/development.md)"
        )
        raise UnsafeTestDatabaseError(msg)
    return url


def _is_loopback(address: str) -> bool:
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False


def check_test_target(database: str, host: str, hostaddr: str) -> None:
    """Raise unless ``database`` is the dedicated test database on this machine.

    ``host`` and ``hostaddr`` are what libpq reports for the live connection. The
    host name alone is not enough: with ``hostaddr`` set, libpq connects to that
    address but still reports the name given as ``host``. A TCP connection must
    therefore have reached a loopback address; a Unix-socket connection has none.
    """
    if database != TEST_DATABASE_NAME:
        msg = (
            f"refusing to use database {database!r}: "
            f"integration tests require {TEST_DATABASE_NAME!r}"
        )
        raise UnsafeTestDatabaseError(msg)
    if host.startswith("/"):
        local = hostaddr == ""
    else:
        local = host in _LOCAL_HOSTS and _is_loopback(hostaddr)
    if not local:
        msg = f"refusing to use non-local database host {host!r} at address {hostaddr!r}"
        raise UnsafeTestDatabaseError(msg)


def _verify(conn: psycopg.Connection[TupleRow]) -> None:
    row = conn.execute("SELECT current_database()").fetchone()
    check_test_target(str(row[0]) if row else "", conn.info.host, conn.info.hostaddr)


def connect_verified(url: str) -> psycopg.Connection[TupleRow]:
    """Open an autocommit connection after verifying it reaches the test database."""
    conn = psycopg.connect(url, autocommit=True)
    try:
        _verify(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def pin_to_server(url: str, conn: psycopg.Connection[TupleRow]) -> str:
    """Return ``url`` restricted to the endpoint ``conn`` is connected to.

    A connection string may list several hosts, tried in order or at random
    (``load_balance_hosts``), so a later connection could reach a server other
    than the verified one. Explicit values also override ``PGHOST``-style
    environment variables.
    """
    info = conn.info
    return make_conninfo(url, host=info.host, hostaddr=info.hostaddr, port=str(info.port))


def with_search_path(url: str, schema: str) -> str:
    """Return ``url`` with ``schema`` as the only schema on the search path."""
    return make_conninfo(url, options=f"-c search_path={schema}")


def new_session_prefix() -> str:
    """Return a schema-name prefix owned by one test session."""
    return f"{SCHEMA_PREFIX}{uuid.uuid4().hex[:12]}_"


def drop_session_schemas(conn: psycopg.Connection[TupleRow], prefix: str) -> list[str]:
    """Drop the schemas named with one session's ``prefix``; return their names.

    Schemas of other sessions, running or abandoned, are never touched.
    """
    if not _SESSION_PREFIX.fullmatch(prefix):
        msg = f"refusing to drop schemas by {prefix!r}: not a session prefix"
        raise ValueError(msg)
    _verify(conn)
    names = [
        str(name)
        for (name,) in conn.execute(
            "SELECT nspname FROM pg_namespace WHERE starts_with(nspname, %s) ORDER BY 1",
            (prefix,),
        )
    ]
    for name in names:
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))
    return names
