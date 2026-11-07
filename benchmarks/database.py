"""The benchmark database guard, and the throwaway schemas benchmark runs work in.

Benchmarks may only touch a dedicated, local database named ``orqestra_bench``,
given in ``ORQESTRA_BENCH_DATABASE_URL``. Every connection is checked against
the server's own ``current_database()`` and the address libpq actually reached,
by the same rule as the integration suite's guard
(``tests/integration/postgres.py``, ``check_test_target``): a Unix socket, or a
loopback TCP address under a local host name. ``inet_server_addr()`` is not
used, because it is NULL over a socket.

Each run works in its own schema, ``orq_bench_<run>`` or ``orq_bench_<run>_<cell>``,
reached through ``search_path``. Only names of that exact form are ever created
or dropped.
"""

import ipaddress
import re
from collections.abc import Mapping

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import TupleRow

BENCH_DATABASE_URL_ENV = "ORQESTRA_BENCH_DATABASE_URL"
BENCH_DATABASE_NAME = "orqestra_bench"
SCHEMA_PREFIX = "orq_bench_"
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_PART = re.compile(r"[0-9a-z]{1,24}")
_RUN_SCHEMA = re.compile(rf"{SCHEMA_PREFIX}[0-9a-z]{{1,24}}(?:_[0-9a-z]{{1,24}})?")

Conn = psycopg.Connection[TupleRow]


class UnsafeBenchDatabaseError(RuntimeError):
    """The configured database is missing or is not the dedicated local benchmark database."""


def require_bench_database_url(environ: Mapping[str, str]) -> str:
    """The configured benchmark database URL; raise if it is not set."""
    url = environ.get(BENCH_DATABASE_URL_ENV, "").strip()
    if not url:
        msg = (
            f"{BENCH_DATABASE_URL_ENV} is not set: benchmarks need a local PostgreSQL "
            f"database named {BENCH_DATABASE_NAME!r}"
        )
        raise UnsafeBenchDatabaseError(msg)
    return url


def _is_loopback(address: str) -> bool:
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False


def check_bench_target(database: str, host: str, hostaddr: str) -> None:
    """Raise unless ``database`` is the benchmark database on this machine.

    ``host`` and ``hostaddr`` are what libpq reports for the live connection.
    With ``hostaddr`` set, libpq connects to that address but still reports the
    name given as ``host``, so a TCP connection must have reached a loopback
    address; a Unix-socket connection has no address at all.
    """
    if database != BENCH_DATABASE_NAME:
        msg = f"refusing to use database {database!r}: benchmarks require {BENCH_DATABASE_NAME!r}"
        raise UnsafeBenchDatabaseError(msg)
    if host.startswith("/"):
        local = hostaddr == ""
    else:
        local = host in _LOCAL_HOSTS and _is_loopback(hostaddr)
    if not local:
        msg = f"refusing to use non-local database host {host!r} at address {hostaddr!r}"
        raise UnsafeBenchDatabaseError(msg)


def verify_connection(conn: Conn) -> None:
    """Raise unless ``conn`` reached the benchmark database on this machine."""
    row = conn.execute("SELECT current_database()").fetchone()
    check_bench_target(str(row[0]) if row else "", conn.info.host, conn.info.hostaddr)


def connect_verified(url: str) -> Conn:
    """An autocommit connection to ``url``; closed again unless it reaches the bench database."""
    conn = psycopg.connect(url, autocommit=True)
    try:
        verify_connection(conn)
    except BaseException:
        conn.close()
        raise
    return conn


def run_schema_name(run_id: str, cell: str | None = None) -> str:
    """``orq_bench_<run_id>`` or ``orq_bench_<run_id>_<cell>``, each part 1-24 of ``[0-9a-z]``."""
    parts = [run_id] if cell is None else [run_id, cell]
    for part in parts:
        if not isinstance(part, str) or _PART.fullmatch(part) is None:
            msg = "run and cell identifiers must be 1 to 24 lowercase letters or digits"
            raise ValueError(msg)
    return SCHEMA_PREFIX + "_".join(parts)


def check_run_schema(schema: str) -> str:
    """Return ``schema`` if it is a run schema name, else raise ``ValueError``."""
    if not isinstance(schema, str) or _RUN_SCHEMA.fullmatch(schema) is None:
        msg = f"not a benchmark run schema (expected {SCHEMA_PREFIX}<run>[_<cell>])"
        raise ValueError(msg)
    return schema


def schema_url(conn: Conn, url: str, schema: str) -> str:
    """``url`` pinned to the server ``conn`` reached, with ``schema`` alone on the search path.

    Pinning keeps a URL that lists several hosts from reaching another server
    than the verified one, and overrides ``PGHOST``-style environment variables.
    """
    check_run_schema(schema)
    info = conn.info
    return make_conninfo(
        url,
        host=info.host,
        hostaddr=info.hostaddr,
        port=str(info.port),
        options=f"-c search_path={schema}",
    )


def create_run_schema(conn: Conn, schema: str) -> None:
    """Create ``schema`` after checking its name and ``conn``'s target. Fails if it exists."""
    name = check_run_schema(schema)
    verify_connection(conn)
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))


def drop_run_schema(conn: Conn, schema: str) -> None:
    """Drop ``schema`` and everything in it, after checking its name and ``conn``'s target."""
    name = check_run_schema(schema)
    verify_connection(conn)
    conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))
