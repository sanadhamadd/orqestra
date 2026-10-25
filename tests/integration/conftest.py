"""PostgreSQL integration fixtures.

Tests fail, never skip, when ``ORQESTRA_TEST_DATABASE_URL`` is missing or does
not reach the dedicated local ``orqestra_test`` database. Each test works in
its own freshly created schema, dropped afterwards, so tests are isolated from
each other and from anything else in the database.

Schema names carry a prefix owned by the test session, and cleanup only drops
that session's schemas, so concurrent sessions do not interfere. Schemas left by
an interrupted session stay until the disposable database is recreated
(``dropdb orqestra_test && createdb orqestra_test``).
"""

import os
import uuid
from collections.abc import Callable, Iterator

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

from orqestra.persistence.migrate import migrate
from tests.integration.postgres import (
    UnsafeTestDatabaseError,
    connect_verified,
    drop_session_schemas,
    new_session_prefix,
    pin_to_server,
    require_test_database_url,
    with_search_path,
)


@pytest.fixture(scope="session")
def database_url() -> str:
    """The test database URL, pinned to the server endpoint that was verified."""
    try:
        url = require_test_database_url(os.environ)
        with connect_verified(url) as conn:
            return pin_to_server(url, conn)
    except UnsafeTestDatabaseError as exc:
        pytest.fail(str(exc), pytrace=False)
    except psycopg.ProgrammingError:
        # libpq parse errors may echo parts of the URL, including a password.
        pytest.fail("ORQESTRA_TEST_DATABASE_URL is not a valid connection string", pytrace=False)
    except psycopg.OperationalError as exc:
        pytest.fail(f"cannot connect to the test database: {exc}", pytrace=False)


@pytest.fixture(scope="session")
def schema_prefix(database_url: str) -> Iterator[str]:
    """Schema-name prefix owned by this session; its leftovers are dropped at the end."""
    prefix = new_session_prefix()
    yield prefix
    with connect_verified(database_url) as conn:
        drop_session_schemas(conn, prefix)


@pytest.fixture
def schema_factory(database_url: str, schema_prefix: str) -> Iterator[Callable[[], str]]:
    """Create empty, uniquely named schemas on demand; drop them all afterwards."""
    created: list[str] = []

    def create() -> str:
        name = f"{schema_prefix}{uuid.uuid4().hex[:12]}"
        with connect_verified(database_url) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))
        created.append(name)
        return with_search_path(database_url, name)

    yield create
    with connect_verified(database_url) as conn:
        for name in created:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))


@pytest.fixture
def schema_url(schema_factory: Callable[[], str]) -> str:
    """Connection URL for one empty schema."""
    return schema_factory()


@pytest.fixture
def db(schema_url: str) -> Iterator[psycopg.Connection[TupleRow]]:
    """Autocommit connection to a schema with all packaged migrations applied."""
    migrate(schema_url)
    with psycopg.connect(schema_url, autocommit=True) as conn:
        yield conn
