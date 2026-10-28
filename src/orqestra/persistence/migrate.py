"""Forward-only PostgreSQL schema migrations.

Migrations are versioned SQL files (``NNNN_snake_case.sql``) shipped inside the
package. ``migrate`` applies the pending ones in order, each in its own
transaction, and records version, name and a content checksum in
``schema_migrations``. It refuses to run when an applied migration was edited,
when the database records migrations this code does not know, or when the
recorded history does not match the files. There are no downgrades, and it
never creates, drops or resets a database.

Safety properties, each covered by integration tests:

- A transaction-level advisory lock serialises concurrent runners. Commit or
  rollback releases it, so it cannot outlive a crashed runner.
- Migration SQL runs through PL/pgSQL ``EXECUTE`` inside a function call. In
  that context PostgreSQL itself rejects transaction control (``BEGIN``,
  ``COMMIT``, ``ROLLBACK``, ``SAVEPOINT``, ``PREPARE TRANSACTION``, and
  ``COMMIT`` inside ``DO`` blocks or procedures) and statements that cannot
  run in a transaction (``CREATE INDEX CONCURRENTLY``, ``VACUUM``). A migration
  therefore cannot end the runner's transaction, and no SQL is parsed here.
- A failed migration rolls back completely, together with its history row.

``verify_current`` is the read-only counterpart a worker runs before it starts
(DEC-051): it requires the database's history to equal the packaged
migrations exactly, and changes nothing.
"""

import hashlib
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources
from importlib.resources.abc import Traversable

import psycopg
from psycopg import sql
from psycopg.rows import TupleRow

from orqestra.domain.errors import OrqestraError

logger = logging.getLogger(__name__)

_FILENAME = re.compile(r"(?P<version>\d{4})_(?P<name>[a-z0-9]+(?:_[a-z0-9]+)*)\.sql")

# Key for pg_advisory_xact_lock: a fixed 64-bit value derived from a readable label.
LOCK_KEY = int.from_bytes(
    hashlib.sha256(b"orqestra.schema_migrations").digest()[:8], "big", signed=True
)

_CREATE_HISTORY = """
CREATE TABLE IF NOT EXISTS {} (
    version    integer     PRIMARY KEY,
    name       text        NOT NULL,
    checksum   text        NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)
"""

_CREATE_APPLY_FUNCTION = """
CREATE OR REPLACE FUNCTION pg_temp.orqestra_apply_migration(migration_sql text)
RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
    EXECUTE migration_sql;
END
$fn$
"""


class MigrationError(OrqestraError):
    """A migration could not be loaded, verified or applied."""

    def __init__(self, message: str, *, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


@dataclass(frozen=True, slots=True)
class Migration:
    """One versioned SQL migration with its normalised text and checksum."""

    version: int
    name: str
    sql: str
    checksum: str

    @property
    def label(self) -> str:
        return f"{self.version:04d}_{self.name}"


def checksum(sql_text: str) -> str:
    """Return the SHA-256 hex digest of normalised migration text."""
    return hashlib.sha256(sql_text.encode("utf-8")).hexdigest()


def load_migrations(directory: Traversable | None = None) -> tuple[Migration, ...]:
    """Load and validate migrations, ordered by version.

    Defaults to the migrations packaged with Orqestra. Line endings are
    normalised to ``\\n`` before checksumming so a checkout's line-ending
    conversion cannot change a checksum. Hidden files are ignored; any other
    unexpected entry is an error.
    """
    root = directory if directory is not None else resources.files(__package__) / "migrations"
    found: list[Migration] = []
    for entry in root.iterdir():
        if entry.name.startswith("."):
            continue
        match = _FILENAME.fullmatch(entry.name)
        if match is None or not entry.is_file():
            msg = f"unexpected entry in migrations directory: {entry.name!r}"
            raise MigrationError(msg)
        try:
            text = entry.read_bytes().decode("utf-8").replace("\r\n", "\n")
        except UnicodeDecodeError as exc:
            msg = f"{entry.name} is not valid UTF-8"
            raise MigrationError(msg) from exc
        if not text.strip():
            msg = f"{entry.name} is empty"
            raise MigrationError(msg)
        found.append(Migration(int(match["version"]), match["name"], text, checksum(text)))
    found.sort(key=lambda migration: migration.version)
    _check_sequence(found)
    return tuple(found)


def migrate(conninfo: str, migrations: Sequence[Migration] | None = None) -> tuple[Migration, ...]:
    """Apply pending migrations in order; return those applied by this call.

    History is kept in ``schema_migrations`` in the connection's current
    schema. Connection failures propagate as ``psycopg`` errors; everything
    else is a ``MigrationError``. Neither carries the connection string.
    """
    migrations = load_migrations() if migrations is None else tuple(migrations)
    _check_sequence(migrations)
    applied: list[Migration] = []
    with psycopg.connect(conninfo, autocommit=True) as conn:
        history = sql.Identifier(_current_schema(conn), "schema_migrations")
        while True:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_KEY,))
                conn.execute(sql.SQL(_CREATE_HISTORY).format(history))
                rows = conn.execute(
                    sql.SQL("SELECT version, name, checksum FROM {} ORDER BY version").format(
                        history
                    )
                ).fetchall()
                recorded = [
                    (int(version), str(name), str(digest)) for version, name, digest in rows
                ]
                _verify_history(recorded, migrations)
                if len(recorded) == len(migrations):
                    break
                migration = migrations[len(recorded)]
                _apply(conn, migration)
                conn.execute(
                    sql.SQL("INSERT INTO {} (version, name, checksum) VALUES (%s, %s, %s)").format(
                        history
                    ),
                    (migration.version, migration.name, migration.checksum),
                )
            applied.append(migration)
            logger.info("applied migration %s", migration.label)
    return tuple(applied)


_HISTORY_EXISTS = (
    "SELECT EXISTS (SELECT FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = %s AND c.relname = 'schema_migrations' AND c.relkind IN ('r', 'p'))"
)


def verify_current(
    conn: psycopg.Connection[TupleRow], migrations: Sequence[Migration] | None = None
) -> None:
    """Raise ``MigrationError`` unless the current schema has exactly the packaged history.

    Read-only: one ``READ ONLY`` transaction of catalog and history reads. It
    never creates the history table, applies a migration or takes the
    migration lock. Missing, extra, renamed and edited migrations are refused,
    and so is a history that is only a prefix of the packaged one: a schema
    that is behind must be migrated first (``orqestra migrate``).
    """
    migrations = load_migrations() if migrations is None else tuple(migrations)
    _check_sequence(migrations)
    with conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        schema = _current_schema(conn)
        row = conn.execute(_HISTORY_EXISTS, (schema,)).fetchone()
        if row is None or not row[0]:
            msg = f"schema {schema!r} has no migration history; run `orqestra migrate`"
            raise MigrationError(msg)
        rows = conn.execute(
            sql.SQL("SELECT version, name, checksum FROM {} ORDER BY version").format(
                sql.Identifier(schema, "schema_migrations")
            )
        ).fetchall()
    recorded = [(int(version), str(name), str(digest)) for version, name, digest in rows]
    _verify_history(recorded, migrations)
    if len(recorded) < len(migrations):
        pending = ", ".join(migration.label for migration in migrations[len(recorded) :])
        msg = f"schema {schema!r} is behind; pending migrations: {pending}; run `orqestra migrate`"
        raise MigrationError(msg)


def _check_sequence(migrations: Sequence[Migration]) -> None:
    for expected, migration in enumerate(migrations, start=1):
        if migration.version != expected:
            msg = (
                "migration versions must be unique and contiguous from 0001: "
                f"expected {expected:04d}, found {migration.label}"
            )
            raise MigrationError(msg)


def _verify_history(
    recorded: Sequence[tuple[int, str, str]], migrations: Sequence[Migration]
) -> None:
    if len(recorded) > len(migrations):
        unknown = ", ".join(
            f"{version:04d}_{name}" for version, name, _ in recorded[len(migrations) :]
        )
        msg = f"database has migrations this code does not know: {unknown}"
        raise MigrationError(msg)
    for (version, name, digest), migration in zip(recorded, migrations, strict=False):
        if (version, name) != (migration.version, migration.name):
            msg = (
                f"migration history mismatch: database has {version:04d}_{name}, "
                f"code has {migration.label}"
            )
            raise MigrationError(msg)
        if digest != migration.checksum:
            msg = f"{migration.label} was modified after it was applied (checksum mismatch)"
            raise MigrationError(msg)


def _current_schema(conn: psycopg.Connection[TupleRow]) -> str:
    row = conn.execute("SELECT current_schema()").fetchone()
    if row is None or row[0] is None:
        msg = "no schema is selected for migration history; check search_path"
        raise MigrationError(msg)
    return str(row[0])


def _apply(conn: psycopg.Connection[TupleRow], migration: Migration) -> None:
    conn.execute(_CREATE_APPLY_FUNCTION)
    try:
        conn.execute("SELECT pg_temp.orqestra_apply_migration(%s)", (migration.sql,))
    except psycopg.Error as exc:
        detail = exc.diag.message_primary or type(exc).__name__
        msg = f"{migration.label} failed and was rolled back: [{exc.sqlstate}] {detail}"
        raise MigrationError(msg, sqlstate=exc.sqlstate) from exc
