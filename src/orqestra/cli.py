"""Command-line entry point.

Intentionally tiny: ``version`` and ``health`` prove the package installs,
configuration loads and logging works; ``migrate`` applies the database
schema. Job submission arrives with later milestones.
"""

import argparse
import json
import sys
from collections.abc import Sequence

import psycopg
from psycopg.conninfo import conninfo_to_dict
from pydantic import ValidationError

from orqestra import __version__
from orqestra.config import Settings
from orqestra.logging import configure_logging
from orqestra.persistence.migrate import MigrationError, migrate


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orqestra", description="Orqestra control CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version", help="print the installed version")
    sub.add_parser("health", help="load configuration and report process health as JSON")
    sub.add_parser("migrate", help="apply pending migrations to ORQESTRA_DATABASE_URL")
    return parser


def _error(message: str) -> int:
    print(json.dumps({"status": "error", "error": message}))
    return 1


def _health() -> int:
    try:
        settings = Settings()
    except ValidationError as exc:
        print(json.dumps({"status": "error", "errors": exc.errors()}, default=str))
        return 1
    configure_logging(settings.log_level)
    print(json.dumps({"status": "ok", "version": __version__, "settings": settings.redacted()}))
    return 0


def _migrate() -> int:
    try:
        settings = Settings()
    except ValidationError as exc:
        print(json.dumps({"status": "error", "errors": exc.errors()}, default=str))
        return 1
    configure_logging(settings.log_level)
    if settings.database_url is None:
        return _error("ORQESTRA_DATABASE_URL is not set")
    dsn = settings.database_url.get_secret_value()
    # libpq parse errors can echo fragments of the URL, password included, so
    # they are reported generically; other errors have the password redacted.
    try:
        password = conninfo_to_dict(dsn).get("password")
    except psycopg.ProgrammingError:
        return _error("ORQESTRA_DATABASE_URL is not a valid PostgreSQL connection string")
    try:
        applied = migrate(dsn)
    except (MigrationError, psycopg.Error) as exc:
        message = str(exc)
        if password:
            message = message.replace(str(password), "***")
        return _error(message)
    print(json.dumps({"status": "ok", "applied": [migration.label for migration in applied]}))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI; returns the process exit code."""
    args = _build_parser().parse_args(argv)
    if args.command == "version":
        print(__version__)
        return 0
    if args.command == "health":
        return _health()
    if args.command == "migrate":
        return _migrate()
    return 2  # pragma: no cover - argparse rejects unknown commands first


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
