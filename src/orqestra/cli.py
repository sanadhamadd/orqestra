"""Command-line entry point.

Intentionally small: ``version`` and ``health`` prove the package installs,
configuration loads and logging works; ``migrate`` applies the database
schema; ``worker run`` runs one single-slot worker until SIGTERM or SIGINT
(DEC-051). Job submission arrives with later milestones.
"""

import argparse
import functools
import json
import sys
from collections.abc import Sequence

import psycopg
from psycopg.conninfo import conninfo_to_dict
from pydantic import ValidationError

from orqestra import __version__
from orqestra.config import Settings
from orqestra.domain.kinds import check_job_kinds
from orqestra.logging import configure_logging
from orqestra.persistence.migrate import MigrationError, migrate, verify_current
from orqestra.worker import builtin
from orqestra.worker.handlers import HandlerRegistry
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.shutdown import run_until_signalled


class _Refused(Exception):  # noqa: N818 - a refusal to start, with a message safe to print
    """Startup refused; the message names no secret."""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orqestra", description="Orqestra control CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version", help="print the installed version")
    sub.add_parser("health", help="load configuration and report process health as JSON")
    sub.add_parser("migrate", help="apply pending migrations to ORQESTRA_DATABASE_URL")
    worker = sub.add_parser("worker", help="run a worker")
    actions = worker.add_subparsers(dest="worker_command", required=True)
    run = actions.add_parser("run", help="run one single-slot worker until SIGTERM or SIGINT")
    run.add_argument(
        "--kind",
        action="append",
        required=True,
        dest="kinds",
        metavar="KIND",
        help=f"a job kind to run, one of: {', '.join(builtin.KINDS)}; repeat for more",
    )
    return parser


def _error(message: str) -> int:
    print(json.dumps({"status": "error", "error": message}))
    return 1


def _connection_string(settings: Settings) -> tuple[str, str | None]:
    """The DSN and its password, if any; ``_Refused`` if there is none, or it is malformed."""
    if settings.database_url is None:
        msg = "ORQESTRA_DATABASE_URL is not set"
        raise _Refused(msg)
    dsn = settings.database_url.get_secret_value()
    # libpq parse errors can echo fragments of the URL, password included, so
    # they are reported generically.
    try:
        password = conninfo_to_dict(dsn).get("password")
    except psycopg.ProgrammingError:
        msg = "ORQESTRA_DATABASE_URL is not a valid PostgreSQL connection string"
        raise _Refused(msg) from None
    return dsn, None if password is None else str(password)


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
    try:
        dsn, password = _connection_string(settings)
    except _Refused as exc:
        return _error(str(exc))
    # Other errors have the password redacted.
    try:
        applied = migrate(dsn)
    except (MigrationError, psycopg.Error) as exc:
        message = str(exc)
        if password:
            message = message.replace(password, "***")
        return _error(message)
    print(json.dumps({"status": "ok", "applied": [migration.label for migration in applied]}))
    return 0


def _registry(
    kinds: Sequence[str], settings: Settings, worker_settings: WorkerSettings
) -> HandlerRegistry:
    """The built-in handlers for exactly ``kinds``; ``_Refused`` for anything else."""
    try:
        requested = check_job_kinds(kinds)
    except (TypeError, ValueError) as exc:
        msg = f"invalid --kind: {exc}"
        raise _Refused(msg) from None
    available = builtin.handlers(worker_settings)
    unknown = [kind for kind in requested if kind not in available]
    if unknown:
        msg = f"unsupported job kinds {unknown}; this worker can run {list(builtin.KINDS)}"
        raise _Refused(msg)
    diagnostic = [kind for kind in requested if kind.startswith(builtin.DIAGNOSTIC_PREFIX)]
    if diagnostic and settings.environment == "production":
        msg = f"diagnostic handlers are refused in production: {diagnostic}"
        raise _Refused(msg)
    return HandlerRegistry({kind: available[kind] for kind in requested})


def _preflight(dsn: str, worker_settings: WorkerSettings) -> None:
    """Reach the database and check, read-only, that its schema is current; or ``_Refused``.

    Driver messages can carry connection details, so only the error's type and
    SQLSTATE are reported.
    """
    try:
        with connect_supervisor(dsn, worker_settings) as conn:
            verify_current(conn)
    except MigrationError as exc:
        msg = f"the database schema is not current: {exc}"
        raise _Refused(msg) from None
    except psycopg.Error as exc:
        code = f", SQLSTATE {exc.sqlstate}" if exc.sqlstate else ""
        msg = f"the database could not be reached or read ({type(exc).__name__}{code})"
        raise _Refused(msg) from None


def _worker_run(kinds: Sequence[str]) -> int:
    """Validate everything, then run a worker until a signal stops it (DEC-051).

    Every refusal comes before a worker exists, so nothing is claimed. Once
    running, the worker's own exits apply: 0 after a controlled stop, 3 to 9
    from the worker, and nonzero for anything unexpected.
    """
    try:
        settings = Settings()
        worker_settings = WorkerSettings()
    except ValidationError as exc:
        errors = exc.errors(include_url=False, include_context=False, include_input=False)
        print(json.dumps({"status": "error", "errors": errors}, default=str))
        return 1
    configure_logging(settings.log_level)
    try:
        registry = _registry(kinds, settings, worker_settings)
        dsn, _ = _connection_string(settings)
        _preflight(dsn, worker_settings)
    except _Refused as exc:
        return _error(str(exc))
    worker = Worker(
        registry=registry,
        settings=worker_settings,
        connect=functools.partial(connect_supervisor, dsn, worker_settings),
    )
    run_until_signalled(worker)
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
    if args.command == "worker":
        return _worker_run(args.kinds)
    return 2  # pragma: no cover - argparse rejects unknown commands first


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
