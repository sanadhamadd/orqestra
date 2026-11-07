"""Command-line entry point.

Intentionally small: ``version`` and ``health`` prove the package installs,
configuration loads and logging works; ``migrate`` applies the database
schema; ``worker run`` runs one single-slot worker until SIGTERM or SIGINT
(DEC-051); ``job submit`` and ``job status`` submit a job from a JSON file of
tasks through ``submit_job`` and read its derived status (DEC-064); ``job
summary`` and ``job usage`` print an invoice job's outcome summary and provider
usage report, with the call budget check on request (DEC-066).
"""

import argparse
import functools
import json
import math
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, NoReturn

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import TupleRow
from pydantic import ValidationError

from orqestra import __version__
from orqestra.aggregation.budget import CallBudgetCheck, read_job_call_budget
from orqestra.aggregation.invoice import WrongJobKindError, summarize_invoice_job
from orqestra.aggregation.usage import JobUsageReport, UnitTotal, report_job_usage
from orqestra.config import Settings
from orqestra.domain.jobs import Job, JobStatus, JobSubmission, TaskSpec
from orqestra.domain.kinds import check_job_kinds
from orqestra.domain.states import TaskState
from orqestra.logging import configure_logging
from orqestra.persistence.jobs import (
    IdempotencyConflictError,
    get_job,
    get_job_progress,
    submit_job,
)
from orqestra.persistence.migrate import MigrationError, migrate, verify_current
from orqestra.worker import builtin
from orqestra.worker.handlers import HandlerRegistry
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.shutdown import run_until_signalled
from orqestra.workloads.invoice.handler import JOB_KIND as INVOICE_JOB_KIND
from orqestra.workloads.invoice.summary import InvoiceJobSummary
from orqestra.workloads.invoice.verify import check_call_cap


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
    job = sub.add_parser("job", help="submit a job, or read its status, summary or usage")
    job_actions = job.add_subparsers(dest="job_command", required=True)
    submit = job_actions.add_parser("submit", help="submit a job from a JSON file of tasks")
    submit.add_argument("--kind", required=True, help="the job's kind")
    submit.add_argument(
        "--idempotency-key",
        metavar="KEY",
        help="return the earlier job instead of creating another if this key was used",
    )
    submit.add_argument(
        "--max-attempts",
        type=int,
        metavar="N",
        help="the retry limit of every task that does not set its own",
    )
    submit.add_argument(
        "tasks_file",
        metavar="TASKS_FILE",
        help="a JSON array of tasks, or - to read it from standard input",
    )
    status = job_actions.add_parser("status", help="print a job's derived status as JSON")
    status.add_argument("job_id", type=uuid.UUID, metavar="JOB_ID")
    summary = job_actions.add_parser(
        "summary", help="print an invoice job's outcome summary as JSON"
    )
    summary.add_argument("job_id", type=uuid.UUID, metavar="JOB_ID")
    usage = job_actions.add_parser(
        "usage", help="print an invoice job's provider usage report as JSON"
    )
    usage.add_argument("job_id", type=uuid.UUID, metavar="JOB_ID")
    usage.add_argument(
        "--max-provider-calls-per-attempt",
        type=int,
        dest="cap",
        metavar="N",
        help=(
            "the per-attempt call cap the job's workers ran with, as you assume it "
            "(it is not stored); adds the call budget check"
        ),
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
    # they are reported generically. A byte that is not UTF-8 in the environment
    # arrives as a lone surrogate, which cannot be encoded for libpq at all.
    try:
        password = conninfo_to_dict(dsn).get("password")
    except (psycopg.ProgrammingError, UnicodeError):
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


# --- job submit / job status (DEC-064) ------------------------------------------------------

_TASK_FIELDS = frozenset({"task_key", "payload", "max_attempts"})


_NOT_FINITE = "a number that is not finite (NaN, Infinity or out of range) is not valid JSON"


def _refuse_constant(name: str) -> NoReturn:
    raise ValueError(_NOT_FINITE)


def _finite_float(literal: str) -> float:
    """A JSON number with a fraction or exponent, refused if it overflows to infinity."""
    value = float(literal)
    if not math.isfinite(value):
        raise ValueError(_NOT_FINITE)
    return value


def _encodable(text: str) -> bool:
    """Whether ``text`` can be sent to PostgreSQL: a lone surrogate has no UTF-8 encoding."""
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _all_encodable(value: object) -> bool:
    """Whether every string and object key inside a parsed JSON value is encodable.

    Iterative, so a value nested as deeply as the parser allows cannot exhaust the stack.
    """
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if not _encodable(item):
                return False
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, dict):
            if not all(_encodable(key) for key in item):
                return False
            pending.extend(item.values())
    return True


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """A JSON object, refused if it repeats a key: ``json`` would keep only the last."""
    obj = dict(pairs)
    if len(obj) != len(pairs):
        msg = "an object repeats a key"
        raise ValueError(msg)
    return obj


def _read_tasks_file(path: str) -> Any:
    """The parsed JSON document in ``path``, or standard input for ``-``.

    Messages name the file and the position of a syntax error, never its content.
    """
    try:
        raw = sys.stdin.buffer.read() if path == "-" else Path(path).read_bytes()
    except OSError as exc:
        msg = f"cannot read TASKS_FILE: {exc.strerror or type(exc).__name__}"
        raise _Refused(msg) from None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        msg = "TASKS_FILE is not UTF-8"
        raise _Refused(msg) from None
    try:
        return json.loads(
            text,
            parse_constant=_refuse_constant,
            parse_float=_finite_float,
            object_pairs_hook=_unique_keys,
        )
    except RecursionError:
        msg = "TASKS_FILE is nested too deeply"
        raise _Refused(msg) from None
    except ValueError as exc:
        msg = f"TASKS_FILE is not valid JSON: {exc}"
        raise _Refused(msg) from None


def _task(index: int, item: object, default_max_attempts: int | None) -> TaskSpec:
    """One task object of the file; messages name the task's index and field, not its values."""
    where = f"task {index}"
    if not isinstance(item, dict):
        msg = f"{where} must be a JSON object"
        raise _Refused(msg)
    unknown = sorted(set(item) - _TASK_FIELDS)
    if unknown:
        msg = f"{where} has unknown fields {unknown}; allowed: {sorted(_TASK_FIELDS)}"
        raise _Refused(msg)
    missing = [field for field in ("task_key", "payload") if field not in item]
    if missing:
        msg = f"{where} is missing {missing}"
        raise _Refused(msg)
    if not isinstance(item["task_key"], str):
        msg = f"{where}: task_key must be a string"
        raise _Refused(msg)
    if not _encodable(item["task_key"]):
        msg = f"{where}: task_key cannot be encoded as UTF-8"
        raise _Refused(msg)
    if not _all_encodable(item["payload"]):
        msg = f"{where}: payload holds a string or key that cannot be encoded as UTF-8"
        raise _Refused(msg)
    if "max_attempts" in item:
        max_attempts = item["max_attempts"]
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            msg = f"{where}: max_attempts must be an integer"
            raise _Refused(msg)
    elif default_max_attempts is None:
        msg = f"{where} sets no max_attempts and --max-attempts was not given"
        raise _Refused(msg)
    else:
        max_attempts = default_max_attempts
    return TaskSpec(item["task_key"], item["payload"], max_attempts)


def _submission(args: argparse.Namespace) -> JobSubmission:
    """The submission ``job submit`` describes; ``_Refused`` before any database access."""
    # Arguments that are not UTF-8 on the command line arrive as lone surrogates.
    for option, value in (("--kind", args.kind), ("--idempotency-key", args.idempotency_key)):
        if value is not None and not _encodable(value):
            msg = f"{option} cannot be encoded as UTF-8"
            raise _Refused(msg)
    try:
        check_job_kinds([args.kind])
    except (TypeError, ValueError) as exc:
        msg = f"invalid --kind: {exc}"
        raise _Refused(msg) from None
    document = _read_tasks_file(args.tasks_file)
    if not isinstance(document, list):
        msg = "TASKS_FILE must hold a JSON array of tasks"
        raise _Refused(msg)
    tasks = tuple(_task(i, item, args.max_attempts) for i, item in enumerate(document))
    try:
        return JobSubmission(args.kind, tasks, args.idempotency_key)
    except ValueError as exc:
        raise _Refused(str(exc)) from None


def _database_error(exc: psycopg.Error) -> str:
    """A database failure described by its type, SQLSTATE and constraint only.

    Driver messages can quote the connection string or the submitted values.
    """
    detail = f", SQLSTATE {exc.sqlstate}" if exc.sqlstate else ""
    if isinstance(exc, psycopg.errors.IntegrityError) and exc.diag.constraint_name:
        detail += f", constraint {exc.diag.constraint_name}"
    return f"the database request failed ({type(exc).__name__}{detail})"


def _submit(conn: psycopg.Connection[TupleRow], submission: JobSubmission) -> dict[str, object]:
    try:
        result = submit_job(conn, submission)
    except IdempotencyConflictError as exc:
        raise _Refused(str(exc)) from None
    return {
        "job_id": str(result.job.id),
        "kind": result.job.kind,
        "idempotency_key": result.job.idempotency_key,
        "created": result.created,
        "created_at": result.job.created_at.isoformat(),
    }


def _status(conn: psycopg.Connection[TupleRow], job_id: uuid.UUID) -> dict[str, object]:
    # A job row never changes and the counts are one statement, so two reads suffice.
    job = get_job(conn, job_id)
    progress = None if job is None else get_job_progress(conn, job_id)
    if job is None or progress is None:
        msg = f"job {job_id} not found"
        raise _Refused(msg)
    return {
        "job_id": str(job.id),
        "kind": job.kind,
        "idempotency_key": job.idempotency_key,
        "created_at": job.created_at.isoformat(),
        "job_status": progress.status.value,
        "task_counts": {state.value: progress.task_counts[state] for state in TaskState},
    }


# --- job summary / job usage (DEC-066) ------------------------------------------------------


def _by_value[K: StrEnum](counts: Mapping[K, int]) -> dict[str, int]:
    """A count per enum member, keyed by its stable value."""
    return {key.value: count for key, count in counts.items()}


def _unit_totals(totals: Mapping[str, UnitTotal]) -> dict[str, dict[str, object]]:
    """Exact amounts per unit, as plain decimal strings so no digit is lost to a float."""
    return {
        unit: {"amount": format(total.amount, "f"), "calls": total.calls}
        for unit, total in totals.items()
    }


def _job_head(job: Job, observed_at: datetime, job_status: JobStatus) -> dict[str, object]:
    return {
        "job_id": str(job.id),
        "kind": job.kind,
        "observed_at": observed_at.isoformat(),
        "job_status": job_status.value,
    }


def summary_document(summary: InvoiceJobSummary) -> dict[str, object]:
    """Every field and property of ``summary`` as JSON values: identifiers and counts only."""
    return {
        **_job_head(summary.job, summary.observed_at, summary.job_status),
        "task_counts": _by_value(summary.task_counts),
        "expected_items": summary.expected_items,
        "processed_items": summary.processed_items,
        "verified": summary.verified,
        "rejected": summary.rejected,
        "needs_review": summary.needs_review,
        "malformed": summary.malformed,
        "provider_assisted_items": summary.provider_assisted_items,
        "unprocessed_items": summary.unprocessed_items,
        "missing_items": summary.missing_items,
        "invalid_items": summary.invalid_items,
        "finished": summary.finished,
        "integrity_ok": summary.integrity_ok,
        "complete": summary.complete,
        "anomalies": _by_value(summary.anomalies),
        "anomaly_samples": [
            {"code": sample.code.value, "task_key": sample.task_key, "index": sample.index}
            for sample in summary.anomaly_samples
        ],
    }


def budget_document(check: CallBudgetCheck) -> dict[str, object]:
    """``check`` as JSON values; ``unknown_reason`` is ``null`` unless the verdict is unknown."""
    return {
        "cap": check.cap,
        "cap_source": check.cap_source,
        "max_attempts_total": check.max_attempts_total,
        "bound": check.bound,
        "calls": check.calls,
        "finality": check.finality.value,
        "verdict": check.verdict.value,
        "unknown_reason": None if check.unknown_reason is None else check.unknown_reason.value,
    }


def usage_document(
    report: JobUsageReport, check: CallBudgetCheck | None = None
) -> dict[str, object]:
    """Every field of ``report`` as JSON values, and ``budget`` when ``check`` is given."""
    document: dict[str, object] = {
        **_job_head(report.job, report.observed_at, report.job_status),
        "finished": report.finished,
        "finality": report.finality.value,
        "attempts_by_capture": _by_value(report.attempts_by_capture),
        "unclosed_attempts": report.unclosed_attempts,
        "calls": report.calls,
        "answered": report.answered,
        "failed": report.failed,
        "unresolved": report.unresolved,
        "settled_without_reported_usage": report.settled_without_reported_usage,
        "reported_totals": _unit_totals(report.reported_totals),
        "by_attempt_state": {
            state.value: {
                "attempts": usage.attempts,
                "calls": usage.calls,
                "answered": usage.answered,
                "failed": usage.failed,
                "unresolved": usage.unresolved,
                "reported_totals": _unit_totals(usage.reported_totals),
            }
            for state, usage in report.by_attempt_state.items()
        },
        "late_intents": report.late_intents,
        "late_results": report.late_results,
        "estimated_exposure_units": report.estimated_exposure_units,
        "unresolved_without_estimate": report.unresolved_without_estimate,
        "assisted_items": report.assisted_items,
        "assisted_checked_tasks": report.assisted_checked_tasks,
        "assisted_unchecked_tasks": report.assisted_unchecked_tasks,
        "anomalies": _by_value(report.anomalies),
        "anomaly_samples": [
            {
                "code": sample.code.value,
                "task_key": sample.task_key,
                "attempt_number": sample.attempt_number,
            }
            for sample in report.anomaly_samples
        ],
    }
    if check is not None:
        document["budget"] = budget_document(check)
    return document


def _not_found(job_id: uuid.UUID) -> NoReturn:
    msg = f"job {job_id} not found"
    raise _Refused(msg)


def _read_invoice_job[T](job_id: uuid.UUID, read: Callable[[], T | None]) -> T:
    """``read()``, refusing an unknown job, another kind and a job without tasks.

    The refusals name the job id only, never the stored kind or any task value.
    """
    try:
        result = read()
    except WrongJobKindError:
        msg = f"job {job_id} is not an {INVOICE_JOB_KIND} job"
        raise _Refused(msg) from None
    except ValueError:
        # submit_job never creates a job without tasks; only a manual edit can.
        msg = f"job {job_id} has no tasks"
        raise _Refused(msg) from None
    if result is None:
        _not_found(job_id)
    return result


def _summary(conn: psycopg.Connection[TupleRow], job_id: uuid.UUID) -> dict[str, object]:
    summary = _read_invoice_job(job_id, lambda: summarize_invoice_job(conn, job_id))
    return summary_document(summary)


def _usage(
    conn: psycopg.Connection[TupleRow], job_id: uuid.UUID, cap: int | None
) -> dict[str, object]:
    if cap is None:
        report = _read_invoice_job(job_id, lambda: report_job_usage(conn, job_id))
        return usage_document(report)
    report, check = _read_invoice_job(job_id, lambda: read_job_call_budget(conn, job_id, cap=cap))
    return usage_document(report, check)


def _checked_cap(cap: int | None) -> int | None:
    """The ``--max-provider-calls-per-attempt`` value, or ``_Refused`` without echoing it."""
    if cap is None:
        return None
    try:
        return check_call_cap("--max-provider-calls-per-attempt", cap)
    except (TypeError, ValueError) as exc:
        raise _Refused(str(exc)) from None


def _on_current_schema(
    dsn: str, action: Callable[[psycopg.Connection[TupleRow]], dict[str, object]]
) -> dict[str, object]:
    """Run ``action`` on an autocommit connection once the schema is checked to be current."""
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            try:
                verify_current(conn)
            except MigrationError as exc:
                msg = f"the database schema is not current: {exc}"
                raise _Refused(msg) from None
            return action(conn)
    except psycopg.Error as exc:
        raise _Refused(_database_error(exc)) from None
    except UnicodeError:
        # A backstop: every string is checked before this point.
        msg = "a value cannot be encoded as UTF-8 for the database"
        raise _Refused(msg) from None


def _job(args: argparse.Namespace) -> int:
    """Any ``job`` command: one JSON line on stdout, exit 0 or 1 (DEC-064, DEC-066)."""
    try:
        settings = Settings()
    except ValidationError as exc:
        errors = exc.errors(include_url=False, include_context=False, include_input=False)
        print(json.dumps({"status": "error", "errors": errors}, default=str))
        return 1
    configure_logging(settings.log_level)
    try:
        action: Callable[[psycopg.Connection[TupleRow]], dict[str, object]]
        if args.job_command == "submit":
            submission = _submission(args)
            action = functools.partial(_submit, submission=submission)
        elif args.job_command == "summary":
            action = functools.partial(_summary, job_id=args.job_id)
        elif args.job_command == "usage":
            cap = _checked_cap(args.cap)
            action = functools.partial(_usage, job_id=args.job_id, cap=cap)
        else:
            action = functools.partial(_status, job_id=args.job_id)
        dsn, _ = _connection_string(settings)
        output = _on_current_schema(dsn, action)
    except _Refused as exc:
        return _error(str(exc))
    print(json.dumps({"status": "ok", **output}))
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
    if args.command == "job":
        return _job(args)
    return 2  # pragma: no cover - argparse rejects unknown commands first


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
