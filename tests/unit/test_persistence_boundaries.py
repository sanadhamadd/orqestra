"""Where the library reads the database, so lease arithmetic only sees UTC instants (DEC-067).

``owned_transaction`` sets ``TimeZone`` to UTC for its own transaction, so every
``timestamptz`` it decodes is a ``datetime.UTC`` value. Values read any other way
keep the session's zone, and Python compares and adds those by wall time. An AST
scan of ``src/orqestra`` keeps the other ways few and reviewed:

- cursors are made only by the shared helpers in ``persistence/_shared.py``;
- a connection runs statements directly only in ``persistence/migrate.py``;
- the reads through ``tuple_cursor`` are a fixed list of read-only functions;
- in ``lifecycle``, the lease, deadline and retry rules run only inside the
  operations that own their transaction, or in private helpers only they call.

A change that fails here adds a read that could feed lease, deadline or retry
arithmetic: run it inside ``owned_transaction``, or convert its instants with
``.astimezone(UTC)`` before any arithmetic (DEC-067), and update the list.
"""

import ast
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

import orqestra

pytestmark = pytest.mark.unit

SRC = Path(orqestra.__file__).parent
PERSISTENCE = SRC / "persistence"
CURSOR_NAMES = frozenset({"cur", "cursor"})
RULES = frozenset(
    {"initial_lease", "renewed_lease", "cancellation_lease", "revocation_reason", "retry_at"}
)
OWNED = frozenset(
    {
        "start_attempt",
        "claim_next_attempt",
        "complete_attempt",
        "renew_lease",
        "revoke_next_expired",
        "request_cancellation",
    }
)
# Display and return only: none of these feeds lease, deadline or retry arithmetic.
READS_OUTSIDE_OWNED = frozenset(
    {
        ("jobs", "get_job"),
        ("jobs", "get_task"),
        ("jobs", "list_tasks"),
        ("jobs", "get_job_progress"),
        ("lifecycle", "get_attempt"),
        ("lifecycle", "list_attempts"),
        ("lifecycle", "read_completion_evidence"),
        ("provider_usage", "read_attempt_usage"),
        ("snapshots", "JobBatchSnapshot.usage_totals"),
        ("snapshots", "JobBatchSnapshot.max_attempts_total"),
        ("snapshots", "job_batch_snapshot"),
    }
)
CURSOR_MAKERS = frozenset(
    {
        ("persistence/_shared.py", "owned_transaction"),
        ("persistence/_shared.py", "tuple_cursor"),
        # The batch snapshot streams rows through a read-only named (server-side)
        # cursor inside its own read transaction; it does no lease arithmetic.
        ("persistence/snapshots.py", "JobBatchSnapshot._open_stream"),
    }
)
DIRECT_EXECUTE = frozenset({"persistence/migrate.py"})


@dataclass(frozen=True, slots=True)
class CallSite:
    path: str  # relative to src/orqestra
    function: str  # the enclosing function, as Class.method for a method; "" at module level
    call: ast.Call


def _calls(path: Path) -> Iterator[CallSite]:
    relative = path.relative_to(SRC).as_posix()

    def visit(node: ast.AST, scope: tuple[str, ...]) -> Iterator[CallSite]:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                yield from visit(child, (*scope, child.name))
                continue
            if isinstance(child, ast.Call):
                yield CallSite(relative, ".".join(scope), child)
            yield from visit(child, scope)

    yield from visit(ast.parse(path.read_text(encoding="utf-8")), ())


def _all_calls() -> list[CallSite]:
    return [site for path in sorted(SRC.rglob("*.py")) for site in _calls(path)]


def _method(site: CallSite) -> tuple[str, str] | None:
    """``(attribute, receiver)`` for a call like ``receiver.attribute(...)``."""
    func = site.call.func
    if isinstance(func, ast.Attribute):
        return func.attr, ast.unparse(func.value)
    return None


def _named(site: CallSite) -> str | None:
    func = site.call.func
    return func.id if isinstance(func, ast.Name) else None


def _top(function: str) -> str:
    return function.split(".")[0]


def test_cursors_are_made_only_by_the_shared_helpers() -> None:
    makers = {
        (site.path, site.function)
        for site in _all_calls()
        if (method := _method(site)) is not None and method[0] == "cursor"
    }
    assert makers <= CURSOR_MAKERS, (
        f"cursors made outside the shared helpers: {sorted(makers - CURSOR_MAKERS)}; "
        "read inside owned_transaction or through a reviewed tuple_cursor read (DEC-067)"
    )


def test_connections_execute_directly_only_in_migrations() -> None:
    direct = {
        (site.path, site.function)
        for site in _all_calls()
        if (method := _method(site)) is not None
        and method[0] in {"execute", "executemany", "stream", "copy"}
        and method[1] not in CURSOR_NAMES
    }
    outside = {item for item in direct if item[0] not in DIRECT_EXECUTE}
    assert not outside, (
        f"statements run on a connection outside migrations: {sorted(outside)}; "
        "their instants keep the session's zone (DEC-067)"
    )


def test_reads_outside_owned_transaction_are_a_reviewed_list() -> None:
    reads = {
        (Path(site.path).stem, site.function)
        for site in _all_calls()
        if site.path.startswith("persistence/")
        and site.path != "persistence/_shared.py"
        and _named(site) == "tuple_cursor"
    }
    assert reads == READS_OUTSIDE_OWNED, (
        "the reads outside owned_transaction changed: "
        f"added {sorted(reads - READS_OUTSIDE_OWNED)}, "
        f"removed {sorted(READS_OUTSIDE_OWNED - reads)}. Their instants keep the session's "
        "zone; one that feeds lease, deadline or retry arithmetic must run inside "
        "owned_transaction or convert with .astimezone(UTC) (DEC-067)"
    )


def test_lifecycle_rules_run_inside_owned_transactions() -> None:
    sites = list(_calls(PERSISTENCE / "lifecycle.py"))
    owned = {_top(site.function) for site in sites if _named(site) == "owned_transaction"}
    assert owned == OWNED

    rule_callers = {_top(site.function) for site in sites if _named(site) in RULES}
    helpers = (rule_callers - OWNED) | {"_now"}  # _now reads the clock the rules are given
    assert all(name.startswith("_") for name in helpers), (
        f"public functions apply lease or retry rules outside owned_transaction: "
        f"{sorted(name for name in helpers if not name.startswith('_'))} (DEC-067)"
    )
    assert helpers == {"_claim", "_now", "_record_running_request"}
    for helper in helpers:
        callers = {_top(site.function) for site in sites if _named(site) == helper}
        assert callers, f"{helper} is never called"
        assert callers <= OWNED, (
            f"{helper} feeds lease or retry rules and is called outside owned_transaction "
            f"from {sorted(callers - OWNED)} (DEC-067)"
        )
