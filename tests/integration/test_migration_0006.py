"""Migration 0006 (the claim index, DEC-069) against real PostgreSQL.

The upgrade adds one partial index and nothing else, fails at once rather than
wait while an open transaction has written to ``tasks``, and leaves nothing
behind when it fails. The claim's discovery statement, imported from lifecycle,
uses the index both as a custom plan and as a prepared, generic plan, at a
stated queue depth after ``ANALYZE``, so the check does not hang on how small a
test table happens to be.
"""

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.domain.jobs import JobSubmission, TaskSpec
from orqestra.persistence.jobs import submit_job
from orqestra.persistence.lifecycle import _LOCK_NEXT_CLAIMABLE_TASK
from orqestra.persistence.migrate import MigrationError, load_migrations, migrate

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

MIGRATIONS = load_migrations()
UP_TO_0005 = list(MIGRATIONS[:5])
UP_TO_0006 = list(MIGRATIONS[:6])  # never a later migration: it has its own tests
CLAIMED = "claim.index"
OTHER = "claim.other"
QUEUED_DEPTH = 10_000  # the stated depth of the plan test
OTHER_DEPTH = 2_000


def _index(conn: Conn) -> tuple[Any, ...] | None:
    return conn.execute(
        "SELECT pg_get_indexdef(i.indexrelid), i.indpred IS NOT NULL "
        "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE c.relname = 'tasks_claim_order' AND c.relnamespace = current_schema()::regnamespace"
    ).fetchone()


def _versions(conn: Conn) -> list[int]:
    return [int(v) for (v,) in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")]


@pytest.fixture
def at_0005(schema_url: str) -> Iterator[Conn]:
    migrate(schema_url, UP_TO_0005)
    with psycopg.connect(schema_url, autocommit=True) as conn:
        yield conn


def test_0006_adds_one_partial_index_once(schema_url: str, at_0005: Conn) -> None:
    assert _index(at_0005) is None
    assert [m.label for m in migrate(schema_url, UP_TO_0006)] == ["0006_claim_index"]
    assert migrate(schema_url, UP_TO_0006) == ()  # a second run applies nothing
    assert _versions(at_0005) == [1, 2, 3, 4, 5, 6]
    found = _index(at_0005)
    assert found is not None
    definition, partial = found
    assert partial is True
    assert definition.endswith("USING btree (created_at, id) WHERE (state = 'queued'::text)")


def test_0006_fails_at_once_while_a_transaction_has_written_to_tasks(
    schema_url: str, at_0005: Conn
) -> None:
    submit_job(at_0005, JobSubmission(CLAIMED, (TaskSpec("a", {}, 1),)))
    with psycopg.connect(schema_url) as writer:
        writer.execute("UPDATE tasks SET updated_at = updated_at")  # open: ROW EXCLUSIVE held
        with pytest.raises(MigrationError) as info:
            migrate(schema_url, UP_TO_0006)
        writer.rollback()
    assert info.value.sqlstate == "55P03"
    # 0006's own text names only a writing transaction; migrate adds the general cause.
    assert "an open transaction has written to it" in str(info.value)
    assert "a conflicting lock on a table this migration locks" in str(info.value)
    assert "autovacuum" in str(info.value)
    assert _index(at_0005) is None
    assert _versions(at_0005) == [1, 2, 3, 4, 5]


def _seed(conn: Conn) -> None:
    for kind, depth in ((CLAIMED, QUEUED_DEPTH), (OTHER, OTHER_DEPTH)):
        specs = tuple(TaskSpec(f"{kind}-{n}", {"n": n}, 1) for n in range(depth))
        submit_job(conn, JobSubmission(kind, specs))
    conn.execute("ANALYZE tasks")
    conn.execute("ANALYZE jobs")


def _nodes(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans", []):
        yield from _nodes(child)


def _uses_the_index(document: Any) -> bool:
    nodes = list(_nodes(document[0]["Plan"]))
    index = any(node.get("Index Name") == "tasks_claim_order" for node in nodes)
    scan = any(
        node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == "tasks"
        for node in nodes
    )
    return index and not scan


def test_the_claim_uses_the_index_as_a_custom_and_as_a_generic_plan(
    schema_url: str, at_0005: Conn
) -> None:
    migrate(schema_url, UP_TO_0006)
    _seed(at_0005)
    with at_0005.transaction(force_rollback=True):
        (custom,) = at_0005.execute(
            f"EXPLAIN (FORMAT JSON) {_LOCK_NEXT_CLAIMABLE_TASK}", ([CLAIMED],)
        ).fetchone() or (None,)
    assert _uses_the_index(custom), custom
    # A prepared statement that the server plans once, for any parameter value.
    generic_sql = _LOCK_NEXT_CLAIMABLE_TASK.replace("%s", "$1")
    assert "%s" not in generic_sql
    with at_0005.transaction(force_rollback=True):
        at_0005.execute("SET LOCAL plan_cache_mode = force_generic_plan")
        at_0005.execute(f"PREPARE claim_probe(text[]) AS {generic_sql}")
        (generic,) = at_0005.execute(
            "EXPLAIN (FORMAT JSON) EXECUTE claim_probe(ARRAY['claim.index'])"
        ).fetchone() or (None,)
        at_0005.execute("DEALLOCATE claim_probe")
    assert _uses_the_index(generic), generic
