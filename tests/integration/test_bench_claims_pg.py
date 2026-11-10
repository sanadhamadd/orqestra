"""The claim-cost measurement at depth 100: real claims, real plans, no lasting effect.

These tests check that the tool measures what it says and changes nothing it
should not. They assert no timing.
"""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.errors import LockNotAvailable
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow

from benchmarks.claims import (
    _EXPLAIN,
    CLAIMED_KIND,
    TABLE_FIELDS,
    UNSUPPORTED_KIND,
    Condition,
    Variant,
    build_record,
    churn_run,
    distribution,
    explain_claims,
    fill,
    heap_pages,
    main,
    measure_depth,
    validate_result,
)
from benchmarks.database import BENCH_DATABASE_NAME, BENCH_DATABASE_URL_ENV

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]
DEPTH = 100
CLAIMS = 20


def scalar(conn: Conn, query: str, *params: object) -> Any:
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return row[0]


def record_of(measurements: list[dict[str, Any]]) -> dict[str, Any]:
    return build_record(
        measurements,
        code={"git_sha": "0" * 40, "git_dirty": False},
        server={"version": "test"},
        settings={},
    )


@pytest.fixture
def worker_conn(db: Conn, schema_url: str) -> Iterator[Conn]:
    """A second autocommit session on the migrated schema, like the tool's own."""
    with psycopg.connect(schema_url, autocommit=True) as conn:
        yield conn


@pytest.mark.parametrize("variant", list(Variant))
def test_a_depth_100_run_records_real_claims_and_a_plan(
    db: Conn, schema_url: str, variant: Variant
) -> None:
    measured = measure_depth(schema_url, depth=DEPTH, variant=variant, claims=CLAIMS, explains=3)

    validate_result(record_of([measured]))
    ready = DEPTH if variant is Variant.READY else DEPTH // 2
    assert (measured["depth"], measured["ready"], measured["claims"]) == (DEPTH, ready, CLAIMS)
    assert measured["latency_ms"]["n"] == CLAIMS
    assert measured["latency_ms"]["p50"] > 0
    plan = measured["explain"]
    assert plan["runs"] == 3
    assert "LockRows" in plan["plan_nodes"]
    assert any(node.endswith("on tasks") for node in plan["plan_nodes"])

    # The claims were real: each made an attempt, and each claimed task was completed.
    assert scalar(db, "SELECT count(*) FROM attempts") == CLAIMS
    assert scalar(db, "SELECT count(*) FROM tasks WHERE state = 'succeeded'") == CLAIMS
    claimed_kinds = db.execute(
        "SELECT DISTINCT j.kind FROM attempts a JOIN tasks t ON t.id = a.task_id "
        "JOIN jobs j ON j.id = t.job_id"
    ).fetchall()
    assert claimed_kinds == [(CLAIMED_KIND,)]
    if variant is Variant.WAITING:
        waiting = "SELECT count(*) FROM tasks WHERE available_at > statement_timestamp()"
        assert scalar(db, waiting) == DEPTH // 2
        assert scalar(db, f"{waiting} AND state <> 'queued'") == 0
    if variant is Variant.UNSUPPORTED:
        held = (
            "SELECT count(*) FROM tasks t JOIN jobs j ON j.id = t.job_id "
            "WHERE j.kind = %s AND t.state = 'queued' AND t.attempt_count = 0"
        )
        assert scalar(db, held, UNSUPPORTED_KIND) == DEPTH // 2


def test_the_waiting_variant_is_measured_on_a_heap_of_its_live_rows(worker_conn: Conn) -> None:
    """Its UPDATE leaves no dead versions for the claim's sequential scan to read (DEC-033).

    Rewriting the heap again shrinks it only if it still held dead versions.
    """
    fill(worker_conn, 2_000, Variant.WAITING)
    measured = heap_pages(worker_conn)
    worker_conn.execute("VACUUM FULL tasks")
    assert heap_pages(worker_conn) == measured > 0


def test_claims_never_exceed_the_claimable_tasks(db: Conn, schema_url: str) -> None:
    measured = measure_depth(schema_url, depth=10, variant=Variant.WAITING, claims=50, explains=1)
    assert (measured["ready"], measured["claims"]) == (5, 5)
    validate_result(record_of([measured]))


def test_explain_rolls_back_and_leaves_no_lock(db: Conn, worker_conn: Conn) -> None:
    fill(worker_conn, DEPTH, Variant.READY)
    before = db.execute("SELECT id, state, attempt_count FROM tasks ORDER BY id").fetchall()

    # Control: EXPLAIN ANALYZE really executes the statement and locks a task, so the
    # probe below would notice a transaction that was not rolled back.
    with worker_conn.transaction(force_rollback=True):
        worker_conn.execute(_EXPLAIN, ([CLAIMED_KIND],))
        with pytest.raises(LockNotAvailable), db.transaction():
            db.execute("SELECT id FROM tasks FOR UPDATE NOWAIT").fetchall()

    plan = explain_claims(worker_conn, 3)

    assert plan["runs"] == 3
    assert worker_conn.info.transaction_status is TransactionStatus.IDLE
    with db.transaction():
        assert len(db.execute("SELECT id FROM tasks FOR UPDATE NOWAIT").fetchall()) == DEPTH
    assert db.execute("SELECT id, state, attempt_count FROM tasks ORDER BY id").fetchall() == before
    assert scalar(db, "SELECT count(*) FROM attempts") == 0


def test_the_command_refuses_any_database_but_the_benchmark_one(
    database_url: str, monkeypatch: pytest.MonkeyPatch, capsys: Any, tmp_path: Path
) -> None:
    out = tmp_path / "claims.json"
    monkeypatch.setenv(BENCH_DATABASE_URL_ENV, database_url)  # the test database

    assert main(["--depths", "100", "--out", str(out), "--allow-dirty"]) == 1

    assert not out.exists()
    assert BENCH_DATABASE_NAME in capsys.readouterr().err
    monkeypatch.delenv(BENCH_DATABASE_URL_ENV)
    assert main(["--out", str(out)]) == 1
    assert not out.exists()


def test_a_malformed_record_is_refused() -> None:
    good: dict[str, Any] = {
        "variant": "ready",
        "depth": 10,
        "ready": 10,
        "heap_pages": 1,
        "claims": 1,
        "latency_ms": distribution([1.0]),
        "explain": {
            "runs": 1,
            "execution_ms": distribution([0.5]),
            "planning_ms": distribution([0.1]),
            "plan_nodes": ["LockRows"],
        },
    }
    validate_result(record_of([good]))
    for broken in (
        {**good, "variant": "other"},
        {**good, "claims": 11},
        {**good, "latency_ms": distribution([1.0, 2.0])},
        {**good, "explain": {**good["explain"], "plan_nodes": []}},
        {**good, "heap_pages": -1},
        {key: value for key, value in good.items() if key != "heap_pages"},
    ):
        with pytest.raises(ValueError, match="must"):
            validate_result(record_of([broken]))
    with pytest.raises(ValueError, match="at least one"):
        validate_result(record_of([]))


def test_the_distribution_uses_nearest_rank() -> None:
    values = [float(n) for n in range(1, 101)]
    assert distribution(values) == {"n": 100, "p50": 50.0, "p95": 95.0, "max": 100.0}
    assert distribution([3.0]) == {"n": 1, "p50": 3.0, "p95": 3.0, "max": 3.0}
    assert distribution([]) == {"n": 0, "p50": 0.0, "p95": 0.0, "max": 0.0}


# --- churn (P2: the claim index under concurrent claiming) ----------------------------


@pytest.mark.parametrize("condition", list(Condition), ids=[c.value for c in Condition])
def test_a_churn_run_claims_every_task_and_records_the_index_it_read(
    db: Conn, schema_url: str, condition: Condition
) -> None:
    run = churn_run(schema_url, condition=condition, clients=2, depth=50, vacuum_every=10)
    assert (run["ready"], run["claims"]) == (50, 50)  # every task claimed exactly once
    assert run["latency_ms"]["n"] == 50
    assert set(run["table"]) == set(TABLE_FIELDS)
    assert run["table"]["n_tup_upd"] >= 2 * 50  # each task: claimed, then completed
    if condition is Condition.NONE:
        assert run["index"] is None
        assert run["derived"]["index_entries_read_per_claim"] is None
    else:
        assert run["index"] is not None
        assert run["index"]["idx_scan"] >= 50  # every claim scanned the index
        assert run["derived"]["index_entries_read_per_claim"] is not None
    assert (run["vacuums"] > 0) == (condition is Condition.INDEX_VACUUM)
    with psycopg.connect(schema_url) as conn:
        assert scalar(conn, "SELECT count(*) FROM tasks WHERE state <> 'succeeded'") == 0
