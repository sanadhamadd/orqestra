"""Claim cost against queue depth (DEC-033; DEC-062, "Claim index").

Every claim runs one discovery statement that filters the queued tasks by
state, ``available_at`` and job kind, sorts them by ``created_at, id`` and
locks the first with ``FOR UPDATE SKIP LOCKED``. No index serves it yet, so its
cost may grow with the queue. This module measures that, for three shapes of
queue at each depth:

- ``ready``: every queued task is claimable;
- ``waiting``: the half that sorts first waits on an ``available_at`` one day
  ahead, as a requeued task waits out its retry delay (DEC-053);
- ``unsupported``: half the tasks belong to an earlier job of a kind the
  claimant does not run, so they sort first and are never claimed.

For each depth and shape it fills a fresh, migrated schema through
``submit_job``, then rewrites ``tasks`` with ``VACUUM (FULL, ANALYZE)``, so every
shape is scanned over a compact heap of its own live rows, and records that
heap's pages. Then it:

1. runs ``EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`` of the exact statement
   ``claim_next_attempt`` runs, imported rather than copied so the two cannot
   drift. ``ANALYZE`` executes the statement, which locks one task, so each run
   is inside a transaction that always rolls back: it changes nothing and
   leaves no lock;
2. times real ``claim_next_attempt`` calls, each completed at once. The
   latency is the client's round trip (``time.perf_counter``), not a database
   clock figure. A run never claims more tasks than are claimable.

The figures depend on the machine and the server. They are recorded, never
asserted, and only a measurement campaign may cite them. Run against the
benchmark database (``benchmarks.database``)::

    uv run python -m benchmarks.claims --out build/bench/claims.json
"""

import argparse
import json
import math
import os
import platform
import sys
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import TupleRow

from benchmarks.database import (
    UnsafeBenchDatabaseError,
    connect_verified,
    create_run_schema,
    drop_run_schema,
    require_bench_database_url,
    run_schema_name,
    schema_url,
    unreachable_message,
)
from benchmarks.result import code_state, server_info
from orqestra.domain.jobs import AttemptSucceeded, JobSubmission, TaskSpec
from orqestra.domain.leases import LeasePolicy
from orqestra.domain.retry import RetryPolicy
from orqestra.persistence.jobs import submit_job

# The discovery statement and its state list, exactly as claim_next_attempt sends them.
from orqestra.persistence.lifecycle import (
    _CLAIMABLE_STATES,
    _LOCK_NEXT_CLAIMABLE_TASK,
    claim_next_attempt,
    complete_attempt,
)
from orqestra.persistence.migrate import migrate

FORMAT = 1
RESULT_KIND = "claim_cost"
CLAIMED_KIND = "diagnostic.echo"
UNSUPPORTED_KIND = "bench.unsupported"
WORKER_ID = "bench-claims"
DEFAULT_DEPTHS = (1_000, 10_000, 50_000, 100_000)
DEFAULT_CLAIMS = 200
DEFAULT_EXPLAINS = 20
# Measurement values, not operating recommendations (DEC-034): long enough that
# no lease ends while a run is measured, so recovery never interferes.
LEASE = LeasePolicy(lease=timedelta(minutes=10), execution_timeout=timedelta(minutes=10))
RETRY = RetryPolicy(base_delay=timedelta(seconds=1), max_delay=timedelta(seconds=1))

_EXPLAIN = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {_LOCK_NEXT_CLAIMABLE_TASK}"
_DELAY_FIRST = (
    "UPDATE tasks SET available_at = statement_timestamp() + interval '1 day' "
    "WHERE id IN (SELECT id FROM tasks ORDER BY created_at, id LIMIT %s)"
)

Conn = psycopg.Connection[TupleRow]


class Variant(StrEnum):
    """The shape of the queue a measurement runs against."""

    READY = "ready"
    WAITING = "waiting"
    UNSUPPORTED = "unsupported"


def distribution(values: Sequence[float]) -> dict[str, float | int]:
    """``n``, and the nearest-rank p50 and p95 and the maximum of ``values`` (zeros when empty)."""
    ordered = sorted(values)
    if not ordered:
        return {"n": 0, "p50": 0.0, "p95": 0.0, "max": 0.0}

    def rank(fraction: float) -> float:
        return ordered[max(math.ceil(fraction * len(ordered)), 1) - 1]

    return {"n": len(ordered), "p50": rank(0.5), "p95": rank(0.95), "max": ordered[-1]}


def _tasks(count: int, prefix: str) -> tuple[TaskSpec, ...]:
    return tuple(TaskSpec(f"{prefix}-{index:07d}", {}, 1) for index in range(count))


def fill(conn: Conn, depth: int, variant: Variant) -> int:
    """Queue ``depth`` tasks shaped as ``variant``; return how many are claimable now."""
    if isinstance(depth, bool) or not isinstance(depth, int) or depth < 2:
        msg = "depth must be an integer of at least 2"
        raise ValueError(msg)
    held = depth // 2
    if variant is Variant.UNSUPPORTED:
        # Submitted first, so its tasks sort ahead of the claimable ones.
        submit_job(conn, JobSubmission(UNSUPPORTED_KIND, _tasks(held, "unsupported")))
        submit_job(conn, JobSubmission(CLAIMED_KIND, _tasks(depth - held, "task")))
    else:
        submit_job(conn, JobSubmission(CLAIMED_KIND, _tasks(depth, "task")))
        if variant is Variant.READY:
            held = 0
        else:
            conn.execute(_DELAY_FIRST, (held,))
    # Every variant is measured on a compact heap of its own live rows: the waiting
    # variant's UPDATE leaves a dead version of half its rows, which the claim's
    # sequential scan would otherwise read too. VACUUM FULL rewrites the table
    # without them; a plain VACUUM would free them but keep the larger heap.
    conn.execute("VACUUM (FULL, ANALYZE) tasks")
    conn.execute("ANALYZE jobs")
    return depth - held


def heap_pages(conn: Conn) -> int:
    """The pages of the ``tasks`` heap, which the claim's sequential scan reads."""
    row = conn.execute(
        "SELECT pg_relation_size('tasks') / current_setting('block_size')::int"
    ).fetchone()
    return int(row[0]) if row else 0


def _plan_nodes(node: dict[str, Any]) -> list[str]:
    """The plan's node types in pre-order, with the relation where there is one."""
    name = str(node["Node Type"])
    if "Relation Name" in node:
        name = f"{name} on {node['Relation Name']}"
    nodes = [name]
    for child in node.get("Plans", []):
        nodes.extend(_plan_nodes(child))
    return nodes


def explain_claims(conn: Conn, runs: int) -> dict[str, Any]:
    """``runs`` executions of the discovery statement under ``EXPLAIN``, each rolled back.

    ``conn`` must be in autocommit mode and outside a transaction.
    """
    if isinstance(runs, bool) or not isinstance(runs, int) or runs < 1:
        msg = "explains must be a positive integer"
        raise ValueError(msg)
    execution: list[float] = []
    planning: list[float] = []
    shapes: list[list[str]] = []
    hit = read = 0
    for _ in range(runs):
        with conn.transaction(force_rollback=True):
            row = conn.execute(_EXPLAIN, (_CLAIMABLE_STATES, [CLAIMED_KIND])).fetchone()
        if row is None:
            msg = "EXPLAIN returned no plan"
            raise RuntimeError(msg)
        document = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        plan = document[0]
        execution.append(float(plan["Execution Time"]))
        planning.append(float(plan["Planning Time"]))
        shapes.append(_plan_nodes(plan["Plan"]))
        hit = int(plan["Plan"].get("Shared Hit Blocks", 0))
        read = int(plan["Plan"].get("Shared Read Blocks", 0))
    return {
        "runs": runs,
        "execution_ms": distribution(execution),
        "planning_ms": distribution(planning),
        "plan_nodes": shapes[0],
        "consistent_shape": all(shape == shapes[0] for shape in shapes),
        "last_shared_hit_blocks": hit,
        "last_shared_read_blocks": read,
    }


def time_claims(conn: Conn, count: int) -> list[float]:
    """Milliseconds of ``count`` real claims, each completed at once after it is timed."""
    latencies: list[float] = []
    for _ in range(count):
        started = time.perf_counter()
        attempt = claim_next_attempt(conn, WORKER_ID, kinds=[CLAIMED_KIND], policy=LEASE)
        latencies.append((time.perf_counter() - started) * 1000)
        if attempt is None:
            msg = "a claimable task was expected"
            raise RuntimeError(msg)
        complete_attempt(
            conn,
            attempt.id,
            generation=attempt.attempt_number,
            outcome=AttemptSucceeded({}),
            retry_policy=RETRY,
        )
    return latencies


def measure_depth(
    url: str, *, depth: int, variant: Variant, claims: int, explains: int
) -> dict[str, Any]:
    """One measurement in the empty, migrated schema ``url`` names.

    The caller owns the target: ``url`` must already reach a schema it verified
    and may fill. ``claims`` is capped at the number of claimable tasks.
    """
    if isinstance(claims, bool) or not isinstance(claims, int) or claims < 0:
        msg = "claims must be a non-negative integer"
        raise ValueError(msg)
    with psycopg.connect(url, autocommit=True) as conn:
        ready = fill(conn, depth, variant)
        pages = heap_pages(conn)
        plan = explain_claims(conn, explains)
        latencies = time_claims(conn, min(claims, ready))
    return {
        "variant": variant.value,
        "depth": depth,
        "ready": ready,
        "heap_pages": pages,
        "claims": len(latencies),
        "latency_ms": distribution(latencies),
        "explain": plan,
    }


def build_record(
    measurements: list[dict[str, Any]],
    *,
    code: dict[str, Any],
    server: dict[str, str],
    settings: dict[str, Any],
) -> dict[str, Any]:
    """The result record (format 1) for ``measurements``."""
    return {
        "format": FORMAT,
        "kind": RESULT_KIND,
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "environment": {
            **code,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "postgres": server,
        },
        "settings": settings,
        "measurements": measurements,
    }


def _check_distribution(name: str, value: object, n: int | None = None) -> None:
    if not isinstance(value, dict) or set(value) != {"n", "p50", "p95", "max"}:
        msg = f"{name} must hold n, p50, p95 and max"
        raise ValueError(msg)
    if n is not None and value["n"] != n:
        msg = f"{name}.n must be {n}"
        raise ValueError(msg)
    if not value["p50"] <= value["p95"] <= value["max"] or value["p50"] < 0:
        msg = f"{name} must be ordered and not negative"
        raise ValueError(msg)


def validate_result(record: object) -> None:
    """Raise ``ValueError`` unless ``record`` is a well-formed claim-cost result."""
    if not isinstance(record, dict):
        msg = "a result must be an object"
        raise ValueError(msg)
    if record.get("format") != FORMAT or record.get("kind") != RESULT_KIND:
        msg = f"a result must have format {FORMAT} and kind {RESULT_KIND!r}"
        raise ValueError(msg)
    environment = record.get("environment")
    if not isinstance(environment, dict) or not {"git_sha", "git_dirty", "postgres"} <= set(
        environment
    ):
        msg = "the environment must name the commit, its state and the server"
        raise ValueError(msg)
    measurements = record.get("measurements")
    if not isinstance(measurements, list) or not measurements:
        msg = "a result must hold at least one measurement"
        raise ValueError(msg)
    for item in measurements:
        if not isinstance(item, dict) or item.get("variant") not in set(Variant):
            msg = "each measurement must name a known variant"
            raise ValueError(msg)
        if not 0 <= item["claims"] <= item["ready"] <= item["depth"]:
            msg = "claims must not exceed the claimable tasks, nor those the depth"
            raise ValueError(msg)
        pages = item.get("heap_pages")
        if isinstance(pages, bool) or not isinstance(pages, int) or pages < 0:
            msg = "each measurement must record its heap's pages"
            raise ValueError(msg)
        _check_distribution("latency_ms", item["latency_ms"], item["claims"])
        plan = item["explain"]
        _check_distribution("explain.execution_ms", plan["execution_ms"], plan["runs"])
        _check_distribution("explain.planning_ms", plan["planning_ms"], plan["runs"])
        if not plan["plan_nodes"] or not all(isinstance(n, str) for n in plan["plan_nodes"]):
            msg = "each measurement must record its plan's node types"
            raise ValueError(msg)


def _depths(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split(",") if part.strip())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.claims", description=__doc__)
    parser.add_argument("--depths", type=_depths, default=DEFAULT_DEPTHS)
    parser.add_argument("--claims", type=int, default=DEFAULT_CLAIMS)
    parser.add_argument("--explains", type=int, default=DEFAULT_EXPLAINS)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--keep-schema", action="store_true")
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args(argv)

    try:
        return _measure(args)
    except (ValueError, UnsafeBenchDatabaseError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError as exc:
        print(f"refused: {unreachable_message(exc)}", file=sys.stderr)
        return 1


def _measure(args: argparse.Namespace) -> int:
    url = require_bench_database_url(os.environ)
    with connect_verified(url) as admin:
        code = code_state()
        if code["git_dirty"] and not args.allow_dirty:
            print("refused: the working tree has uncommitted changes", file=sys.stderr)
            return 1
        run_id = uuid.uuid4().hex[:12]
        measurements = []
        for variant in Variant:
            for depth in args.depths:
                schema = run_schema_name(run_id, f"{variant.value[:4]}{depth}")
                create_run_schema(admin, schema)
                try:
                    target = schema_url(admin, url, schema)
                    migrate(target)
                    measurements.append(
                        measure_depth(
                            target,
                            depth=depth,
                            variant=variant,
                            claims=args.claims,
                            explains=args.explains,
                        )
                    )
                finally:
                    if not args.keep_schema:
                        drop_run_schema(admin, schema)
        server = server_info(admin)
    settings = {
        "depths": list(args.depths),
        "claims": args.claims,
        "explains": args.explains,
        "lease": LEASE.lease.total_seconds(),
        "claimed_kind": CLAIMED_KIND,
    }
    record = build_record(measurements, code=code, server=server, settings=settings)
    validate_result(record)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
