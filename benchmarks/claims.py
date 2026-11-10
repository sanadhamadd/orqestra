"""Claim cost against queue depth (DEC-033; DEC-062, "Claim index").

Every claim runs one discovery statement that filters the queued tasks by
state, ``available_at`` and job kind, sorts them by ``created_at, id`` and
locks the first with ``FOR UPDATE SKIP LOCKED``. Since migration 0006 the partial
index ``tasks_claim_order`` serves it (DEC-069); without it, the cost grows with
the queue. This module measures that cost, for three shapes of queue at each
depth:

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

``churn`` (a subcommand) instead claims a whole ready queue with several
concurrent clients, each a thread with its own connection that runs the real
``claim_next_attempt`` and then ``complete_attempt``, and records what the
claim index (migration 0006) reads while it does: the index's own counters
(``pg_stat_user_indexes``), so the index entries each claim reads, and the
table's update, dead-tuple and vacuum counters. It compares three conditions:
``none`` (the index dropped in the run's own schema), ``index``, and
``index+vacuum`` (a separate connection runs ``VACUUM tasks`` every so many
completed claims). It has no worker processes, idle polling, heartbeats or
sweeps; it measures the claim path alone::

    uv run python -m benchmarks.claims churn --out build/bench/churn.json
"""

import argparse
import json
import math
import os
import platform
import sys
import threading
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

# The discovery statement, exactly as claim_next_attempt sends it.
from orqestra.persistence.lifecycle import (
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
            row = conn.execute(_EXPLAIN, ([CLAIMED_KIND],)).fetchone()
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


# --- churn: the claim index under concurrent claiming ---------------------------------

CHURN_KIND = "claim_churn"
CHURN_DEPTH = 10_000
CHURN_CLIENTS = (1, 4, 8)
CHURN_REPETITIONS = 3
CHURN_VACUUM_EVERY = 500
# Counters are published at most once a second, and when a session ends.
STATS_SETTLE = 2.0
INDEX = "tasks_claim_order"


class Condition(StrEnum):
    """What serves the claim during a churn run."""

    NONE = "none"  # the index dropped in the run's own schema: a scan and a sort
    INDEX = "index"  # migration 0006, default autovacuum
    INDEX_VACUUM = "index+vacuum"  # 0006, and VACUUM tasks every vacuum_every claims


_INDEX_COUNTERS = (
    "SELECT idx_scan::float8, idx_tup_read::float8, idx_tup_fetch::float8 "
    "FROM pg_stat_user_indexes WHERE schemaname = current_schema() AND indexrelname = %s"
)
_TABLE_COUNTERS = (
    "SELECT n_tup_upd::float8, n_tup_hot_upd::float8, n_dead_tup::float8, "
    "vacuum_count::float8, autovacuum_count::float8 "
    "FROM pg_stat_user_tables WHERE schemaname = current_schema() AND relname = 'tasks'"
)
INDEX_FIELDS = ("idx_scan", "idx_tup_read", "idx_tup_fetch")
TABLE_FIELDS = ("n_tup_upd", "n_tup_hot_upd", "n_dead_tup", "vacuum_count", "autovacuum_count")


def churn_counters(conn: Conn) -> tuple[dict[str, float] | None, dict[str, float]]:
    """The claim index's counters (``None`` without the index) and ``tasks``'s, as published."""
    index = conn.execute(_INDEX_COUNTERS, (INDEX,)).fetchone()
    table = conn.execute(_TABLE_COUNTERS).fetchone()
    if table is None:
        msg = "no statistics for tasks in this schema"
        raise RuntimeError(msg)
    return (
        None if index is None else dict(zip(INDEX_FIELDS, index, strict=True)),
        dict(zip(TABLE_FIELDS, table, strict=True)),
    )


def _delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    return {name: after[name] - before[name] for name in before}


def churn_run(
    url: str, *, condition: Condition, clients: int, depth: int, vacuum_every: int
) -> dict[str, Any]:
    """Claim and complete a ready queue of ``depth`` tasks with ``clients`` concurrent clients."""
    if clients < 1 or depth < 1 or vacuum_every < 1:
        msg = "clients, depth and vacuum_every must be positive"
        raise ValueError(msg)
    with psycopg.connect(url, autocommit=True) as conn:
        if condition is Condition.NONE:
            conn.execute(f"DROP INDEX {INDEX}")
        ready = fill(conn, depth, Variant.READY)
        time.sleep(STATS_SETTLE)
        index_before, table_before = churn_counters(conn)

        lock = threading.Lock()
        done = [0]
        latencies: list[list[float]] = [[] for _ in range(clients)]
        failures: list[BaseException] = []
        vacuum_due = threading.Event()
        finished = threading.Event()
        vacuums = [0]

        def claim_all(own: list[float]) -> None:
            try:
                with psycopg.connect(url, autocommit=True) as client:
                    while True:
                        started = time.perf_counter()
                        attempt = claim_next_attempt(
                            client, WORKER_ID, kinds=[CLAIMED_KIND], policy=LEASE
                        )
                        if attempt is None:
                            return
                        own.append((time.perf_counter() - started) * 1000)
                        complete_attempt(
                            client,
                            attempt.id,
                            generation=attempt.attempt_number,
                            outcome=AttemptSucceeded({}),
                            retry_policy=RETRY,
                        )
                        with lock:
                            done[0] += 1
                            if done[0] % vacuum_every == 0:
                                vacuum_due.set()
            except BaseException as exc:  # reported after every client has ended
                failures.append(exc)

        def vacuum_periodically() -> None:
            try:
                with psycopg.connect(url, autocommit=True) as vacuumer:
                    while not finished.is_set():
                        if vacuum_due.wait(0.05):
                            vacuum_due.clear()
                            vacuumer.execute("VACUUM tasks")
                            vacuums[0] += 1
            except BaseException as exc:
                failures.append(exc)

        threads = [
            threading.Thread(target=claim_all, args=(own,), name=f"churn-client-{n}")
            for n, own in enumerate(latencies)
        ]
        vacuumer = (
            threading.Thread(target=vacuum_periodically, name="churn-vacuum")
            if condition is Condition.INDEX_VACUUM
            else None
        )
        if vacuumer is not None:
            vacuumer.start()
        began = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        elapsed = time.perf_counter() - began
        finished.set()
        if vacuumer is not None:
            vacuumer.join()
        if failures:
            raise failures[0]
        time.sleep(STATS_SETTLE)  # the clients' sessions ended, which publishes their counters
        index_after, table_after = churn_counters(conn)

    claims = sum(len(own) for own in latencies)
    index = (
        None if index_before is None or index_after is None else _delta(index_before, index_after)
    )
    return {
        "condition": condition.value,
        "clients": clients,
        "depth": depth,
        "ready": ready,
        "claims": claims,
        "elapsed_s": elapsed,
        "latency_ms": distribution([value for own in latencies for value in own]),
        "vacuums": vacuums[0],
        "index": index,
        "table": _delta(table_before, table_after),
        "derived": {
            "formula": "per claim: the index counter's growth over the run / claims",
            "index_entries_read_per_claim": (
                None if index is None or not claims else index["idx_tup_read"] / claims
            ),
            "heap_fetches_per_claim": (
                None if index is None or not claims else index["idx_tup_fetch"] / claims
            ),
        },
    }


_AUTOVACUUM_SETTINGS = (
    "SELECT name, setting FROM pg_settings WHERE name LIKE 'autovacuum%' ORDER BY name"
)


def validate_churn(record: object) -> None:
    """Raise ``ValueError`` unless ``record`` is a well-formed churn result."""
    if not isinstance(record, dict):
        msg = "a result must be an object"
        raise ValueError(msg)
    if record.get("format") != FORMAT or record.get("kind") != CHURN_KIND:
        msg = f"a result must have format {FORMAT} and kind {CHURN_KIND!r}"
        raise ValueError(msg)
    environment = record.get("environment")
    if not isinstance(environment, dict) or not {
        "git_sha",
        "git_dirty",
        "postgres",
        "autovacuum",
    } <= set(environment):
        msg = "the environment must name the commit, its state, the server and autovacuum"
        raise ValueError(msg)
    runs = record.get("runs")
    if not isinstance(runs, list) or not runs:
        msg = "a result must hold at least one run"
        raise ValueError(msg)
    for run in runs:
        if not isinstance(run, dict) or run.get("condition") not in set(Condition):
            msg = "each run must name a known condition"
            raise ValueError(msg)
        if run["clients"] < 1 or not 0 <= run["claims"] <= run["ready"] <= run["depth"]:
            msg = "each run must have clients, and claims within its ready tasks"
            raise ValueError(msg)
        if run["elapsed_s"] < 0:
            msg = "elapsed_s must not be negative"
            raise ValueError(msg)
        _check_distribution("latency_ms", run["latency_ms"], run["claims"])
        if (run["index"] is None) != (run["condition"] == Condition.NONE):
            msg = "index counters must be recorded exactly when the index exists"
            raise ValueError(msg)
        if run["vacuums"] and run["condition"] != Condition.INDEX_VACUUM:
            msg = "vacuums must be 0 unless the condition is index+vacuum"
            raise ValueError(msg)
        if not isinstance(run["table"], dict) or set(run["table"]) != set(TABLE_FIELDS):
            msg = "each run must record the table's counters"
            raise ValueError(msg)


def _clients(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split(",") if part.strip())


def churn_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks.claims churn",
        description="The claim index under concurrent claiming (see the module docstring).",
    )
    parser.add_argument("--depth", type=int, default=CHURN_DEPTH)
    parser.add_argument("--clients", type=_clients, default=CHURN_CLIENTS)
    parser.add_argument("--repetitions", type=int, default=CHURN_REPETITIONS)
    parser.add_argument("--vacuum-every", type=int, default=CHURN_VACUUM_EVERY)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--keep-schema", action="store_true")
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args(argv)
    try:
        return _churn(args)
    except (ValueError, UnsafeBenchDatabaseError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError as exc:
        print(f"refused: {unreachable_message(exc)}", file=sys.stderr)
        return 1


def _churn(args: argparse.Namespace) -> int:
    url = require_bench_database_url(os.environ)
    with connect_verified(url) as admin:
        code = code_state()
        if code["git_dirty"] and not args.allow_dirty:
            print("refused: the working tree has uncommitted changes", file=sys.stderr)
            return 1
        run_id = uuid.uuid4().hex[:12]
        runs = []
        # Interleaved: every condition and client count once per repetition.
        for repetition in range(1, args.repetitions + 1):
            for number, condition in enumerate(Condition):
                for clients in args.clients:
                    schema = run_schema_name(run_id, f"churn{number}c{clients}r{repetition}")
                    create_run_schema(admin, schema)
                    try:
                        target = schema_url(admin, url, schema)
                        migrate(target)
                        run = churn_run(
                            target,
                            condition=condition,
                            clients=clients,
                            depth=args.depth,
                            vacuum_every=args.vacuum_every,
                        )
                        runs.append({**run, "repetition": repetition})
                    finally:
                        if not args.keep_schema:
                            drop_run_schema(admin, schema)
        server = server_info(admin)
        autovacuum = {str(name): str(value) for name, value in admin.execute(_AUTOVACUUM_SETTINGS)}
    record = {
        "format": FORMAT,
        "kind": CHURN_KIND,
        "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "environment": {
            **code,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "postgres": server,
            "autovacuum": autovacuum,
        },
        "settings": {
            "depth": args.depth,
            "clients": list(args.clients),
            "repetitions": args.repetitions,
            "vacuum_every": args.vacuum_every,
            "conditions": [condition.value for condition in Condition],
            "claimed_kind": CLAIMED_KIND,
        },
        "runs": runs,
    }
    validate_churn(record)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.out)
    return 0


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
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] == ["churn"]:
        return churn_main(arguments[1:])
    args = parser.parse_args(arguments)

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
