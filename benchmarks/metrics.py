"""What a benchmark cell measures, and how each figure is derived.

Every latency and throughput figure comes from timestamps PostgreSQL stored
(``jobs``, ``tasks``, ``attempts``), so one clock serves them all and each can
be derived again from the rows after the run. Harness clocks are used only for
what the database cannot see: the submission command and the idle window.

- **Percentiles** are nearest-rank: the value at rank ``ceil(q * n)`` of the
  sorted values, so every figure reported is one that was observed.
- **Steady throughput** counts the middle 80 % of completions by
  ``finished_at``: the first and last 10 % are dropped, and the items of the
  completions after the window's first one are divided by the window's length.
- **Database load** is the delta of the ``pg_stat_database`` counters, and of
  ``pg_stat_user_tables`` for the job tables, from just before the submission
  to a moment after the job ended. A backend publishes its counters at most
  once a second, so that second reading waits a little (``run.STATS_SETTLE``)
  and includes the idle claims made meanwhile. The figures are approximate.
- **Idle claim rate** is the ``xact_commit`` delta across an idle window that
  follows, divided by the window's length on the harness clock. Every idle
  claim is one committed transaction; the publication delay is about the same
  at both ends of the window, and the harness's own reads also commit, so it is
  an estimate. It is recorded, never asserted.
- **Worker CPU and memory** come from ``os.wait4`` once a worker has exited:
  ``ru_maxrss`` is in bytes on macOS and in KiB on Linux, normalised to bytes.
  CPU covers the whole process, start-up and idle tail included, so its
  utilization over the makespan can exceed 1 for a short cell.
- **Database CPU and memory** come from ``docker stats`` only when the server
  is a named container; otherwise they are ``not_measured``.
"""

import itertools
import json
import math
import os
import resource
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import TupleRow

Conn = psycopg.Connection[TupleRow]
NOT_MEASURED = "not_measured"
STEADY_TRIM_PERCENT = 10  # dropped from each end: the middle 80 % remain

DATABASE_COUNTERS = (
    "xact_commit",
    "xact_rollback",
    "tup_inserted",
    "tup_updated",
    "tup_fetched",
    "blks_read",
    "blks_hit",
    "deadlocks",
    "temp_bytes",
)
TABLE_COUNTERS = ("seq_scan", "idx_scan", "n_tup_upd", "n_dead_tup")
TABLES = ("tasks", "attempts", "provider_calls")


def percentiles(values: Sequence[float]) -> dict[str, float | int | None]:
    """``n`` and the nearest-rank p50, p95, p99 and maximum of ``values``; ``None`` when empty."""
    ordered = sorted(values)
    if not ordered:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "max": None}

    def rank(fraction: float) -> float:
        return ordered[max(math.ceil(fraction * len(ordered)), 1) - 1]

    return {
        "n": len(ordered),
        "p50": rank(0.50),
        "p95": rank(0.95),
        "p99": rank(0.99),
        "max": ordered[-1],
    }


def delta(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    """``after - before`` for every counter, recursing into nested mappings.

    Both snapshots must name the same counters. A counter may go down (a reset,
    or ``n_dead_tup`` after a vacuum); the delta is then negative, never clamped.
    """
    if set(before) != set(after):
        msg = "both snapshots must hold the same counters"
        raise ValueError(msg)
    result: dict[str, Any] = {}
    for key in sorted(before):
        old, new = before[key], after[key]
        if isinstance(old, Mapping) and isinstance(new, Mapping):
            result[key] = delta(old, new)
        elif _number(old) and _number(new):
            result[key] = new - old
        else:
            msg = f"counter {key!r} is not a number in both snapshots"
            raise ValueError(msg)
    return result


def _number(value: object) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def rate(count: float, seconds: float | None) -> float | None:
    """``count / seconds``, or ``None`` when there is no positive duration."""
    if seconds is None or seconds <= 0:
        return None
    return count / seconds


def steady_throughput(completions: Sequence[tuple[float, int]]) -> dict[str, Any]:
    """Items per second over the middle 80 % of ``(finished_at, items)`` pairs."""
    ordered = sorted(completions)
    trim = len(ordered) * STEADY_TRIM_PERCENT // 100  # integers: 10 completions trim exactly 1
    window = ordered[trim : len(ordered) - trim]
    if len(window) < 2:
        return {"completions": len(window), "seconds": None, "items_per_s": None}
    seconds = window[-1][0] - window[0][0]
    items = sum(count for _, count in window[1:])
    return {"completions": len(window), "seconds": seconds, "items_per_s": rate(items, seconds)}


def claim_gaps(attempts: Sequence[tuple[str, float, float | None]]) -> list[float]:
    """Per worker, each attempt's ``started_at`` minus the previous attempt's ``finished_at``.

    ``attempts`` are ``(worker_id, started_at, finished_at)``. An attempt that
    never finished opens no gap.
    """
    by_worker: dict[str, list[tuple[float, float | None]]] = defaultdict(list)
    for worker_id, started, finished in attempts:
        by_worker[worker_id].append((started, finished))
    gaps: list[float] = []
    for runs in by_worker.values():
        runs.sort()
        for (_, finished), (started, _) in itertools.pairwise(runs):
            if finished is not None:
                gaps.append(started - finished)
    return gaps


def maxrss_bytes(maxrss: int, platform: str = sys.platform) -> int:
    """``ru_maxrss`` in bytes: macOS reports bytes, Linux KiB."""
    return maxrss if platform == "darwin" else maxrss * 1024


# --- stored timestamps -------------------------------------------------------------------


_TASKS = """
SELECT t.task_key,
       EXTRACT(EPOCH FROM t.created_at)::float8,
       EXTRACT(EPOCH FROM t.finished_at)::float8,
       CASE WHEN jsonb_typeof(t.payload -> 'items') = 'array'
            THEN jsonb_array_length(t.payload -> 'items') ELSE 1 END,
       (SELECT EXTRACT(EPOCH FROM min(a.started_at))::float8
          FROM attempts a WHERE a.task_id = t.id)
  FROM tasks t WHERE t.job_id = %s
"""
_ATTEMPTS = """
SELECT a.worker_id, a.state,
       EXTRACT(EPOCH FROM a.started_at)::float8,
       EXTRACT(EPOCH FROM a.finished_at)::float8
  FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s
"""
_JOB_CREATED = "SELECT EXTRACT(EPOCH FROM created_at)::float8 FROM jobs WHERE id = %s"


@dataclass(frozen=True, slots=True)
class StoredTimes:
    """The stored timestamps of one job, as epoch seconds on the server's clock."""

    job_created: float
    # (task_key, created_at, finished_at, items, first attempt's started_at)
    tasks: tuple[tuple[str, float, float | None, int, float | None], ...]
    # (worker_id, state, started_at, finished_at)
    attempts: tuple[tuple[str, str, float, float | None], ...]


def read_stored_times(conn: Conn, job_id: uuid.UUID) -> StoredTimes:
    row = conn.execute(_JOB_CREATED, (job_id,)).fetchone()
    if row is None:
        msg = f"job {job_id} not found"
        raise ValueError(msg)
    tasks = tuple(
        (str(key), float(created), _opt(finished), int(items), _opt(first))
        for key, created, finished, items, first in conn.execute(_TASKS, (job_id,))
    )
    attempts = tuple(
        (str(worker), str(state), float(started), _opt(finished))
        for worker, state, started, finished in conn.execute(_ATTEMPTS, (job_id,))
    )
    return StoredTimes(float(row[0]), tasks, attempts)


def _opt(value: object) -> float | None:
    return None if value is None else float(value)  # type: ignore[arg-type]


def timing_figures(times: StoredTimes) -> dict[str, Any]:
    """Makespan, the three throughputs, the latency distributions and the claim gaps."""
    finished = [task[2] for task in times.tasks if task[2] is not None]
    started = [attempt[2] for attempt in times.attempts]
    items = sum(task[3] for task in times.tasks)
    last = max(finished) if finished else None
    makespan = None if last is None else last - times.job_created
    processing = None if last is None or not started else last - min(started)
    completions = [(task[2], task[3]) for task in times.tasks if task[2] is not None]
    return {
        "timing": {"makespan_s": makespan, "processing_s": processing},
        "throughput": {
            "makespan_items_per_s": rate(items, makespan),
            "makespan_tasks_per_s": rate(len(times.tasks), makespan),
            "processing_items_per_s": rate(items, processing),
            "steady": steady_throughput(completions),
        },
        "latency": {
            "queue_wait_s": percentiles(
                [task[4] - task[1] for task in times.tasks if task[4] is not None]
            ),
            "service_s": percentiles(
                [
                    attempt[3] - attempt[2]
                    for attempt in times.attempts
                    if attempt[1] == "succeeded" and attempt[3] is not None
                ]
            ),
            "end_to_end_s": percentiles(
                [task[2] - times.job_created for task in times.tasks if task[2] is not None]
            ),
        },
        "claim_gap": percentiles(
            claim_gaps([(attempt[0], attempt[2], attempt[3]) for attempt in times.attempts])
        ),
    }


# --- server statistics -------------------------------------------------------------------


def database_counters(conn: Conn) -> dict[str, Any]:
    """The ``pg_stat_database`` counters of this database, and those of the job tables.

    The tables are read in the connection's own schema (its ``search_path``).
    """
    columns = ", ".join(f"{name}::float8" for name in DATABASE_COUNTERS)
    row = conn.execute(
        f"SELECT {columns} FROM pg_stat_database WHERE datname = current_database()"
    ).fetchone()
    database = dict(zip(DATABASE_COUNTERS, row or (), strict=True))
    table_columns = ", ".join(f"{name}::float8" for name in TABLE_COUNTERS)
    tables: dict[str, Any] = {name: dict.fromkeys(TABLE_COUNTERS, 0.0) for name in TABLES}
    for name, *values in conn.execute(
        f"SELECT relname, {table_columns} FROM pg_stat_user_tables "
        "WHERE schemaname = current_schema() AND relname = ANY(%s)",
        (list(TABLES),),
    ):
        tables[str(name)] = dict(zip(TABLE_COUNTERS, (float(v or 0) for v in values), strict=True))
    return {"database": {k: float(v or 0) for k, v in database.items()}, "tables": tables}


def contention_sample(conn: Conn) -> dict[str, int]:
    """Backends of this database by ``state/wait_event_type``."""
    counts: dict[str, int] = {}
    for state, wait, count in conn.execute(
        "SELECT coalesce(state, 'none'), coalesce(wait_event_type, 'none'), count(*) "
        "FROM pg_stat_activity WHERE datname = current_database() GROUP BY 1, 2"
    ):
        counts[f"{state}/{wait}"] = int(count)
    return counts


def summarize_contention(samples: Sequence[Mapping[str, int]]) -> dict[str, Any]:
    """The largest and mean count of each ``state/wait_event_type`` over the samples."""
    keys = sorted({key for sample in samples for key in sample})
    return {
        "samples": len(samples),
        "by_state": {
            key: {
                "max": max(sample.get(key, 0) for sample in samples),
                "mean": sum(sample.get(key, 0) for sample in samples) / len(samples),
            }
            for key in keys
        },
    }


_UNITS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "kB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
}


def parse_size(text: str) -> float | None:
    """A ``docker stats`` size such as ``123.4MiB`` in bytes; ``None`` if unreadable."""
    text = text.strip()
    for unit in sorted(_UNITS, key=len, reverse=True):
        if text.endswith(unit):
            try:
                return float(text[: -len(unit)]) * _UNITS[unit]
            except ValueError:
                return None
    return None


def docker_stats_sample(container: str) -> dict[str, float | None]:
    """One ``docker stats --no-stream`` reading of ``container``: CPU percent and memory bytes."""
    output = subprocess.run(  # a fixed command; the container name is one argument
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout
    data = json.loads(output.strip().splitlines()[0])
    cpu = str(data.get("CPUPerc", "")).rstrip("%")
    memory = str(data.get("MemUsage", "")).split("/")[0]
    try:
        cpu_percent: float | None = float(cpu)
    except ValueError:
        cpu_percent = None
    return {"cpu_percent": cpu_percent, "memory_bytes": parse_size(memory)}


def summarize_docker(samples: Sequence[Mapping[str, float | None]]) -> dict[str, Any]:
    cpu = [s["cpu_percent"] for s in samples if s.get("cpu_percent") is not None]
    memory = [s["memory_bytes"] for s in samples if s.get("memory_bytes") is not None]
    return {
        "samples": len(samples),
        "cpu_percent": percentiles([float(v) for v in cpu if v is not None]),
        "memory_bytes_max": max((float(v) for v in memory if v is not None), default=None),
    }


class Sampler:
    """Calls ``sample`` about once per ``interval`` on its own thread until stopped.

    A failing sample is counted and skipped, so a sampler never ends a run.
    """

    def __init__(self, sample: Callable[[], Any], interval: float = 1.0) -> None:
        self._sample, self._interval = sample, interval
        self._stop = threading.Event()
        self.samples: list[Any] = []
        self.errors = 0
        self._thread = threading.Thread(target=self._loop, name="bench-sampler", daemon=True)

    def start(self) -> "Sampler":
        self._thread.start()
        return self

    def _loop(self) -> None:
        while not self._stop.is_set():
            began = time.monotonic()
            try:
                self.samples.append(self._sample())
            except Exception:  # a lost sample must never end a run
                self.errors += 1
            self._stop.wait(max(self._interval - (time.monotonic() - began), 0))

    def stop(self) -> list[Any]:
        self._stop.set()
        self._thread.join()
        return self.samples


# --- processes ---------------------------------------------------------------------------


def poll_exit(process: subprocess.Popen[Any]) -> dict[str, Any] | None:
    """``process``'s accounting if it has exited, else ``None``; never waits.

    Uses ``os.wait4``, so the CPU times and peak RSS are the process's own.
    ``Popen.poll``, ``Popen.wait`` and ``Popen.send_signal`` (which polls first)
    must not be used on the same process: they reap it too, and its accounting
    would be lost. Signal it with ``os.kill``; until it is reaped here, its pid
    cannot be reused.
    """
    pid, status, usage = os.wait4(process.pid, os.WNOHANG)
    return _accounting(process, status, usage, killed=False) if pid else None


def reap(process: subprocess.Popen[Any], timeout: float) -> dict[str, Any]:
    """Wait up to ``timeout`` seconds for ``process`` to exit, then SIGKILL it; its accounting.

    ``killed`` records whether it had to be killed; ``exit_code`` is negative
    for a signal, as in ``subprocess``.
    """
    deadline = time.monotonic() + timeout
    while (exited := poll_exit(process)) is None:
        if time.monotonic() >= deadline:
            # os.kill, not Popen.send_signal: that polls first, and could reap the
            # process before wait4 sees it, losing its accounting.
            os.kill(process.pid, signal.SIGKILL)
            _, status, usage = os.wait4(process.pid, 0)
            return _accounting(process, status, usage, killed=True)
        time.sleep(0.05)
    return exited


def _accounting(
    process: subprocess.Popen[Any], status: int, usage: resource.struct_rusage, *, killed: bool
) -> dict[str, Any]:
    code = os.waitstatus_to_exitcode(status)
    process.returncode = code  # reaped here, so Popen must not wait for it again
    return {
        "pid": process.pid,
        "exit_code": code,
        "killed": killed,
        "user_s": usage.ru_utime,
        "sys_s": usage.ru_stime,
        "maxrss_bytes": maxrss_bytes(usage.ru_maxrss),
    }


def harness_usage() -> dict[str, Any]:
    """This process's own CPU times and peak RSS."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "user_s": usage.ru_utime,
        "sys_s": usage.ru_stime,
        "maxrss_bytes": maxrss_bytes(usage.ru_maxrss),
    }
