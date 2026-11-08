"""Named ladders of cells, run in interleaved order with stop rules (plan 2.4).

``python -m benchmarks.ladder <name>`` runs every cell of a ladder against the
benchmark database (``benchmarks.run``), one result file per cell:

=================  ========  ==========================  ==============  ==========  ====
ladder             profile   size                        batch           workers     reps
=================  ========  ==========================  ==============  ==========  ====
``warm-up``        invoice   1,000                       100             1           1
``scale``          invoice   1,000 / 5,000 / 10,000      100             1, 2, 4, 8  3
``overhead``       echo      10,000                      none            1, 2, 4, 8  3
``handler-bound``  wait      400                         none            1, 2, 4, 8  3
``batch``          invoice   10,000                      10, 100, 1,000  4           3
``large``          invoice   50,000                      100             chosen      1-3
=================  ========  ==========================  ==============  ==========  ====

The warm-up is a ladder of its own, so its result is recorded and kept out of
every other ladder's tables. Every cell uses ``benchmarks.run.BENCH_WORKER_SETTINGS``.

- **Order.** For each size, repetitions are interleaved by worker count (1, 2,
  4, 8, 1, 2, ...), so drift spreads across cells. A worker count above
  ``os.cpu_count()`` is skipped and listed as skipped in the ladder's log,
  ``build/bench/<run_id>/ladder.json``.
- **Stop rule.** A cell stops after the median 1-worker makespan of the
  previous size, times the size ratio, times 2, and never after more than 60
  minutes; the first size has the 60-minute limit. A stopped cell is
  ``incomplete`` and is never extrapolated.
- **The ``large`` rung** runs only if every 10,000-invoice ``scale`` result of
  this commit is ``complete`` and 5 times their median makespan at the chosen
  worker count is at most 60 minutes. That projection serves only as this gate
  and the rung's stop (twice the projection, at most 120 minutes); it is never
  printed or recorded as an estimate of the rung.
"""

import argparse
import json
import os
import statistics
import sys
import uuid
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from benchmarks.database import UnsafeBenchDatabaseError, unreachable_message
from benchmarks.result import RAW_DIR, DirtyTreeError, Status, code_state
from benchmarks.run import Cell, Profile, add_common_arguments, refuse_dirty, run_in_bench_database

CELL_LIMIT = 60 * 60.0
LARGE_LIMIT = 120 * 60.0
LARGE_GATE_SIZE = 10_000
LARGE_PROJECTION_FACTOR = 5  # 50,000 / 10,000
MAX_LARGE_REPETITIONS = 3


@dataclass(frozen=True, slots=True)
class Ladder:
    name: str
    profile: Profile
    sizes: tuple[int, ...]
    batches: tuple[int | None, ...]
    workers: tuple[int, ...]
    repetitions: int


LADDERS = {
    ladder.name: ladder
    for ladder in (
        Ladder("warm-up", Profile.INVOICE, (1_000,), (100,), (1,), 1),
        Ladder("scale", Profile.INVOICE, (1_000, 5_000, 10_000), (100,), (1, 2, 4, 8), 3),
        Ladder("overhead", Profile.ECHO, (10_000,), (None,), (1, 2, 4, 8), 3),
        Ladder("handler-bound", Profile.WAIT, (400,), (None,), (1, 2, 4, 8), 3),
        Ladder("batch", Profile.INVOICE, (10_000,), (10, 100, 1_000), (4,), 3),
        Ladder("large", Profile.INVOICE, (50_000,), (100,), (), 1),
    )
}


def plan_cells(ladder: Ladder, cpu_count: int) -> list[tuple[Cell, bool]]:
    """Every cell of ``ladder`` in run order, each with whether it is skipped."""
    cells = []
    for size in ladder.sizes:
        for repetition in range(1, ladder.repetitions + 1):
            for batch in ladder.batches:
                for workers in ladder.workers:
                    cell = Cell(ladder.profile, size, batch, workers, repetition)
                    cells.append((cell, workers > cpu_count))
    return cells


def stop_limit(previous_one_worker_makespans: Sequence[float], size_ratio: float) -> float:
    """A cell's stop, in seconds, from the previous size's complete 1-worker makespans."""
    if not previous_one_worker_makespans:
        return CELL_LIMIT
    return min(2 * statistics.median(previous_one_worker_makespans) * size_ratio, CELL_LIMIT)


def large_stop(
    scale_records: Iterable[dict[str, Any]], workers: int, cpu_count: int
) -> float | None:
    """The ``large`` rung's stop in seconds if its gate passes, else ``None``.

    ``scale_records`` are the ``scale`` results of the commit being measured.
    Every 10,000-invoice cell that is not skipped must be present, and every
    record of such a cell must be ``complete``: a repeated run does not hide an
    earlier incomplete one.
    """
    wanted = {
        (cell.workers, cell.repetition)
        for cell, skipped in plan_cells(LADDERS["scale"], cpu_count)
        if cell.size == LARGE_GATE_SIZE and not skipped
    }
    found: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for record in scale_records:
        cell = record["cell"]
        if record["ladder"] == "scale" and cell["size"] == LARGE_GATE_SIZE:
            found[(cell["workers"], cell["repetition"])].append(record)
    if not wanted or not wanted <= set(found):
        return None
    if any(record["status"] != Status.COMPLETE for key in wanted for record in found[key]):
        return None
    makespans = [
        float(record["timing"]["makespan_s"])
        for key in sorted(wanted)
        if key[0] == workers
        for record in found[key]
    ]
    if not makespans:
        return None
    projection = LARGE_PROJECTION_FACTOR * statistics.median(makespans)
    if projection > CELL_LIMIT:
        return None
    return min(2 * projection, LARGE_LIMIT)


def committed_records(directory: Path, sha: str) -> list[dict[str, Any]]:
    """The result records in ``directory`` measured on commit ``sha`` from a clean tree."""
    records = []
    for path in sorted(directory.glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        environment = record.get("environment", {})
        if environment.get("git_sha") == sha and not environment.get("git_dirty"):
            records.append(record)
    return records


def run_ladder(ladder: Ladder, args: argparse.Namespace) -> int:
    cpu_count = os.cpu_count() or 1
    run_id = uuid.uuid4().hex[:12]
    large: float | None = None
    if ladder.name == "large":
        if args.workers is None:
            print("refused: the large rung needs --workers, the best count from scale")
            return 1
        if not 1 <= args.repetitions <= MAX_LARGE_REPETITIONS:
            print(f"refused: the large rung runs 1 to {MAX_LARGE_REPETITIONS} repetitions")
            return 1
        sha = code_state()["git_sha"]
        large = large_stop(committed_records(args.out_dir, sha), args.workers, cpu_count)
        if large is None:
            print("refused: the large rung's gate did not pass")
            return 1
        ladder = Ladder(
            ladder.name,
            ladder.profile,
            ladder.sizes,
            ladder.batches,
            (args.workers,),
            args.repetitions,
        )
    log: list[dict[str, Any]] = []
    one_worker: dict[int, list[float]] = defaultdict(list)
    failures = 0
    for cell, skipped in plan_cells(ladder, cpu_count):
        entry: dict[str, Any] = {"cell": cell.name, "skipped": skipped}
        log.append(entry)
        if skipped:
            print(f"skipped {cell.name}: more workers than cores")
            continue
        previous = [size for size in ladder.sizes if size < cell.size]
        if large is not None:
            limit = large
        elif previous:
            limit = stop_limit(one_worker[previous[-1]], cell.size / previous[-1])
        else:
            limit = CELL_LIMIT
        record, path = run_in_bench_database(
            cell,
            run_id=run_id,
            ladder=ladder.name,
            stop_after=limit,
            out_dir=args.out_dir,
            keep_schema=args.keep_schema,
            allow_dirty=args.allow_dirty,
            container=args.db_container,
            note=args.note,
        )
        entry |= {"status": record["status"], "result": str(path)}
        if record["status"] == Status.COMPLETE and cell.workers == 1:
            one_worker[cell.size].append(record["timing"]["makespan_s"])
        failures += record["status"] != Status.COMPLETE
        print(f"{record['status']} {cell.name} {path}")
    log_dir = RAW_DIR / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "ladder.json").write_text(
        json.dumps({"ladder": ladder.name, "cells": log}, indent=2)
    )
    return 1 if failures else 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.ladder", description=__doc__)
    parser.add_argument("ladder", choices=sorted(LADDERS))
    parser.add_argument("--workers", type=int, help="the large rung's worker count")
    parser.add_argument("--repetitions", type=int, default=1, help="the large rung's repetitions")
    add_common_arguments(parser)
    args = parser.parse_args(argv)
    try:
        refuse_dirty(args.allow_dirty)
        return run_ladder(LADDERS[args.ladder], args)
    except (ValueError, DirtyTreeError, UnsafeBenchDatabaseError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    except psycopg.OperationalError as exc:
        print(f"refused: {unreachable_message(exc)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
