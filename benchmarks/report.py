"""The generated tables of ``docs/performance.md``: ``python -m benchmarks.report``.

The tables are rendered from the committed result files, never typed (plan
2.7). Every file is validated first, and an invalid one stops the report:

- cell results: ``benchmarks/results/*.json`` (``benchmarks.result``, format 1);
- claim-cost results: ``benchmarks/results/claims/*.json`` (``benchmarks.claims``);
- drill results: ``benchmarks/results/drills/*.json``, cell records whose
  ``drill`` is set. Only the cell, status, gate, worker exit codes and the
  drill's ``name`` are read from them.

**What enters the tables.** Only a ``complete`` cell from a clean tree. Every
other cell, and every claim-cost result from a dirty tree, is listed under
"Not in the tables" with its reason, never dropped. The ``warm-up`` ladder has
a table of its own and is never pooled with the others. Drill results have
their own table and never enter the performance tables.

**Figures.** A cell's repetitions are summarized as the median with the
minimum and maximum, ``median [min, max]`` (plan 2.8), never as a mean alone.
A figure a repetition lacks is left out of that column's median, and the
count is then shown. The derived figures of plan 2.12 are computed from those
medians and labelled with their formulas:

- ``S(W)`` = median makespan items/s at ``W`` workers / the median at 1 worker,
  for the same profile, size and batch;
- efficiency = ``S(W) / W``;
- engine overhead per task = median makespan x ``W`` / tasks - the median of
  the repetitions' service time p50.

**Byte-identical.** Files are read in sorted order, every table is sorted,
numbers have a fixed format, and nothing about the run itself (time, absolute
paths) is written, so the same files always render the same text.

**The document.** The generated text sits between two markers in
``docs/performance.md``; the prose around them is written by hand. ``--write``
replaces the text between the markers and never creates the file; ``--check``
exits 1 if it differs from what the result files render.
"""

import argparse
import json
import statistics
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from benchmarks import claims
from benchmarks.result import RESULTS_DIR, ROOT, Status, WaitEnded, validate_result

BEGIN = "<!-- BEGIN GENERATED: python -m benchmarks.report -->"
END = "<!-- END GENERATED -->"
DOC = ROOT / "docs" / "performance.md"
CLAIMS_DIR = "claims"
DRILLS_DIR = "drills"
WARM_UP = "warm-up"
LADDER_ORDER = ("scale", "overhead", "handler-bound", "batch", "large")
DASH = "—"
NO_RESULTS = "_No results._"


class ReportError(ValueError):
    """A result file or the document cannot be used as it is."""


@dataclass(frozen=True, slots=True)
class Loaded:
    """One result file: its path relative to the results root, and its record."""

    name: str
    record: dict[str, Any]


# --- reading -------------------------------------------------------------------------------


def _validate_cell(record: object) -> None:
    validate_result(record)
    assert isinstance(record, dict)
    if record["drill"] is not None:
        msg = f"a drill result belongs under {DRILLS_DIR}/"
        raise ValueError(msg)


def _validate_drill(record: object) -> None:
    validate_result(record)
    assert isinstance(record, dict)
    if not isinstance(record["drill"], dict):
        msg = "a drill result must carry its drill record"
        raise ValueError(msg)


def _load(root: Path, sub: str | None, validate: Callable[[object], None]) -> list[Loaded]:
    directory = root / sub if sub else root
    if not directory.is_dir():
        return []
    loaded = []
    for path in sorted(directory.glob("*.json")):
        name = path.relative_to(root).as_posix()
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            validate(record)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            msg = f"{name}: {exc}"
            raise ReportError(msg) from None
        loaded.append(Loaded(name, record))
    return loaded


# --- formatting ----------------------------------------------------------------------------


def fixed(value: float | None, digits: int) -> str:
    """``value`` with exactly ``digits`` decimals; ``—`` for ``None``. Never ``-0``."""
    if value is None:
        return DASH
    if round(value, digits) == 0:
        value = 0.0
    return f"{value:.{digits}f}"


def summary(values: Sequence[float | None], digits: int) -> str:
    """``median [min, max]`` of the values present, with their count if any is missing."""
    present = [value for value in values if value is not None]
    if not present:
        return DASH
    text = (
        f"{fixed(statistics.median(present), digits)} "
        f"[{fixed(min(present), digits)}, {fixed(max(present), digits)}]"
    )
    if len(present) < len(values):
        text += f" (n={len(present)})"
    return text


def _cell_text(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _table(header: Sequence[str], rows: Iterable[Sequence[object]]) -> list[str]:
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    lines.extend("| " + " | ".join(_cell_text(cell) for cell in row) + " |" for row in rows)
    return lines


def _files(names: Iterable[str]) -> str:
    return ", ".join(f"`{name}`" for name in sorted(set(names)))


def _number(record: Mapping[str, Any], *path: str) -> float | None:
    value: Any = record
    for key in path:
        if not isinstance(value, Mapping) or key not in value:
            return None
        value = value[key]
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


# --- environment ---------------------------------------------------------------------------


def _postgres_version(postgres: object) -> str:
    if isinstance(postgres, Mapping) and isinstance(postgres.get("version"), str):
        return str(postgres["version"]).split(" on ")[0]
    return DASH


def _memory(value: object) -> str:
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value / 1024**3:.1f} GiB"
    return str(value)


def _server(postgres: object) -> str:
    """The PostgreSQL version and every recorded server setting, in a fixed order.

    Results whose settings differ are different measurements, so the settings are
    part of a group's key and shown with it.
    """
    version = _postgres_version(postgres)
    if not isinstance(postgres, Mapping):
        return version
    settings = ", ".join(
        f"{name} {postgres[name]}" for name in sorted(postgres) if name != "version"
    )
    return f"{version} ({settings})" if settings else version


def _docker_vm(docker: object) -> str:
    """The Docker VM's CPUs and memory, or ``not measured``."""
    if not isinstance(docker, Mapping):
        return "not measured"
    cpus, memory = docker.get("vm_cpus"), docker.get("vm_memory_bytes")
    if not isinstance(cpus, int) or not isinstance(memory, int):
        return "not measured"
    return f"{cpus} CPUs, {_memory(memory)}"


def cell_environment(record: Mapping[str, Any]) -> tuple[str, str, str, str]:
    """``(sha, machine, server and settings, Docker VM)`` of a cell or drill result."""
    environment = record["environment"]
    machine = (
        f"{environment['cpu_model']}, {environment['cores']} cores, "
        f"{_memory(environment['memory_bytes'])} memory"
    )
    return (
        str(environment["git_sha"]),
        machine,
        _server(environment["postgres"]),
        _docker_vm(environment.get("docker")),
    )


def claims_environment(record: Mapping[str, Any]) -> tuple[str, str, str]:
    """``(sha, machine, server and settings)`` of a claim-cost result."""
    environment = record["environment"]
    machine = f"{environment.get('platform', DASH)}, {environment.get('cpu_count', DASH)} CPUs"
    return str(environment["git_sha"]), machine, _server(environment["postgres"])


def _environment_line(sha: str, machine: str, server: str, vm: str | None = None) -> str:
    line = f"Commit `{sha[:12]}` · machine: {machine} · server: {server}"
    return line if vm is None else f"{line} · Docker VM: {vm}"


# --- cells ---------------------------------------------------------------------------------

CellKey = tuple[str, int, int, int]  # profile, size, batch (0 when none), workers


def cell_key(record: Mapping[str, Any]) -> CellKey:
    cell = record["cell"]
    return (str(cell["profile"]), int(cell["size"]), int(cell["batch"] or 0), int(cell["workers"]))


def cell_label(key: CellKey) -> str:
    profile, size, batch, workers = key
    return f"{profile} {size}" + (f" b{batch}" if batch else "") + f" w{workers}"


def clean(record: Mapping[str, Any]) -> bool:
    """Whether a cell result may enter the tables: complete, from a clean tree."""
    return record["status"] == Status.COMPLETE and record["environment"]["git_dirty"] is False


# The gate checks a job stopped at its limit fails by being stopped: its tasks are not
# all ended, and an invoice job's summary and usage compare job totals with the oracle.
# The attempt-history checks look only at what did end, so a timeout explains none of them.
TIMEOUT_CHECKS = frozenset({"terminal", "nothing_running", "worker_exits", "summary", "usage"})


def reason(record: Mapping[str, Any]) -> str:
    """Why a cell result is not in the tables."""
    reasons = []
    timing = record.get("timing")
    ended = timing.get("wait_ended") if isinstance(timing, Mapping) else None
    waited = fixed(_number(record, "timing", "waited_s"), 1)
    if record["status"] == Status.INCOMPLETE:
        if ended == WaitEnded.STOP_AFTER:
            reasons.append(f"incomplete: stopped at the limit after {waited} s")
        else:  # a record from before the wait was recorded
            stop = _number(record, "timing", "stop_after_s")
            reasons.append(
                f"incomplete: stopped before every task ended (limit {fixed(stop, 1)} s)"
            )
        # A timeout explains only these failures; any other is a finding of its own.
        others = [
            str(check["name"])
            for check in record["gate"]
            if not check["passed"] and check["name"] not in TIMEOUT_CHECKS
        ]
        if others:
            reasons.append("also failed: " + ", ".join(others))
    elif record["status"] == Status.FAILED_CORRECTNESS:
        failed = ", ".join(str(check["name"]) for check in record["gate"] if not check["passed"])
        if ended == WaitEnded.NO_WORKERS:
            reasons.append(f"failed_correctness: every worker exited after {waited} s; {failed}")
        else:
            reasons.append(f"failed_correctness: {failed}")
    if record["environment"]["git_dirty"]:
        reasons.append("dirty tree (dirty_allowed)")
    return "; ".join(reasons)


def _worker_cpu(record: Mapping[str, Any]) -> float | None:
    workers = record.get("resources", {}).get("workers", [])
    values = [
        float(worker["cpu_utilization"])
        for worker in workers
        if isinstance(worker, Mapping) and _number(worker, "cpu_utilization") is not None
    ]
    return statistics.median(values) if values else None


# (header, how a repetition's value is read, decimals)
COLUMNS: tuple[tuple[str, Callable[[Mapping[str, Any]], float | None], int], ...] = (
    ("Makespan s", lambda r: _number(r, "timing", "makespan_s"), 3),
    ("Items/s (makespan)", lambda r: _number(r, "throughput", "makespan_items_per_s"), 1),
    ("Items/s (processing)", lambda r: _number(r, "throughput", "processing_items_per_s"), 1),
    ("Items/s (steady)", lambda r: _number(r, "throughput", "steady", "items_per_s"), 1),
    ("Queue wait p95 s", lambda r: _number(r, "latency", "queue_wait_s", "p95"), 3),
    ("Service p50 s", lambda r: _number(r, "latency", "service_s", "p50"), 3),
    ("End-to-end p95 s", lambda r: _number(r, "latency", "end_to_end_s", "p95"), 3),
    ("Claim gap p50 s", lambda r: _number(r, "claim_gap", "p50"), 3),
    ("Worker CPU", _worker_cpu, 2),
    ("Commits/task", lambda r: _number(r, "database_stats", "commits_per_task"), 2),
)


def _by_cell(loaded: Sequence[Loaded]) -> dict[CellKey, list[Loaded]]:
    cells: dict[CellKey, list[Loaded]] = defaultdict(list)
    for item in loaded:
        cells[cell_key(item.record)].append(item)
    return cells


def _median_of(
    items: Sequence[Loaded], read: Callable[[Mapping[str, Any]], float | None]
) -> float | None:
    values = [value for item in items if (value := read(item.record)) is not None]
    return statistics.median(values) if values else None


def cell_table(loaded: Sequence[Loaded]) -> list[str]:
    cells = _by_cell(loaded)
    header = ["Cell", "n", *(column[0] for column in COLUMNS), "Files"]
    rows = []
    for key in sorted(cells):
        items = cells[key]
        rows.append(
            [
                cell_label(key),
                len(items),
                *(
                    summary([read(item.record) for item in items], digits)
                    for _, read, digits in COLUMNS
                ),
                _files(item.name for item in items),
            ]
        )
    return _table(header, rows)


DERIVED_NOTE = (
    "Derived, not measured. S(W) = median items/s (makespan) at W workers / the median at "
    "1 worker, for the same profile, size and batch; efficiency = S(W) / W; engine overhead "
    "per task = median makespan x W / tasks - the median of the repetitions' service time p50."
)


def derived_table(loaded: Sequence[Loaded]) -> list[str]:
    cells = _by_cell(loaded)
    rate = COLUMNS[1][1]
    makespan = COLUMNS[0][1]
    service = COLUMNS[5][1]
    rows = []
    for key in sorted(cells):
        items = cells[key]
        workers = key[3]
        base = cells.get((*key[:3], 1), [])
        speedup = efficiency = None
        here, one = _median_of(items, rate), _median_of(base, rate)
        if here is not None and one is not None and one > 0:
            speedup = here / one
            efficiency = speedup / workers
        overhead = None
        span, service_p50 = _median_of(items, makespan), _median_of(items, service)
        tasks = _number(items[0].record, "timing", "tasks")
        if span is not None and service_p50 is not None and tasks:
            overhead = span * workers / tasks - service_p50
        rows.append(
            [
                cell_label(key),
                fixed(speedup, 2),
                fixed(efficiency, 2),
                fixed(overhead, 3),
                _files(item.name for item in [*items, *base]),
            ]
        )
    return _table(["Cell", "S(W)", "Efficiency", "Engine overhead per task s", "Files"], rows)


def _ladder_order(name: str) -> tuple[int, str]:
    return (LADDER_ORDER.index(name), "") if name in LADDER_ORDER else (len(LADDER_ORDER), name)


def _groups(loaded: Sequence[Loaded]) -> dict[tuple[str, str, str, str, str], list[Loaded]]:
    groups: dict[tuple[str, str, str, str, str], list[Loaded]] = defaultdict(list)
    for item in loaded:
        groups[(str(item.record["ladder"]), *cell_environment(item.record))].append(item)
    return groups


def ladder_sections(loaded: Sequence[Loaded]) -> list[str]:
    groups = _groups(loaded)
    lines: list[str] = []
    for key in sorted(groups, key=lambda k: (_ladder_order(k[0]), k[1:])):
        ladder, *environment = key
        lines += [f"### Ladder `{ladder}`", "", _environment_line(*environment), ""]
        lines += cell_table(groups[key])
        lines += ["", f"_{DERIVED_NOTE}_", ""]
        lines += derived_table(groups[key])
        lines.append("")
    return lines


# --- claim cost and drills -----------------------------------------------------------------


def claims_sections(loaded: Sequence[Loaded]) -> list[str]:
    lines: list[str] = []
    for item in loaded:
        environment = claims_environment(item.record)
        lines += [f"### Claim cost: `{item.name}`", "", _environment_line(*environment), ""]
        measurements = sorted(
            item.record["measurements"], key=lambda m: (str(m["variant"]), int(m["depth"]))
        )
        rows = [
            [
                m["variant"],
                m["depth"],
                m["ready"],
                m["claims"],
                fixed(_number(m, "latency_ms", "p50"), 3),
                fixed(_number(m, "latency_ms", "p95"), 3),
                fixed(_number(m, "latency_ms", "max"), 3),
                fixed(_number(m, "explain", "execution_ms", "p50"), 3),
                " > ".join(str(node) for node in m["explain"]["plan_nodes"]),
                "yes" if m["explain"].get("consistent_shape") else "no",
            ]
            for m in measurements
        ]
        header = [
            "Variant",
            "Depth",
            "Ready",
            "Claims",
            "Latency p50 ms",
            "Latency p95 ms",
            "Latency max ms",
            "EXPLAIN execution p50 ms",
            "Plan",
            "Same plan every run",
        ]
        lines += _table(header, rows)
        lines.append("")
    return lines


def drill_sections(loaded: Sequence[Loaded]) -> list[str]:
    groups: dict[tuple[str, str, str, str], list[Loaded]] = defaultdict(list)
    for item in loaded:
        groups[cell_environment(item.record)].append(item)
    lines: list[str] = []
    for key in sorted(groups):
        lines += ["### Drills", "", _environment_line(*key), ""]
        rows = []
        for item in sorted(groups[key], key=lambda i: (str(i.record["drill"].get("name")), i.name)):
            record = item.record
            failed = [str(check["name"]) for check in record["gate"] if not check["passed"]]
            status = str(record["status"])
            if record["environment"]["git_dirty"]:
                status += " (dirty tree)"
            exits = record.get("failures", {}).get("worker_exit_codes")
            rows.append(
                [
                    record["drill"].get("name", DASH),
                    cell_label(cell_key(record)),
                    status,
                    "all passed" if not failed else "failed: " + ", ".join(failed),
                    DASH if exits is None else ", ".join(str(code) for code in exits),
                    f"`{item.name}`",
                ]
            )
        lines += _table(["Drill", "Cell", "Status", "Gate", "Worker exit codes", "File"], rows)
        lines.append("")
    return lines


# --- the whole section ---------------------------------------------------------------------


def render(root: Path = RESULTS_DIR) -> str:
    """The generated Markdown for the result files under ``root``."""
    cells = _load(root, None, _validate_cell)
    claim_results = _load(root, CLAIMS_DIR, claims.validate_result)
    drills = _load(root, DRILLS_DIR, _validate_drill)

    tables = [i for i in cells if clean(i.record) and i.record["ladder"] != WARM_UP]
    warm_up = [i for i in cells if clean(i.record) and i.record["ladder"] == WARM_UP]
    excluded = [
        [
            f"`{i.name}`",
            i.record["ladder"],
            cell_label(cell_key(i.record)),
            i.record["status"],
            reason(i.record),
        ]
        for i in cells
        if not clean(i.record)
    ]
    clean_claims = [i for i in claim_results if i.record["environment"]["git_dirty"] is False]
    excluded += [
        [f"`{i.name}`", "claim cost", DASH, DASH, "dirty tree"]
        for i in claim_results
        if i.record["environment"]["git_dirty"] is not False
    ]

    lines = [
        "Generated by `python -m benchmarks.report` from the result files under "
        "`benchmarks/results/`. Do not edit by hand: regenerate with `--write`.",
        "",
        "Each figure is the median over a cell's repetitions, with the minimum and maximum "
        "in brackets.",
        "",
        "## Performance results",
        "",
    ]
    lines += ladder_sections(tables) or [NO_RESULTS, ""]
    lines += ["## Warm-up (recorded, excluded from the other tables)", ""]
    if warm_up:
        for key, items in sorted(_groups(warm_up).items()):
            lines += [_environment_line(*key[1:]), "", *cell_table(items), ""]
    else:
        lines += [NO_RESULTS, ""]
    lines += ["## Claim cost against queue depth", ""]
    lines += claims_sections(clean_claims) or [NO_RESULTS, ""]
    lines += ["## Failure drills (never in the performance tables)", ""]
    lines += drill_sections(drills) or [NO_RESULTS, ""]
    lines += ["## Not in the tables", ""]
    if excluded:
        lines += _table(["File", "Ladder", "Cell", "Status", "Reason"], sorted(excluded))
        lines.append("")
    else:
        lines += ["_None._", ""]
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines) + "\n"


# --- the document --------------------------------------------------------------------------


def split_document(text: str) -> tuple[str, str, str]:
    """The text before, between and after the markers; exactly one pair is required."""
    if text.count(BEGIN) != 1 or text.count(END) != 1:
        msg = "the document must hold exactly one pair of generated-section markers"
        raise ReportError(msg)
    start = text.index(BEGIN) + len(BEGIN)
    end = text.index(END)
    if end < start:
        msg = "the end marker comes before the begin marker"
        raise ReportError(msg)
    return text[:start], text[start:end], text[end:]


def section(body: str) -> str:
    """What stands between the markers for ``body``."""
    return "\n" + body


def write_document(doc: Path, body: str) -> None:
    before, _, after = split_document(_read_document(doc))
    doc.write_text(before + section(body) + after, encoding="utf-8")


def check_document(doc: Path, body: str) -> bool:
    _, between, _ = split_document(_read_document(doc))
    return between == section(body)


def _read_document(doc: Path) -> str:
    try:
        return doc.read_text(encoding="utf-8")
    except OSError as exc:
        msg = f"cannot read {doc}: {exc.strerror or exc}"
        raise ReportError(msg) from None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m benchmarks.report", description=__doc__)
    parser.add_argument("--results", type=Path, default=RESULTS_DIR)
    parser.add_argument("--doc", type=Path, default=DOC)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--print", dest="mode", action="store_const", const="print")
    mode.add_argument("--write", dest="mode", action="store_const", const="write")
    mode.add_argument("--check", dest="mode", action="store_const", const="check")
    args = parser.parse_args(argv)
    try:
        body = render(args.results)
        if args.mode == "write":
            write_document(args.doc, body)
        elif args.mode == "check":
            if not check_document(args.doc, body):
                print(f"{args.doc}: the generated section is out of date", file=sys.stderr)
                return 1
        else:
            sys.stdout.write(body)
    except ReportError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
