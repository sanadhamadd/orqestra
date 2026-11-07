"""The result record of one benchmark cell (``format: 1``), its environment, and its file.

A record holds aggregates only; per-task rows and worker logs stay under
``build/bench/<run_id>/``, outside the repository. One file is written per cell
and repetition, named
``<UTC yyyymmddThhmmssZ>-<sha7>-<profile>-<size>-b<batch>-w<workers>-r<rep>.json``,
where ``b0`` marks a profile whose tasks are not batches.

``status`` is ``complete``, ``incomplete`` (stopped before every task ended) or
``failed_correctness`` (some gate check failed). Only a ``complete`` record
carries throughput figures, and it must have passed every gate check.

A result from a working tree with uncommitted changes cannot say what code it
measured, so writing one is refused unless explicitly allowed; an allowed one
is marked ``dirty_allowed`` and must never be committed.
"""

import hashlib
import json
import os
import platform
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, NoReturn

from benchmarks.claims import server_info
from benchmarks.metrics import NOT_MEASURED, Conn

FORMAT = 1
ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "benchmarks" / "results"
RAW_DIR = ROOT / "build" / "bench"

TOP_LEVEL = frozenset(
    {
        "format",
        "run_id",
        "ladder",
        "status",
        "recorded_at",
        "cell",
        "environment",
        "worker_settings",
        "provider",
        "timing",
        "throughput",
        "latency",
        "claim_gap",
        "idle_claim_rate",
        "resources",
        "database_stats",
        "contention",
        "failures",
        "usage",
        "gate",
        "drill",
    }
)
CELL_FIELDS = frozenset({"profile", "size", "batch", "workers", "repetition", "seed"})
ENVIRONMENT_FIELDS = frozenset(
    {
        "git_sha",
        "git_dirty",
        "uv_lock_sha256",
        "python",
        "platform",
        "cpu_model",
        "cores",
        "memory_bytes",
        "postgres",
        "docker",
        "compose_project",
        "note",
    }
)


class Status(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    FAILED_CORRECTNESS = "failed_correctness"


class DirtyTreeError(RuntimeError):
    """The working tree has uncommitted changes, and a dirty result was not allowed."""


def _command(*args: str) -> str:
    return subprocess.run(  # fixed commands only
        list(args), capture_output=True, text=True, check=True, timeout=30, cwd=ROOT
    ).stdout.strip()


def _optional(*args: str) -> str | None:
    try:
        return _command(*args)
    except (OSError, subprocess.SubprocessError):
        return None


def code_state() -> dict[str, Any]:
    """The commit measured, and whether the working tree differs from it."""
    return {
        "git_sha": _command("git", "rev-parse", "HEAD"),
        "git_dirty": bool(_command("git", "status", "--porcelain")),
    }


def machine() -> dict[str, Any]:
    """The CPU model and the memory of this machine, as the system reports them."""
    model: str | None = None
    memory: int | None = None
    if sys.platform == "darwin":
        model = _optional("sysctl", "-n", "machdep.cpu.brand_string")
        size = _optional("sysctl", "-n", "hw.memsize")
        memory = int(size) if size and size.isdigit() else None
    else:
        model, memory = _linux_machine()
    return {
        "cpu_model": model or NOT_MEASURED,
        "cores": os.cpu_count(),
        "memory_bytes": memory if memory is not None else NOT_MEASURED,
    }


def _linux_machine() -> tuple[str | None, int | None]:
    model = memory = None
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                memory = int(line.split()[1]) * 1024
                break
    except OSError:
        pass
    return model, memory


def docker_state(container: str | None) -> dict[str, Any] | str:
    """The server container's image and the Docker VM's CPUs and memory, when there is one."""
    if container is None:
        return NOT_MEASURED
    image = _optional("docker", "inspect", "--format", "{{.Config.Image}} {{.Image}}", container)
    info = _optional("docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}")
    reference, _, digest = (image or "").partition(" ")
    cpus, _, memory = (info or "").partition(" ")
    return {
        "container": container,
        "image": reference or NOT_MEASURED,
        "image_id": digest or NOT_MEASURED,
        "vm_cpus": int(cpus) if cpus.isdigit() else NOT_MEASURED,
        "vm_memory_bytes": int(memory) if memory.isdigit() else NOT_MEASURED,
    }


def environment(conn: Conn, *, container: str | None = None, note: str = "") -> dict[str, Any]:
    """Everything needed to say where and on what a cell ran (plan 2.7 and 2.8)."""
    lock = ROOT / "uv.lock"
    return {
        **code_state(),
        "uv_lock_sha256": hashlib.sha256(lock.read_bytes()).hexdigest(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        **machine(),
        "postgres": server_info(conn),
        "docker": docker_state(container),
        "compose_project": os.environ.get("COMPOSE_PROJECT_NAME") or NOT_MEASURED,
        "note": note,
    }


def now() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def result_filename(record: Mapping[str, Any]) -> str:
    cell = record["cell"]
    sha = str(record["environment"]["git_sha"])[:7]
    return (
        f"{record['recorded_at']}-{sha}-{cell['profile']}-{cell['size']}"
        f"-b{cell['batch'] or 0}-w{cell['workers']}-r{cell['repetition']}.json"
    )


def _fail(message: str) -> NoReturn:
    raise ValueError(message)


def validate_result(record: object) -> None:
    """Raise ``ValueError`` unless ``record`` is a well-formed format 1 cell result."""
    if not isinstance(record, dict):
        _fail("a result must be an object")
    missing = TOP_LEVEL - set(record)
    if missing:
        _fail(f"a result is missing {sorted(missing)}")
    if record["format"] != FORMAT:
        _fail(f"a result must have format {FORMAT}")
    if record["status"] not in set(Status):
        _fail(f"status must be one of {sorted(Status)}")
    cell = record["cell"]
    if not isinstance(cell, dict) or set(cell) != CELL_FIELDS:
        _fail(f"the cell must hold exactly {sorted(CELL_FIELDS)}")
    for name in ("size", "workers", "repetition"):
        value = cell[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            _fail(f"cell.{name} must be a positive integer")
    environment = record["environment"]
    if not isinstance(environment, dict) or not set(environment) >= ENVIRONMENT_FIELDS:
        _fail(f"the environment must hold {sorted(ENVIRONMENT_FIELDS)}")
    if not isinstance(environment["git_dirty"], bool):
        _fail("environment.git_dirty must be a boolean")
    if environment["git_dirty"] and environment.get("dirty_allowed") is not True:
        _fail("a result from a dirty tree must be marked dirty_allowed")
    gate = record["gate"]
    if not isinstance(gate, list) or not gate:
        _fail("the gate must list its checks")
    for check in gate:
        if (
            not isinstance(check, dict)
            or not isinstance(check.get("name"), str)
            or not isinstance(check.get("passed"), bool)
        ):
            _fail("each gate check must have a name and a boolean passed")
    passed = all(check["passed"] for check in gate)
    if record["status"] == Status.COMPLETE and not passed:
        _fail("a complete result must have passed every gate check")
    if record["status"] == Status.FAILED_CORRECTNESS and passed:
        _fail("a failed_correctness result must name a failed gate check")
    if record["status"] != Status.COMPLETE and record["throughput"] is not None:
        _fail("only a complete result may report throughput")


def write_result(record: dict[str, Any], directory: Path, *, allow_dirty: bool = False) -> Path:
    """Validate ``record`` and write it to a new file in ``directory``; return its path.

    Refuses (``DirtyTreeError``) a record from a dirty tree unless
    ``allow_dirty``, which marks it ``dirty_allowed``. Never overwrites a file.
    """
    if record["environment"]["git_dirty"]:
        if not allow_dirty:
            msg = "refusing to write a result from a working tree with uncommitted changes"
            raise DirtyTreeError(msg)
        record["environment"]["dirty_allowed"] = True
    validate_result(record)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / result_filename(record)
    with path.open("x", encoding="utf-8") as out:
        out.write(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return path
