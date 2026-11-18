"""The shared lock and atomic publish of the benchmark's shared files (``benchmarks.filelock``)."""

import fcntl
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from benchmarks import filelock
from benchmarks.filelock import LOCK_SUFFIX, lock_path, locked, publish
from benchmarks.result import ROOT

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


def held(path: Path) -> int:
    """Another holder of ``path``'s lock (a separate open file description)."""
    descriptor = os.open(lock_path(path), os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def free(path: Path) -> bool:
    descriptor = os.open(lock_path(path), os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    finally:
        os.close(descriptor)
    return True


def test_the_lock_file_sits_beside_the_data_file(tmp_path: Path) -> None:
    assert lock_path(tmp_path / "counter") == tmp_path / ("counter" + LOCK_SUFFIX)


def test_a_free_lock_is_had_at_once_and_reports_no_wait(tmp_path: Path) -> None:
    path = tmp_path / "counter"
    with locked(path, 1.0) as waited:
        assert waited == 0.0
        assert not free(path)
    assert free(path)
    with locked(path) as waited:
        assert waited == 0.0


def test_a_bounded_wait_on_a_busy_lock_runs_out(tmp_path: Path) -> None:
    path = tmp_path / "counter"
    holder = held(path)
    try:
        started = time.monotonic()
        with locked(path, 0.05) as waited:
            assert waited is None  # ran out: the block must do nothing that needs the lock
        assert 0.05 <= time.monotonic() - started < 5.0
    finally:
        os.close(holder)
    assert free(path)


def test_a_wait_reports_how_long_the_lock_was_busy(tmp_path: Path) -> None:
    path = tmp_path / "counter"
    holder = held(path)
    threading.Timer(0.1, os.close, (holder,)).start()  # released after about 0.1 s
    with locked(path, 30.0) as waited:
        assert waited is not None
        assert 0.05 <= waited < 10.0


def test_an_unbounded_wait_blocks_until_the_lock_is_released(tmp_path: Path) -> None:
    path = tmp_path / "counter"
    holder = held(path)
    threading.Timer(0.1, os.close, (holder,)).start()
    started = time.monotonic()
    with locked(path) as waited:
        assert waited == 0.0  # an unbounded wait does not measure
    assert time.monotonic() - started >= 0.05


def test_the_lock_is_released_when_the_block_raises(tmp_path: Path) -> None:
    path = tmp_path / "counter"
    with pytest.raises(RuntimeError), locked(path):
        raise RuntimeError
    assert free(path)


def test_publish_replaces_the_file_whole_and_keeps_the_lock_file(tmp_path: Path) -> None:
    path = tmp_path / "counter"
    path.write_text("1\n", encoding="ascii")
    with locked(path):
        lock_inode = lock_path(path).stat().st_ino
    for count in range(2, 6):
        before = path.stat().st_ino
        with locked(path):
            publish(path, f"{count}\n")
        assert path.read_text(encoding="ascii") == f"{count}\n"
        assert path.stat().st_ino != before  # a new file, swapped in
    assert lock_path(path).stat().st_ino == lock_inode  # every process locks one inode
    assert sorted(p.name for p in tmp_path.iterdir()) == ["counter", "counter.lock"]


def test_publish_syncs_the_directory_after_the_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "counter"
    path.write_text("1\n", encoding="ascii")
    seen: list[tuple[Path, str]] = []

    def record(directory: Path) -> None:
        seen.append((directory, path.read_text(encoding="ascii")))

    monkeypatch.setattr(filelock, "sync_directory", record)
    publish(path, "2\n")
    assert seen == [(tmp_path, "2\n")]  # the parent, once the new content is in place


@pytest.mark.parametrize("failing", ["write", "replace"])
def test_a_failed_publish_keeps_the_old_content_and_leaves_no_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failing: str
) -> None:
    path = tmp_path / "counter"
    path.write_text("1\n", encoding="ascii")

    def refuse(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    if failing == "write":
        monkeypatch.setattr(os, "fsync", refuse)  # the temporary file's own sync fails
    else:
        monkeypatch.setattr(os, "replace", refuse)
    with pytest.raises(OSError, match="No space"):
        publish(path, "2\n")
    assert path.read_text(encoding="ascii") == "1\n"
    assert [p.name for p in tmp_path.iterdir()] == ["counter"]


# A process that dies inside publish: just before the replace, or just after it.
DIES = """
import os, sys
from pathlib import Path
from benchmarks import filelock
if sys.argv[2] == "before":
    os.replace = lambda *args, **kwargs: os._exit(9)
else:
    filelock.sync_directory = lambda directory: os._exit(9)
filelock.publish(Path(sys.argv[1]), "2\\n")
"""


@pytest.mark.parametrize(("when", "left"), [("before", "1\n"), ("after", "2\n")])
def test_a_process_killed_inside_publish_leaves_the_old_or_the_new_content(
    tmp_path: Path, when: str, left: str
) -> None:
    path = tmp_path / "counter"
    path.write_text("1\n", encoding="ascii")
    child = subprocess.run(  # a fixed command
        [sys.executable, "-c", DIES, str(path), when],
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT.resolve())},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert child.returncode == 9, child.stderr
    assert path.read_text(encoding="ascii") == left  # complete, never torn
