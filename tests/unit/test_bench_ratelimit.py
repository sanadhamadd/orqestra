"""The cell's shared pace (``benchmarks.ratelimit``): slots, bounds, and a file that never tears."""

import fcntl
import math
import os
import time
from pathlib import Path

import pytest

from benchmarks.ratelimit import HORIZON, RateLimiter, check_rate, lock_path, read_slot

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


class Clock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def limiter(path: Path, per_s: float = 4.0, clock: Clock | None = None) -> RateLimiter:
    return RateLimiter(path, per_s, clock=clock or Clock())


def test_slots_are_one_interval_apart(tmp_path: Path) -> None:
    clock = Clock()
    pace = limiter(tmp_path / "pace", 4.0, clock)
    assert [pace.reserve(10.0) for _ in range(3)] == [0.0, 0.25, 0.5]
    assert read_slot(tmp_path / "pace") == pytest.approx(1_000.75)
    clock.now += 2.0  # idle: the next slot is free now, with no burst saved up
    assert pace.reserve(10.0) == 0.0
    assert pace.reserve(10.0) == pytest.approx(0.25)


def test_every_process_on_the_file_shares_the_slots(tmp_path: Path) -> None:
    clock = Clock()
    one, two = limiter(tmp_path / "pace", 2.0, clock), limiter(tmp_path / "pace", 2.0, clock)
    assert [one.reserve(10.0), two.reserve(10.0), one.reserve(10.0)] == [0.0, 0.5, 1.0]


def test_a_slot_further_than_the_wait_is_not_reserved(tmp_path: Path) -> None:
    pace = limiter(tmp_path / "pace", 1.0)
    assert pace.reserve(10.0) == 0.0
    before = read_slot(tmp_path / "pace")
    assert pace.reserve(0.5) is None  # the next slot is 1 s away
    assert read_slot(tmp_path / "pace") == before  # nothing reserved
    assert pace.reserve(1.0) == 1.0


@pytest.mark.parametrize("text", [None, "", "garbage\n", "nan\n", "inf\n"])
def test_a_missing_or_unreadable_file_is_a_free_slot_now(tmp_path: Path, text: str | None) -> None:
    path = tmp_path / "pace"
    if text is not None:
        path.write_text(text, encoding="ascii")
    assert limiter(path).reserve(0.0) == 0.0
    assert read_slot(path) == pytest.approx(1_000.25)


def test_a_slot_past_the_horizon_is_not_believed(tmp_path: Path) -> None:
    path = tmp_path / "pace"
    path.write_text(f"{1_000.0 + HORIZON + 5}\n", encoding="ascii")  # the clock went back
    assert limiter(path).reserve(0.0) == 0.0
    path.write_text(f"{1_000.0 + 3}\n", encoding="ascii")  # within the horizon: believed
    assert limiter(path).reserve(10.0) == pytest.approx(3.0)


def test_the_file_is_replaced_and_the_lock_file_kept(tmp_path: Path) -> None:
    path = tmp_path / "pace"
    pace = limiter(path)
    pace.reserve(10.0)
    file_inode, lock_inode = path.stat().st_ino, lock_path(path).stat().st_ino
    pace.reserve(10.0)
    assert path.stat().st_ino != file_inode  # a new file, swapped in whole
    assert lock_path(path).stat().st_ino == lock_inode  # every process locks the same inode
    assert sorted(p.name for p in tmp_path.iterdir()) == ["pace", "pace.lock"]


def test_a_busy_lock_is_waited_for_only_until_the_wait_runs_out(tmp_path: Path) -> None:
    path = tmp_path / "pace"
    holder = os.open(lock_path(path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX)
        started = time.monotonic()
        assert limiter(path).reserve(0.05) is None
        assert time.monotonic() - started < 5.0  # the 0.05 s wait ran out; loose under load
    finally:
        os.close(holder)
    assert limiter(path).reserve(0.05) == 0.0  # released: the slot is had


@pytest.mark.parametrize("rate", [0, -1.0, math.nan, math.inf, 1000.5, True, "5"])
def test_a_rate_out_of_range_is_refused(rate: object) -> None:
    with pytest.raises(ValueError, match="rate"):
        check_rate(rate)  # type: ignore[arg-type]
    assert check_rate(1000) == 1000.0
