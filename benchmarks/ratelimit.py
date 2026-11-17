"""A cell's shared pace for model calls: at most ``per_s`` calls a second, across processes.

Every worker process of a cell builds a ``RateLimiter`` on the same file. Each call
first reserves the next free slot (``reserve``); the slots are ``1 / per_s`` seconds
apart, so the whole fleet starts at most ``per_s`` calls in any second, with no burst.
It paces; it never counts spend (the cell's ``CallBudget`` does that, DEC-071).

The file holds one number, the wall-clock time of the next free slot. It is updated
like the call counter (DEC-071, K1): under an exclusive ``flock`` on ``<path>.lock``, a
lock file whose identity never changes, the new value is written to a temporary file
and ``os.replace``d over ``path``. A process killed at any point leaves the old value or
the new one, both complete, and the kernel releases its lock. A missing file is a free
slot now; the first reservation creates it, so the harness only names the path.

Nothing waits without a bound. The lock is taken with ``LOCK_NB`` and polled until the
caller's ``max_wait`` runs out, so a stopped holder cannot wedge anyone; the delay to a
granted slot is returned, never slept here, so no lock is held while a caller waits.
A file that cannot be read, or a slot further ahead than ``HORIZON`` (the wall clock
went back, or the file was damaged), counts as a free slot now: failing open only
paces less for one step, and the call budget still bounds what is sent (DEC-073).
"""

import contextlib
import fcntl
import math
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path

LOCK_SUFFIX = ".lock"
# The most a rate may be: one slot per millisecond.
MAX_PER_S = 1000.0
# A stored slot further ahead than this is not believed (see the module docstring).
HORIZON = 60.0
# How often a busy lock is tried again.
LOCK_POLL = 0.005
# The most one document waits for its slot. A paced attempt keeps this much time for each
# document still to call, beside the calls themselves, so the harness sizes a paced cell's
# execution timeout with batch x LIMITER_MAX_WAIT on top of the calls (DEC-073).
LIMITER_MAX_WAIT = 10.0


def check_rate(per_s: float) -> float:
    """``per_s`` if it is a finite rate in (0, ``MAX_PER_S``]; ``ValueError`` otherwise."""
    if isinstance(per_s, bool) or not isinstance(per_s, int | float):
        msg = "the rate must be a number of calls per second"
        raise ValueError(msg)
    if not math.isfinite(per_s) or not 0 < per_s <= MAX_PER_S:
        msg = f"the rate must be above 0 and at most {MAX_PER_S:g} calls per second"
        raise ValueError(msg)
    return float(per_s)


def lock_path(path: Path) -> Path:
    """The lock file every update of the limiter file ``path`` holds while it runs."""
    return path.with_name(path.name + LOCK_SUFFIX)


def read_slot(path: Path) -> float | None:
    """The stored next free slot, or ``None`` if there is none or it cannot be read."""
    try:
        text = path.read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError):
        return None
    try:
        value = float(text.strip())
    except ValueError:
        return None
    return value if math.isfinite(value) else None


class RateLimiter:
    """Slots ``1 / per_s`` seconds apart, shared by every process that uses ``path``."""

    __slots__ = ("_clock", "_poll", "interval", "path", "per_s")

    def __init__(
        self,
        path: Path,
        per_s: float,
        *,
        clock: Callable[[], float] = time.time,
        poll: Callable[[float], None] = time.sleep,
    ) -> None:
        self.per_s = check_rate(per_s)
        self.interval = 1.0 / self.per_s
        self.path = path
        self._clock = clock  # the wall clock: the one clock every process shares
        self._poll = poll

    def reserve(self, max_wait: float) -> float | None:
        """Reserve the next free slot; return the seconds until it, or ``None``.

        ``None`` means no slot could be had within ``max_wait`` seconds: the lock stayed
        busy that long, or the next free slot is further away. Nothing is reserved then.
        The caller waits the returned delay itself before it calls.
        """
        allowed = max(max_wait, 0.0)
        deadline = time.monotonic() + allowed
        lock = os.open(lock_path(self.path), os.O_RDWR | os.O_CREAT, 0o644)
        try:  # closing the descriptor releases the lock on every way out
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        return None
                    self._poll(min(LOCK_POLL, left))
                    allowed = max(deadline - time.monotonic(), 0.0)  # the lock's wait counts
            now = self._clock()
            stored = read_slot(self.path)
            if stored is None or stored - now > HORIZON:
                stored = now
            slot = max(stored, now)
            delay = slot - now
            if delay > allowed:
                return None
            self._publish(slot + self.interval)
            return delay
        finally:
            os.close(lock)

    def _publish(self, value: float) -> None:
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="ascii") as file:
                file.write(f"{value!r}\n")
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.path)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()
            raise
