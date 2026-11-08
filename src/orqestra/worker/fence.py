"""Local fencing for a worker process: a watchdog that ends the process (DEC-045).

A Python thread cannot be stopped, so the only way to stop a handler that may
no longer own its task is to end the whole process. The supervisor does that
itself when a heartbeat shows ownership is lost. It cannot be relied on for
the rest: a supervisor database call can stall indefinitely (a frozen server
or a stalled network still acknowledges TCP, so neither ``statement_timeout``
nor keepalives end the call). The watchdog is a separate thread that ends the
process when an armed deadline passes, whatever the main thread is doing.

It holds at most one deadline per ``Guard``; the earliest expiry ends the process:

- ``OWNERSHIP``: armed while a handler runs. Its deadline is the attempt's
  lease, mapped to the local clock so that it is never later than the lease
  really ends (``local_deadline``), minus the fencing margin. It moves later
  only when the supervisor has a confirmed renewal, earlier when a
  cancellation request shortens the lease, and is disarmed by the handler
  thread the moment the handler's work has ended.
- ``OPERATION``: armed around each supervisor database call, so a stalled call
  ends the process instead of hanging it. It says nothing about ownership.
- ``DRAIN``: armed once, when a stopping worker still holds a task: the end of
  the time ``shutdown_timeout`` gives it to settle that task (DEC-049). It can
  only end the process sooner, never extend a lease or another guard.

Each arming returns a token; only the current token can extend, restrict or
disarm its guard, so an old arming can never touch a newer one, and the
watchdog only ever checks the deadlines armed now.

Neither guard can stop a handler that holds the GIL in native code (the
watchdog thread then cannot run), or processes a handler started and detached.
"""

import json
import os
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from typing import NoReturn


class ExitCode(IntEnum):
    """Exit codes of a worker process that ended itself; values are stable."""

    FENCED = 3  # it could not establish that it still owns its running task
    STALLED = 4  # a database call outlived its bound
    HANDLER_ABORTED = 5  # the handler raised a control-plane signal; nothing was recorded
    AMBIGUOUS = 6  # whether a completion committed could not be established in time
    DEFECT = 7  # an invariant broke, or a valid outcome was rejected by the database
    # The outcome, or a provider call's intent or result (DEC-059), could not be
    # recorded before the attempt's deadline.
    UNRECORDED = 8
    SHUTDOWN = 9  # a stopping worker's task was not settled in time, or a second signal came


class Guard(StrEnum):
    """What a watchdog deadline protects."""

    OWNERSHIP = "ownership"
    OPERATION = "operation"
    DRAIN = "drain"


_EXIT_FOR = {
    Guard.OWNERSHIP: ExitCode.FENCED,
    Guard.OPERATION: ExitCode.STALLED,
    Guard.DRAIN: ExitCode.SHUTDOWN,
}


@dataclass(frozen=True, slots=True)
class Expiry:
    """A guard whose deadline passed while armed."""

    guard: Guard
    deadline: float
    now: float
    context: Mapping[str, str]


def end_process(code: ExitCode, event: str, message: str, context: Mapping[str, str]) -> NoReturn:
    """Write one JSON log line to stderr, best effort, and end the process at once with ``code``.

    ``os._exit`` ends every thread, including a handler that is still running;
    nothing else runs first. The line is written with ``os.write`` so that no
    lock another thread might hold is needed. The exit is in ``finally``: a
    failed write (stderr closed, a broken pipe) or anything else raised while
    writing can never prevent it.
    """
    try:
        line = {
            "ts": datetime.now(UTC).isoformat(),
            "level": "CRITICAL",
            "logger": "orqestra.worker",
            "message": message,
            "event": event,
            "exit_code": int(code),
            **context,
        }
        os.write(2, (json.dumps(line) + "\n").encode())
    finally:
        os._exit(int(code))


def _end_for_expiry(expiry: Expiry) -> None:
    end_process(
        _EXIT_FOR[expiry.guard],
        f"{expiry.guard.value}_deadline_passed",
        f"watchdog: the {expiry.guard.value} deadline passed; ending the process",
        expiry.context,
    )


def local_deadline(*, anchor: float, started_at: datetime, instant: datetime) -> float:
    """``instant`` on the database clock, as a local clock reading never later than it.

    ``anchor`` is the local clock read just before the claim was sent and
    ``started_at`` the claim instant the database recorded, which is later.
    So ``anchor + (instant - started_at)`` comes no later than ``instant``
    does, assuming both clocks run at the same rate; the fencing margin
    absorbs small rate differences.

    Both instants are converted to UTC first, so the difference is elapsed
    time, not wall time, whatever zone they were decoded in (DEC-067). A naive
    instant is refused: converting it would silently read it as local time.
    """
    if started_at.utcoffset() is None or instant.utcoffset() is None:
        msg = "local_deadline needs timezone-aware database instants"
        raise TypeError(msg)
    return anchor + (instant.astimezone(UTC) - started_at.astimezone(UTC)).total_seconds()


@dataclass(slots=True)
class _Armed:
    token: int
    deadline: float
    context: Mapping[str, str]


class Watchdog:
    """One daemon thread that ends the process when an armed deadline passes.

    ``clock`` and ``terminate`` are injectable: tests drive ``check`` with a
    fake clock, or run the thread with a ``terminate`` that only records.
    After an expiry every guard is disarmed, so ``terminate`` is called once
    per expiry even if it returns.
    """

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        terminate: Callable[[Expiry], None] = _end_for_expiry,
    ) -> None:
        self._clock = clock
        self._terminate = terminate
        self._condition = threading.Condition()
        self._armed: dict[Guard, _Armed] = {}
        self._tokens = 0
        self._stopping = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the watchdog thread (once)."""
        with self._condition:
            if self._thread is not None:
                msg = "the watchdog is already started"
                raise RuntimeError(msg)
            self._thread = threading.Thread(target=self._run, name="orqestra-watchdog", daemon=True)
        self._thread.start()

    def stop(self, timeout: float) -> None:
        """Stop the thread and wait up to ``timeout`` seconds for it; armed guards are dropped."""
        with self._condition:
            self._stopping = True
            self._armed.clear()
            self._condition.notify_all()
            thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def arm(self, guard: Guard, deadline: float, context: Mapping[str, str]) -> int:
        """Arm ``guard`` to expire at ``deadline`` (local clock); return its token."""
        with self._condition:
            if guard in self._armed:
                msg = f"the {guard.value} guard is already armed"
                raise RuntimeError(msg)
            self._tokens += 1
            self._armed[guard] = _Armed(self._tokens, deadline, dict(context))
            self._condition.notify_all()
            return self._tokens

    def extend(self, token: int, deadline: float) -> bool:
        """Move the deadline of ``token``'s guard later, never earlier.

        Returns ``False``, changing nothing, if ``token`` is no longer armed.
        """
        return self._move(token, lambda current: max(current, deadline))

    def restrict(self, token: int, deadline: float) -> bool:
        """Move the deadline of ``token``'s guard earlier, never later."""
        return self._move(token, lambda current: min(current, deadline))

    def disarm(self, token: int) -> bool:
        """Disarm ``token``'s guard; ``False`` if it was not armed (already disarmed or expired)."""
        with self._condition:
            for guard, armed in self._armed.items():
                if armed.token == token:
                    del self._armed[guard]
                    self._condition.notify_all()
                    return True
            return False

    def deadline(self, guard: Guard) -> float | None:
        """The armed deadline of ``guard``, or ``None``."""
        with self._condition:
            armed = self._armed.get(guard)
            return None if armed is None else armed.deadline

    def check(self) -> Expiry | None:
        """Expire the first armed guard whose deadline has passed, if any, and terminate."""
        with self._condition:
            return self._check()

    def _move(self, token: int, move: Callable[[float], float]) -> bool:
        with self._condition:
            for armed in self._armed.values():
                if armed.token == token:
                    armed.deadline = move(armed.deadline)
                    self._condition.notify_all()
                    return True
            return False

    def _check(self) -> Expiry | None:
        now = self._clock()
        due = [
            (armed.deadline, guard) for guard, armed in self._armed.items() if armed.deadline <= now
        ]
        if not due:
            return None
        _, guard = min(due)
        expiry = Expiry(guard, self._armed[guard].deadline, now, self._armed[guard].context)
        self._armed.clear()
        self._terminate(expiry)  # under the lock: a racing disarm or extend waits for it
        return expiry

    def _run(self) -> None:
        with self._condition:
            while not self._stopping:
                self._check()
                deadlines = [armed.deadline for armed in self._armed.values()]
                timeout = None if not deadlines else max(0.0, min(deadlines) - self._clock())
                self._condition.wait(timeout)
