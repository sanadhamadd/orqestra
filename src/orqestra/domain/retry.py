"""When a requeued task may run again: bounded exponential backoff with deterministic jitter.

Pure, no I/O (DEC-053). After attempt ``n`` of a task ends in a way that
requeues it (a transient failure, or a ``lease_expired`` or
``deadline_exceeded`` revocation), the task becomes eligible again
``retry_delay(policy, task_id, n)`` after the database's clock at the requeue.

The delay is the capped exponential ``min(max_delay, base_delay * 2**(n-1))``,
scaled by a jitter factor in ``[0.5, 1]``. The factor comes from a SHA-256
digest of the task's UUID and ``n``, so the same task and attempt always get
the same delay, in any process and after any restart, while different tasks
and attempts spread out. Arithmetic is in whole microseconds, the precision of
both ``timedelta`` and PostgreSQL ``timestamptz``, so it never overflows,
never goes below half the capped delay (rounded up) and never exceeds
``max_delay``.

There are no defaults: production values are not decided (DEC-034).
"""

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from orqestra.domain.leases import check_duration

_MICROSECOND = timedelta(microseconds=1)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How long a requeued task waits before it may run again.

    ``base_delay`` follows the first ended attempt; each later one doubles it,
    up to ``max_delay``. Both must be positive and representable, and
    ``base_delay`` cannot exceed ``max_delay``.
    """

    base_delay: timedelta
    max_delay: timedelta

    def __post_init__(self) -> None:
        check_duration("base_delay", self.base_delay)
        check_duration("max_delay", self.max_delay)
        if self.base_delay > self.max_delay:
            msg = f"base_delay {self.base_delay} must not exceed max_delay {self.max_delay}"
            raise ValueError(msg)


def retry_delay(policy: RetryPolicy, task_id: uuid.UUID, attempt_number: int) -> timedelta:
    """The wait before ``task_id`` may run again after its attempt ``attempt_number`` ended.

    ``attempt_number`` is the ended attempt's generation, from 1 and below
    2**64 (stored generations are far smaller; the bound is the width of the
    jitter input). The result lies in ``[ceil(capped / 2), capped]``, where
    ``capped`` is the capped exponential delay.
    """
    if isinstance(attempt_number, bool) or not isinstance(attempt_number, int):
        msg = f"attempt_number must be an integer, got {type(attempt_number).__name__}"
        raise TypeError(msg)
    if attempt_number < 1:
        msg = f"attempt_number must be at least 1, got {attempt_number}"
        raise ValueError(msg)
    if attempt_number >= 1 << 64:
        msg = f"attempt_number must be below 2**64, got {attempt_number}"
        raise ValueError(msg)
    base = policy.base_delay // _MICROSECOND
    cap = policy.max_delay // _MICROSECOND
    shift = attempt_number - 1
    # Once base << shift could exceed the cap, the cap applies; never build a huge int.
    capped = cap if shift >= cap.bit_length() else min(cap, base << shift)
    digest = hashlib.sha256(task_id.bytes + attempt_number.to_bytes(8, "big")).digest()
    fraction = int.from_bytes(digest[:8], "big")  # uniform in [0, 2**64)
    low = (capped + 1) // 2  # half the capped delay, rounded up
    return timedelta(microseconds=low + (((capped - low) * fraction) >> 64))


def retry_at(
    policy: RetryPolicy, task_id: uuid.UUID, attempt_number: int, now: datetime
) -> datetime:
    """When the task may run again: ``now``, the database's clock, plus its delay.

    Raises ``ValueError`` when that is not a representable timestamp.
    """
    delay = retry_delay(policy, task_id, attempt_number)
    try:
        return now + delay
    except OverflowError as exc:
        msg = f"the retry time {now} + {delay} is not representable"
        raise ValueError(msg) from exc
