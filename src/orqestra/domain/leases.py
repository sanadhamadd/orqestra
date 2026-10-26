"""Attempt leases, execution deadlines and when ownership may be revoked.

Pure rules, no I/O. Persistence supplies every instant from the database
clock and stores what these functions return, so the lease arithmetic and
the expiry rule live in one place instead of being restated in SQL.

A claim gives its attempt a lease and a non-renewable execution deadline,
both measured from the claim instant. A heartbeat extends the lease, never
past the deadline and never backwards. An attempt whose lease has expired is
eligible for recovery; it loses ownership only when recovery commits its
revocation, so a completion that commits first still counts (ADR-0003).

A lease proves that a worker is alive, not that its work progresses: a
worker can keep renewing the lease of a hung task. The deadline bounds that
case, because no heartbeat can move the lease past it.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

# No duration longer than the whole range of datetime can ever be added to a
# claim instant; anything shorter is checked against the actual instant.
_LONGEST_REPRESENTABLE = datetime.max - datetime.min


class RevocationReason(StrEnum):
    """Why recovery revoked an attempt. Values are stable identifiers for storage."""

    LEASE_EXPIRED = "lease_expired"  # the heartbeats stopped before the deadline
    DEADLINE_EXCEEDED = "deadline_exceeded"  # the lease ran out at its deadline cap


@dataclass(frozen=True, slots=True)
class LeasePolicy:
    """How long a claim's lease lasts and how long its attempt may run in total.

    ``lease`` is the time each claim or heartbeat grants; ``execution_timeout``
    fixes the attempt's deadline at claim time. Both must be positive and
    ``lease`` cannot exceed ``execution_timeout``. There are no defaults:
    operational values, and any per-job-kind policy, are not decided yet.
    """

    lease: timedelta
    execution_timeout: timedelta

    def __post_init__(self) -> None:
        for name in ("lease", "execution_timeout"):
            value = getattr(self, name)
            if not isinstance(value, timedelta):
                msg = f"{name} must be a timedelta, got {type(value).__name__}"
                raise TypeError(msg)
            if value <= timedelta(0):
                msg = f"{name} must be positive, got {value}"
                raise ValueError(msg)
            if value > _LONGEST_REPRESENTABLE:
                msg = f"{name} {value} is longer than any representable timestamp range"
                raise ValueError(msg)
        if self.lease > self.execution_timeout:
            msg = (
                f"lease {self.lease} must not exceed execution_timeout "
                f"{self.execution_timeout}: no lease can outlast its deadline"
            )
            raise ValueError(msg)


def initial_lease(policy: LeasePolicy, claimed_at: datetime) -> tuple[datetime, datetime]:
    """Return ``(lease_expires_at, deadline_at)`` for an attempt claimed at ``claimed_at``.

    Raises ``ValueError`` when the deadline is not a representable timestamp.
    """
    try:
        deadline_at = claimed_at + policy.execution_timeout
    except OverflowError as exc:
        msg = f"the deadline {claimed_at} + {policy.execution_timeout} is not representable"
        raise ValueError(msg) from exc
    return min(claimed_at + policy.lease, deadline_at), deadline_at


def renewed_lease(
    policy: LeasePolicy, *, now: datetime, lease_expires_at: datetime, deadline_at: datetime
) -> datetime:
    """Return the lease a heartbeat at ``now`` grants: one lease from now, capped at the deadline.

    It never shortens the current lease, even if the clock moved backwards.
    """
    try:
        candidate = min(now + policy.lease, deadline_at)
    except OverflowError:
        candidate = deadline_at
    return max(lease_expires_at, candidate)


def revocation_reason(
    *, lease_expires_at: datetime, deadline_at: datetime, now: datetime
) -> RevocationReason | None:
    """Return why an attempt may be revoked at ``now``, or ``None`` while its lease holds.

    A lease expires at its ``lease_expires_at`` instant, inclusively. The
    reason follows from the stored facts alone, not from when recovery runs:
    a lease that reached its deadline cap exceeded the deadline; any other
    expired lease was not renewed in time.
    """
    if now < lease_expires_at:
        return None
    if lease_expires_at >= deadline_at:
        return RevocationReason.DEADLINE_EXCEEDED
    return RevocationReason.LEASE_EXPIRED
