from datetime import UTC, datetime, timedelta

import pytest

from orqestra.domain.leases import (
    LeasePolicy,
    RevocationReason,
    initial_lease,
    renewed_lease,
    revocation_reason,
)

pytestmark = pytest.mark.unit

T0 = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
MICROSECOND = timedelta(microseconds=1)
POLICY = LeasePolicy(lease=timedelta(minutes=5), execution_timeout=timedelta(hours=1))


# --- LeasePolicy ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("lease", "timeout"),
    [
        (MICROSECOND, MICROSECOND),
        (timedelta(seconds=30), timedelta(minutes=10)),
        (timedelta(hours=2), timedelta(hours=2)),
        (timedelta(days=1), datetime.max - datetime.min),
    ],
)
def test_valid_policies(lease: timedelta, timeout: timedelta) -> None:
    policy = LeasePolicy(lease, timeout)
    assert (policy.lease, policy.execution_timeout) == (lease, timeout)


@pytest.mark.parametrize(
    ("lease", "timeout", "message"),
    [
        (timedelta(0), timedelta(minutes=1), "lease must be positive"),
        (-MICROSECOND, timedelta(minutes=1), "lease must be positive"),
        (timedelta(minutes=1), timedelta(0), "execution_timeout must be positive"),
        (timedelta(minutes=1), -timedelta(days=1), "execution_timeout must be positive"),
        (timedelta(minutes=2), timedelta(minutes=1), "must not exceed execution_timeout"),
        (MICROSECOND, timedelta.max, "longer than any representable"),
    ],
)
def test_invalid_policies_are_rejected(lease: timedelta, timeout: timedelta, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        LeasePolicy(lease, timeout)


@pytest.mark.parametrize(
    ("lease", "timeout"),
    [(30, timedelta(minutes=1)), (timedelta(seconds=1), 60.0), (None, timedelta(minutes=1))],
)
def test_durations_must_be_explicit_timedeltas(lease: object, timeout: object) -> None:
    with pytest.raises(TypeError, match="must be a timedelta"):
        LeasePolicy(lease, timeout)  # type: ignore[arg-type]


# --- initial_lease -------------------------------------------------------------


def test_initial_lease_measures_both_from_the_claim_instant() -> None:
    assert initial_lease(POLICY, T0) == (T0 + timedelta(minutes=5), T0 + timedelta(hours=1))


def test_initial_lease_never_exceeds_the_deadline() -> None:
    equal = LeasePolicy(timedelta(hours=1), timedelta(hours=1))
    assert initial_lease(equal, T0) == (T0 + timedelta(hours=1), T0 + timedelta(hours=1))


def test_an_unrepresentable_deadline_is_rejected() -> None:
    distant = LeasePolicy(timedelta(days=1), timedelta(days=365 * 8000))
    with pytest.raises(ValueError, match="not representable"):
        initial_lease(distant, T0)


# --- renewed_lease -------------------------------------------------------------


def test_a_renewal_grants_one_lease_from_now() -> None:
    lease, deadline = initial_lease(POLICY, T0)
    now = T0 + timedelta(minutes=3)
    assert renewed_lease(POLICY, now=now, lease_expires_at=lease, deadline_at=deadline) == (
        now + POLICY.lease
    )


def test_a_renewal_is_capped_at_the_deadline() -> None:
    _, deadline = initial_lease(POLICY, T0)
    now = deadline - timedelta(minutes=1)
    assert renewed_lease(POLICY, now=now, lease_expires_at=now, deadline_at=deadline) == deadline


def test_a_renewal_never_shortens_the_lease() -> None:
    # A shorter policy, or a database clock that stepped back, keeps the current lease.
    current = T0 + timedelta(minutes=30)
    deadline = T0 + timedelta(hours=1)
    brief = LeasePolicy(timedelta(seconds=1), timedelta(hours=1))
    assert renewed_lease(brief, now=T0, lease_expires_at=current, deadline_at=deadline) == current
    earlier = T0 - timedelta(minutes=10)
    assert (
        renewed_lease(POLICY, now=earlier, lease_expires_at=current, deadline_at=deadline)
        == current
    )


def test_a_renewal_near_the_end_of_time_is_capped_not_overflowed() -> None:
    end = datetime.max.replace(tzinfo=UTC)
    policy = LeasePolicy(timedelta(days=2), timedelta(days=2))
    now = end - timedelta(days=1)
    assert renewed_lease(policy, now=now, lease_expires_at=now, deadline_at=end) == end


# --- revocation_reason ---------------------------------------------------------


def test_a_lease_holds_until_its_expiry_instant() -> None:
    lease, deadline = T0 + timedelta(minutes=5), T0 + timedelta(hours=1)
    for now in (T0, lease - MICROSECOND):
        assert revocation_reason(lease_expires_at=lease, deadline_at=deadline, now=now) is None


@pytest.mark.parametrize("after", [timedelta(0), MICROSECOND, timedelta(days=30)])
def test_an_expired_lease_short_of_the_deadline_was_not_renewed(after: timedelta) -> None:
    # Expiry is inclusive, and the reason does not depend on when recovery runs:
    # even long after the deadline, a lease that stopped short of it expired.
    lease, deadline = T0 + timedelta(minutes=5), T0 + timedelta(hours=1)
    reason = revocation_reason(lease_expires_at=lease, deadline_at=deadline, now=lease + after)
    assert reason is RevocationReason.LEASE_EXPIRED


@pytest.mark.parametrize("after", [timedelta(0), MICROSECOND, timedelta(days=30)])
def test_a_lease_capped_at_the_deadline_exceeded_it(after: timedelta) -> None:
    deadline = T0 + timedelta(hours=1)
    reason = revocation_reason(
        lease_expires_at=deadline, deadline_at=deadline, now=deadline + after
    )
    assert reason is RevocationReason.DEADLINE_EXCEEDED


def test_revocation_reason_values_are_stable() -> None:
    assert [reason.value for reason in RevocationReason] == ["lease_expired", "deadline_exceeded"]
