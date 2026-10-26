from datetime import UTC, datetime, timedelta

import pytest

from orqestra.domain.leases import (
    LeasePolicy,
    RevocationReason,
    cancellation_lease,
    check_duration,
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


def reason_at(
    now: datetime,
    *,
    lease: datetime,
    deadline: datetime,
    requested: datetime | None = None,
) -> RevocationReason | None:
    return revocation_reason(
        lease_expires_at=lease, deadline_at=deadline, cancel_requested_at=requested, now=now
    )


def test_a_lease_holds_until_its_expiry_instant() -> None:
    lease, deadline = T0 + timedelta(minutes=5), T0 + timedelta(hours=1)
    for now in (T0, lease - MICROSECOND):
        assert reason_at(now, lease=lease, deadline=deadline) is None
        # A pending request does not end the lease early either.
        assert reason_at(now, lease=lease, deadline=deadline, requested=T0) is None


@pytest.mark.parametrize("after", [timedelta(0), MICROSECOND, timedelta(days=30)])
def test_an_expired_lease_short_of_the_deadline_was_not_renewed(after: timedelta) -> None:
    # Expiry is inclusive, and the reason does not depend on when recovery runs:
    # even long after the deadline, a lease that stopped short of it expired.
    lease, deadline = T0 + timedelta(minutes=5), T0 + timedelta(hours=1)
    assert (
        reason_at(lease + after, lease=lease, deadline=deadline) is RevocationReason.LEASE_EXPIRED
    )


@pytest.mark.parametrize("after", [timedelta(0), MICROSECOND, timedelta(days=30)])
def test_a_lease_capped_at_the_deadline_exceeded_it(after: timedelta) -> None:
    deadline = T0 + timedelta(hours=1)
    reason = reason_at(deadline + after, lease=deadline, deadline=deadline)
    assert reason is RevocationReason.DEADLINE_EXCEEDED


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        (T0, RevocationReason.CANCELLATION_GRACE_EXPIRED),
        (T0 + timedelta(minutes=5) - MICROSECOND, RevocationReason.CANCELLATION_GRACE_EXPIRED),
        # At or after expiry the lease was already lost: a later request does
        # not turn that into a grace expiry.
        (T0 + timedelta(minutes=5), RevocationReason.LEASE_EXPIRED),
        (T0 + timedelta(minutes=6), RevocationReason.LEASE_EXPIRED),
    ],
    ids=["well-before", "just-before", "at-expiry", "after-expiry"],
)
def test_a_lease_that_ran_out_after_a_request_ran_out_its_grace(
    requested: datetime, expected: RevocationReason
) -> None:
    lease, deadline = T0 + timedelta(minutes=5), T0 + timedelta(hours=1)
    for now in (lease, lease + timedelta(days=30)):
        assert reason_at(now, lease=lease, deadline=deadline, requested=requested) is expected


def test_the_deadline_takes_precedence_over_a_grace_period() -> None:
    # A request made while the lease held, but the lease had reached the deadline.
    deadline = T0 + timedelta(hours=1)
    reason = reason_at(deadline, lease=deadline, deadline=deadline, requested=T0)
    assert reason is RevocationReason.DEADLINE_EXCEEDED


def test_revocation_reason_values_are_stable() -> None:
    assert [reason.value for reason in RevocationReason] == [
        "lease_expired",
        "deadline_exceeded",
        "cancellation_grace_expired",
    ]


# --- cancellation_lease and grace durations -----------------------------------

GRACE = timedelta(minutes=1)


def grant(
    requested: datetime, lease: datetime, deadline: datetime, grace: timedelta = GRACE
) -> datetime:
    return cancellation_lease(
        requested_at=requested, grace=grace, lease_expires_at=lease, deadline_at=deadline
    )


def test_a_request_shortens_a_long_lease_to_the_grace() -> None:
    assert grant(T0, T0 + timedelta(minutes=30), T0 + timedelta(hours=1)) == T0 + GRACE


def test_a_request_near_the_end_of_a_short_lease_lengthens_it_to_the_grace() -> None:
    lease = T0 + timedelta(seconds=5)
    assert grant(T0 + timedelta(seconds=4), lease, T0 + timedelta(hours=1)) == (
        T0 + timedelta(seconds=4) + GRACE
    )
    # Even one microsecond before expiry the lease still holds.
    assert grant(lease - MICROSECOND, lease, T0 + timedelta(hours=1)) == lease - MICROSECOND + GRACE


@pytest.mark.parametrize("requested", [T0 + timedelta(seconds=5), T0 + timedelta(days=1)])
def test_a_request_at_or_after_expiry_never_revives_the_lease(requested: datetime) -> None:
    lease = T0 + timedelta(seconds=5)
    assert grant(requested, lease, T0 + timedelta(days=2)) == lease


def test_a_request_never_extends_past_the_deadline() -> None:
    deadline = T0 + timedelta(seconds=30)
    assert grant(T0, T0 + timedelta(seconds=10), deadline) == deadline  # grace outlasts it
    assert grant(T0, deadline, deadline) == deadline  # lease already at the deadline


def test_a_request_near_the_end_of_time_is_capped_not_overflowed() -> None:
    end = datetime.max.replace(tzinfo=UTC)
    now = end - timedelta(days=1)
    assert grant(now, end, end, grace=timedelta(days=2)) == end


@pytest.mark.parametrize(
    ("value", "error", "message"),
    [
        (timedelta(0), ValueError, "grace must be positive"),
        (-MICROSECOND, ValueError, "grace must be positive"),
        (timedelta.max, ValueError, "longer than any representable"),
        (60, TypeError, "grace must be a timedelta"),
        (None, TypeError, "grace must be a timedelta"),
    ],
)
def test_invalid_grace_durations_are_rejected(
    value: object, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        check_duration("grace", value)


def test_valid_grace_durations_are_accepted() -> None:
    for value in (MICROSECOND, GRACE, datetime.max - datetime.min):
        check_duration("grace", value)
