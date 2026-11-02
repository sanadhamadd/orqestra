"""Retry backoff (DEC-053): bounded exponential delay with deterministic jitter; no clock."""

import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from orqestra.domain.retry import RetryPolicy, retry_at, retry_delay

pytestmark = pytest.mark.unit

US = timedelta(microseconds=1)
POLICY = RetryPolicy(base_delay=timedelta(seconds=1), max_delay=timedelta(minutes=5))  # examples
TASKS = [uuid.UUID(int=n) for n in range(1, 41)] + [uuid.uuid5(uuid.NAMESPACE_DNS, "orqestra")]


def capped(policy: RetryPolicy, n: int) -> timedelta:
    """The capped exponential, in whole microseconds (Python ints never overflow)."""
    base, cap = policy.base_delay // US, policy.max_delay // US
    return timedelta(microseconds=min(cap, base * 2 ** (n - 1)))


# --- the policy ----------------------------------------------------------------------------


def test_a_policy_needs_both_delays() -> None:
    with pytest.raises(TypeError):
        RetryPolicy(timedelta(seconds=1))  # type: ignore[call-arg]


@pytest.mark.parametrize("field", ["base_delay", "max_delay"])
@pytest.mark.parametrize(
    ("value", "error"),
    [
        (timedelta(0), ValueError),
        (-US, ValueError),
        (timedelta(days=4_000_000), ValueError),  # longer than any representable range
        (5, TypeError),
        (None, TypeError),
    ],
)
def test_a_delay_must_be_a_positive_representable_duration(
    field: str, value: Any, error: type[Exception]
) -> None:
    values: dict[str, Any] = {"base_delay": timedelta(seconds=1), "max_delay": timedelta(hours=1)}
    values[field] = value
    with pytest.raises(error, match=field):
        RetryPolicy(**values)


def test_the_base_delay_cannot_exceed_the_maximum() -> None:
    assert RetryPolicy(timedelta(seconds=5), timedelta(seconds=5)).max_delay == timedelta(seconds=5)
    with pytest.raises(ValueError, match="must not exceed max_delay"):
        RetryPolicy(timedelta(seconds=5) + US, timedelta(seconds=5))


def test_a_policy_is_immutable() -> None:
    with pytest.raises(AttributeError):
        POLICY.base_delay = timedelta(seconds=2)  # type: ignore[misc]


# --- the delay -----------------------------------------------------------------------------


@pytest.mark.parametrize("task", TASKS)
def test_every_delay_lies_between_half_and_all_of_the_capped_exponential(task: uuid.UUID) -> None:
    for n in range(1, 101):  # every generation a task can have (max_attempts <= 100)
        delay, limit = retry_delay(POLICY, task, n), capped(POLICY, n)
        assert -(-limit // 2) <= delay <= limit <= POLICY.max_delay  # ceil(limit / 2) <= delay
        assert delay > timedelta(0)


def test_the_capped_exponential_doubles_until_the_maximum() -> None:
    assert [capped(POLICY, n).total_seconds() for n in range(1, 11)] == [
        1, 2, 4, 8, 16, 32, 64, 128, 256, 300,
    ]  # fmt: skip
    # The delays follow it: never above it, and the cap holds every later generation.
    task = TASKS[0]
    assert all(retry_delay(POLICY, task, n) <= POLICY.max_delay for n in range(9, 101))


def test_a_huge_generation_is_capped_without_overflow() -> None:
    tiny = RetryPolicy(US, timedelta(days=3_000_000))  # the largest representable cap, roughly
    for n in (64, 1_000, 10**6):
        assert retry_delay(tiny, TASKS[0], n) <= tiny.max_delay
    assert retry_delay(POLICY, TASKS[0], 10**9) <= POLICY.max_delay


def test_a_one_microsecond_policy_waits_one_microsecond() -> None:
    one = RetryPolicy(US, US)
    assert {retry_delay(one, task, n) for task in TASKS for n in (1, 2, 50)} == {US}


def test_the_delay_is_the_same_for_the_same_task_and_attempt() -> None:
    task = TASKS[3]
    assert [retry_delay(POLICY, task, n) for n in range(1, 21)] == [
        retry_delay(POLICY, uuid.UUID(str(task)), n) for n in range(1, 21)
    ]


def test_the_delay_is_the_same_in_another_process() -> None:
    # Not Python's per-process randomised hash(): a fresh interpreter agrees.
    code = (
        "import uuid; from datetime import timedelta as d;"
        "from orqestra.domain.retry import RetryPolicy, retry_delay;"
        "print(retry_delay(RetryPolicy(d(seconds=1), d(minutes=5)), uuid.UUID(int=7), 6))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout.strip()
    assert out == str(retry_delay(POLICY, uuid.UUID(int=7), 6))


def test_the_jitter_spreads_across_tasks_and_generations() -> None:
    at_cap = [retry_delay(POLICY, task, 20) for task in TASKS]  # all capped at five minutes
    assert len(set(at_cap)) == len(TASKS)  # no two tasks retry at the same instant
    assert min(at_cap) < timedelta(minutes=3) < timedelta(minutes=4) < max(at_cap)
    per_generation = [retry_delay(POLICY, TASKS[0], n) / capped(POLICY, n) for n in range(9, 30)]
    assert len(set(per_generation)) == len(per_generation)  # a new factor for every attempt


@pytest.mark.parametrize(
    ("attempt_number", "error"),
    [
        (0, ValueError),
        (-1, ValueError),
        (2**64, ValueError),  # wider than the jitter input
        (True, TypeError),
        (1.0, TypeError),
        ("1", TypeError),
    ],
)
def test_the_attempt_number_is_a_generation_from_one(attempt_number: Any, error: type) -> None:
    with pytest.raises(error, match="attempt_number"):
        retry_delay(POLICY, TASKS[0], attempt_number)


def test_the_largest_generation_the_jitter_can_hold_is_accepted() -> None:
    limit = POLICY.max_delay  # 2**64 - 1 shifts far past the cap, so the cap applies
    for task in TASKS[:5]:
        delay = retry_delay(POLICY, task, 2**64 - 1)
        assert -(-limit // 2) <= delay <= limit


def test_the_first_ended_attempt_uses_the_base_delay() -> None:
    # Generation 1 is the first attempt; its delay is jittered from base_delay, not 2x.
    for task in TASKS:
        assert retry_delay(POLICY, task, 1) <= POLICY.base_delay


# --- the retry time ------------------------------------------------------------------------


def test_the_retry_time_is_now_plus_the_delay() -> None:
    now = datetime(2026, 10, 29, 12, tzinfo=UTC)
    assert retry_at(POLICY, TASKS[0], 3, now) == now + retry_delay(POLICY, TASKS[0], 3)


def test_an_unrepresentable_retry_time_is_refused() -> None:
    late = datetime(9999, 12, 31, 23, 59, tzinfo=UTC)
    with pytest.raises(ValueError, match="not representable"):
        retry_at(POLICY, TASKS[0], 20, late)  # at least 2.5 minutes past 9999
