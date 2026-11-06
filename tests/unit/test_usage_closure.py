"""Usage closure: the ``capture_of`` classification and the lifecycle arguments' checks.

Both are pure: the classifier reads an ``AttemptUsage`` only, and the lifecycle refuses
a bad closure or tier map before it touches the connection. The PostgreSQL behaviour is
covered by ``tests/integration/test_usage_closure_pg.py``.
"""

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any, NoReturn, cast

import pytest

from orqestra.domain.jobs import AttemptSucceeded
from orqestra.domain.retry import RetryPolicy
from orqestra.domain.states import AttemptState
from orqestra.persistence.lifecycle import complete_attempt, revoke_next_expired
from orqestra.persistence.provider_usage import (
    AttemptCapture,
    AttemptUsage,
    CallIntent,
    CallOutcome,
    CallResult,
    CaptureState,
    CaptureTier,
    ClosedBy,
    StoredCall,
    StoredClosure,
    StoredResult,
    UsageCall,
    UsageClosure,
    capture_of,
)

pytestmark = pytest.mark.unit

ATTEMPT = uuid.UUID(int=1)
NOW = datetime(2026, 1, 1, tzinfo=UTC)
RETRY = RetryPolicy(base_delay=timedelta(seconds=1), max_delay=timedelta(seconds=1))
FINISHED = [state for state in AttemptState if state is not AttemptState.RUNNING]


def call(seq: int, *, answered: bool = True) -> UsageCall:
    intent = CallIntent(
        attempt_id=ATTEMPT, call_seq=seq, provider="mock", route_id="r", capability="c"
    )
    stored = StoredCall(intent, uuid.UUID(int=2), uuid.UUID(int=3), AttemptState.RUNNING, NOW)
    if not answered:
        return UsageCall(stored, None)
    result = CallResult(attempt_id=ATTEMPT, call_seq=seq, outcome=CallOutcome.ANSWERED)
    return UsageCall(stored, StoredResult(result, AttemptState.RUNNING, NOW))


def complete(count: int) -> StoredClosure:
    closure = UsageClosure(
        attempt_id=ATTEMPT,
        capture=CaptureState.COMPLETE,
        closed_by=ClosedBy.WORKER,
        capture_tier=CaptureTier.T1,
        call_count=count,
    )
    return StoredClosure(closure, NOW)


def incomplete() -> StoredClosure:
    closure = UsageClosure(
        attempt_id=ATTEMPT,
        capture=CaptureState.INCOMPLETE,
        closed_by=ClosedBy.RECOVERY,
        capture_tier=CaptureTier.T3,
    )
    return StoredClosure(closure, NOW)


def usage(
    state: AttemptState, calls: tuple[UsageCall, ...], closure: StoredClosure | None
) -> AttemptUsage:
    return AttemptUsage(
        attempt_id=ATTEMPT,
        job_id=uuid.UUID(int=2),
        task_id=uuid.UUID(int=3),
        attempt_state=state,
        observed_at=NOW,
        calls=calls,
        closure=closure,
    )


# --- capture_of -------------------------------------------------------------------------

TWO = (call(0), call(1))


@pytest.mark.parametrize(
    ("calls", "closure", "expected"),
    [
        ((), None, AttemptCapture.NOT_CAPTURED),
        (TWO, None, AttemptCapture.LOWER_BOUND),  # calls are known; whether all is not
        ((), complete(0), AttemptCapture.CLOSED),
        (TWO, complete(2), AttemptCapture.CLOSED),
        (TWO, complete(1), AttemptCapture.LOWER_BOUND),  # an intent recorded after closing
        ((call(0), call(1, answered=False)), complete(2), AttemptCapture.LOWER_BOUND),
        ((), incomplete(), AttemptCapture.LOWER_BOUND),
        (TWO, incomplete(), AttemptCapture.LOWER_BOUND),
    ],
    ids=[
        "nothing",
        "calls-no-closure",
        "complete-0",
        "complete-matches",
        "complete-undercounts",
        "complete-unanswered",
        "incomplete-empty",
        "incomplete-calls",
    ],
)
@pytest.mark.parametrize("state", FINISHED)
def test_a_finished_attempt_is_classified_by_its_closure_and_calls(
    state: AttemptState,
    calls: tuple[UsageCall, ...],
    closure: StoredClosure | None,
    expected: AttemptCapture,
) -> None:
    assert capture_of(usage(state, calls, closure)) is expected


@pytest.mark.parametrize("closure", [None, complete(2), incomplete()])
def test_a_running_attempt_is_open_whatever_is_recorded(closure: StoredClosure | None) -> None:
    assert capture_of(usage(AttemptState.RUNNING, TWO, closure)) is AttemptCapture.OPEN


def test_a_classification_never_reports_a_total() -> None:
    assert {member.value for member in AttemptCapture} == {
        "open",
        "closed",
        "lower_bound",
        "not_captured",
    }


# --- argument checks before any I/O -----------------------------------------------------


class Untouchable:
    """A connection that fails the test if anything at all is asked of it."""

    def __getattr__(self, name: str) -> NoReturn:
        msg = f"the connection was used ({name}) before the arguments were checked"
        raise AssertionError(msg)


@pytest.fixture
def conn() -> Any:
    return cast(Any, Untouchable())


def worker_closure(**changes: Any) -> UsageClosure:
    fields: dict[str, Any] = {
        "attempt_id": ATTEMPT,
        "capture": CaptureState.INCOMPLETE,
        "closed_by": ClosedBy.WORKER,
        "capture_tier": CaptureTier.T1,
    }
    fields.update(changes)
    return UsageClosure(**fields)


@pytest.mark.parametrize(
    ("closure", "error", "message"),
    [
        ("incomplete", TypeError, "must be a UsageClosure, got str"),
        ({"attempt_id": ATTEMPT}, TypeError, "must be a UsageClosure, got dict"),
        (worker_closure(attempt_id=uuid.UUID(int=9)), ValueError, "names attempt"),
        (
            worker_closure(closed_by=ClosedBy.RECOVERY, capture_tier=CaptureTier.T3),
            ValueError,
            "closed by WORKER",
        ),
    ],
    ids=["str", "dict", "other-attempt", "recovery"],
)
def test_a_completion_refuses_a_bad_closure_before_any_io(
    conn: Any, closure: Any, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        complete_attempt(
            conn,
            ATTEMPT,
            generation=1,
            outcome=AttemptSucceeded(None),
            retry_policy=RETRY,
            usage_closure=closure,
        )


def test_a_valid_closure_passes_the_checks(conn: Any) -> None:
    with pytest.raises(AssertionError, match="the connection was used"):
        complete_attempt(
            conn,
            ATTEMPT,
            generation=1,
            outcome=AttemptSucceeded(None),
            retry_policy=RETRY,
            usage_closure=worker_closure(),
        )


@pytest.mark.parametrize(
    ("usage_capture", "error", "message"),
    [
        ("invoice", TypeError, "must be a mapping of job kinds, got str"),
        (["invoice"], TypeError, "must be a mapping of job kinds, got list"),
        ({}, ValueError, "at least one"),
        ({" ": CaptureTier.T3}, ValueError, "must not be blank"),
        ({"kind\x00": CaptureTier.T3}, ValueError, "NUL"),
        ({1: CaptureTier.T3}, TypeError, "each kind must be a str"),
        ({"invoice": "t3"}, TypeError, "must be a CaptureTier, got str"),
        ({"invoice": None}, TypeError, "must be a CaptureTier, got NoneType"),
    ],
    ids=["str", "list", "empty", "blank", "nul", "non-str", "plain-str-tier", "none-tier"],
)
def test_recovery_refuses_a_bad_tier_map_before_any_io(
    conn: Any, usage_capture: Any, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        revoke_next_expired(conn, retry_policy=RETRY, usage_capture=usage_capture)


@pytest.mark.parametrize(
    "usage_capture",
    [
        {"invoice": CaptureTier.T3},
        MappingProxyType({"invoice": CaptureTier.T1, "other": CaptureTier.T2}),
    ],
    ids=["dict", "read-only-mapping"],
)
def test_a_valid_tier_map_passes_the_checks(conn: Any, usage_capture: Mapping[str, Any]) -> None:
    with pytest.raises(AssertionError, match="the connection was used"):
        revoke_next_expired(conn, retry_policy=RETRY, usage_capture=usage_capture)
