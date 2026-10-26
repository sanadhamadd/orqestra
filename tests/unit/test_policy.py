import asyncio
import itertools

import pytest

from orqestra.domain.errors import (
    InvalidTransitionError,
    OrqestraError,
    PermanentError,
    TransientError,
    is_retryable,
)
from orqestra.domain.policy import (
    FailureKind,
    classify_failure,
    next_task_state,
    state_after_cancel_request,
)
from orqestra.domain.states import TASK_TRANSITIONS, AttemptState, TaskState

pytestmark = pytest.mark.unit


class RateLimitedError(TransientError):
    pass


class RetryableCustomError(OrqestraError):
    retryable = True


EXCEPTIONS: list[Exception] = [
    TransientError("timeout"),
    RateLimitedError("429"),
    RetryableCustomError("custom"),
    PermanentError("invalid invoice"),
    InvalidTransitionError("bad move"),
    OrqestraError("unspecified"),
    RuntimeError("a bug"),
    KeyError("missing"),
    ConnectionResetError("infrastructure"),
]


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (TransientError("timeout"), FailureKind.TRANSIENT),
        (RateLimitedError("429"), FailureKind.TRANSIENT),
        (RetryableCustomError("custom"), FailureKind.TRANSIENT),
        (PermanentError("invalid invoice"), FailureKind.PERMANENT),
        (InvalidTransitionError("bad move"), FailureKind.PERMANENT),
        (OrqestraError("unspecified"), FailureKind.UNCLASSIFIED),
        (RuntimeError("a bug"), FailureKind.UNCLASSIFIED),
        (KeyError("missing"), FailureKind.UNCLASSIFIED),
        (ConnectionResetError("infrastructure"), FailureKind.UNCLASSIFIED),
    ],
)
def test_classify_failure(exc: Exception, expected: FailureKind) -> None:
    assert classify_failure(exc) is expected


@pytest.mark.parametrize("exc", EXCEPTIONS)
def test_transient_classification_matches_is_retryable(exc: Exception) -> None:
    assert (classify_failure(exc) is FailureKind.TRANSIENT) == is_retryable(exc)


@pytest.mark.parametrize(
    "signal",
    [KeyboardInterrupt(), SystemExit(1), asyncio.CancelledError(), GeneratorExit()],
)
def test_control_plane_signals_are_not_handler_failures(signal: BaseException) -> None:
    with pytest.raises(TypeError, match="not a handler failure"):
        classify_failure(signal)


def test_failure_kind_values_are_stable() -> None:
    assert [kind.value for kind in FailureKind] == ["transient", "permanent", "unclassified"]


# (outcome, failure kind, attempts left?, cancel requested?) -> next task state
EXPECTED: dict[tuple[AttemptState, FailureKind | None, bool, bool], TaskState] = {}
for left, cancel in itertools.product([True, False], repeat=2):
    EXPECTED[(AttemptState.SUCCEEDED, None, left, cancel)] = TaskState.SUCCEEDED
    EXPECTED[(AttemptState.FAILED, FailureKind.PERMANENT, left, cancel)] = TaskState.FAILED
    EXPECTED[(AttemptState.FAILED, FailureKind.UNCLASSIFIED, left, cancel)] = TaskState.FAILED
    EXPECTED[(AttemptState.FAILED, FailureKind.TRANSIENT, left, cancel)] = (
        (TaskState.CANCELLED if cancel else TaskState.QUEUED) if left else TaskState.FAILED
    )
    if cancel:
        EXPECTED[(AttemptState.CANCELLED, None, left, cancel)] = TaskState.CANCELLED
    # Revocation is not a handler failure, but it moves the task like a transient
    # one; with no attempts left it fails, even with a request pending (DEC-038).
    EXPECTED[(AttemptState.REVOKED, None, left, cancel)] = (
        (TaskState.CANCELLED if cancel else TaskState.QUEUED) if left else TaskState.FAILED
    )


def _attempts(left: bool) -> tuple[int, int]:
    """(attempt_count, max_attempts) with or without attempts remaining."""
    return (2, 3) if left else (3, 3)


@pytest.mark.parametrize(("case", "expected"), list(EXPECTED.items()))
def test_next_task_state_decision_table(
    case: tuple[AttemptState, FailureKind | None, bool, bool], expected: TaskState
) -> None:
    outcome, kind, left, cancel = case
    attempt_count, max_attempts = _attempts(left)
    result = next_task_state(
        outcome,
        kind,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        cancel_requested=cancel,
    )
    assert result is expected
    # Every decision is a legal move out of RUNNING, and a requeued task stays
    # claimable, matching the tasks_queued_has_attempts_left constraint.
    assert result in TASK_TRANSITIONS[TaskState.RUNNING]
    if result is TaskState.QUEUED:
        assert attempt_count < max_attempts


@pytest.mark.parametrize("max_attempts", [1, 2, 3])
@pytest.mark.parametrize("cancel", [False, True])
def test_a_revocation_moves_the_task_exactly_like_a_transient_failure(
    max_attempts: int, cancel: bool
) -> None:
    for attempt_count in range(1, max_attempts + 1):
        revoked = next_task_state(
            AttemptState.REVOKED,
            None,
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            cancel_requested=cancel,
        )
        transient = next_task_state(
            AttemptState.FAILED,
            FailureKind.TRANSIENT,
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            cancel_requested=cancel,
        )
        assert revoked is transient


def test_single_attempt_task_fails_on_first_transient_error() -> None:
    result = next_task_state(
        AttemptState.FAILED,
        FailureKind.TRANSIENT,
        attempt_count=1,
        max_attempts=1,
        cancel_requested=False,
    )
    assert result is TaskState.FAILED


def test_cancelled_attempt_without_request_is_rejected() -> None:
    with pytest.raises(ValueError, match="cancellation request"):
        next_task_state(
            AttemptState.CANCELLED,
            None,
            attempt_count=1,
            max_attempts=3,
            cancel_requested=False,
        )


def test_running_is_not_an_outcome() -> None:
    with pytest.raises(ValueError, match="has not ended"):
        next_task_state(
            AttemptState.RUNNING, None, attempt_count=1, max_attempts=3, cancel_requested=False
        )


@pytest.mark.parametrize(
    ("outcome", "kind"),
    [
        (AttemptState.FAILED, None),
        (AttemptState.SUCCEEDED, FailureKind.TRANSIENT),
        (AttemptState.CANCELLED, FailureKind.PERMANENT),
        (AttemptState.REVOKED, FailureKind.TRANSIENT),
    ],
)
def test_failure_kind_must_match_outcome(outcome: AttemptState, kind: FailureKind | None) -> None:
    with pytest.raises(ValueError, match="failure_kind"):
        next_task_state(outcome, kind, attempt_count=1, max_attempts=3, cancel_requested=True)


@pytest.mark.parametrize(("attempt_count", "max_attempts"), [(0, 3), (4, 3), (-1, 3)])
def test_attempt_count_must_identify_a_started_attempt(
    attempt_count: int, max_attempts: int
) -> None:
    with pytest.raises(ValueError, match="attempt_count"):
        next_task_state(
            AttemptState.SUCCEEDED,
            None,
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            cancel_requested=False,
        )


@pytest.mark.parametrize(
    ("current", "expected"),
    [
        (TaskState.QUEUED, TaskState.CANCELLED),
        (TaskState.RUNNING, TaskState.RUNNING),
        (TaskState.SUCCEEDED, TaskState.SUCCEEDED),
        (TaskState.FAILED, TaskState.FAILED),
        (TaskState.CANCELLED, TaskState.CANCELLED),
    ],
)
def test_state_after_cancel_request(current: TaskState, expected: TaskState) -> None:
    result = state_after_cancel_request(current)
    assert result is expected
    assert result is current or result in TASK_TRANSITIONS[current]
