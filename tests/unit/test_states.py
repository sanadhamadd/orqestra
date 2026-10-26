import pytest

from orqestra.domain.errors import InvalidTransitionError
from orqestra.domain.states import (
    ATTEMPT_TRANSITIONS,
    TASK_TRANSITIONS,
    TERMINAL_ATTEMPT_STATES,
    TERMINAL_TASK_STATES,
    AttemptState,
    TaskState,
    allowed_sources,
    can_transition,
    can_transition_attempt,
    transition,
    transition_attempt,
)

pytestmark = pytest.mark.unit

ALLOWED = [
    (TaskState.QUEUED, TaskState.RUNNING),
    (TaskState.QUEUED, TaskState.CANCELLED),
    (TaskState.RUNNING, TaskState.SUCCEEDED),
    (TaskState.RUNNING, TaskState.FAILED),
    (TaskState.RUNNING, TaskState.QUEUED),
    (TaskState.RUNNING, TaskState.CANCELLED),
]
ALL_PAIRS = [(a, b) for a in TaskState for b in TaskState]
DISALLOWED = [pair for pair in ALL_PAIRS if pair not in ALLOWED]


@pytest.mark.parametrize(("current", "new"), ALLOWED)
def test_allowed_transitions(current: TaskState, new: TaskState) -> None:
    assert can_transition(current, new)
    assert transition(current, new) is new


@pytest.mark.parametrize(("current", "new"), DISALLOWED)
def test_disallowed_transitions_raise(current: TaskState, new: TaskState) -> None:
    assert not can_transition(current, new)
    with pytest.raises(InvalidTransitionError, match=f"{current.value}.*{new.value}"):
        transition(current, new)


def test_terminal_states_have_no_exits() -> None:
    expected = {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}
    assert expected == TERMINAL_TASK_STATES
    for state in TERMINAL_TASK_STATES:
        assert TASK_TRANSITIONS[state] == frozenset()


def test_every_state_is_reachable_from_queued() -> None:
    seen = {TaskState.QUEUED}
    frontier = [TaskState.QUEUED]
    while frontier:
        for target in TASK_TRANSITIONS[frontier.pop()]:
            if target not in seen:
                seen.add(target)
                frontier.append(target)
    assert seen == set(TaskState)


def test_transition_table_covers_every_state() -> None:
    assert set(TASK_TRANSITIONS) == set(TaskState)


def test_transition_table_is_immutable() -> None:
    with pytest.raises(TypeError):
        TASK_TRANSITIONS[TaskState.SUCCEEDED] = frozenset({TaskState.QUEUED})  # type: ignore[index]


@pytest.mark.parametrize("target", list(TaskState))
def test_allowed_sources_are_derived_from_the_table(target: TaskState) -> None:
    expected = {state for state in TaskState if can_transition(state, target)}
    assert allowed_sources(target) == expected


def test_allowed_sources_for_each_target() -> None:
    assert allowed_sources(TaskState.QUEUED) == {TaskState.RUNNING}
    assert allowed_sources(TaskState.RUNNING) == {TaskState.QUEUED}
    assert allowed_sources(TaskState.SUCCEEDED) == {TaskState.RUNNING}
    assert allowed_sources(TaskState.FAILED) == {TaskState.RUNNING}
    assert allowed_sources(TaskState.CANCELLED) == {TaskState.QUEUED, TaskState.RUNNING}


def test_storage_values_are_stable() -> None:
    assert [state.value for state in TaskState] == [
        "queued",
        "running",
        "succeeded",
        "failed",
        "cancelled",
    ]
    assert [state.value for state in AttemptState] == [
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "revoked",
    ]


ATTEMPT_ALLOWED = [
    (AttemptState.RUNNING, AttemptState.SUCCEEDED),
    (AttemptState.RUNNING, AttemptState.FAILED),
    (AttemptState.RUNNING, AttemptState.CANCELLED),
    (AttemptState.RUNNING, AttemptState.REVOKED),
]
ATTEMPT_DISALLOWED = [
    (a, b) for a in AttemptState for b in AttemptState if (a, b) not in ATTEMPT_ALLOWED
]


@pytest.mark.parametrize(("current", "new"), ATTEMPT_ALLOWED)
def test_allowed_attempt_transitions(current: AttemptState, new: AttemptState) -> None:
    assert can_transition_attempt(current, new)
    assert transition_attempt(current, new) is new


@pytest.mark.parametrize(("current", "new"), ATTEMPT_DISALLOWED)
def test_disallowed_attempt_transitions_raise(current: AttemptState, new: AttemptState) -> None:
    assert not can_transition_attempt(current, new)
    with pytest.raises(InvalidTransitionError, match=f"attempt.*{current.value}.*{new.value}"):
        transition_attempt(current, new)


def test_attempts_end_exactly_once() -> None:
    assert set(ATTEMPT_TRANSITIONS) == set(AttemptState)
    assert {
        AttemptState.SUCCEEDED,
        AttemptState.FAILED,
        AttemptState.CANCELLED,
        AttemptState.REVOKED,
    } == TERMINAL_ATTEMPT_STATES
    assert ATTEMPT_TRANSITIONS[AttemptState.RUNNING] == TERMINAL_ATTEMPT_STATES


def test_attempt_transition_table_is_immutable() -> None:
    with pytest.raises(TypeError):
        ATTEMPT_TRANSITIONS[AttemptState.FAILED] = frozenset({AttemptState.RUNNING})  # type: ignore[index]
