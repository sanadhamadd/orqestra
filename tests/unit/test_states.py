import pytest

from orqestra.domain.errors import InvalidTransitionError
from orqestra.domain.states import (
    TASK_TRANSITIONS,
    TERMINAL_TASK_STATES,
    TaskState,
    can_transition,
    transition,
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
