"""``read_completion_evidence`` against PostgreSQL (DEC-047).

Every case records a real completion (or revocation) with the lifecycle
operations, then asks whether a given outcome is what was recorded. Results are
compared with PostgreSQL's ``jsonb`` equality, so the cases include values on
which Python equality and ``jsonb`` disagree.
"""

import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from psycopg import errors

from orqestra.domain.jobs import AttemptCancelled, AttemptFailed, AttemptOutcome, AttemptSucceeded
from orqestra.domain.policy import FailureKind
from orqestra.persistence.lifecycle import (
    CompletionEvidence,
    read_completion_evidence,
    request_cancellation,
    start_attempt,
)
from tests.integration.test_attempt_lifecycle import (
    EXPIRING,
    GRACE,
    PERMANENT,
    POLICY,
    SUCCESS,
    TRANSIENT,
    Conn,
    assert_invariants,
    complete,
    new_task,
    revoke,
    task_now,
    until_expired,
)

pytestmark = pytest.mark.integration

UNCLASSIFIED = AttemptFailed(FailureKind.UNCLASSIFIED, "builtins.KeyError", "'total'")
ALL_OUTCOMES: list[AttemptOutcome] = [
    SUCCESS,
    AttemptSucceeded(None),
    TRANSIENT,
    PERMANENT,
    UNCLASSIFIED,
    AttemptCancelled(),
]


@pytest.fixture
def checked(db: Conn) -> Iterator[Conn]:
    """The migrated test connection; the lifecycle invariants must hold when the test ends."""
    yield db
    assert_invariants(db)


def evidence(
    db: Conn, attempt_id: uuid.UUID, outcome: AttemptOutcome, generation: int = 1
) -> CompletionEvidence:
    return read_completion_evidence(db, attempt_id, generation=generation, outcome=outcome)


@pytest.mark.parametrize("outcome", ALL_OUTCOMES, ids=repr)
def test_a_running_attempt_shows_that_nothing_committed(
    checked: Conn, outcome: AttemptOutcome
) -> None:
    attempt = start_attempt(checked, new_task(checked).id, "w", policy=POLICY)
    assert evidence(checked, attempt.id, outcome) is CompletionEvidence.RUNNING


@pytest.mark.parametrize(
    ("stored", "asked", "expected"),
    [
        ({"total": 3}, {"total": 3}, CompletionEvidence.MATCHES),
        (
            {"b": 1, "a": 2},
            {"a": 2, "b": 1},
            CompletionEvidence.MATCHES,
        ),  # key order is not part of jsonb
        (None, None, CompletionEvidence.MATCHES),  # JSON null is a result
        (1e308, 1e308, CompletionEvidence.MATCHES),  # jsonb stores 309 digits; Python's == says no
        (1.0, 1, CompletionEvidence.MATCHES),  # numeric equality
        (-0.0, 0, CompletionEvidence.MATCHES),
        (True, True, CompletionEvidence.MATCHES),
        (True, 1, CompletionEvidence.CONFLICTS),  # Python's True == 1; jsonb keeps them apart
        (0, False, CompletionEvidence.CONFLICTS),
        (None, {}, CompletionEvidence.CONFLICTS),
        ({"total": 3}, {"total": 4}, CompletionEvidence.CONFLICTS),
        ("1", 1, CompletionEvidence.CONFLICTS),
    ],
    ids=[
        "equal",
        "key-order",
        "json-null",
        "1e308",
        "float-int",
        "negative-zero",
        "true",
        "true-vs-1",
        "0-vs-false",
        "null-vs-empty",
        "different",
        "string-vs-number",
    ],
)
def test_a_success_matches_only_a_jsonb_equal_result(
    checked: Conn, stored: Any, asked: Any, expected: CompletionEvidence
) -> None:
    attempt = start_attempt(checked, new_task(checked).id, "w", policy=POLICY)
    complete(checked, attempt, AttemptSucceeded(stored))
    assert evidence(checked, attempt.id, AttemptSucceeded(asked)) is expected


@pytest.mark.parametrize("recorded", [PERMANENT, UNCLASSIFIED], ids=["permanent", "unclassified"])
def test_a_final_failure_matches_only_its_exact_details(
    checked: Conn, recorded: AttemptFailed
) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    complete(checked, attempt, recorded)
    assert evidence(checked, attempt.id, recorded) is CompletionEvidence.MATCHES
    for other in (
        AttemptFailed(recorded.kind, recorded.error_type, "another message"),
        AttemptFailed(recorded.kind, "another.Type", recorded.error_message),
        AttemptFailed(recorded.kind, recorded.error_type, None),
        AttemptFailed(
            FailureKind.PERMANENT
            if recorded.kind is FailureKind.UNCLASSIFIED
            else FailureKind.UNCLASSIFIED,
            recorded.error_type,
            recorded.error_message,
        ),
        SUCCESS,
        AttemptCancelled(),
        TRANSIENT,
    ):
        assert evidence(checked, attempt.id, other) is CompletionEvidence.CONFLICTS, other


def test_a_cancelled_completion_matches_only_cancellation(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    request_cancellation(checked, task.id, grace=GRACE)
    complete(checked, attempt, AttemptCancelled())
    assert evidence(checked, attempt.id, AttemptCancelled()) is CompletionEvidence.MATCHES
    assert evidence(checked, attempt.id, SUCCESS) is CompletionEvidence.CONFLICTS
    assert evidence(checked, attempt.id, PERMANENT) is CompletionEvidence.CONFLICTS


def test_a_transient_failure_matches_after_its_task_moved_to_another_generation(
    checked: Conn,
) -> None:
    task = new_task(checked)
    first = start_attempt(checked, task.id, "w1", policy=POLICY)
    complete(checked, first, TRANSIENT)  # requeued
    assert evidence(checked, first.id, TRANSIENT) is CompletionEvidence.MATCHES
    second = start_attempt(checked, task.id, "w2", policy=POLICY)  # generation 2 now runs
    assert evidence(checked, first.id, TRANSIENT) is CompletionEvidence.MATCHES
    complete(checked, second, SUCCESS)  # and finished differently
    assert task_now(checked, task.id).attempt_count == 2
    assert evidence(checked, first.id, TRANSIENT) is CompletionEvidence.MATCHES
    assert evidence(checked, first.id, AttemptFailed(FailureKind.TRANSIENT, "other", "429")) is (
        CompletionEvidence.CONFLICTS
    )
    assert evidence(checked, second.id, SUCCESS, generation=2) is CompletionEvidence.MATCHES


def test_a_transient_failure_that_cancelled_or_exhausted_its_task_still_matches(
    checked: Conn,
) -> None:
    cancelled = new_task(checked)
    attempt = start_attempt(checked, cancelled.id, "w", policy=POLICY)
    request_cancellation(checked, cancelled.id, grace=GRACE)
    complete(checked, attempt, TRANSIENT)  # a pending request cancels instead of requeueing
    assert evidence(checked, attempt.id, TRANSIENT) is CompletionEvidence.MATCHES
    exhausted = new_task(checked, max_attempts=1)
    last = start_attempt(checked, exhausted.id, "w", policy=POLICY)
    complete(checked, last, TRANSIENT)  # no attempts left: failed
    assert evidence(checked, last.id, TRANSIENT) is CompletionEvidence.MATCHES


@pytest.mark.parametrize("outcome", ALL_OUTCOMES, ids=repr)
def test_a_revoked_attempt_shows_that_recovery_won(checked: Conn, outcome: AttemptOutcome) -> None:
    attempt = start_attempt(checked, new_task(checked).id, "w", policy=EXPIRING)
    until_expired(checked, attempt)
    revoke(checked)
    assert evidence(checked, attempt.id, outcome) is CompletionEvidence.REVOKED


def test_a_revoked_attempt_whose_task_was_claimed_again_is_still_revoked(checked: Conn) -> None:
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w1", policy=EXPIRING)
    until_expired(checked, attempt)
    revoke(checked)
    start_attempt(checked, task.id, "w2", policy=POLICY)  # generation 2
    assert evidence(checked, attempt.id, SUCCESS) is CompletionEvidence.REVOKED


def test_missing_or_contradictory_records_are_inconsistent(checked: Conn) -> None:
    assert evidence(checked, uuid.uuid4(), SUCCESS) is CompletionEvidence.INCONSISTENT
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    assert evidence(checked, attempt.id, SUCCESS, generation=2) is CompletionEvidence.INCONSISTENT
    assert evidence(checked, attempt.id, SUCCESS, generation=0) is CompletionEvidence.INCONSISTENT
    # A running attempt whose task is not running: corrupted behind the operations' back.
    checked.execute(
        "UPDATE tasks SET state = 'failed', finished_at = now() WHERE id = %s", (task.id,)
    )
    assert evidence(checked, attempt.id, SUCCESS) is CompletionEvidence.INCONSISTENT
    # A finished attempt whose task contradicts it.
    checked.execute(
        "UPDATE attempts SET state = 'failed', failure_kind = 'permanent', error_type = 'x', "
        "finished_at = now() WHERE id = %s",
        (attempt.id,),
    )
    assert evidence(checked, attempt.id, AttemptFailed(FailureKind.PERMANENT, "x")) is (
        CompletionEvidence.MATCHES  # failed at this generation: consistent again
    )
    checked.execute(
        "UPDATE tasks SET state = 'cancelled', cancel_requested_at = now() WHERE id = %s",
        (task.id,),
    )
    assert evidence(checked, attempt.id, AttemptFailed(FailureKind.PERMANENT, "x")) is (
        CompletionEvidence.INCONSISTENT
    )
    checked.execute(
        "UPDATE tasks SET state = 'failed', cancel_requested_at = NULL WHERE id = %s", (task.id,)
    )


def test_reading_evidence_writes_nothing(checked: Conn) -> None:
    attempt = start_attempt(checked, new_task(checked).id, "w", policy=POLICY)
    before = checked.execute("SELECT * FROM attempts ORDER BY id").fetchall()
    for outcome in ALL_OUTCOMES:
        evidence(checked, attempt.id, outcome)
    assert checked.execute("SELECT * FROM attempts ORDER BY id").fetchall() == before


# --- the task beside a revoked or transient-failed attempt -----------------------------------
#
# Ending generation N with a revocation or a transient failure leaves the task
# queued, cancelled or failed at N (``next_task_state``); later, only another
# claim moves it on, to N + 1. A task still running or succeeded at N contradicts
# such an attempt. The schema's constraints are single-row, so they allow it; the
# evidence must call it inconsistent.

TASK_ROW = "SELECT state, result, finished_at, cancel_requested_at FROM tasks WHERE id = %s"


def ended(db: Conn, how: str, *, max_attempts: int = 3) -> tuple[uuid.UUID, uuid.UUID]:
    """A task whose attempt 1 was revoked or failed transiently; (task id, attempt id)."""
    task = new_task(db, max_attempts=max_attempts)
    if how == "revoked":
        attempt = start_attempt(db, task.id, "w", policy=EXPIRING)
        until_expired(db, attempt)
        revoke(db)
    else:
        attempt = start_attempt(db, task.id, "w", policy=POLICY)
        complete(db, attempt, TRANSIENT)
    return task.id, attempt.id


def asked(how: str) -> AttemptOutcome:
    return SUCCESS if how == "revoked" else TRANSIENT


def expected(how: str) -> CompletionEvidence:
    return CompletionEvidence.REVOKED if how == "revoked" else CompletionEvidence.MATCHES


def test_a_task_cannot_reenter_running_at_the_same_generation(checked: Conn) -> None:
    # What the schema itself guarantees: the fencing trigger only lets a task
    # become running by advancing its generation (a claim).
    task_id, _ = ended(checked, "transient")  # queued at generation 1
    with pytest.raises(errors.CheckViolation, match="advance attempt_count by exactly 1"):
        checked.execute("UPDATE tasks SET state = 'running' WHERE id = %s", (task_id,))


@pytest.mark.parametrize("how", ["revoked", "transient"])
def test_an_ended_attempt_beside_its_still_running_task_is_inconsistent(
    checked: Conn, how: str
) -> None:
    # The trigger above rules out a task re-entering running, but an attempt row can
    # still be ended behind the operations' back while its task never left running.
    task = new_task(checked)
    attempt = start_attempt(checked, task.id, "w", policy=POLICY)
    if how == "revoked":
        checked.execute(
            "UPDATE attempts SET state = 'revoked', revoked_reason = 'lease_expired', "
            "finished_at = lease_expires_at WHERE id = %s",
            (attempt.id,),
        )
    else:
        checked.execute(
            "UPDATE attempts SET state = 'failed', failure_kind = 'transient', "
            "error_type = 'test.RateLimited', error_message = '429', finished_at = now() "
            "WHERE id = %s",
            (attempt.id,),
        )
    try:
        assert evidence(checked, attempt.id, asked(how)) is CompletionEvidence.INCONSISTENT
    finally:  # back to running, then ended the proper way for the final invariant check
        checked.execute(
            "UPDATE attempts SET state = 'running', failure_kind = NULL, error_type = NULL, "
            "error_message = NULL, revoked_reason = NULL, finished_at = NULL WHERE id = %s",
            (attempt.id,),
        )
        complete(checked, attempt, SUCCESS)
    assert evidence(checked, attempt.id, SUCCESS) is CompletionEvidence.MATCHES


@pytest.mark.parametrize("how", ["revoked", "transient"])
def test_an_ended_attempt_beside_a_task_succeeded_at_its_generation_is_inconsistent(
    checked: Conn, how: str
) -> None:
    task_id, attempt_id = ended(checked, how)
    original = checked.execute(TASK_ROW, (task_id,)).fetchone()
    assert original is not None
    checked.execute(  # behind the operations' back, as the single-row constraints allow
        "UPDATE tasks SET state = 'succeeded', result = '{\"ok\": true}', finished_at = now() "
        "WHERE id = %s",
        (task_id,),
    )
    try:
        assert evidence(checked, attempt_id, asked(how)) is CompletionEvidence.INCONSISTENT
    finally:
        checked.execute(
            "UPDATE tasks SET state = %s, result = %s, finished_at = %s, cancel_requested_at = %s "
            "WHERE id = %s",
            (*original, task_id),
        )
    assert evidence(checked, attempt_id, asked(how)) is expected(how)  # consistent again


def cancel_queued(db: Conn, task_id: uuid.UUID) -> None:
    request_cancellation(db, task_id, grace=GRACE)


def claim_again(db: Conn, task_id: uuid.UUID) -> None:
    start_attempt(db, task_id, "w2", policy=POLICY)


def claim_again_and_succeed(db: Conn, task_id: uuid.UUID) -> None:
    from orqestra.persistence.lifecycle import complete_attempt

    attempt = start_attempt(db, task_id, "w2", policy=POLICY)
    complete_attempt(db, attempt.id, generation=attempt.attempt_number, outcome=SUCCESS)


@pytest.mark.parametrize(
    ("aftermath", "task_state"),
    [
        (None, "queued"),  # requeued with attempts left
        (cancel_queued, "cancelled"),  # then cancelled while queued, still generation 1
        (claim_again, "running"),  # claimed again: generation 2
        (claim_again_and_succeed, "succeeded"),  # generation 2 succeeded
    ],
    ids=["requeued", "cancelled-while-queued", "claimed-again", "claimed-again-and-succeeded"],
)
@pytest.mark.parametrize("how", ["revoked", "transient"])
def test_every_legitimate_aftermath_keeps_its_evidence(
    checked: Conn, how: str, aftermath: Any, task_state: str
) -> None:
    task_id, attempt_id = ended(checked, how)
    if aftermath is not None:
        aftermath(checked, task_id)
    assert checked.execute("SELECT state FROM tasks WHERE id = %s", (task_id,)).fetchone() == (
        task_state,
    )
    assert evidence(checked, attempt_id, asked(how)) is expected(how)


@pytest.mark.parametrize("how", ["revoked", "transient"])
def test_an_exhausted_or_cancelled_end_keeps_its_evidence(checked: Conn, how: str) -> None:
    exhausted_task, exhausted = ended(checked, how, max_attempts=1)  # no attempt left: failed
    assert checked.execute(
        "SELECT state FROM tasks WHERE id = %s", (exhausted_task,)
    ).fetchone() == ("failed",)
    assert evidence(checked, exhausted, asked(how)) is expected(how)
    # A request pending when the attempt ended cancels the task at generation 1.
    task = new_task(checked)
    if how == "revoked":
        attempt = start_attempt(checked, task.id, "w", policy=POLICY)
        request_cancellation(checked, task.id, grace=timedelta(microseconds=1))  # grace runs out
        until_expired(checked, task_now_attempt(checked, attempt.id))
        revocation = revoke(checked)
        assert revocation.attempt.revoked_reason is not None
        assert revocation.attempt.revoked_reason.value == "cancellation_grace_expired"
    else:
        attempt = start_attempt(checked, task.id, "w", policy=POLICY)
        request_cancellation(checked, task.id, grace=GRACE)
        complete(checked, attempt, TRANSIENT)
    assert task_now(checked, task.id).state.value == "cancelled"
    assert evidence(checked, attempt.id, asked(how)) is expected(how)


def task_now_attempt(db: Conn, attempt_id: uuid.UUID) -> Any:
    from orqestra.persistence.lifecycle import get_attempt

    attempt = get_attempt(db, attempt_id)
    assert attempt is not None
    return attempt
