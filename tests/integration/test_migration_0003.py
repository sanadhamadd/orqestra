"""Migration 0003 (cancellation-grace revocation reason) against real PostgreSQL.

Each test starts from a genuine schema at migration 0002 and upgrades to 0003
only (later migrations have their own tests). History is built as the code of
that time built it: the claim, completion and recovery come from ``historical``,
since the current ones need 0004's column (DEC-053); submission, cancellation
requests and heartbeats are the current code, which 0004 did not change.
Revoking an attempt whose cancellation grace ran out fails on 0002: the schema
rejects that reason, and the whole revocation rolls back. The upgrade must keep
every stored row valid, leave running attempts alone, fail at once rather than
wait while a session uses ``attempts``, and roll back completely when it fails
(DEC-041).
"""

import contextlib
import random
import re
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any

import psycopg
import pytest
from psycopg import errors
from psycopg.rows import TupleRow

from orqestra.domain.jobs import JobSubmission, Task, TaskSpec
from orqestra.domain.leases import RevocationReason
from orqestra.domain.states import TaskState
from orqestra.persistence.jobs import list_tasks, submit_job
from orqestra.persistence.lifecycle import (
    CancellationRequestedError,
    LeaseExpiredError,
    get_attempt,
    renew_lease,
    request_cancellation,
)
from orqestra.persistence.migrate import MigrationError, load_migrations, migrate
from tests.integration import historical
from tests.integration.test_attempt_lifecycle import (
    DEADLINE_EXPIRING,
    EXPIRING,
    GRACE,
    POLICY,
    assert_invariants,
    until_expired,
)

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

INITIAL, LEASES, GRACE_REASON, *_ = load_migrations()
UP_TO_0002 = [INITIAL, LEASES]
UP_TO_0003 = [INITIAL, LEASES, GRACE_REASON]  # never 0004: it has its own tests
BRIEF_GRACE = timedelta(microseconds=1)
KINDS = ("cancellation",)  # the job kind new_tasks submits
OLD_REASONS = {"lease_expired", "deadline_exceeded"}
ALL_REASONS = {reason.value for reason in RevocationReason}


@pytest.fixture
def at_0002(schema_url: str) -> Iterator[Callable[[], Conn]]:
    """Bring ``schema_url`` to migration 0002; return a factory of connections to it."""
    migrate(schema_url, UP_TO_0002)
    opened: list[Conn] = []

    def open_connection() -> Conn:
        conn = connect(schema_url)
        opened.append(conn)
        return conn

    yield open_connection
    for conn in opened:
        conn.close()


def connect(url: str, *, autocommit: bool = True) -> Conn:
    conn = psycopg.connect(url, autocommit=autocommit)
    conn.execute("SET statement_timeout = '20s'")  # a hang fails the test instead
    if not autocommit:
        conn.commit()
    return conn


def new_tasks(conn: Conn, count: int) -> list[Task]:
    specs = tuple(TaskSpec(f"t{n}", {}, 3) for n in range(count))
    job = submit_job(conn, JobSubmission("cancellation", specs)).job
    return list(list_tasks(conn, job.id))


def versions(conn: Conn) -> list[int]:
    return [int(v) for (v,) in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")]


def allowed_reasons(conn: Conn) -> set[str]:
    row = conn.execute(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conname = 'attempts_revoked_reason_valid' "
        "AND connamespace = current_schema()::regnamespace"
    ).fetchone()
    assert row is not None
    return set(re.findall(r"'([a-z_]+)'::text", str(row[0])))


def assert_not_upgraded(conn: Conn) -> None:
    assert versions(conn) == [1, 2]
    assert allowed_reasons(conn) == OLD_REASONS


def snapshot(conn: Conn) -> dict[str, list[tuple[Any, ...]]]:
    return {
        table: conn.execute(f"SELECT * FROM {table} ORDER BY id").fetchall()
        for table in ("tasks", "attempts")
    }


def upgrade_refused(url: str) -> MigrationError:
    """Run the upgrade, which must refuse promptly; a blocked upgrade fails the test."""
    outcome: list[object] = []

    def run() -> None:
        try:
            outcome.append(migrate(url, UP_TO_0003))
        except Exception as exc:  # returned to the test thread, which inspects it
            outcome.append(exc)

    upgrading = threading.Thread(target=run, daemon=True)
    upgrading.start()
    upgrading.join(timeout=10)
    if upgrading.is_alive():
        pytest.fail("the upgrade waited for a lock instead of refusing at once")
    (result,) = outcome
    assert isinstance(result, MigrationError), f"expected a refusal, got {result!r}"
    return result


def grace_expired_attempt(conn: Conn, task: Task) -> None:
    """Claim ``task``, request its cancellation with a brief grace, and wait for it to pass."""
    attempt = historical.start(conn, task.id, "w", policy=POLICY)
    request_cancellation(conn, task.id, grace=BRIEF_GRACE)
    shortened = get_attempt(conn, attempt.id)
    assert shortened is not None
    until_expired(conn, shortened)


# --- tests ------------------------------------------------------------------------


def test_upgrades_from_0002_keeping_revoked_history_and_running_attempts(
    schema_url: str, at_0002: Callable[[], Conn]
) -> None:
    conn = at_0002()
    lost, overdue, running, cancelling = new_tasks(conn, 4)
    for task, policy in ((lost, EXPIRING), (overdue, DEADLINE_EXPIRING)):
        attempt = historical.start(conn, task.id, "w", policy=policy)
        until_expired(conn, attempt)
        assert historical.revoke(conn) is not None
    live = historical.start(conn, running.id, "w", policy=POLICY)
    asked = historical.start(conn, cancelling.id, "w", policy=POLICY)
    request_cancellation(conn, cancelling.id, grace=GRACE)
    reasons = {
        str(r)
        for (r,) in conn.execute("SELECT revoked_reason FROM attempts WHERE state = 'revoked'")
    }
    assert reasons == OLD_REASONS
    before = snapshot(conn)

    assert [m.label for m in migrate(schema_url, UP_TO_0003)] == [GRACE_REASON.label]

    assert versions(conn) == [1, 2, 3]
    assert allowed_reasons(conn) == ALL_REASONS
    assert snapshot(conn) == before  # history and running attempts untouched
    # Running attempts carry on: one renews; the other, cancelled, is refused.
    renew_lease(conn, live.id, generation=1, worker_id="w", policy=POLICY)
    with pytest.raises(CancellationRequestedError):
        renew_lease(conn, asked.id, generation=1, worker_id="w", policy=POLICY)
    assert_invariants(conn)


def test_the_new_reason_is_rejected_before_0003_and_accepted_after(
    schema_url: str, at_0002: Callable[[], Conn]
) -> None:
    conn = at_0002()
    (task,) = new_tasks(conn, 1)
    grace_expired_attempt(conn, task)
    before = snapshot(conn)
    with pytest.raises(errors.CheckViolation) as info:
        historical.revoke(conn)  # the schema refuses; the revocation rolls back
    assert info.value.diag.constraint_name == "attempts_revoked_reason_valid"
    assert snapshot(conn) == before  # neither the attempt nor the task changed

    migrate(schema_url, UP_TO_0003)
    assert historical.revoke(conn) == (
        RevocationReason.CANCELLATION_GRACE_EXPIRED,
        TaskState.CANCELLED,
    )
    assert_invariants(conn)


def test_fails_at_once_while_a_reader_holds_attempts(
    schema_url: str, at_0002: Callable[[], Conn]
) -> None:
    conn = at_0002()
    with connect(schema_url, autocommit=False) as reader:
        reader.execute("SELECT count(*) FROM attempts")  # left idle in its transaction
        refused = upgrade_refused(schema_url)
        assert refused.sqlstate == "55P03"
        assert "could not lock attempts" in str(refused)
        assert_not_upgraded(conn)  # rolled back: no history row, old reasons only
        reader.rollback()
    assert [m.label for m in migrate(schema_url, UP_TO_0003)] == [GRACE_REASON.label]


def test_fails_at_once_while_a_lifecycle_write_holds_an_attempt(
    schema_url: str, at_0002: Callable[[], Conn]
) -> None:
    conn, holder = at_0002(), at_0002()
    (task,) = new_tasks(conn, 1)
    attempt = historical.start(conn, task.id, "w", policy=POLICY)
    holder.execute("BEGIN")
    holder.execute("SELECT id FROM attempts WHERE id = %s FOR UPDATE", (attempt.id,))
    assert upgrade_refused(schema_url).sqlstate == "55P03"
    assert_not_upgraded(conn)
    holder.execute("ROLLBACK")
    assert [m.label for m in migrate(schema_url, UP_TO_0003)] == [GRACE_REASON.label]


def test_does_not_lock_the_tasks_table(schema_url: str, at_0002: Callable[[], Conn]) -> None:
    # A transaction holding a task row (and nothing on attempts) does not stop
    # the upgrade: it changes only attempts.
    conn, holder = at_0002(), at_0002()
    (task,) = new_tasks(conn, 1)
    holder.execute("BEGIN")
    holder.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task.id,))
    started = time.monotonic()
    assert [m.label for m in migrate(schema_url, UP_TO_0003)] == [GRACE_REASON.label]
    assert time.monotonic() - started < 5
    holder.execute("ROLLBACK")


def test_a_retried_upgrade_applies_between_live_transactions_and_work_carries_on(
    schema_url: str, at_0002: Callable[[], Conn]
) -> None:
    # The operator's retry under live work, step by step: a worker holding an
    # attempt in an open transaction makes the upgrade fail at once with no
    # effect; once that transaction commits the retry applies, while every
    # session stays connected; and the same sessions carry on afterwards. The
    # worker uses server-side prepared statements from its first call, so the
    # statements it prepared before the upgrade are reused after it.
    admin, requester = at_0002(), at_0002()
    worker = at_0002()
    worker.prepare_threshold = 0
    running, cancelling, queued = new_tasks(admin, 3)
    held = historical.start(worker, running.id, "w", policy=POLICY)
    asked = historical.start(worker, cancelling.id, "w", policy=POLICY)
    request_cancellation(requester, cancelling.id, grace=BRIEF_GRACE)  # before the upgrade

    worker.execute("BEGIN")  # mid-transaction on an attempt row, as a heartbeat would be
    worker.execute("SELECT id FROM attempts WHERE id = %s FOR UPDATE", (held.id,))
    before = snapshot(admin)
    assert upgrade_refused(schema_url).sqlstate == "55P03"
    assert_not_upgraded(admin)
    assert snapshot(admin) == before
    worker.execute("COMMIT")

    assert [m.label for m in migrate(schema_url, UP_TO_0003)] == [GRACE_REASON.label]  # the retry
    assert versions(admin) == [1, 2, 3]

    # The same sessions carry on across the upgrade.
    renew_lease(worker, held.id, generation=1, worker_id="w", policy=POLICY)
    with pytest.raises(LeaseExpiredError) as expired:  # its brief grace has run out
        renew_lease(worker, asked.id, generation=1, worker_id="w", policy=POLICY)
    assert expired.value.reason is RevocationReason.CANCELLATION_GRACE_EXPIRED
    assert historical.succeed(worker, held) is TaskState.SUCCEEDED
    claimed = historical.claim(worker, "w", policy=POLICY, kinds=KINDS)
    assert claimed is not None
    assert claimed.task_id == queued.id
    # The request made before the upgrade ends in the new reason after it.
    shortened = get_attempt(admin, asked.id)
    assert shortened is not None
    until_expired(admin, shortened)
    assert historical.revoke(requester) == (
        RevocationReason.CANCELLATION_GRACE_EXPIRED,
        TaskState.CANCELLED,
    )
    assert_invariants(admin)


def upgrade_during_live_work(url: str) -> tuple[list[str], list[str]]:
    """Race an operator retrying the upgrade against three workers and a canceller.

    The workers claim, heartbeat and complete as the code of that time did
    (``historical``, and the current heartbeat), which works on either schema.
    The canceller requests cancellation of queued and running tasks. Returns
    every upgrade outcome in order and how each participant stopped.
    """
    barrier = threading.Barrier(5)

    def work(seed: int) -> str:
        idle = random.Random(seed)
        with connect(url) as conn:
            barrier.wait()
            while (
                attempt := historical.claim(conn, f"w{seed}", policy=POLICY, kinds=KINDS)
            ) is not None:
                # The worker would wind down after a refused heartbeat; it still completes below.
                with contextlib.suppress(CancellationRequestedError):
                    renew_lease(
                        conn,
                        attempt.id,
                        generation=attempt.attempt_number,
                        worker_id=attempt.worker_id,
                        policy=POLICY,
                    )
                historical.succeed(conn, attempt)
                time.sleep(idle.uniform(0, 0.002))  # idle between tasks, holding nothing
        return "drained"

    def cancel() -> str:
        with connect(url) as conn:
            barrier.wait()
            for task in list(list_tasks_of_schema(conn))[::3]:
                request_cancellation(conn, task, grace=GRACE)
        return "cancelled"

    def upgrade() -> list[str]:
        barrier.wait()
        outcomes: list[str] = []
        for _ in range(2000):  # retried at once, as a script would; each refusal is instant
            try:
                outcomes.append(",".join(m.label for m in migrate(url, UP_TO_0003)))
                break
            except MigrationError as exc:
                outcomes.append(str(exc.sqlstate))
        return outcomes

    with ThreadPoolExecutor(5) as pool:
        workers = [pool.submit(work, seed) for seed in range(3)]
        canceller = pool.submit(cancel)
        upgrading = pool.submit(upgrade)
        stops = [future.result(timeout=60) for future in workers]
        stops.append(canceller.result(timeout=60))
        return upgrading.result(timeout=60), stops


def list_tasks_of_schema(conn: Conn) -> list[Any]:
    return [task_id for (task_id,) in conn.execute("SELECT id FROM tasks ORDER BY id")]


def test_upgrading_during_live_work_never_deadlocks(
    schema_factory: Callable[[], str],
) -> None:
    # Unforced races: every attempt to upgrade applies or fails at once with
    # 55P03, never deadlocks (a DeadlockDetected would escape a participant),
    # and the lifecycle keeps working on both sides of it. Whether the upgrade
    # lands while workers are still claiming, or only after they drain, is left
    # to scheduling and not asserted; the deterministic test above forces an
    # upgrade that applies between live transactions.
    for _ in range(10):
        url = schema_factory()
        migrate(url, UP_TO_0002)
        with connect(url) as setup:
            new_tasks(setup, 30)
        outcomes, stops = upgrade_during_live_work(url)
        assert outcomes[-1] == GRACE_REASON.label
        assert set(outcomes[:-1]) <= {"55P03"}
        assert stops == ["drained", "drained", "drained", "cancelled"]
        with connect(url) as conn:
            assert versions(conn) == [1, 2, 3]
            states = {
                TaskState(state) for (state,) in conn.execute("SELECT DISTINCT state FROM tasks")
            }
            assert states <= {TaskState.SUCCEEDED, TaskState.CANCELLED}
            assert_invariants(conn)
