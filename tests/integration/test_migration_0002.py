"""Migration 0002 (attempt leases) against real PostgreSQL: upgrade safety under load.

Each test starts from a schema at migration 0001 and plays the code that ran
before 0002 with that era's own SQL. The upgrade must refuse, at once and
without any effect, while that code holds locks or has work running; it must
keep finished history intact; and afterwards a claim from the old code must
fail rather than create an attempt without a lease (DEC-039).
"""

import random
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg
import pytest
from psycopg import errors
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import TupleRow

from orqestra.persistence.lifecycle import claim_next_attempt, get_attempt, list_attempts
from orqestra.persistence.migrate import MigrationError, load_migrations, migrate
from tests.integration.test_attempt_lifecycle import POLICY, assert_invariants

pytestmark = pytest.mark.integration

Conn = psycopg.Connection[TupleRow]

INITIAL, LEASES = load_migrations()
LEASE_COLUMNS = ("lease_expires_at", "deadline_at", "revoked_reason")


@pytest.fixture
def legacy(schema_url: str) -> Iterator[Callable[[], Conn]]:
    """Bring ``schema_url`` to migration 0001; return a factory of connections to it."""
    migrate(schema_url, [INITIAL])
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


# --- the lifecycle as code from before 0002 ran it ------------------------------


def legacy_job(conn: Conn, tasks: int, *, max_attempts: int = 3) -> list[uuid.UUID]:
    job_id = uuid.uuid4()
    task_ids = [uuid.uuid4() for _ in range(tasks)]
    with conn.transaction():
        conn.execute("INSERT INTO jobs (id, kind) VALUES (%s, 'legacy')", (job_id,))
        for n, task_id in enumerate(task_ids):
            conn.execute(
                "INSERT INTO tasks (id, job_id, task_key, payload, max_attempts) "
                "VALUES (%s, %s, %s, '{}', %s)",
                (task_id, job_id, f"t{n}", max_attempts),
            )
    return sorted(task_ids)


def legacy_claim_steps(conn: Conn, task_id: uuid.UUID) -> uuid.UUID:
    """The 0001-era claim's writes, in a transaction the caller owns."""
    row = conn.execute(
        "UPDATE tasks SET state = 'running', attempt_count = attempt_count + 1, "
        "updated_at = now() WHERE id = %s RETURNING attempt_count",
        (task_id,),
    ).fetchone()
    assert row is not None
    attempt_id = uuid.uuid4()
    conn.execute(
        "INSERT INTO attempts (id, task_id, attempt_number, worker_id) "
        "VALUES (%s, %s, %s, 'legacy')",
        (attempt_id, task_id, row[0]),
    )
    return attempt_id


def legacy_claim(conn: Conn, task_id: uuid.UUID) -> uuid.UUID:
    with conn.transaction():
        conn.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task_id,))
        return legacy_claim_steps(conn, task_id)


def legacy_claim_next(conn: Conn) -> tuple[uuid.UUID, uuid.UUID] | None:
    with conn.transaction():
        row = conn.execute(
            "SELECT id FROM tasks WHERE state = 'queued' "
            "ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED"
        ).fetchone()
        if row is None:
            return None
        return row[0], legacy_claim_steps(conn, row[0])


def legacy_finish(
    conn: Conn,
    task_id: uuid.UUID,
    attempt_id: uuid.UUID,
    *,
    attempt_state: str,
    task_state: str,
    failure_kind: str | None = None,
) -> None:
    with conn.transaction():
        conn.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task_id,))
        conn.execute("SELECT id FROM attempts WHERE id = %s FOR UPDATE", (attempt_id,))
        conn.execute(
            "UPDATE attempts SET state = %s, failure_kind = %s, error_type = %s, "
            "finished_at = now() WHERE id = %s",
            (attempt_state, failure_kind, failure_kind and "legacy.Error", attempt_id),
        )
        conn.execute(
            "UPDATE tasks SET state = %s, updated_at = now(), "
            "result = CASE WHEN %s = 'succeeded' THEN '{}'::jsonb END, "
            "finished_at = CASE WHEN %s IN ('succeeded', 'failed', 'cancelled') THEN now() END "
            "WHERE id = %s",
            (task_state, task_state, task_state, task_id),
        )


# --- upgrade state ----------------------------------------------------------------


def versions(conn: Conn) -> list[int]:
    return [int(v) for (v,) in conn.execute("SELECT version FROM schema_migrations ORDER BY 1")]


def assert_not_upgraded(conn: Conn) -> None:
    """Nothing of 0002 exists: no history row, no column, constraint value or trigger."""
    assert versions(conn) == [1]
    row = conn.execute(
        "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() "
        "AND table_name = 'attempts' AND column_name = ANY(%s)",
        (list(LEASE_COLUMNS),),
    ).fetchone()
    assert row == (0,)
    row = conn.execute(
        "SELECT pg_get_constraintdef(oid) LIKE '%%revoked%%' FROM pg_constraint "
        "WHERE conname = 'attempts_state_valid' AND connamespace = current_schema()::regnamespace"
    ).fetchone()
    assert row == (False,)
    row = conn.execute(
        "SELECT count(*) FROM pg_trigger WHERE tgname = 'attempts_deadline_immutable'"
    ).fetchone()
    assert row == (0,)


def upgrade_refused(url: str) -> MigrationError:
    """Run the upgrade, which must refuse promptly rather than wait for a lock.

    It runs in a daemon thread so that an upgrade blocked behind a lock this
    test holds fails the test instead of hanging it.
    """
    outcome: list[BaseException | object] = []
    upgrading = threading.Thread(target=lambda: outcome.append(_try_migrate(url)), daemon=True)
    upgrading.start()
    upgrading.join(timeout=10)
    if upgrading.is_alive():
        pytest.fail("the upgrade waited for a lock instead of refusing at once")
    (result,) = outcome
    assert isinstance(result, MigrationError), f"expected a refusal, got {result!r}"
    return result


def _try_migrate(url: str) -> BaseException | object:
    try:
        return migrate(url)
    except Exception as exc:  # returned to the test thread, which inspects it
        return exc


def snapshot(conn: Conn) -> dict[str, list[tuple[Any, ...]]]:
    return {
        "tasks": conn.execute("SELECT * FROM tasks ORDER BY id").fetchall(),
        "attempts": conn.execute(
            "SELECT id, task_id, attempt_number, worker_id, state, failure_kind, error_type, "
            "error_message, started_at, finished_at FROM attempts ORDER BY id"
        ).fetchall(),
    }


# --- tests ------------------------------------------------------------------------


def test_upgrades_a_quiescent_database_and_keeps_its_history(
    schema_url: str, legacy: Callable[[], Conn]
) -> None:
    conn = legacy()
    succeeded, failed, cancelled, requeued, queued, withdrawn = legacy_job(conn, 6)
    attempt = legacy_claim(conn, succeeded)
    legacy_finish(conn, succeeded, attempt, attempt_state="succeeded", task_state="succeeded")
    attempt = legacy_claim(conn, failed)
    legacy_finish(
        conn, failed, attempt, attempt_state="failed", task_state="queued", failure_kind="transient"
    )
    attempt = legacy_claim(conn, failed)
    legacy_finish(
        conn, failed, attempt, attempt_state="failed", task_state="failed", failure_kind="permanent"
    )
    attempt = legacy_claim(conn, cancelled)
    conn.execute("UPDATE tasks SET cancel_requested_at = now() WHERE id = %s", (cancelled,))
    legacy_finish(conn, cancelled, attempt, attempt_state="cancelled", task_state="cancelled")
    attempt = legacy_claim(conn, requeued)
    legacy_finish(
        conn,
        requeued,
        attempt,
        attempt_state="failed",
        task_state="queued",
        failure_kind="transient",
    )
    conn.execute(
        "UPDATE tasks SET state = 'cancelled', cancel_requested_at = now(), finished_at = now() "
        "WHERE id = %s",
        (withdrawn,),
    )
    before = snapshot(conn)

    assert [m.label for m in migrate(schema_url)] == [LEASES.label]

    assert versions(conn) == [1, 2]
    assert snapshot(conn) == before  # every task and every finished attempt as it was
    row = conn.execute(
        "SELECT count(*) FROM attempts WHERE lease_expires_at IS NOT NULL "
        "OR deadline_at IS NOT NULL OR revoked_reason IS NOT NULL"
    ).fetchone()
    assert row == (0,)  # history is not given leases or revocations it never had
    history = list_attempts(conn, failed)
    assert [(a.state.value, a.lease_expires_at) for a in history] == [
        ("failed", None),
        ("failed", None),
    ]
    # The upgraded schema serves the new code: both queued tasks are claimable.
    claimed = {
        a.task_id: a
        for a in (claim_next_attempt(conn, "w", policy=POLICY) for _ in range(2))
        if a is not None
    }
    assert set(claimed) == {requeued, queued}
    assert claimed[requeued].attempt_number == 2
    assert all(a.lease_expires_at is not None for a in claimed.values())
    assert claim_next_attempt(conn, "w", policy=POLICY) is None
    assert_invariants(conn)


def test_refuses_to_upgrade_while_an_attempt_is_running(
    schema_url: str, legacy: Callable[[], Conn]
) -> None:
    conn = legacy()
    (task,) = legacy_job(conn, 1)
    attempt = legacy_claim(conn, task)
    before = snapshot(conn)

    refused = upgrade_refused(schema_url)
    assert refused.sqlstate == "55000"
    assert "refuses to run while tasks or attempts are running" in str(refused)
    assert_not_upgraded(conn)
    assert snapshot(conn) == before  # legacy work is not expired or revoked

    legacy_finish(conn, task, attempt, attempt_state="succeeded", task_state="succeeded")
    assert [m.label for m in migrate(schema_url)] == [LEASES.label]
    finished = get_attempt(conn, attempt)
    assert finished is not None
    assert (finished.state.value, finished.lease_expires_at) == ("succeeded", None)


def test_refuses_a_running_task_without_a_running_attempt(
    schema_url: str, legacy: Callable[[], Conn]
) -> None:
    # Recovery finds work through running attempts, so a running task without
    # one could never be recovered: the upgrade refuses it too.
    conn = legacy()
    (task,) = legacy_job(conn, 1)
    attempt = legacy_claim(conn, task)
    conn.execute(
        "UPDATE attempts SET state = 'failed', failure_kind = 'permanent', error_type = 'x', "
        "finished_at = now() WHERE id = %s",
        (attempt,),
    )
    assert upgrade_refused(schema_url).sqlstate == "55000"
    assert_not_upgraded(conn)


def test_fails_at_once_while_a_claim_holds_a_task(
    schema_url: str, legacy: Callable[[], Conn]
) -> None:
    conn, claimant = legacy(), legacy()
    (task,) = legacy_job(conn, 1)
    claimant.execute("BEGIN")
    claimant.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task,))  # claim in flight

    refused = upgrade_refused(schema_url)
    assert refused.sqlstate == "55P03"
    assert "needs a quiescent database" in str(refused)
    assert_not_upgraded(conn)

    # The failed upgrade holds nothing: the claim finishes without waiting.
    claimant.execute("SET LOCAL lock_timeout = '500ms'")
    legacy_claim_steps(claimant, task)
    claimant.execute("COMMIT")
    # Now the attempt is running, so the upgrade still refuses, for that reason.
    assert upgrade_refused(schema_url).sqlstate == "55000"
    assert_not_upgraded(conn)


def test_fails_at_once_while_a_completion_holds_its_rows(
    schema_url: str, legacy: Callable[[], Conn]
) -> None:
    conn, completer = legacy(), legacy()
    (task,) = legacy_job(conn, 1)
    attempt = legacy_claim(conn, task)
    completer.execute("BEGIN")
    completer.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task,))
    completer.execute("SELECT id FROM attempts WHERE id = %s FOR UPDATE", (attempt,))

    assert upgrade_refused(schema_url).sqlstate == "55P03"  # the lock check comes first
    assert_not_upgraded(conn)

    completer.execute("ROLLBACK")
    legacy_finish(conn, task, attempt, attempt_state="succeeded", task_state="succeeded")
    assert [m.label for m in migrate(schema_url)] == [LEASES.label]


def test_fails_at_once_while_a_reader_holds_attempts(
    schema_url: str, legacy: Callable[[], Conn]
) -> None:
    conn = legacy()
    (task,) = legacy_job(conn, 1)
    with connect(schema_url, autocommit=False) as reader:
        reader.execute("SELECT count(*) FROM attempts")  # left idle in its transaction

        refused = upgrade_refused(schema_url)
        assert refused.sqlstate == "55P03"
        assert_not_upgraded(conn)
        # The tasks lock taken before the attempts lock failed was released too.
        with conn.transaction():
            conn.execute("SET LOCAL lock_timeout = '500ms'")
            conn.execute("SELECT id FROM tasks WHERE id = %s FOR UPDATE", (task,))
        reader.rollback()

    assert [m.label for m in migrate(schema_url)] == [LEASES.label]


def test_requires_read_committed(schema_url: str, legacy: Callable[[], Conn]) -> None:
    conn = legacy()
    options = conninfo_to_dict(schema_url)["options"]
    serializable = make_conninfo(
        schema_url, options=f"{options} -c default_transaction_isolation=serializable"
    )
    refused = upgrade_refused(serializable)
    assert refused.sqlstate == "55000"
    assert "requires READ COMMITTED isolation" in str(refused)
    assert_not_upgraded(conn)


def test_after_the_upgrade_a_claim_without_a_lease_is_rejected(
    schema_url: str, legacy: Callable[[], Conn]
) -> None:
    conn = legacy()
    (task,) = legacy_job(conn, 1)
    migrate(schema_url)
    with pytest.raises(errors.CheckViolation) as info:
        legacy_claim(conn, task)  # code from before the upgrade, still running
    assert info.value.diag.constraint_name == "attempts_lease_required"
    row = conn.execute("SELECT state, attempt_count FROM tasks WHERE id = %s", (task,)).fetchone()
    assert row == ("queued", 0)  # the whole claim rolled back
    assert list_attempts(conn, task) == ()


def upgrade_during_legacy_work(url: str) -> tuple[list[str], list[str]]:
    """Race an operator retrying the upgrade against three legacy workers and a reader.

    Returns every upgrade outcome in order (SQLSTATEs, then the applied label)
    and how each worker stopped: ``"drained"``, or the constraint that
    rejected its claim once the upgrade had applied under it.
    """
    barrier = threading.Barrier(5)

    def work(seed: int) -> str:
        idle = random.Random(seed)
        with connect(url) as conn:
            barrier.wait()
            while True:
                try:
                    claimed = legacy_claim_next(conn)
                except errors.CheckViolation as exc:  # the upgrade has applied
                    return str(exc.diag.constraint_name)
                if claimed is None:
                    return "drained"
                task_id, attempt_id = claimed
                legacy_finish(
                    conn, task_id, attempt_id, attempt_state="succeeded", task_state="succeeded"
                )
                # Idle between tasks, holding nothing: the windows in which the
                # upgrade can apply while this worker is still alive.
                time.sleep(idle.uniform(0, 0.002))

    def read() -> str:
        with connect(url) as conn:
            barrier.wait()
            for _ in range(50):
                conn.execute("SELECT count(*) FROM attempts")
        return "read"

    def upgrade() -> list[str]:
        barrier.wait()
        outcomes: list[str] = []
        for _ in range(2000):  # retried at once, as a script would; each refusal is instant
            try:
                outcomes.append(",".join(m.label for m in migrate(url)))
                break
            except MigrationError as exc:
                outcomes.append(str(exc.sqlstate))
        return outcomes

    with ThreadPoolExecutor(5) as pool:
        workers = [pool.submit(work, seed) for seed in range(3)]
        reader = pool.submit(read)
        upgrading = pool.submit(upgrade)
        stops = [future.result(timeout=60) for future in workers]
        assert reader.result(timeout=60) == "read"
        return upgrading.result(timeout=60), stops


def test_upgrading_while_legacy_work_runs_never_deadlocks_or_strands_work(
    schema_factory: Callable[[], str],
) -> None:
    # Unforced races. Every attempt to upgrade either applies or refuses with
    # one of its own errors, never deadlocks (a DeadlockDetected would escape a
    # worker or the upgrade), and never leaves a running attempt without a
    # lease. Legacy workers drain the queue or, once the upgrade applied under
    # them, have their next claim rejected; across the rounds both happen.
    rejected_rounds = 0
    for _ in range(10):
        url = schema_factory()
        migrate(url, [INITIAL])
        with connect(url) as setup:
            legacy_job(setup, 40)
        outcomes, stops = upgrade_during_legacy_work(url)
        assert outcomes[-1] == LEASES.label
        assert set(outcomes[:-1]) <= {"55P03", "55000"}
        assert set(stops) <= {"drained", "attempts_lease_required"}
        rejected_rounds += "attempts_lease_required" in stops
        with connect(url) as conn:
            assert versions(conn) == [1, 2]
            row = conn.execute(
                "SELECT count(*) FROM attempts WHERE state = 'running' AND lease_expires_at IS NULL"
            ).fetchone()
            assert row == (0,)
            assert_invariants(conn)
    assert rejected_rounds > 0  # the upgrade did apply under live legacy workers
