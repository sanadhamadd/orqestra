import os
from datetime import timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from orqestra.config import Settings
from orqestra.domain.leases import LeasePolicy
from orqestra.domain.retry import RetryPolicy
from orqestra.worker.settings import WorkerSettings

pytestmark = pytest.mark.unit

# Test values only: they satisfy the relationships and are not recommendations.
# Production values are undecided (DEC-034).
VALID: dict[str, Any] = {
    "lease": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=10),
    "heartbeat_interval": timedelta(seconds=5),
    "db_operation_timeout": timedelta(seconds=2),
    "lock_timeout": timedelta(seconds=1),
    "fence_margin": timedelta(seconds=3),
    "poll_interval": timedelta(seconds=1),
    "recovery_interval": timedelta(seconds=10),
    "recovery_batch_limit": 10,
    "shutdown_timeout": timedelta(seconds=30),
    "retry_base_delay": timedelta(seconds=1),
    "retry_max_delay": timedelta(minutes=5),
}
FIELDS = tuple(VALID)
DURATIONS = tuple(name for name in FIELDS if name != "recovery_batch_limit")
POSITIVE = tuple(name for name in DURATIONS if name != "shutdown_timeout")
US = timedelta(microseconds=1)


@pytest.fixture(autouse=True)
def no_worker_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the process environment from supplying or overriding any value."""
    for name in list(os.environ):
        if name.startswith("ORQESTRA_WORKER_"):
            monkeypatch.delenv(name)


def settings(**overrides: Any) -> WorkerSettings:
    return WorkerSettings(_env_file=None, **{**VALID, **overrides})


def errors(info: pytest.ExceptionInfo[ValidationError]) -> list[tuple[tuple[Any, ...], str]]:
    return [(e["loc"], e["msg"]) for e in info.value.errors()]


def test_valid_settings_give_their_lease_policy() -> None:
    worker = settings()
    assert {name: getattr(worker, name) for name in FIELDS} == VALID
    assert worker.lease_policy == LeasePolicy(timedelta(seconds=30), timedelta(minutes=10))


def test_settings_are_frozen() -> None:
    worker = settings()
    with pytest.raises(ValidationError, match="frozen"):
        worker.lease = timedelta(seconds=1)  # type: ignore[misc]


def test_nothing_has_a_default() -> None:
    with pytest.raises(ValidationError) as info:
        WorkerSettings(_env_file=None)
    assert sorted(loc for loc, _ in errors(info)) == sorted((name,) for name in FIELDS)
    assert {msg for _, msg in errors(info)} == {"Field required"}


@pytest.mark.parametrize("missing", FIELDS)
def test_each_setting_is_required(missing: str) -> None:
    values = {name: value for name, value in VALID.items() if name != missing}
    with pytest.raises(ValidationError) as info:
        WorkerSettings(_env_file=None, **values)
    assert errors(info) == [((missing,), "Field required")]


def test_settings_are_read_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "LEASE": "PT30S",  # ISO 8601 durations
        "EXECUTION_TIMEOUT": "PT10M",
        "HEARTBEAT_INTERVAL": "PT5S",
        "DB_OPERATION_TIMEOUT": "PT2S",
        "LOCK_TIMEOUT": "PT0.25S",
        "FENCE_MARGIN": "00:00:03",  # or HH:MM:SS
        "POLL_INTERVAL": "PT0.5S",
        "RECOVERY_INTERVAL": "PT10S",
        "RECOVERY_BATCH_LIMIT": "10",
        "SHUTDOWN_TIMEOUT": "PT0S",
        "RETRY_BASE_DELAY": "PT2S",
        "RETRY_MAX_DELAY": "PT10M",
    }.items():
        monkeypatch.setenv(f"ORQESTRA_WORKER_{name}", value)
    worker = WorkerSettings(_env_file=None)
    assert worker.lease == timedelta(seconds=30)
    assert worker.execution_timeout == timedelta(minutes=10)
    assert worker.poll_interval == timedelta(milliseconds=500)
    assert worker.lock_timeout == timedelta(milliseconds=250)
    assert worker.retry_policy == RetryPolicy(timedelta(seconds=2), timedelta(minutes=10))
    assert worker.recovery_batch_limit == 10
    assert worker.shutdown_timeout == timedelta(0)


@pytest.mark.parametrize("value", ["30", "0.5", "0", "thirty seconds"])
def test_an_environment_duration_needs_a_unit(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("ORQESTRA_WORKER_LEASE", value)
    values = {name: v for name, v in VALID.items() if name != "lease"}
    with pytest.raises(ValidationError) as info:
        WorkerSettings(_env_file=None, **values)
    assert [loc for loc, _ in errors(info)] == [("lease",)]


def test_worker_concurrency_is_neither_read_nor_affected(monkeypatch: pytest.MonkeyPatch) -> None:
    # ORQESTRA_WORKER_CONCURRENCY shares the worker prefix but belongs to Settings.
    monkeypatch.setenv("ORQESTRA_WORKER_CONCURRENCY", "8")
    worker = settings()
    assert "concurrency" not in type(worker).model_fields
    assert Settings().worker_concurrency == 8


@pytest.mark.parametrize("value", [timedelta(0), -US, timedelta(seconds=-5)], ids=str)
@pytest.mark.parametrize("name", POSITIVE)
def test_durations_must_be_positive(name: str, value: timedelta) -> None:
    with pytest.raises(ValidationError, match=f"{name} must be positive"):
        settings(**{name: value})


@pytest.mark.parametrize("name", DURATIONS)
def test_durations_must_be_representable(name: str) -> None:
    with pytest.raises(ValidationError, match="longer than any representable"):
        settings(**{name: timedelta(days=4_000_000)})


def test_shutdown_timeout_may_be_zero_but_not_negative() -> None:
    assert settings(shutdown_timeout=timedelta(0)).shutdown_timeout == timedelta(0)
    with pytest.raises(ValidationError, match="shutdown_timeout must not be negative"):
        settings(shutdown_timeout=-US)


@pytest.mark.parametrize(("limit", "valid"), [(1, True), (0, False), (-1, False)])
def test_recovery_batch_limit_is_at_least_one(limit: int, valid: bool) -> None:
    if valid:
        assert settings(recovery_batch_limit=limit).recovery_batch_limit == limit
    else:
        with pytest.raises(ValidationError, match="greater than or equal to 1"):
            settings(recovery_batch_limit=limit)


def test_recovery_batch_limit_must_be_an_integer() -> None:
    with pytest.raises(ValidationError, match="valid integer"):
        settings(recovery_batch_limit="many")


def test_the_database_timeout_is_at_least_one_millisecond() -> None:
    one_ms = timedelta(milliseconds=1)
    with pytest.raises(ValidationError, match="db_operation_timeout must be at least 1 ms"):
        settings(db_operation_timeout=one_ms - US)
    # With a lock wait of at least 1 ms that must be shorter (DEC-050), 2 ms is the least.
    two_ms = 2 * one_ms
    assert settings(db_operation_timeout=two_ms, lock_timeout=one_ms).db_operation_timeout == two_ms
    with pytest.raises(ValidationError, match="must be shorter than db_operation_timeout"):
        settings(db_operation_timeout=one_ms, lock_timeout=one_ms)


def test_the_lock_timeout_is_at_least_one_millisecond() -> None:
    one_ms = timedelta(milliseconds=1)
    assert settings(lock_timeout=one_ms).lock_timeout == one_ms
    with pytest.raises(ValidationError, match="lock_timeout must be at least 1 ms"):
        settings(lock_timeout=one_ms - US)


@pytest.mark.parametrize(
    ("lock", "valid"),
    [
        (timedelta(milliseconds=1_999), True),  # 1999 ms < 2000 ms
        (timedelta(microseconds=1_999_001), False),  # rounds up to 2000 ms: not shorter
        (timedelta(seconds=2), False),
        (timedelta(seconds=3), False),
    ],
    ids=["shorter", "equal-after-rounding", "equal", "longer"],
)
def test_a_lock_wait_must_end_before_the_database_call_does(lock: timedelta, valid: bool) -> None:
    if valid:
        assert settings(lock_timeout=lock).lock_timeout == lock
    else:
        with pytest.raises(ValidationError, match="must be shorter than db_operation_timeout"):
            settings(lock_timeout=lock)


def test_the_lock_relationship_uses_postgresqls_exact_milliseconds() -> None:
    # 2007 ms, which floating point would round to 2008, against a 2006.5 ms lock wait.
    database = timedelta(milliseconds=2_007)
    with pytest.raises(ValidationError, match=r"\(2007 ms in PostgreSQL\)"):
        settings(
            db_operation_timeout=database,
            heartbeat_interval=timedelta(seconds=3),
            lock_timeout=timedelta(microseconds=2_006_500),
        )


def test_the_lease_cannot_outlast_the_execution_timeout() -> None:
    equal = settings(execution_timeout=VALID["lease"])
    assert equal.lease_policy.lease == equal.lease_policy.execution_timeout
    with pytest.raises(ValidationError, match="must not exceed execution_timeout"):
        settings(execution_timeout=VALID["lease"] - US)


def test_a_database_call_must_end_before_the_next_heartbeat() -> None:
    interval = VALID["heartbeat_interval"]
    assert settings(db_operation_timeout=interval - US).db_operation_timeout == interval - US
    with pytest.raises(ValidationError, match="must be shorter than heartbeat_interval"):
        settings(db_operation_timeout=interval)


def test_a_heartbeat_must_be_confirmed_before_the_fencing_margin() -> None:
    # 5 s interval + 2 s database time + 3 s margin = 10 s, which the lease must exceed.
    # The claim's latency is deliberately not included (DEC-044 fencing correction note).
    needed = timedelta(seconds=10)
    assert settings(lease=needed + US).lease == needed + US
    for lease in (needed, needed - US):
        with pytest.raises(ValidationError, match="must be shorter than lease"):
            settings(lease=lease)
    with pytest.raises(ValidationError, match="must be shorter than lease"):
        settings(fence_margin=VALID["lease"])  # a margin as long as the lease


def test_a_step_must_fit_between_heartbeats_that_take_their_whole_bound() -> None:
    # DEC-070: a 4 s interval is shorter than 2 x the 3 s bound, so a step after a
    # full-bound heartbeat must fit before the fence instead: lease >= 3 x 3 s + 1 s.
    tight = {
        "heartbeat_interval": timedelta(seconds=4),
        "db_operation_timeout": timedelta(seconds=3),
        "fence_margin": timedelta(seconds=1),
    }
    assert settings(**tight, lease=timedelta(seconds=10)).lease == timedelta(seconds=10)
    with pytest.raises(ValidationError, match="no usage record or sweep step could start"):
        settings(**tight, lease=timedelta(seconds=10) - US)  # 4 + 3 + 1 < lease still holds
    # An interval of 2 x the bound needs nothing from the lease.
    roomy = settings(
        heartbeat_interval=timedelta(seconds=6),
        **{"db_operation_timeout": timedelta(seconds=3), "fence_margin": timedelta(seconds=1)},
        lease=timedelta(seconds=10) + US,
    )
    assert roomy.heartbeat_interval == timedelta(seconds=6)


@pytest.mark.parametrize(
    ("name", "what"),
    [("execution_timeout", "deadline"), ("retry_max_delay", "retry time")],
)
def test_a_deadline_or_retry_time_must_stay_representable_for_a_century(
    name: str, what: str
) -> None:
    century = timedelta(days=36525)
    assert getattr(settings(**{name: century}), name) == century
    with pytest.raises(ValidationError, match=f"must leave every {what} representable"):
        settings(**{name: timedelta(days=365 * 8000)})  # a representable duration


def test_a_relationship_is_reported_once_its_fields_are_valid() -> None:
    # An invalid field is reported on its own; the relationships are not checked.
    with pytest.raises(ValidationError) as info:
        settings(lease=timedelta(0), heartbeat_interval=timedelta(hours=1))
    assert [loc for loc, _ in errors(info)] == [("lease",)]


def test_the_retry_base_delay_cannot_exceed_its_maximum() -> None:
    equal = settings(retry_base_delay=timedelta(minutes=5), retry_max_delay=timedelta(minutes=5))
    assert equal.retry_policy == RetryPolicy(timedelta(minutes=5), timedelta(minutes=5))
    with pytest.raises(ValidationError, match=r"base_delay .* must not exceed max_delay"):
        settings(retry_base_delay=timedelta(minutes=5) + US, retry_max_delay=timedelta(minutes=5))


def test_the_retry_policy_is_built_from_the_two_settings() -> None:
    assert settings().retry_policy == RetryPolicy(timedelta(seconds=1), timedelta(minutes=5))
