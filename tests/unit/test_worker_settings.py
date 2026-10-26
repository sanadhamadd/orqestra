import os
from datetime import timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from orqestra.config import Settings
from orqestra.domain.leases import LeasePolicy
from orqestra.worker.settings import WorkerSettings

pytestmark = pytest.mark.unit

# Test values only: they satisfy the relationships and are not recommendations.
# Production values are undecided (DEC-034).
VALID: dict[str, Any] = {
    "lease": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=10),
    "heartbeat_interval": timedelta(seconds=5),
    "db_operation_timeout": timedelta(seconds=2),
    "fence_margin": timedelta(seconds=3),
    "poll_interval": timedelta(seconds=1),
    "recovery_interval": timedelta(seconds=10),
    "recovery_batch_limit": 10,
    "shutdown_timeout": timedelta(seconds=30),
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
        "FENCE_MARGIN": "00:00:03",  # or HH:MM:SS
        "POLL_INTERVAL": "PT0.5S",
        "RECOVERY_INTERVAL": "PT10S",
        "RECOVERY_BATCH_LIMIT": "10",
        "SHUTDOWN_TIMEOUT": "PT0S",
    }.items():
        monkeypatch.setenv(f"ORQESTRA_WORKER_{name}", value)
    worker = WorkerSettings(_env_file=None)
    assert worker.lease == timedelta(seconds=30)
    assert worker.execution_timeout == timedelta(minutes=10)
    assert worker.poll_interval == timedelta(milliseconds=500)
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
    assert settings(db_operation_timeout=one_ms).db_operation_timeout == one_ms
    with pytest.raises(ValidationError, match="at least 1 ms"):
        settings(db_operation_timeout=one_ms - US)


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
    needed = timedelta(seconds=10)
    assert settings(lease=needed + US).lease == needed + US
    for lease in (needed, needed - US):
        with pytest.raises(ValidationError, match="must be shorter than lease"):
            settings(lease=lease)
    with pytest.raises(ValidationError, match="must be shorter than lease"):
        settings(fence_margin=VALID["lease"])  # a margin as long as the lease


def test_a_relationship_is_reported_once_its_fields_are_valid() -> None:
    # An invalid field is reported on its own; the relationships are not checked.
    with pytest.raises(ValidationError) as info:
        settings(lease=timedelta(0), heartbeat_interval=timedelta(hours=1))
    assert [loc for loc, _ in errors(info)] == [("lease",)]
