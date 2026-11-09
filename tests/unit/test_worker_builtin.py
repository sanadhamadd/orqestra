"""The built-in diagnostic handlers (DEC-051), through the real outcome boundary."""

import math
import threading
import time
import uuid
from datetime import timedelta
from typing import Any

import pytest

from orqestra.domain.jobs import AttemptCancelled, AttemptFailed, AttemptSucceeded, JsonValue
from orqestra.domain.policy import FailureKind
from orqestra.worker import builtin
from orqestra.worker.handlers import CancellationSignal, HandlerRegistry, TaskContext, run_handler
from tests.unit.test_worker_runtime import SETTINGS

pytestmark = pytest.mark.unit

LIMIT = timedelta(seconds=30)  # the bound for these tests


def context(cancel: threading.Event | None = None) -> TaskContext:
    return TaskContext(
        uuid.UUID(int=1),
        uuid.UUID(int=2),
        1,
        builtin.WAIT,
        CancellationSignal(cancel or threading.Event()),
    )


@pytest.mark.parametrize("payload", [None, 0, "text", [1, {"a": None}], {"nested": {"x": 1.5}}])
def test_echo_returns_its_payload_unchanged(payload: JsonValue) -> None:
    assert run_handler(builtin.echo, context(), payload) == AttemptSucceeded(payload)


@pytest.mark.parametrize("seconds", [0, 0.01, 1e-6])
def test_wait_returns_what_it_waited(seconds: float) -> None:
    outcome = run_handler(builtin.waiting(LIMIT), context(), {"seconds": seconds})
    assert outcome == AttemptSucceeded({"waited_seconds": seconds})


def test_wait_accepts_exactly_its_bound() -> None:
    cancel = threading.Event()
    cancel.set()  # so the 30 s wait ends at once, as cancelled
    outcome = run_handler(builtin.waiting(LIMIT), context(cancel), {"seconds": 30})
    assert outcome == AttemptCancelled()


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"seconds": 30.000001}, "must not exceed"),
        ({"seconds": 10**1000}, "must not exceed"),  # no float conversion: no OverflowError
        ({"seconds": -(10**1000)}, "not negative"),
        ({"seconds": True}, "must be a number"),
        ({"seconds": False}, "must be a number"),
        ({"seconds": "1"}, "must be a number"),
        ({"seconds": None}, "must be a number"),
        ({"seconds": [1]}, "must be a number"),
        ({"seconds": -1}, "not negative"),
        ({"seconds": -0.5}, "not negative"),
        ({"seconds": math.inf}, "finite"),
        ({"seconds": math.nan}, "finite"),
        ({}, "exactly"),
        ({"seconds": 1, "extra": 2}, "exactly"),
        ({"Seconds": 1}, "exactly"),
        ([1], "exactly"),
        (1, "exactly"),
        (None, "exactly"),
    ],
)
def test_wait_refuses_a_malformed_or_unbounded_request_as_permanent(
    payload: Any, reason: str
) -> None:
    started = time.monotonic()
    outcome = run_handler(builtin.waiting(LIMIT), context(), payload)
    assert time.monotonic() - started < 1.0  # refused before any wait
    assert isinstance(outcome, AttemptFailed)
    assert outcome.kind is FailureKind.PERMANENT
    assert outcome.error_type == "orqestra.domain.errors.PermanentError"
    assert reason in (outcome.error_message or "")


CENTURIES = timedelta(days=400 * 365)  # longer than threading.TIMEOUT_MAX


def test_wait_refuses_a_wait_longer_than_the_platform_supports_as_permanent() -> None:
    assert CENTURIES.total_seconds() > 1e10 > threading.TIMEOUT_MAX
    started = time.monotonic()
    outcome = run_handler(builtin.waiting(CENTURIES), context(), {"seconds": 1e10})
    assert time.monotonic() - started < 1.0
    assert isinstance(outcome, AttemptFailed)
    assert outcome.kind is FailureKind.PERMANENT
    assert "longest wait this platform supports" in (outcome.error_message or "")


def test_wait_accepts_the_longest_wait_the_platform_supports() -> None:
    cancel = threading.Event()
    cancel.set()  # a set event returns at once, whatever the timeout
    outcome = run_handler(
        builtin.waiting(CENTURIES), context(cancel), {"seconds": threading.TIMEOUT_MAX}
    )
    assert outcome == AttemptCancelled()


def test_wait_ends_early_and_cancelled_once_told() -> None:
    cancel = threading.Event()
    outcomes: list[Any] = []
    thread = threading.Thread(
        target=lambda: outcomes.append(
            run_handler(builtin.waiting(LIMIT), context(cancel), {"seconds": 30})
        )
    )
    thread.start()
    started = time.monotonic()
    cancel.set()  # what the worker does after a heartbeat confirms a request
    thread.join(10)
    assert not thread.is_alive()
    assert time.monotonic() - started < 5.0  # not the 30 s it asked for
    assert outcomes == [AttemptCancelled()]


def test_the_built_in_handlers_form_a_valid_registry_bounded_by_the_settings() -> None:
    handlers = builtin.handlers(SETTINGS)
    assert tuple(handlers) == builtin.KINDS == ("diagnostic.echo", "diagnostic.wait")
    assert HandlerRegistry(handlers).kinds == builtin.KINDS
    too_long: JsonValue = {"seconds": SETTINGS.execution_timeout.total_seconds() + 1}
    outcome = run_handler(handlers[builtin.WAIT], context(), too_long)
    assert isinstance(outcome, AttemptFailed)
    assert outcome.kind is FailureKind.PERMANENT
    assert all(kind.startswith(builtin.DIAGNOSTIC_PREFIX) for kind in builtin.KINDS)
