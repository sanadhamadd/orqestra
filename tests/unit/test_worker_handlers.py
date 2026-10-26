import asyncio
import copy
import dataclasses
import datetime
import decimal
import enum
import functools
import gc
import inspect
import json
import math
import re
import threading
import uuid
import warnings
from collections import OrderedDict
from collections.abc import AsyncIterator, Generator, Iterator
from typing import Any

import pytest

from orqestra.domain.errors import PermanentError, TransientError
from orqestra.domain.jobs import (
    AttemptCancelled,
    AttemptFailed,
    AttemptSucceeded,
    JsonValue,
)
from orqestra.domain.kinds import check_job_kinds
from orqestra.domain.policy import FailureKind
from orqestra.worker.handlers import (
    CancellationSignal,
    ControlPlaneSignal,
    HandlerCancelled,
    HandlerRegistry,
    TaskContext,
    outcome_of_exception,
    outcome_of_result,
    run_handler,
    storable_text,
)

pytestmark = pytest.mark.unit

INVALID = "orqestra.worker.handlers.InvalidHandlerResult"


def context(*, cancelled: bool = False, event: threading.Event | None = None) -> TaskContext:
    """A context whose read-only signal views ``event`` (the worker's), set if ``cancelled``."""
    event = threading.Event() if event is None else event
    if cancelled:
        event.set()
    return TaskContext(uuid.uuid4(), uuid.uuid4(), 1, "invoice", CancellationSignal(event))


def echo(ctx: TaskContext, payload: JsonValue) -> JsonValue:
    return payload


def invalid(outcome: object) -> str:
    """The message of an InvalidHandlerResult failure; fail if it is anything else."""
    assert isinstance(outcome, AttemptFailed)
    assert (outcome.kind, outcome.error_type) == (FailureKind.UNCLASSIFIED, INVALID)
    assert outcome.error_message is not None
    return outcome.error_message


# --- task context -------------------------------------------------------------


def test_the_context_carries_no_database_connection_and_is_frozen() -> None:
    ctx = context()
    assert {f.name for f in dataclasses.fields(ctx)} == {
        "task_id",
        "attempt_id",
        "attempt_number",
        "kind",
        "cancellation",
    }
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.kind = "other"  # type: ignore[misc]


def test_the_cancellation_signal_reports_and_waits_but_cannot_be_set() -> None:
    event = threading.Event()
    signal = CancellationSignal(event)
    assert (signal.is_set(), signal.wait(0)) == (False, False)
    assert repr(signal) == "CancellationSignal(set=False)"
    for name in ("set", "clear"):
        assert not hasattr(signal, name)
    with pytest.raises(AttributeError, match="read-only"):
        signal._event = threading.Event()
    with pytest.raises(AttributeError, match="read-only"):
        del signal._event
    with pytest.raises(AttributeError):
        signal.extra = 1
    assert not event.is_set()
    event.set()  # only the worker, holding the event, can set it
    assert (signal.is_set(), signal.wait(0), signal.wait(None)) == (True, True, True)


@pytest.mark.parametrize("value", [None, True, object(), "set"])
def test_a_cancellation_signal_views_only_a_threading_event(value: Any) -> None:
    with pytest.raises(TypeError, match=r"views a threading\.Event"):
        CancellationSignal(value)


@pytest.mark.parametrize(
    "value", [threading.Event(), None, object()], ids=["raw-event", "none", "object"]
)
def test_a_context_takes_only_a_read_only_signal(value: Any) -> None:
    # Handing a handler the worker's own event would let it set the event.
    with pytest.raises(TypeError, match="read-only CancellationSignal"):
        TaskContext(uuid.uuid4(), uuid.uuid4(), 1, "invoice", value)


def test_a_handler_cannot_forge_an_observed_cancellation() -> None:
    event = threading.Event()  # the worker's; never set here

    def forger(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        ctx.cancellation.set()  # type: ignore[attr-defined]
        raise HandlerCancelled

    outcome = run_handler(forger, context(event=event), None)
    # The attempt is an ordinary handler failure, never cancelled, and the event is untouched.
    assert isinstance(outcome, AttemptFailed)
    assert (outcome.kind, outcome.error_type) == (
        FailureKind.UNCLASSIFIED,
        "builtins.AttributeError",
    )
    assert not event.is_set()


def test_a_cancellation_set_by_the_worker_while_the_handler_waits_is_observed() -> None:
    event = threading.Event()
    waiting = threading.Event()

    def handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        waiting.set()
        if ctx.cancellation.wait(10):  # bounded; the worker sets it below
            raise HandlerCancelled
        return "timed out"

    def observe_request() -> None:  # the worker, once a committed request is seen
        waiting.wait(10)
        event.set()

    worker = threading.Thread(target=observe_request)
    worker.start()
    try:
        assert run_handler(handler, context(event=event), None) == AttemptCancelled()
    finally:
        worker.join(10)


# --- registry -----------------------------------------------------------------


def test_a_registry_maps_exact_kinds_to_their_handlers() -> None:
    def other(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        return None

    source = {"invoice": echo, "Invoice": other, "\t": echo}
    registry = HandlerRegistry(source)
    assert registry.kinds == ("\t", "Invoice", "invoice")  # sorted, exact
    assert registry["invoice"] is echo
    assert registry["Invoice"] is other
    assert len(registry) == 3
    assert list(registry) == list(registry.kinds)
    assert repr(registry) == "HandlerRegistry(kinds=['\\t', 'Invoice', 'invoice'])"
    # The kinds are exactly what claim_next_attempt accepts.
    assert check_job_kinds(registry.kinds) == list(registry.kinds)
    source["late"] = echo  # changing the source afterwards changes nothing
    assert "late" not in registry


@pytest.mark.parametrize("kind", ["INVOICE", "invoice ", " invoice", "invoic", "\n", "*"])
def test_a_registry_has_no_fallback_for_other_kinds(kind: str) -> None:
    registry = HandlerRegistry({"invoice": echo, "\t": echo})
    assert kind not in registry
    with pytest.raises(KeyError):
        registry[kind]


def test_a_registry_is_immutable() -> None:
    registry = HandlerRegistry({"invoice": echo})
    with pytest.raises(TypeError):
        registry["other"] = echo  # type: ignore[index]
    with pytest.raises(AttributeError):
        registry.extra = 1  # type: ignore[attr-defined]


def test_callable_objects_and_partials_are_handlers() -> None:
    class Handler:
        def __call__(self, ctx: TaskContext, payload: JsonValue) -> JsonValue:
            return payload

    def scaled(factor: int, ctx: TaskContext, payload: JsonValue) -> JsonValue:
        return factor

    registry = HandlerRegistry({"a": Handler(), "b": functools.partial(scaled, 2)})
    assert registry.kinds == ("a", "b")


async def coroutine_handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
    return None


def generator_handler(ctx: TaskContext, payload: JsonValue) -> Iterator[JsonValue]:
    yield None


async def async_generator_handler(ctx: TaskContext, payload: JsonValue) -> AsyncIterator[JsonValue]:
    yield None


@pytest.mark.parametrize(
    ("handlers", "error", "message"),
    [
        ({}, ValueError, "at least one"),
        ([("invoice", echo)], TypeError, "must map job kinds"),
        (None, TypeError, "must map job kinds"),
        ({"": echo}, ValueError, "must not be blank"),
        ({" ": echo}, ValueError, "must not be blank"),
        ({"in\x00voice": echo}, ValueError, "NUL character"),
        ({1: echo}, TypeError, "must be a str"),
        ({b"invoice": echo}, TypeError, "must be a str"),
        ({"invoice": None}, TypeError, "must be callable"),
        ({"invoice": "echo"}, TypeError, "must be callable"),
        ({"invoice": coroutine_handler}, TypeError, "synchronous"),
        ({"invoice": functools.partial(coroutine_handler)}, TypeError, "synchronous"),
        ({"invoice": generator_handler}, TypeError, "synchronous"),
        ({"invoice": async_generator_handler}, TypeError, "synchronous"),
    ],
    ids=[
        "empty",
        "pairs",
        "none",
        "empty-kind",
        "blank-kind",
        "nul-kind",
        "int-kind",
        "bytes-kind",
        "none-handler",
        "str-handler",
        "coroutine-function",
        "coroutine-partial",
        "generator-function",
        "async-generator-function",
    ],
)
def test_invalid_registries_are_refused(
    handlers: Any, error: type[Exception], message: str
) -> None:
    with pytest.raises(error, match=message):
        HandlerRegistry(handlers)


# --- results ------------------------------------------------------------------


shared = [1, 2]


@pytest.mark.parametrize(
    "result",
    [
        None,  # JSON null is a result (DEC-030)
        True,
        False,
        0,
        -7,
        10**100,
        1.5,
        -0.0,
        1e308,
        "",
        "text with é, 😀 and a \t tab",
        [],
        {},
        [None, [None]],
        {"a": [1, {"b": None, "c": [True, "x"]}], "": 0},
        {"one": shared, "two": shared},  # shared, not cyclic
    ],
    ids=[
        "null",
        "true",
        "false",
        "zero",
        "negative",
        "big-int",
        "float",
        "negative-zero",
        "large-float",
        "empty-str",
        "unicode",
        "empty-list",
        "empty-dict",
        "nested-nulls",
        "nested",
        "shared-reference",
    ],
)
def test_valid_json_results_succeed_as_an_equal_independent_copy(result: JsonValue) -> None:
    outcome = outcome_of_result(result)
    assert outcome == AttemptSucceeded(result)
    assert isinstance(outcome, AttemptSucceeded)
    assert json.loads(json.dumps(result)) == result
    same_json_types(outcome.result, result)


def same_json_types(snapshot: object, original: object) -> None:
    """Equal values of exactly the same types; containers new, scalars reused."""
    assert type(snapshot) is type(original)
    if isinstance(original, list):
        assert snapshot is not original
        assert isinstance(snapshot, list)
        assert len(snapshot) == len(original)
        for copied, item in zip(snapshot, original, strict=True):
            same_json_types(copied, item)
    elif isinstance(original, dict):
        assert snapshot is not original
        assert isinstance(snapshot, dict)
        assert list(snapshot) == list(original)  # same keys, same order
        for key, item in original.items():
            same_json_types(snapshot[key], item)
    else:
        assert snapshot is original  # immutable scalars are not converted or copied
        if isinstance(original, float):
            assert math.copysign(1, cast_float(snapshot)) == math.copysign(1, original)


def cast_float(value: object) -> float:
    assert isinstance(value, float)
    return value


def test_changing_the_original_after_validation_cannot_change_the_outcome() -> None:
    inner: list[Any] = [1, {"total": 2.5}]
    original: dict[str, Any] = {"items": inner, "note": "ok"}
    expected = copy.deepcopy(original)
    outcome = outcome_of_result(original)
    assert outcome == AttemptSucceeded(expected)
    # Afterwards the handler's objects become invalid and different in every way.
    inner.append(float("nan"))
    inner[1]["total"] = float("inf")
    original["note"] = "\x00"
    original[1] = (1, 2)  # type: ignore[index]
    del original["items"]
    assert outcome == AttemptSucceeded(expected)
    assert isinstance(outcome, AttemptSucceeded)
    assert json.dumps(outcome.result, allow_nan=False) == json.dumps(expected)


def test_a_shared_container_is_copied_once_per_place_it_appears() -> None:
    outcome = outcome_of_result({"one": shared, "two": shared})
    assert isinstance(outcome, AttemptSucceeded)
    assert isinstance(outcome.result, dict)
    assert outcome.result["one"] == outcome.result["two"] == shared
    assert outcome.result["one"] is not outcome.result["two"]  # JSON has no shared values


class Colour(enum.StrEnum):
    RED = "red"


def cyclic_list() -> list[Any]:
    value: list[Any] = [1]
    value.append([value])
    return value


def cyclic_dict() -> dict[str, Any]:
    value: dict[str, Any] = {}
    value["self"] = value
    return value


def nested(depth: int) -> list[Any]:
    value: list[Any] = []
    for _ in range(depth):
        value = [value]
    return value


@pytest.mark.parametrize(
    ("result", "message"),
    [
        (float("nan"), "result is nan"),
        (float("inf"), "result is inf"),
        (float("-inf"), "result is -inf"),
        ([1, float("nan")], r"result\[1\] is nan"),
        ({"a": {"b": float("inf")}}, r"result\['a'\]\['b'\] is inf"),
        ("a\x00b", "result contains a NUL character"),
        ({"k": ["ok", "\x00"]}, r"result\['k'\]\[1\] contains a NUL"),
        ({"a\x00": 1}, "a key of result contains a NUL"),
        ("\ud800", "unpaired surrogate"),
        ({"\udfff": 1}, "unpaired surrogate"),
        ((1, 2), "result is a tuple"),
        ({1, 2}, "result is a set"),
        (b"bytes", "result is a bytes"),
        (bytearray(b"x"), "result is a bytearray"),
        (decimal.Decimal("1.5"), "result is a Decimal"),
        (datetime.date(2026, 1, 1), "result is a date"),
        (uuid.UUID(int=1), "result is a UUID"),
        (object(), "result is a object"),
        (Colour.RED, "result is a Colour"),
        (OrderedDict(a=1), "result is a OrderedDict"),
        ([(1, 2)], r"result\[0\] is a tuple"),
        ({1: "a"}, "has a int key 1"),
        ({None: "a"}, "has a NoneType key None"),
        ({True: "a"}, "has a bool key True"),
        ({"a": [{"b": {2.5: 0}}]}, "has a float key 2.5"),
        (cyclic_list(), "contains itself"),
        (cyclic_dict(), "contains itself"),
        (nested(5_000), "nested too deeply"),
        (10**5_000, "cannot be encoded as JSON"),
    ],
    ids=[
        "nan",
        "infinity",
        "negative-infinity",
        "nested-nan",
        "nested-infinity",
        "nul",
        "nested-nul",
        "nul-key",
        "surrogate",
        "surrogate-key",
        "tuple",
        "set",
        "bytes",
        "bytearray",
        "decimal",
        "date",
        "uuid",
        "object",
        "str-enum",
        "dict-subclass",
        "nested-tuple",
        "int-key",
        "none-key",
        "bool-key",
        "nested-float-key",
        "cyclic-list",
        "cyclic-dict",
        "too-deep",
        "huge-int",
    ],
)
def test_invalid_results_are_unclassified_failures_before_anything_is_stored(
    result: object, message: str
) -> None:
    assert re.search(message, invalid(outcome_of_result(result)))


def test_an_unstarted_coroutine_is_closed_without_running_or_warning() -> None:
    ran: list[str] = []

    async def coroutine() -> int:
        try:
            ran.append("body")
        finally:
            ran.append("finally")
        return 1

    pending = coroutine()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert "is a coroutine" in invalid(outcome_of_result(pending))
        assert inspect.getcoroutinestate(pending) == inspect.CORO_CLOSED
        del pending
        gc.collect()
    assert [str(w.message) for w in caught] == []  # no "was never awaited"
    assert ran == []


def test_a_dropped_unstarted_coroutine_would_otherwise_warn() -> None:
    # The warning the closing above prevents, shown without the boundary.
    async def coroutine() -> None:
        pass

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        coroutine()  # type: ignore[unused-coroutine]
        gc.collect()
    assert any("was never awaited" in str(w.message) for w in caught)


def test_a_started_coroutine_or_any_generator_is_reported_but_left_alone() -> None:
    ran: list[str] = []

    async def started() -> None:
        try:
            ran.append("started")
            await asyncio.sleep(0)
        finally:
            ran.append("coroutine finally")

    def generator() -> Generator[int]:
        try:
            ran.append("generator body")
            yield 1
        finally:
            ran.append("generator finally")

    running = started()
    running.send(None)  # runs to its first suspension, as a handler might have
    fresh, suspended = generator(), generator()
    next(suspended)
    try:
        assert "is a coroutine" in invalid(outcome_of_result(running))
        assert "is a generator" in invalid(outcome_of_result(fresh))
        assert "is a generator" in invalid(outcome_of_result(suspended))
        # Closing these would run handler code: they are not touched.
        assert inspect.getcoroutinestate(running) == inspect.CORO_SUSPENDED
        assert inspect.getgeneratorstate(fresh) == inspect.GEN_CREATED
        assert inspect.getgeneratorstate(suspended) == inspect.GEN_SUSPENDED
        assert ran == ["started", "generator body"]
    finally:
        running.close()
        suspended.close()


def test_other_awaitables_are_reported_and_left_alone() -> None:
    closed: list[str] = []

    class Awaitable:
        def __await__(self) -> Generator[None, None, None]:
            yield None

        def close(self) -> None:
            closed.append("close")  # arbitrary code the boundary must not call

    assert "is a Awaitable" in invalid(outcome_of_result(Awaitable()))
    assert closed == []


# --- exceptions -----------------------------------------------------------------


@pytest.mark.parametrize("observed", [False, True], ids=["no-request", "request-observed"])
@pytest.mark.parametrize(
    ("exc", "kind", "error_type", "message"),
    [
        (TransientError("rate limited"), FailureKind.TRANSIENT, "TransientError", "rate limited"),
        (PermanentError("bad invoice"), FailureKind.PERMANENT, "PermanentError", "bad invoice"),
        (ValueError("bug"), FailureKind.UNCLASSIFIED, "builtins.ValueError", "bug"),
        (RuntimeError(), FailureKind.UNCLASSIFIED, "builtins.RuntimeError", None),
    ],
    ids=["transient", "permanent", "unclassified", "no-message"],
)
def test_handler_exceptions_keep_the_domain_classification(
    exc: Exception, kind: FailureKind, error_type: str, message: str | None, observed: bool
) -> None:
    # Even after a request was observed, a failure is reported as what it is.
    outcome = outcome_of_exception(exc, cancellation_observed=observed)
    assert outcome == AttemptFailed.from_exception(exc)
    assert isinstance(outcome, AttemptFailed)
    assert outcome.kind is kind
    assert outcome.error_type.endswith(error_type)
    assert outcome.error_message == message


def test_cancellation_is_an_outcome_only_once_a_request_was_observed() -> None:
    assert (
        outcome_of_exception(HandlerCancelled(), cancellation_observed=True) == AttemptCancelled()
    )
    unasked = outcome_of_exception(HandlerCancelled("stopping"), cancellation_observed=False)
    assert unasked == AttemptFailed(
        FailureKind.UNCLASSIFIED, "orqestra.worker.handlers.HandlerCancelled", "stopping"
    )


class Custom(BaseException):
    pass


@pytest.mark.parametrize("observed", [False, True], ids=["no-request", "request-observed"])
@pytest.mark.parametrize(
    ("signal", "name"),
    [
        (SystemExit(3), "builtins.SystemExit"),
        (KeyboardInterrupt(), "builtins.KeyboardInterrupt"),
        (GeneratorExit(), "builtins.GeneratorExit"),
        (asyncio.CancelledError(), "asyncio.exceptions.CancelledError"),
        (Custom(), f"{__name__}.Custom"),
    ],
    ids=["system-exit", "keyboard-interrupt", "generator-exit", "asyncio-cancelled", "custom"],
)
def test_control_plane_signals_never_become_attempt_outcomes(
    signal: BaseException, name: str, observed: bool
) -> None:
    outcome = outcome_of_exception(signal, cancellation_observed=observed)
    assert outcome == ControlPlaneSignal(name)
    # The domain contract is unchanged: such signals are never classified.
    with pytest.raises(TypeError, match="not a handler failure"):
        AttemptFailed.from_exception(signal)


def test_failure_text_is_stored_with_escapes_postgresql_accepts() -> None:
    nul = outcome_of_exception(ValueError("a\x00b"), cancellation_observed=False)
    assert isinstance(nul, AttemptFailed)
    assert nul.error_message == "a\\u0000b"
    surrogate = outcome_of_exception(ValueError("x\ud800y"), cancellation_observed=False)
    assert isinstance(surrogate, AttemptFailed)
    assert surrogate.error_message == "x\\ud800y"

    class OddError(Exception):
        pass

    OddError.__module__, OddError.__qualname__ = "mod\ud800", "Odd\x00Name"  # Python allows both
    renamed = outcome_of_exception(OddError("m"), cancellation_observed=False)
    assert isinstance(renamed, AttemptFailed)
    assert renamed.error_type == "mod\\ud800.Odd\\u0000Name"


@pytest.mark.parametrize(
    ("text", "stored"),
    [
        ("plain é 😀", "plain é 😀"),
        ("\x00", "\\u0000"),
        ("a\x00b\x00", "a\\u0000b\\u0000"),
        ("\ud83d", "\\ud83d"),
        ("\\u0000", "\\u0000"),  # already escaped text is kept as it is
    ],
)
def test_storable_text_is_deterministic_and_encodable(text: str, stored: str) -> None:
    assert storable_text(text) == stored
    assert storable_text(text).encode("utf-8")
    assert "\x00" not in storable_text(text)


# --- running a handler ------------------------------------------------------------


def test_run_handler_passes_the_context_and_payload_and_returns_the_result() -> None:
    seen: list[tuple[TaskContext, JsonValue]] = []

    def handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        seen.append((ctx, payload))
        return {"total": 3}

    ctx = context()
    assert run_handler(handler, ctx, {"invoice": 1}) == AttemptSucceeded({"total": 3})
    assert seen == [(ctx, {"invoice": 1})]


def test_run_handler_reports_a_none_result_as_a_json_null_success() -> None:
    assert run_handler(lambda ctx, payload: None, context(), {}) == AttemptSucceeded(None)


def test_run_handler_reports_an_invalid_result_as_a_failure() -> None:
    def handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        return float("nan")

    assert "result is nan" in invalid(run_handler(handler, context(), {}))


@pytest.mark.parametrize(
    "signal", [SystemExit(1), KeyboardInterrupt()], ids=["system-exit", "keyboard-interrupt"]
)
def test_run_handler_contains_control_plane_signals(signal: BaseException) -> None:
    def handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        raise signal

    outcome = run_handler(handler, context(cancelled=True), {})
    assert isinstance(outcome, ControlPlaneSignal)
    assert outcome.signal_type == f"builtins.{type(signal).__name__}"


def cooperative(ctx: TaskContext, payload: JsonValue) -> JsonValue:
    if ctx.cancellation.wait(0):
        raise HandlerCancelled
    return "done"


def test_a_cooperative_handler_is_cancelled_only_after_a_request_was_observed() -> None:
    assert run_handler(cooperative, context(cancelled=True), {}) == AttemptCancelled()
    assert run_handler(cooperative, context(cancelled=False), {}) == AttemptSucceeded("done")


def test_a_handler_that_ignores_a_request_still_succeeds() -> None:
    # A legitimate success may still win after a request (DEC-020).
    assert run_handler(echo, context(cancelled=True), [1]) == AttemptSucceeded([1])


def test_a_request_observed_while_the_handler_ran_counts() -> None:
    event = threading.Event()

    def handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        event.set()  # stands in for the worker observing a request mid-run
        raise HandlerCancelled

    assert run_handler(handler, context(event=event), None) == AttemptCancelled()
