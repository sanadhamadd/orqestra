"""Task handlers, the registry of supported job kinds, and handler outcomes (DEC-042, DEC-043).

A handler is a plain synchronous function of a ``TaskContext`` and the task's
payload that returns the task's result as a JSON value. It gets no database
connection: recording the outcome is the worker's job. It may raise the
domain's ``TransientError`` or ``PermanentError`` to choose how a failure is
retried, and it learns of a cancellation request through ``context.cancellation``,
a read-only view of an event that only the worker can set.

``run_handler`` is the boundary between a handler and the attempt lifecycle.
It turns what the handler did into exactly one ``HandlerOutcome``:

- a valid JSON result is ``AttemptSucceeded`` holding a checked copy of it
  (JSON ``null`` included), so later changes to the handler's object cannot
  reach the outcome;
- an invalid result is ``AttemptFailed``, unclassified, as an
  ``InvalidHandlerResult``, detected before anything is written;
- an exception is classified by ``AttemptFailed.from_exception``, except that
  ``HandlerCancelled`` is ``AttemptCancelled`` once cancellation was observed;
- a control-plane signal (``SystemExit``, ``KeyboardInterrupt`` or any other
  ``BaseException`` that is not an ``Exception``) is a ``ControlPlaneSignal``:
  no outcome may be recorded for it (DEC-009).

Nothing here catches or reclassifies database errors: they happen when the
worker records the outcome, after this boundary.
"""

import dataclasses
import inspect
import json
import math
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import NoReturn, cast

from orqestra.domain.jobs import (
    AttemptCancelled,
    AttemptFailed,
    AttemptOutcome,
    AttemptSucceeded,
    JsonValue,
)
from orqestra.domain.kinds import check_job_kinds


class CancellationSignal:
    """A read-only view of the worker's cancellation event for one attempt.

    The worker owns the ``threading.Event`` and sets it once it has observed a
    committed cancellation request. A handler gets only this view: it can ask
    whether the event is set and wait for it, but the view has no way to set or
    clear it, and its event cannot be replaced. Python cannot prevent deliberate
    introspection, so the authority stays with the database: an attempt is
    recorded as cancelled only after a committed request (``next_task_state``).
    """

    __slots__ = ("_event",)
    _event: threading.Event

    def __init__(self, event: threading.Event) -> None:
        object.__setattr__(self, "_event", _event_of(event))

    def is_set(self) -> bool:
        """Whether the worker has observed a committed cancellation request."""
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        """Wait up to ``timeout`` seconds (forever if ``None``); return ``is_set()``."""
        return self._event.wait(timeout)

    def __setattr__(self, name: str, value: object) -> NoReturn:
        msg = "a CancellationSignal is read-only"
        raise AttributeError(msg)

    def __delattr__(self, name: str) -> NoReturn:
        msg = "a CancellationSignal is read-only"
        raise AttributeError(msg)

    def __repr__(self) -> str:
        return f"CancellationSignal(set={self.is_set()})"


def _event_of(event: object) -> threading.Event:
    if not isinstance(event, threading.Event):
        msg = f"a CancellationSignal views a threading.Event, got {type(event).__name__}"
        raise TypeError(msg)
    return event


@dataclass(frozen=True, slots=True)
class TaskContext:
    """What a handler knows about the attempt it runs. There is no database connection.

    ``cancellation`` must be a ``CancellationSignal``, never the worker's event
    itself, so a handler cannot set it.
    """

    task_id: uuid.UUID
    attempt_id: uuid.UUID
    attempt_number: int
    kind: str
    cancellation: CancellationSignal

    def __post_init__(self) -> None:
        _require_signal(self.cancellation)


def _require_signal(cancellation: object) -> None:
    if not isinstance(cancellation, CancellationSignal):
        msg = (
            "TaskContext.cancellation must be a read-only CancellationSignal, "
            f"got {type(cancellation).__name__}"
        )
        raise TypeError(msg)


type Handler = Callable[[TaskContext, JsonValue], JsonValue]


class HandlerCancelled(Exception):  # noqa: N818 - it reports a stop, not an error
    """Raised by a handler that stopped because cancellation was requested.

    It ends the attempt as cancelled only if the worker had observed a request
    (``context.cancellation`` is set); otherwise it is an ordinary,
    unclassified failure.
    """


class InvalidHandlerResult(Exception):  # noqa: N818 - the name is the stored error_type
    """A handler returned a value that cannot be stored as a task's JSON result."""


@dataclass(frozen=True, slots=True)
class ControlPlaneSignal:
    """The handler raised a control-plane signal; no attempt outcome may be recorded.

    Such signals are not handler failures and are never classified (DEC-009):
    the worker must not complete the attempt for them. ``signal_type`` is the
    signal's qualified class name, for logs.
    """

    signal_type: str


type HandlerOutcome = AttemptOutcome | ControlPlaneSignal


class HandlerRegistry(Mapping[str, Handler]):
    """An immutable map from exact job kinds to the handlers that run them.

    It needs at least one kind. Kinds follow the claim's rule (``check_job_kinds``)
    and match exactly. Each handler must be a synchronous callable: coroutine,
    generator and async-generator functions are refused. ``kinds`` is what a
    worker passes to ``claim_next_attempt``; there is no fallback handler.
    """

    __slots__ = ("_handlers",)

    def __init__(self, handlers: Mapping[str, Handler]) -> None:
        self._handlers = MappingProxyType(_checked_handlers(handlers))

    @property
    def kinds(self) -> tuple[str, ...]:
        """The supported job kinds, sorted, for ``claim_next_attempt(..., kinds=...)``."""
        return tuple(self._handlers)

    def __getitem__(self, kind: str) -> Handler:
        return self._handlers[kind]

    def __iter__(self) -> Iterator[str]:
        return iter(self._handlers)

    def __len__(self) -> int:
        return len(self._handlers)

    def __repr__(self) -> str:
        return f"HandlerRegistry(kinds={list(self._handlers)!r})"


def _checked_handlers(handlers: object) -> dict[str, Handler]:
    """The handlers by kind, in kind order, checked; raise for anything else."""
    if not isinstance(handlers, Mapping):
        msg = f"handlers must map job kinds to handlers, got {type(handlers).__name__}"
        raise TypeError(msg)
    kinds = check_job_kinds(handlers.keys())
    for kind in kinds:
        _check_handler(kind, handlers[kind])
    return {kind: cast(Handler, handlers[kind]) for kind in kinds}


def _check_handler(kind: str, handler: object) -> None:
    if not callable(handler):
        msg = f"the handler for kind {kind!r} must be callable, got {type(handler).__name__}"
        raise TypeError(msg)
    if (
        inspect.iscoroutinefunction(handler)
        or inspect.isasyncgenfunction(handler)
        or inspect.isgeneratorfunction(handler)
    ):
        msg = (
            f"the handler for kind {kind!r} must be a synchronous function that returns its result"
        )
        raise TypeError(msg)


def run_handler(handler: Handler, context: TaskContext, payload: JsonValue) -> HandlerOutcome:
    """Call ``handler`` and return the outcome of what it did; never raise for it.

    Cancellation counts as observed if ``context.cancellation`` is set when
    the handler has finished.
    """
    try:
        result = handler(context, payload)
    except BaseException as raised:  # control-plane signals become ControlPlaneSignal
        return outcome_of_exception(raised, cancellation_observed=context.cancellation.is_set())
    return outcome_of_result(result)


def outcome_of_result(result: object) -> AttemptOutcome:
    """``AttemptSucceeded`` for a valid JSON result; otherwise an ``InvalidHandlerResult`` failure.

    A valid result is built only from ``None``, ``bool``, ``int``, finite
    ``float``, ``str``, ``list`` and ``dict`` with ``str`` keys (exact types,
    so nothing is silently converted on the way to ``jsonb``), with no NUL
    character or unpaired surrogate in any string, no cycle, and nesting
    shallow enough to check.

    The outcome holds a checked copy: new lists and dicts with the same keys,
    order and immutable scalar values, so it equals the result as JSON but is
    not the handler's object, and nothing done to that object afterwards can
    change it. A coroutine or generator returned by mistake is reported and
    never run; an unstarted native coroutine is closed, which runs none of its
    code and avoids a "never awaited" warning. Other awaitables are left alone.
    """
    try:
        snapshot = _snapshot(result, "result", set())
        json.dumps(snapshot, allow_nan=False)  # what jsonb storage will need, e.g. int size
    except InvalidHandlerResult as invalid:
        return _failure(invalid)
    except RecursionError:
        return _failure(InvalidHandlerResult("result is nested too deeply to store"))
    except ValueError as unencodable:
        return _failure(InvalidHandlerResult(f"result cannot be encoded as JSON: {unencodable}"))
    return AttemptSucceeded(snapshot)


def outcome_of_exception(exc: BaseException, *, cancellation_observed: bool) -> HandlerOutcome:
    """The outcome of a handler that raised ``exc``.

    A ``BaseException`` that is not an ``Exception`` is a ``ControlPlaneSignal``
    and is never classified. ``HandlerCancelled`` is ``AttemptCancelled`` only
    if cancellation was observed. Any other exception is classified by
    ``AttemptFailed.from_exception``.
    """
    if not isinstance(exc, Exception):
        return ControlPlaneSignal(_qualified_name(type(exc)))
    if isinstance(exc, HandlerCancelled) and cancellation_observed:
        return AttemptCancelled()
    return _failure(exc)


def storable_text(text: str) -> str:
    """``text`` as PostgreSQL can store it: NUL and unpaired surrogates written as escapes.

    The mapping is deterministic, so the stored text is exactly what was sent.
    """
    return text.replace("\x00", "\\u0000").encode("utf-8", "backslashreplace").decode("utf-8")


def _failure(exc: Exception) -> AttemptFailed:
    failure = AttemptFailed.from_exception(exc)
    message = failure.error_message
    return dataclasses.replace(
        failure,
        error_type=storable_text(failure.error_type),
        error_message=None if message is None else storable_text(message),
    )


def _qualified_name(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _snapshot(value: object, path: str, containers: set[int]) -> JsonValue:
    """A checked copy of ``value``: new lists and dicts, the same immutable scalars.

    Raise ``InvalidHandlerResult`` unless ``value`` is a storable JSON value.
    ``containers`` holds the ids of the lists and dicts being copied above
    ``value``, to refuse cycles.
    """
    kind = type(value)
    if value is None or kind is bool or kind is int:
        return cast(JsonValue, value)
    if kind is float:
        if not math.isfinite(cast(float, value)):
            msg = f"{path} is {value}, which JSON cannot represent"
            raise InvalidHandlerResult(msg)
        return cast(float, value)
    if kind is str:
        _check_text(cast(str, value), path)
        return cast(str, value)
    if kind is list or kind is dict:
        if id(value) in containers:
            msg = f"{path} contains itself"
            raise InvalidHandlerResult(msg)
        containers.add(id(value))
        copy: JsonValue
        # Each container's entries are taken in one step (a C-level copy under
        # the GIL), so another thread still changing it cannot break the copy.
        if kind is list:
            items = list(cast(list[object], value))
            copy = [
                _snapshot(item, f"{path}[{index}]", containers) for index, item in enumerate(items)
            ]
        else:
            entries: dict[str, JsonValue] = {}
            for key, item in list(cast(dict[object, object], value).items()):
                if type(key) is not str:
                    msg = f"{path} has a {type(key).__name__} key {key!r}; JSON keys are strings"
                    raise InvalidHandlerResult(msg)
                _check_text(key, f"a key of {path}")
                entries[key] = _snapshot(item, f"{path}[{key!r}]", containers)
            copy = entries
        containers.discard(id(value))
        return copy
    if inspect.iscoroutine(value) and inspect.getcoroutinestate(value) == inspect.CORO_CREATED:
        value.close()  # an unstarted coroutine has no frame to run: closing executes nothing
    if inspect.isawaitable(value) or inspect.isgenerator(value):
        msg = f"{path} is a {kind.__name__}; handlers must be synchronous and return their result"
        raise InvalidHandlerResult(msg)
    msg = f"{path} is a {kind.__name__}, not a JSON value"
    raise InvalidHandlerResult(msg)


def _check_text(text: str, path: str) -> None:
    if "\x00" in text:
        msg = f"{path} contains a NUL character, which PostgreSQL cannot store"
        raise InvalidHandlerResult(msg)
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        msg = f"{path} contains an unpaired surrogate, which is not valid Unicode"
        raise InvalidHandlerResult(msg) from None
