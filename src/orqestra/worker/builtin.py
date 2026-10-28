"""The fixed, code-reviewed handlers ``orqestra worker run`` can register (DEC-051).

There is no production workload yet, so the only handlers are two diagnostics
for development and tests. Neither is an invoice or production handler, and
``orqestra worker run`` refuses them when ``ORQESTRA_ENVIRONMENT`` is
``production``. A worker runs only the kinds it is asked for with ``--kind``;
nothing is imported dynamically.

- ``diagnostic.echo`` returns its payload unchanged.
- ``diagnostic.wait`` takes ``{"seconds": n}``: it waits up to ``n`` seconds on
  the attempt's read-only cancellation signal, then returns
  ``{"waited_seconds": n}``, or raises ``HandlerCancelled`` once told of a
  committed cancellation request. ``n`` must be a finite, non-negative number,
  not a boolean, and no longer than the worker's ``execution_timeout``; any
  other payload is a permanent failure.
"""

import math
from collections.abc import Mapping
from datetime import timedelta
from types import MappingProxyType

from orqestra.domain.errors import PermanentError
from orqestra.domain.jobs import JsonValue
from orqestra.worker.handlers import Handler, HandlerCancelled, TaskContext
from orqestra.worker.settings import WorkerSettings

DIAGNOSTIC_PREFIX = "diagnostic."
ECHO = "diagnostic.echo"
WAIT = "diagnostic.wait"
KINDS = (ECHO, WAIT)


def echo(context: TaskContext, payload: JsonValue) -> JsonValue:
    """Return the payload unchanged."""
    return payload


def waiting(limit: timedelta) -> Handler:
    """``diagnostic.wait``, refusing any request longer than ``limit``."""
    bound = limit.total_seconds()

    def wait(context: TaskContext, payload: JsonValue) -> JsonValue:
        seconds = _seconds(payload, bound)
        if context.cancellation.wait(seconds):  # an event wait: no polling
            raise HandlerCancelled
        return {"waited_seconds": seconds}

    return wait


def _seconds(payload: JsonValue, bound: float) -> int | float:
    if not isinstance(payload, dict) or set(payload) != {"seconds"}:
        msg = 'diagnostic.wait takes exactly {"seconds": <number>}'
        raise PermanentError(msg)
    seconds = payload["seconds"]
    if isinstance(seconds, bool) or not isinstance(seconds, int | float):
        msg = f"seconds must be a number, got {type(seconds).__name__}"
        raise PermanentError(msg)
    # Only a float can be infinite or NaN; an int of any size compares exactly, whereas
    # math.isfinite would first convert it to a float and overflow (10**1000).
    if (isinstance(seconds, float) and not math.isfinite(seconds)) or seconds < 0:
        msg = "seconds must be finite and not negative"
        raise PermanentError(msg)
    if seconds > bound:
        msg = f"seconds must not exceed the worker's execution_timeout ({bound} s)"
        raise PermanentError(msg)
    return seconds


def handlers(settings: WorkerSettings) -> Mapping[str, Handler]:
    """Every built-in handler by kind; ``diagnostic.wait`` is bounded by ``execution_timeout``."""
    return MappingProxyType({ECHO: echo, WAIT: waiting(settings.execution_timeout)})
