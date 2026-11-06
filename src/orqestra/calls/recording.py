"""What is recorded about each provider call, and the interface a recorder implements.

A caller records a ``ProviderCallIntent`` before it calls a provider and a
``ProviderCallResult`` once the call has ended in a known way. A call whose end
is unknown (the provider raised something that is not a provider error, returned
something that is not a ``ProviderResponse``, or the process was interrupted) has
an intent and no result: it stays *unresolved*.

The records carry only closed codes, numbers, route and capability names and the
provider's own ``ProviderUsage``. They never carry a raw string, a request id,
the reply's value, a prompt or ``reported_cost_units``. ``outcome`` uses the
usage ledger's values, and ``call_seq`` and ``item_index`` (``MAX_CALL_SEQ``) and
``estimated_cost_units`` (``MAX_COST_UNITS``) its ranges. ``route_id`` and
``capability`` are only checked to be a ``str`` here. On the invoice verification
path, ``route_id`` comes from a checked ``RouteOption``, which uses the ledger's
identifier pattern, and ``capability`` is the constant ``interpret_amount``, so
the ledger accepts every intent made there once a durable recorder adds the
provider. A record built some other way can carry a ``route_id`` the ledger
refuses; a durable recorder then raises in ``record_intent``, before the call.
``usage`` is bounded only by ``ProviderUsage``, which is looser than the ledger's
amount range (below 1E+20, at most 18 decimal places): what a durable recorder does
with a reported amount outside that range is its own decision, and it learns of it
only after the call was made. A wrong type raises ``TypeError`` and a bad value
``ValueError``; no message quotes the rejected value.

A record does not name the provider, job, task or prompt version: a durable
recorder adds those from its own wiring, so this module never depends on the
worker.

This module performs no I/O. ``InMemoryCallRecorder`` keeps records in memory
only, for tests and local runs; it is not durable.
"""

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from orqestra.calls.provider import FieldKind, PermanentCode, ProviderUsage, TransientCode

MAX_CALL_SEQ = 2**31 - 1
MAX_COST_UNITS = 2**63 - 1


class CallOutcome(StrEnum):
    """How a provider call ended, when that is known. The usage ledger's values."""

    ANSWERED = "answered"
    FAILED = "failed"


def _check_uuid(name: str, value: object) -> None:
    if not isinstance(value, uuid.UUID):
        msg = f"{name} must be a UUID, got {type(value).__name__}"
        raise TypeError(msg)


def _check_int(name: str, value: object, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{name} must be an integer, got {type(value).__name__}"
        raise TypeError(msg)
    if value < 0:
        msg = f"{name} must not be negative"
        raise ValueError(msg)
    if value > maximum:
        msg = f"{name} must be at most {maximum}"
        raise ValueError(msg)


def _check_str(name: str, value: object) -> None:
    if not isinstance(value, str):
        msg = f"{name} must be a str, got {type(value).__name__}"
        raise TypeError(msg)


def _check_type(name: str, value: object, *kinds: type) -> None:
    if not isinstance(value, kinds):
        expected = " or ".join(kind.__name__ for kind in kinds)
        msg = f"{name} must be a {expected}, got {type(value).__name__}"
        raise TypeError(msg)


@dataclass(frozen=True, slots=True, kw_only=True)
class ProviderCallIntent:
    """Call ``call_seq`` of attempt ``attempt_id`` is about to be made.

    ``item_index`` is the absolute index of the item the call is about (the
    batch offset plus its position), at most ``MAX_CALL_SEQ``, and
    ``estimated_cost_units`` the chosen route's estimate, at most
    ``MAX_COST_UNITS``; the estimate is not accounting.
    """

    attempt_id: uuid.UUID
    call_seq: int
    item_index: int
    route_id: str
    capability: str
    field_kind: FieldKind
    estimated_cost_units: int

    def __post_init__(self) -> None:
        _check_uuid("attempt_id", self.attempt_id)
        _check_int("call_seq", self.call_seq, MAX_CALL_SEQ)
        _check_int("item_index", self.item_index, MAX_CALL_SEQ)
        _check_str("route_id", self.route_id)
        _check_str("capability", self.capability)
        _check_type("field_kind", self.field_kind, FieldKind)
        _check_int("estimated_cost_units", self.estimated_cost_units, MAX_COST_UNITS)


@dataclass(frozen=True, slots=True, kw_only=True)
class ProviderCallResult:
    """How call ``call_seq`` of attempt ``attempt_id`` ended.

    ``failure_code`` is the provider error's code, set exactly when ``outcome``
    is ``FAILED``. ``usage`` is the provider's own report, if it gave one.
    """

    attempt_id: uuid.UUID
    call_seq: int
    outcome: CallOutcome
    failure_code: TransientCode | PermanentCode | None = None
    usage: ProviderUsage | None = None

    def __post_init__(self) -> None:
        _check_uuid("attempt_id", self.attempt_id)
        _check_int("call_seq", self.call_seq, MAX_CALL_SEQ)
        _check_type("outcome", self.outcome, CallOutcome)
        if self.failure_code is not None:
            _check_type("failure_code", self.failure_code, TransientCode, PermanentCode)
        if (self.failure_code is not None) != (self.outcome is CallOutcome.FAILED):
            msg = "failure_code must be set exactly when the outcome is FAILED"
            raise ValueError(msg)
        if self.usage is not None:
            _check_type("usage", self.usage, ProviderUsage)


@runtime_checkable
class ProviderCallRecorder(Protocol):
    """Where provider call records go.

    ``record_intent`` returns only once the intent is recorded; if it raises, the
    caller does not make the call. Whatever either method raises propagates to the
    caller unchanged.
    """

    def record_intent(self, intent: ProviderCallIntent) -> None: ...

    def record_result(self, result: ProviderCallResult) -> None: ...


type CallEvent = ProviderCallIntent | ProviderCallResult


class InMemoryCallRecorder:
    """A ``ProviderCallRecorder`` that keeps every record in memory, in order. Not durable.

    It raises ``ValueError``, and records nothing, for a defect in what it is
    given: per attempt, ``call_seq`` must start at 0 and go up by 1; a result
    needs a recorded intent; and a call has at most one result. It takes no lock:
    each attempt has a single writer thread.
    """

    def __init__(self) -> None:
        self._events: list[CallEvent] = []
        self._next_seq: dict[uuid.UUID, int] = {}
        self._resolved: set[tuple[uuid.UUID, int]] = set()

    @property
    def events(self) -> tuple[CallEvent, ...]:
        """Every intent and result, in the order they were recorded."""
        return tuple(self._events)

    @property
    def intents(self) -> tuple[ProviderCallIntent, ...]:
        return tuple(e for e in self._events if isinstance(e, ProviderCallIntent))

    @property
    def results(self) -> tuple[ProviderCallResult, ...]:
        return tuple(e for e in self._events if isinstance(e, ProviderCallResult))

    def unresolved(self) -> tuple[ProviderCallIntent, ...]:
        """The intents that have no result, in order."""
        return tuple(
            intent
            for intent in self.intents
            if (intent.attempt_id, intent.call_seq) not in self._resolved
        )

    def record_intent(self, intent: ProviderCallIntent) -> None:
        _check_type("intent", intent, ProviderCallIntent)
        expected = self._next_seq.get(intent.attempt_id, 0)
        if intent.call_seq != expected:
            msg = "call_seq must start at 0 and go up by 1 within an attempt"
            raise ValueError(msg)
        self._next_seq[intent.attempt_id] = expected + 1
        self._events.append(intent)

    def record_result(self, result: ProviderCallResult) -> None:
        _check_type("result", result, ProviderCallResult)
        key = (result.attempt_id, result.call_seq)
        if result.call_seq >= self._next_seq.get(result.attempt_id, 0):
            msg = "a result needs a recorded intent"
            raise ValueError(msg)
        if key in self._resolved:
            msg = "a call has at most one result"
            raise ValueError(msg)
        self._resolved.add(key)
        self._events.append(result)
