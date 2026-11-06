"""The worker's durable provider usage sink: intent first, at capture tier T3 (DEC-059).

A ``UsageSink`` is the durable ``ProviderCallRecorder`` of DEC-057. A handler
records through ``sink.recorder``; the worker's supervisor, which alone uses the
database (DEC-045), writes each record to the usage ledger (DEC-058) and answers:

- ``record_intent`` converts the record, adding the provider, model and prompt
  version from the sink's wiring, posts it and waits. It returns only once the
  intent row has committed, so the call is made only then. It raises
  ``ValueError``, before anything is posted, for an invalid record: a route the
  wiring does not map, an attempt that is not the one this worker runs, a
  ``call_seq`` that does not start at 0 and go up by 1, or a value the ledger
  cannot store. A second post while one is waiting is a ``RuntimeError``. Once
  cancellation is known, it raises ``HandlerCancelled`` instead of posting, and
  an intent still waiting when the supervisor learns of a cancellation is
  withdrawn the same way: no row and no call.
- ``record_result`` posts the result and waits until it has committed. A result
  is never withdrawn and never raises for a ledger failure. An amount the ledger
  cannot store (``1E+20`` or more, or more than 18 decimal places) is not written,
  clamped or zeroed: the call stays unresolved, ``usage_result_unrepresentable``
  is logged with ids and ``call_seq`` only, and the handler goes on. A result for
  a call whose intent was not acknowledged, or that already has one, is refused
  with ``ValueError``, as ``InMemoryCallRecorder`` refuses it.

A ledger failure is never the handler's: the supervisor retries, replays after a
lost answer, or ends the process (``runtime.py``). While the handler waits, the
ownership fence stays armed before the attempt's deadline (the lease is capped
there), so in practice the fence ends the process first: exit 3, no call is
made, and nothing is recorded for the attempt. Exit 8
(``usage_intent_unrecorded``, ``usage_result_unrecorded``) is the supervisor's
backstop behind the fence. While heartbeats succeed or are not due, its worst
case is the deadline D plus four database bounds: a retry due at D may still wait
for a step in progress and a heartbeat, then lose a write's answer and fail its
immediate replay. Heartbeats that keep failing are retried within one bound,
which holds usage writes back as it holds sweeps, so exit 8 may then never come.
If the supervisor never answers, the handler's wait ends at D plus five bounds
(``usage_handoff_bound`` plus two more) with ``UsageHandoffExpiredError``, a
``BaseException`` that is never classified (exit 5): strictly later than the
four-bound worst case, and the end when failing heartbeats hold writes back and
the fence has not ended the process first.

**The T3 contract.** Every handler registered for one of the sink's kinds must
record every provider call through ``sink.recorder``. The worker closes such an
attempt as ``complete(n)`` when every intent it confirmed has a confirmed result,
and recovery closes it at T3 too: a call made without its intent would make both
claim ``complete`` for fewer calls than were made. Nothing enforces this yet;
checking it when a handler is registered is a required future step. Every worker
of a fleet must use the same kind-to-tier map.

``usage_handoff_bound`` and ``check_provider_call_budget`` are the explicit limits
a wiring checks, for the case with no failures. Retries and replays are bounded by
the attempt's deadline and by the ownership fence, not by these limits. Nothing
here does I/O.
"""

import logging
import threading
import uuid
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType

from orqestra.calls.recording import MAX_CALL_SEQ, ProviderCallIntent, ProviderCallResult
from orqestra.domain.kinds import check_job_kinds
from orqestra.domain.leases import check_duration
from orqestra.persistence.provider_usage import (
    AMOUNT_LIMIT,
    AMOUNT_MAX_SCALE,
    CallFailure,
    CallIntent,
    CallOutcome,
    CallResult,
    CaptureTier,
)
from orqestra.worker.handlers import HandlerCancelled
from orqestra.worker.settings import WorkerSettings

logger = logging.getLogger("orqestra.worker")

_PROBE_ATTEMPT = uuid.UUID(int=0)  # only to check the wiring against the ledger's rules
_PROBE_CAPABILITY = "wiring_check"
_POLL = 0.1  # seconds: how often a waiting handler re-reads the clock for its backstop
_MICROSECOND = timedelta(microseconds=1)


class UsageHandoffExpiredError(BaseException):
    """The supervisor did not answer a usage record before the handler's backstop.

    A ``BaseException`` and not an ``Exception``: it is never classified, so
    nothing is recorded for the attempt (exit 5) and no call is made.
    """


class Reply(StrEnum):
    """The supervisor's answer to a posted record."""

    PENDING = "pending"
    ACKED = "acked"  # the row has committed
    CANCELLED = "cancelled"  # an intent withdrawn after a cancellation: no call


@dataclass(slots=True, eq=False)
class UsageRequest:
    """One posted record. Only the supervisor changes ``due`` and ``uncertain``."""

    record: CallIntent | CallResult
    due: float  # on the worker's clock: when the supervisor should write it
    reply: Reply = Reply.PENDING
    uncertain: bool = False  # an earlier write of it may have committed (a lost answer)

    @property
    def is_intent(self) -> bool:
        return isinstance(self.record, CallIntent)

    @property
    def call_seq(self) -> int:
        return self.record.call_seq


@dataclass(slots=True)
class _Binding:
    """The attempt the sink serves, from its handler's start until it is settled."""

    attempt_id: uuid.UUID
    cancel: threading.Event
    backstop: float  # on the worker's clock
    context: Mapping[str, str]
    next_seq: int = 0
    open: set[int] = field(default_factory=set)  # acknowledged intents awaiting a result
    request: UsageRequest | None = None


def usage_handoff_bound(settings: WorkerSettings) -> timedelta:
    """How long one record may wait for the supervisor: three database bounds.

    One for a supervisor step already in progress, one for a heartbeat that comes
    first, and one for the write itself. This is the limit with no failures: a
    retry or a replay is bounded by the attempt's deadline and by the ownership
    fence, not by this bound.
    """
    return 3 * settings.db_operation_timeout


def check_provider_call_budget(
    settings: WorkerSettings,
    *,
    max_provider_calls_per_attempt: int,
    provider_call_timeout: timedelta,
) -> None:
    """Raise unless an attempt's calls, each with its two handoffs, fit in its execution time.

    The rule is ``cap * (provider_call_timeout + 2 * usage_handoff_bound) <=
    execution_timeout - fence_margin``, compared exactly. The cap must be an
    ``int`` (not a ``bool``) from 0 to ``MAX_CALL_SEQ``, and the timeout a positive
    ``timedelta``: ``TypeError`` or ``ValueError`` otherwise. The budget covers the
    case with no failures; retries and replays are bounded by the attempt's
    deadline and by the ownership fence, not by this budget.
    """
    cap = max_provider_calls_per_attempt
    if isinstance(cap, bool) or not isinstance(cap, int):
        msg = f"max_provider_calls_per_attempt must be an integer, got {type(cap).__name__}"
        raise TypeError(msg)
    if not 0 <= cap <= MAX_CALL_SEQ:
        msg = f"max_provider_calls_per_attempt must be between 0 and {MAX_CALL_SEQ}"
        raise ValueError(msg)
    check_duration("provider_call_timeout", provider_call_timeout)
    per_call = provider_call_timeout + 2 * usage_handoff_bound(settings)
    available = settings.execution_timeout - settings.fence_margin
    # Whole microseconds, as integers: exact, and no timedelta overflow for a large cap.
    if cap * (per_call // _MICROSECOND) > available // _MICROSECOND:
        msg = (
            "max_provider_calls_per_attempt * (provider_call_timeout + 2 * the usage handoff "
            "bound) must not exceed execution_timeout - fence_margin"
        )
        raise ValueError(msg)


def representable_amount(amount: Decimal) -> bool:
    """Whether the ledger can store ``amount``: below ``1E+20``, at most 18 decimal places."""
    exponent = amount.as_tuple().exponent
    if not isinstance(exponent, int) or amount.is_signed():
        return False
    return amount < AMOUNT_LIMIT and max(0, -exponent) <= AMOUNT_MAX_SCALE


def ledger_result(result: ProviderCallResult) -> CallResult | None:
    """``result`` as the ledger stores it; ``None`` if its amount cannot be stored.

    Codes map by value. The provider's amount text becomes a ``Decimal``
    exactly, with its scale, and its unit is kept verbatim. Nothing is rounded.
    """
    amount: Decimal | None = None
    unit: str | None = None
    if result.usage is not None:
        amount = Decimal(result.usage.amount)  # exact: plain decimal text, no exponent
        if not representable_amount(amount):
            return None
        unit = result.usage.unit
    return CallResult(
        attempt_id=result.attempt_id,
        call_seq=result.call_seq,
        outcome=CallOutcome(result.outcome.value),
        failure_code=(
            None if result.failure_code is None else CallFailure(result.failure_code.value)
        ),
        reported_amount=amount,
        reported_unit=unit,
    )


def _checked_routes(name: str, value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        msg = f"{name} must map route ids to str, got {type(value).__name__}"
        raise TypeError(msg)
    routes: dict[str, str] = {}
    for route, target in value.items():
        if not isinstance(route, str) or not isinstance(target, str):
            msg = f"{name} must map str route ids to str values"
            raise TypeError(msg)
        routes[route] = target
    return routes


class SinkRecorder:
    """The ``ProviderCallRecorder`` a handler is given; it only forwards to its sink."""

    __slots__ = ("_sink",)

    def __init__(self, sink: "UsageSink") -> None:
        self._sink = sink

    def record_intent(self, intent: ProviderCallIntent) -> None:
        self._sink._record_intent(intent)

    def record_result(self, result: ProviderCallResult) -> None:
        self._sink._record_result(result)


class UsageSink:
    """A durable, intent-first recorder for the attempts of ``kinds``; see the module docstring.

    ``provider_for_route`` maps each route id to its provider name, and
    ``model_for_route`` optionally to a model; ``prompt_version`` is recorded with
    every intent. They are checked now against the ledger's identifier and text
    rules (``TypeError`` or ``ValueError``). Construction does no I/O. A sink is
    bound to one ``Worker`` (``Worker(usage=sink)``) and serves the attempt it
    runs.
    """

    def __init__(
        self,
        kinds: Collection[str],
        provider_for_route: Mapping[str, str],
        model_for_route: Mapping[str, str] | None = None,
        prompt_version: str | None = None,
    ) -> None:
        self.kinds: tuple[str, ...] = tuple(check_job_kinds(kinds))
        providers = _checked_routes("provider_for_route", provider_for_route)
        models = (
            {} if model_for_route is None else _checked_routes("model_for_route", model_for_route)
        )
        if not providers:
            msg = "provider_for_route must map at least one route"
            raise ValueError(msg)
        if not set(models) <= set(providers):
            msg = "model_for_route names a route that provider_for_route does not map"
            raise ValueError(msg)
        for route, provider in providers.items():
            # The ledger's own checks, so a record built later cannot fail on the wiring.
            CallIntent(
                attempt_id=_PROBE_ATTEMPT,
                call_seq=0,
                provider=provider,
                route_id=route,
                capability=_PROBE_CAPABILITY,
                prompt_version=prompt_version,
                model=models.get(route),
            )
        self._providers: Mapping[str, str] = MappingProxyType(providers)
        self._models: Mapping[str, str] = MappingProxyType(models)
        self._prompt_version = prompt_version
        self._condition = threading.Condition()
        self._clock: Callable[[], float] | None = None
        self._wake: Callable[[], None] | None = None
        self._binding: _Binding | None = None
        self.recorder = SinkRecorder(self)

    @property
    def tiers(self) -> Mapping[str, CaptureTier]:
        """The kind-to-tier map recovery uses for this sink's kinds: T3 for each."""
        return MappingProxyType(dict.fromkeys(self.kinds, CaptureTier.T3))

    def ledger_intent(self, intent: ProviderCallIntent) -> CallIntent:
        """``intent`` as the ledger stores it, with this sink's wiring. Pure.

        ``ValueError`` for a route the wiring does not map, or a value the
        ledger refuses; no message quotes a value.
        """
        provider = self._providers.get(intent.route_id)
        if provider is None:
            msg = "the intent's route has no provider in the usage sink's wiring"
            raise ValueError(msg)
        return CallIntent(
            attempt_id=intent.attempt_id,
            call_seq=intent.call_seq,
            provider=provider,
            route_id=intent.route_id,
            capability=intent.capability,
            prompt_version=self._prompt_version,
            model=self._models.get(intent.route_id),
            field_kind=intent.field_kind,
            item_index=intent.item_index,
            estimated_cost_units=intent.estimated_cost_units,
        )

    # --- the worker's side (supervisor thread) ---------------------------------------

    def bind(self, *, clock: Callable[[], float], wake: Callable[[], None]) -> None:
        """Bind the sink to one worker's clock and wake pipe; a sink is bound once."""
        with self._condition:
            if self._clock is not None:
                msg = "the usage sink is already bound to a worker"
                raise RuntimeError(msg)
            self._clock, self._wake = clock, wake

    def begin(
        self,
        attempt_id: uuid.UUID,
        *,
        cancel: threading.Event,
        backstop: float,
        context: Mapping[str, str],
    ) -> None:
        """Serve ``attempt_id`` until ``end``; ``backstop`` bounds each handler wait."""
        with self._condition:
            if self._binding is not None:
                msg = "the usage sink already serves an attempt"
                raise RuntimeError(msg)
            self._binding = _Binding(attempt_id, cancel, backstop, dict(context))

    def end(self) -> None:
        """Stop serving the attempt; any later record of it is refused."""
        with self._condition:
            self._binding = None
            self._condition.notify_all()

    def pending(self) -> UsageRequest | None:
        """The record waiting for the supervisor, if any."""
        with self._condition:
            binding = self._binding
            if binding is None or binding.request is None:
                return None
            return binding.request

    def answer(self, request: UsageRequest, reply: Reply) -> None:
        """Answer ``request``: ``ACKED`` once its row committed, or ``CANCELLED`` (intents only)."""
        with self._condition:
            binding = self._binding
            if binding is None or binding.request is not request:
                msg = "the usage request is not the one waiting"
                raise RuntimeError(msg)
            if reply is Reply.CANCELLED and not request.is_intent:
                msg = "a result is never withdrawn"
                raise RuntimeError(msg)
            if reply is Reply.ACKED and request.is_intent:
                binding.next_seq += 1
                binding.open.add(request.call_seq)
            request.reply = reply
            binding.request = None
            self._condition.notify_all()

    def withdraw(self) -> UsageRequest | None:
        """Withdraw an intent that is still waiting (``CANCELLED``); return it, if there was one.

        Called right after the cancellation event is set. A result is never withdrawn.
        """
        with self._condition:  # re-entrant: answer takes it again
            binding = self._binding
            request = None if binding is None else binding.request
            if request is None or not request.is_intent:
                return None
            self.answer(request, Reply.CANCELLED)
            return request

    # --- the handler's side --------------------------------------------------------------

    def _record_intent(self, intent: object) -> None:  # checked: a caller may be untyped
        if not isinstance(intent, ProviderCallIntent):
            msg = f"intent must be a ProviderCallIntent, got {type(intent).__name__}"
            raise TypeError(msg)
        record = self.ledger_intent(intent)
        with self._condition:
            binding = self._bound_to(intent.attempt_id)
            if intent.call_seq != binding.next_seq:
                msg = "call_seq must start at 0 and go up by 1 within an attempt"
                raise ValueError(msg)
            if binding.request is not None:
                msg = "another usage record of this attempt is still waiting"
                raise RuntimeError(msg)
            if binding.cancel.is_set():
                raise HandlerCancelled
            request = self._post(binding, record)
        if self._wait(binding, request) is Reply.CANCELLED:
            raise HandlerCancelled

    def _record_result(self, result: object) -> None:  # checked: a caller may be untyped
        if not isinstance(result, ProviderCallResult):
            msg = f"result must be a ProviderCallResult, got {type(result).__name__}"
            raise TypeError(msg)
        with self._condition:
            binding = self._bound_to(result.attempt_id)
            if result.call_seq not in binding.open:
                msg = "a result needs an acknowledged intent that has no result yet"
                raise ValueError(msg)
            if binding.request is not None:
                msg = "another usage record of this attempt is still waiting"
                raise RuntimeError(msg)
            binding.open.discard(result.call_seq)
            record = ledger_result(result)
            if record is None:
                logger.warning(
                    "usage_result_unrepresentable: the reported amount cannot be stored; "
                    "the call stays unresolved",
                    extra={
                        **binding.context,
                        "call_seq": str(result.call_seq),
                        "event": "usage_result_unrepresentable",
                    },
                )
                return
            request = self._post(binding, record)
        self._wait(binding, request)  # a result is only ever acknowledged

    def _bound_to(self, attempt_id: uuid.UUID) -> _Binding:
        binding = self._binding
        if binding is None or binding.attempt_id != attempt_id:
            msg = "the record's attempt is not the one this worker runs"
            raise ValueError(msg)
        return binding

    def _post(self, binding: _Binding, record: CallIntent | CallResult) -> UsageRequest:
        clock, wake = self._clock, self._wake
        if clock is None or wake is None:  # pragma: no cover - a binding needs a bound worker
            msg = "the usage sink is not bound to a worker"
            raise RuntimeError(msg)
        request = UsageRequest(record, due=clock())
        binding.request = request
        wake()  # one byte to the wake pipe, no lock: safe under this condition
        return request

    def _wait(self, binding: _Binding, request: UsageRequest) -> Reply:
        clock = self._clock
        if clock is None:  # pragma: no cover - _post checked it
            msg = "the usage sink is not bound to a worker"
            raise RuntimeError(msg)
        with self._condition:
            while request.reply is Reply.PENDING:
                left = binding.backstop - clock()
                if left <= 0:
                    msg = "the supervisor did not answer a usage record before the backstop"
                    raise UsageHandoffExpiredError(msg)
                self._condition.wait(min(left, _POLL))
            return request.reply
