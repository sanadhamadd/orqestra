"""Verify one invoice: deterministic checks first, a provider only for ambiguous amounts.

The decision order is fixed:

1. Normalize. A value that does not fit the schema is ``malformed``; the
   provider is not called.
2. Run the checks. Any ``FAIL`` is ``rejected``; the provider is not called. All
   ``PASS`` is ``verified``.
3. Otherwise some amount is an ambiguous raw string. Choose one route for the
   invoice (``choose_route``). With none eligible the status is ``needs_review``
   (``no_eligible_route``) and the provider is not called.
4. If the invoice needs more calls than the attempt has left under its cap
   (``ProviderCalls.remaining``), the status is ``needs_review``
   (``provider_call_cap_reached``) and the provider is not called. The check is per
   invoice: a later invoice that needs fewer calls may still be called.
5. Call the chosen route's provider once per ambiguous amount, in document order,
   asking only to interpret that string. A reply must be a canonical digit string
   (``[0-9]{1,64}``) and a valid ``reported_cost_units``; the first invalid reply
   ends the invoice as ``needs_review`` (``provider_response_invalid``).
6. With every reply valid, substitute the values and run the same checks again.
   Only those checks decide the status, and the result is marked
   ``provider_assisted``. A provider never states a verdict.

A provider could still return a plausible number that makes the arithmetic hold;
``provider_assisted`` marks every such result.

Every call goes through the attempt's ``ProviderCalls``, which numbers it and, with
a recorder, records it: ``cancelled()``, then ``record_intent``, then the call, then
``record_result``, then the reply is judged. A ``ProviderResponse``, valid or not,
is recorded as ``answered`` with its ``usage``; a provider error as ``failed`` with
its code. Anything else leaves the call without a result (unresolved). Whatever a
recorder raises propagates unchanged; if ``record_intent`` raises, or the intent
itself is out of the recordable range, no call is made.

Provider failures are not item results; they end the call by raising:

- ``ProviderTransientError`` and ``ProviderPermanentError`` are raised again as
  fresh errors with the same code (the original text is never kept);
- any other exception, or a return that is not a ``ProviderResponse``, becomes
  ``PermanentError("provider_protocol_violation")``, fixed text.

This module logs nothing and never puts a raw string, identifier or provider text
into an outcome or a record. ``VerificationOutcome.routing`` and
``reported_cost_units`` stay in memory; the handler decides what is stored.
"""

import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import cast

from orqestra.calls.provider import (
    PermanentCode,
    Provider,
    ProviderPermanentError,
    ProviderRequest,
    ProviderResponse,
    ProviderTransientError,
    ProviderUsage,
    TransientCode,
    make_request,
)
from orqestra.calls.recording import (
    MAX_CALL_SEQ,
    CallOutcome,
    ProviderCallIntent,
    ProviderCallRecorder,
    ProviderCallResult,
)
from orqestra.calls.routing import RouteOption, check_routes, check_units, choose_route
from orqestra.domain.errors import PermanentError
from orqestra.domain.leases import check_duration
from orqestra.workloads.invoice.checks import Finding, Verdict, run_checks
from orqestra.workloads.invoice.model import (
    Invoice,
    Issue,
    Malformed,
    ambiguous_fields,
    normalize_invoice,
    parse_canonical,
    substitute,
)

REQUIRED_CAPABILITY = "interpret_amount"
PROTOCOL_VIOLATION = "provider_protocol_violation"


class Status(StrEnum):
    """The outcome for one invoice. Values are stable identifiers for storage."""

    VERIFIED = "verified"
    REJECTED = "rejected"
    NEEDS_REVIEW = "needs_review"
    MALFORMED = "malformed"


class Reason(StrEnum):
    """Why an invoice needs review."""

    NO_ELIGIBLE_ROUTE = "no_eligible_route"
    PROVIDER_RESPONSE_INVALID = "provider_response_invalid"
    PROVIDER_CALL_CAP_REACHED = "provider_call_cap_reached"


class VerificationCancelled(Exception):  # noqa: N818 - it reports a stop, not an error
    """``cancelled()`` was true before a provider call.

    It is raised so this module need not import the worker; the handler turns it
    into ``HandlerCancelled``.
    """


@dataclass(frozen=True, slots=True)
class RoutingAudit:
    """Which route was chosen and, in memory only, what was estimated and called.

    Only ``route_id`` is ever stored. ``provider_calls`` counts this invoice's
    ``interpret`` invocations, including repeats a mock answers from its cache;
    it is 0 when the cap stopped the invoice. Each invocation is also numbered and,
    with a recorder, recorded by ``ProviderCalls``.
    """

    route_id: str
    estimated_cost_units: int
    provider_calls: int


@dataclass(frozen=True, slots=True)
class VerificationOutcome:
    status: Status
    provider_assisted: bool
    findings: tuple[Finding, ...]
    issues: tuple[Issue, ...]
    reason: Reason | None
    routing: RoutingAudit | None
    # The sum of the provider's own usage reports, or None if it reported none. In memory
    # only: never stored, and never used to decide anything.
    reported_cost_units: int | None = None


def _never() -> bool:
    return False


def check_call_cap(name: str, value: object) -> int:
    """Return ``value`` if it is an ``int`` (not a ``bool``) from 0 to ``MAX_CALL_SEQ``."""
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{name} must be an integer, got {type(value).__name__}"
        raise TypeError(msg)
    if not 0 <= value <= MAX_CALL_SEQ:
        msg = f"{name} must be between 0 and {MAX_CALL_SEQ}"
        raise ValueError(msg)
    return value


def check_recorder(value: object) -> ProviderCallRecorder | None:
    """Return ``value`` if it is ``None`` or a ``ProviderCallRecorder``; else ``TypeError``."""
    if value is None or isinstance(value, ProviderCallRecorder):
        return value
    msg = f"recorder must be a ProviderCallRecorder, got {type(value).__name__}"
    raise TypeError(msg)


def _check_instance[T](name: str, value: object, kind: type[T]) -> T:
    if not isinstance(value, kind):
        msg = f"{name} must be a {kind.__name__}, got {type(value).__name__}"
        raise TypeError(msg)
    return value


class ProviderCalls:
    """One attempt's provider calls: the only source of its ``call_seq`` and its cap.

    ``cap`` is the most calls the attempt may make, 0 to ``MAX_CALL_SEQ``. With a
    ``recorder``, every call is recorded; with ``None``, calls are only numbered
    and counted. ``made`` counts the calls whose intent was recorded (or, without
    a recorder, that were made), so a call whose intent could not be recorded is
    not counted and was not made. It takes no lock: an attempt has a single writer.
    """

    def __init__(
        self, *, attempt_id: uuid.UUID, cap: int, recorder: ProviderCallRecorder | None
    ) -> None:
        self._attempt_id = _check_instance("attempt_id", attempt_id, uuid.UUID)
        self._cap = check_call_cap("cap", cap)
        self._recorder = check_recorder(recorder)
        self._made = 0

    @property
    def made(self) -> int:
        return self._made

    @property
    def remaining(self) -> int:
        return self._cap - self._made

    def call(
        self,
        provider: Provider,
        request: ProviderRequest,
        *,
        item_index: int,
        estimated_cost_units: int,
    ) -> ProviderResponse:
        """Record the intent, make the call, record how it ended; return the reply.

        Raises ``RuntimeError`` if the cap leaves no call. A provider error is
        raised again as a fresh error with the same code, and anything else the
        provider does wrong as ``PermanentError("provider_protocol_violation")``.
        Whatever the recorder raises propagates unchanged, and an intent outside
        the recordable range raises ``ValueError``; in both cases before the call.
        """
        if self.remaining <= 0:
            msg = "the attempt has no provider call left under its cap"
            raise RuntimeError(msg)
        call_seq = self._made
        if self._recorder is not None:
            self._recorder.record_intent(
                ProviderCallIntent(
                    attempt_id=self._attempt_id,
                    call_seq=call_seq,
                    item_index=item_index,
                    route_id=request.route_id,
                    capability=request.capability,
                    field_kind=request.field_kind,
                    estimated_cost_units=estimated_cost_units,
                )
            )
        self._made += 1
        reply = _ask(provider, request)  # a BaseException passes through: no result
        if isinstance(reply, ProviderResponse):
            self._record_result(call_seq, CallOutcome.ANSWERED, None, reply.usage)
            return reply
        if isinstance(reply, ProviderTransientError | ProviderPermanentError):
            self._record_result(call_seq, CallOutcome.FAILED, reply.code, None)
        raise reply from None

    def _record_result(
        self,
        call_seq: int,
        outcome: CallOutcome,
        failure_code: TransientCode | PermanentCode | None,
        usage: ProviderUsage | None,
    ) -> None:
        if self._recorder is None:
            return
        result = ProviderCallResult(
            attempt_id=self._attempt_id,
            call_seq=call_seq,
            outcome=outcome,
            failure_code=failure_code,
            usage=usage,
        )
        self._recorder.record_result(result)


def check_wiring(
    routes: Sequence[RouteOption], providers: Mapping[str, Provider], timeout: timedelta
) -> None:
    """Raise unless every route has a provider and ``timeout`` is a positive duration."""
    check_duration("timeout", timeout)
    for route in check_routes(routes):
        if route.route_id not in providers:
            msg = f"route {route.route_id!r} has no provider"
            raise ValueError(msg)


def verify_invoice(
    raw: object,
    *,
    routes: Sequence[RouteOption],
    providers: Mapping[str, Provider],
    timeout: timedelta,
    calls: ProviderCalls,
    item_index: int,
    max_cost_units: int | None = None,
    cancelled: Callable[[], bool] = _never,
) -> VerificationOutcome:
    """Verify one invoice value; see the module docstring for the order of decisions.

    ``calls`` is the attempt's ``ProviderCalls`` and ``item_index`` the invoice's
    absolute index, which every recorded intent carries.
    """
    check_wiring(routes, providers, timeout)
    _check_instance("calls", calls, ProviderCalls)
    check_units("item_index", item_index)
    if max_cost_units is not None:
        check_units("max_cost_units", max_cost_units)
    normalized = normalize_invoice(raw)
    if isinstance(normalized, Malformed):
        return VerificationOutcome(Status.MALFORMED, False, (), normalized.issues, None, None)
    invoice = normalized
    findings = run_checks(invoice)
    verdicts = {finding.verdict for finding in findings}
    if Verdict.FAIL in verdicts:
        return VerificationOutcome(Status.REJECTED, False, findings, (), None, None)
    if Verdict.INDETERMINATE not in verdicts:
        return VerificationOutcome(Status.VERIFIED, False, findings, (), None, None)

    decision = choose_route(routes, capability=REQUIRED_CAPABILITY, max_cost_units=max_cost_units)
    route = decision.chosen
    if route is None:
        return VerificationOutcome(
            Status.NEEDS_REVIEW, False, findings, (), Reason.NO_ELIGIBLE_ROUTE, None
        )
    estimate = cast(int, route.estimated_cost_units)  # an eligible route has an estimate
    fields = ambiguous_fields(invoice)
    if calls.remaining < len(fields):
        # Spending part of the cap would still leave the invoice undecided.
        audit = RoutingAudit(route.route_id, estimate, 0)
        return VerificationOutcome(
            Status.NEEDS_REVIEW, False, findings, (), Reason.PROVIDER_CALL_CAP_REACHED, audit
        )
    provider = providers[route.route_id]
    values: dict[str, int] = {}
    asked = 0
    reported: int | None = None
    for field in fields:
        if cancelled():
            raise VerificationCancelled
        request = make_request(
            capability=REQUIRED_CAPABILITY,
            route_id=route.route_id,
            field_kind=field.kind,
            raw=field.raw,
            timeout=timeout,
        )
        response = calls.call(
            provider, request, item_index=item_index, estimated_cost_units=estimate
        )
        asked += 1
        value = _valid_value(response)
        if value is None:
            audit = RoutingAudit(route.route_id, estimate, asked)
            return VerificationOutcome(
                Status.NEEDS_REVIEW,
                False,
                findings,
                (),
                Reason.PROVIDER_RESPONSE_INVALID,
                audit,
                reported,
            )
        values[field.path] = value
        if response.reported_cost_units is not None:
            reported = (reported or 0) + response.reported_cost_units
    audit = RoutingAudit(route.route_id, estimate, asked)
    return _decided_with_provider(substitute(invoice, values), audit, reported)


def _decided_with_provider(
    invoice: Invoice, audit: RoutingAudit, reported: int | None
) -> VerificationOutcome:
    findings = run_checks(invoice)
    failed = any(finding.verdict is Verdict.FAIL for finding in findings)
    status = Status.REJECTED if failed else Status.VERIFIED
    return VerificationOutcome(status, True, findings, (), None, audit, reported)


def _ask(provider: Provider, request: ProviderRequest) -> ProviderResponse | Exception:
    """The provider's reply, or the fixed-text error to raise in place of whatever it did.

    The error is returned, not raised, so the caller raises it outside the handlers
    below (nothing is chained) and records the call outside them too: a recorder's
    exception is never caught here.
    """
    try:
        reply = _reply_of(provider, request)
    except ProviderTransientError as exc:
        return ProviderTransientError(exc.code)
    except ProviderPermanentError as exc:
        return ProviderPermanentError(exc.code)
    except Exception:  # whatever it was, its text is not ours to store
        return PermanentError(PROTOCOL_VIOLATION)
    if isinstance(reply, ProviderResponse):
        return reply
    return PermanentError(PROTOCOL_VIOLATION)


def _reply_of(provider: Provider, request: ProviderRequest) -> object:
    """What the provider returned, typed as unchecked: a broken provider may return anything."""
    return provider.interpret(request)


def _valid_value(response: ProviderResponse) -> int | None:
    """The integer the reply denotes, or ``None`` if the reply is not acceptable."""
    cost: object = response.reported_cost_units
    if cost is not None and (isinstance(cost, bool) or not isinstance(cost, int) or cost < 0):
        return None
    return parse_canonical(response.value)
