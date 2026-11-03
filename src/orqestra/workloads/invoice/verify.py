"""Verify one invoice: deterministic checks first, a provider only for ambiguous amounts.

The decision order is fixed:

1. Normalize. A value that does not fit the schema is ``malformed``; the
   provider is not called.
2. Run the checks. Any ``FAIL`` is ``rejected``; the provider is not called. All
   ``PASS`` is ``verified``.
3. Otherwise some amount is an ambiguous raw string. Choose one route for the
   invoice (``choose_route``). With none eligible the status is ``needs_review``
   (``no_eligible_route``) and the provider is not called.
4. Call the chosen route's provider once per ambiguous amount, in document order,
   asking only to interpret that string. A reply must be a canonical digit string
   (``[0-9]{1,64}``) and a valid ``reported_cost_units``; the first invalid reply
   ends the invoice as ``needs_review`` (``provider_response_invalid``).
5. With every reply valid, substitute the values and run the same checks again.
   Only those checks decide the status, and the result is marked
   ``provider_assisted``. A provider never states a verdict.

A provider could still return a plausible number that makes the arithmetic hold;
``provider_assisted`` marks every such result.

Provider failures are not item results; they end the call by raising:

- ``ProviderTransientError`` and ``ProviderPermanentError`` are raised again as
  fresh errors with the same code (the original text is never kept);
- any other exception, or a return that is not a ``ProviderResponse``, becomes
  ``PermanentError("provider_protocol_violation")``, fixed text.

This module logs nothing and never puts a raw string, identifier or provider text
into an outcome. ``VerificationOutcome.routing`` and ``reported_cost_units`` stay
in memory; the handler decides what is stored.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import cast

from orqestra.ai.provider import (
    Provider,
    ProviderPermanentError,
    ProviderRequest,
    ProviderResponse,
    ProviderTransientError,
    make_request,
)
from orqestra.ai.routing import RouteOption, check_routes, check_units, choose_route
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


class VerificationCancelled(Exception):  # noqa: N818 - it reports a stop, not an error
    """``cancelled()`` was true before a provider call.

    It is raised so this module need not import the worker; the handler turns it
    into ``HandlerCancelled``.
    """


@dataclass(frozen=True, slots=True)
class RoutingAudit:
    """Which route was chosen and, in memory only, what was estimated and called.

    Only ``route_id`` is ever stored. ``estimated_cost_units`` and
    ``provider_calls`` (every ``interpret`` invocation, including repeats a mock
    answers from its cache) are for a future usage ledger.
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
    max_cost_units: int | None = None,
    cancelled: Callable[[], bool] = _never,
) -> VerificationOutcome:
    """Verify one invoice value; see the module docstring for the order of decisions."""
    check_wiring(routes, providers, timeout)
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
    provider = providers[route.route_id]
    values: dict[str, int] = {}
    calls = 0
    reported: int | None = None
    for field in ambiguous_fields(invoice):
        if cancelled():
            raise VerificationCancelled
        request = make_request(
            capability=REQUIRED_CAPABILITY,
            route_id=route.route_id,
            field_kind=field.kind,
            raw=field.raw,
            timeout=timeout,
        )
        calls += 1
        response = _ask(provider, request)
        value = _valid_value(response)
        if value is None:
            audit = RoutingAudit(route.route_id, estimate, calls)
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
    audit = RoutingAudit(route.route_id, estimate, calls)
    return _decided_with_provider(substitute(invoice, values), audit, reported)


def _decided_with_provider(
    invoice: Invoice, audit: RoutingAudit, reported: int | None
) -> VerificationOutcome:
    findings = run_checks(invoice)
    failed = any(finding.verdict is Verdict.FAIL for finding in findings)
    status = Status.REJECTED if failed else Status.VERIFIED
    return VerificationOutcome(status, True, findings, (), None, audit, reported)


def _ask(provider: Provider, request: ProviderRequest) -> ProviderResponse:
    """The provider's reply, or the fixed-text error that replaces whatever it raised."""
    failure: Exception
    try:
        reply = _reply_of(provider, request)
    except ProviderTransientError as exc:
        failure = ProviderTransientError(exc.code)
    except ProviderPermanentError as exc:
        failure = ProviderPermanentError(exc.code)
    except Exception:  # whatever it was, its text is not ours to store
        failure = PermanentError(PROTOCOL_VIOLATION)
    else:
        if isinstance(reply, ProviderResponse):
            return reply
        failure = PermanentError(PROTOCOL_VIOLATION)
    raise failure from None  # raised outside the handlers above: nothing is chained


def _reply_of(provider: Provider, request: ProviderRequest) -> object:
    """What the provider returned, typed as unchecked: a broken provider may return anything."""
    return provider.interpret(request)


def _valid_value(response: ProviderResponse) -> int | None:
    """The integer the reply denotes, or ``None`` if the reply is not acceptable."""
    cost: object = response.reported_cost_units
    if cost is not None and (isinstance(cost, bool) or not isinstance(cost, int) or cost < 0):
        return None
    return parse_canonical(response.value)
