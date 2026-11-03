"""The ``invoice_verification`` batch handler, built by a factory (not registered anywhere).

It follows the engine's contracts and adds none:

- the payload is the partitioner's batch, ``{"offset": int, "items": [invoice, ...]}``
  (DEC-052); any other shape is ``PermanentError("batch_payload_invalid")``;
- the result is ``{"offset": n, "results": [item result, ...]}``, one result per
  item in order, built only from JSON types;
- cancellation is checked before each item and before each provider call; an
  in-flight call is not interrupted. ``VerificationCancelled`` becomes
  ``HandlerCancelled``;
- a provider error fails the whole batch task with the error's closed code, and a
  retry runs the whole batch again (DEC-009, DEC-053).

An item result is::

    {"index": offset + i, "status": ..., "provider_assisted": bool,
     "findings": [{"rule", "verdict", "path"}], "issues": [{"code", "path"}],
     "reason": str | null, "routing": {"route_id": str} | null}

``routing`` records only which route the routing decision chose, for the attempt
that completed; it is not usage accounting. Estimated costs, call counts and
provider-reported usage are never stored, and neither are raw values,
identifiers or provider text.

An item that is ``rejected``, ``malformed`` or ``needs_review`` is a successful
item result: the task still succeeds, so a job whose items were all rejected
still derives as ``succeeded`` (DEC-031). Read the item results to aggregate.
"""

from collections.abc import Mapping, Sequence
from datetime import timedelta
from types import MappingProxyType

from orqestra.ai.provider import Provider
from orqestra.ai.routing import RouteOption, check_routes, check_units
from orqestra.domain.errors import PermanentError
from orqestra.domain.jobs import JsonValue
from orqestra.worker.handlers import Handler, HandlerCancelled, TaskContext
from orqestra.workloads.invoice.verify import (
    VerificationCancelled,
    VerificationOutcome,
    check_wiring,
    verify_invoice,
)

JOB_KIND = "invoice_verification"
BATCH_PAYLOAD_INVALID = "batch_payload_invalid"


def invoice_verification_handler(
    providers: Mapping[str, Provider],
    routes: Sequence[RouteOption],
    timeout: timedelta,
    max_cost_units: int | None = None,
) -> Handler:
    """The handler for ``JOB_KIND``.

    Every route needs a provider, ``timeout`` must be a positive duration and
    ``max_cost_units``, if given, a non-negative integer; otherwise this raises
    now, not when a task runs. The routes and providers are copied.
    """
    checked_routes = check_routes(routes)
    checked_providers: Mapping[str, Provider] = MappingProxyType(dict(providers))
    check_wiring(checked_routes, checked_providers, timeout)
    if max_cost_units is not None:
        check_units("max_cost_units", max_cost_units)

    def handle(context: TaskContext, payload: JsonValue) -> JsonValue:
        offset, items = _batch(payload)
        results: list[JsonValue] = []
        for position, item in enumerate(items):
            if context.cancellation.is_set():
                raise HandlerCancelled
            try:
                outcome = verify_invoice(
                    item,
                    routes=checked_routes,
                    providers=checked_providers,
                    timeout=timeout,
                    max_cost_units=max_cost_units,
                    cancelled=context.cancellation.is_set,
                )
            except VerificationCancelled:
                raise HandlerCancelled from None
            results.append(_item_result(offset + position, outcome))
        return {"offset": offset, "results": results}

    return handle


def _batch(payload: JsonValue) -> tuple[int, list[JsonValue]]:
    """The offset and items of a batch payload; raise ``PermanentError`` for any other shape."""
    if not isinstance(payload, dict) or set(payload) != {"offset", "items"}:
        raise PermanentError(BATCH_PAYLOAD_INVALID)
    offset = payload["offset"]
    items = payload["items"]
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise PermanentError(BATCH_PAYLOAD_INVALID)
    if not isinstance(items, list):
        raise PermanentError(BATCH_PAYLOAD_INVALID)
    return offset, items


def _item_result(index: int, outcome: VerificationOutcome) -> JsonValue:
    findings: list[JsonValue] = []
    for finding in outcome.findings:
        findings.append(
            {"rule": finding.rule.value, "verdict": finding.verdict.value, "path": finding.path}
        )
    issues: list[JsonValue] = []
    for issue in outcome.issues:
        issues.append({"code": issue.code.value, "path": issue.path})
    routing: JsonValue = None
    if outcome.routing is not None:
        routing = {"route_id": outcome.routing.route_id}
    return {
        "index": index,
        "status": outcome.status.value,
        "provider_assisted": outcome.provider_assisted,
        "findings": findings,
        "issues": issues,
        "reason": None if outcome.reason is None else outcome.reason.value,
        "routing": routing,
    }
