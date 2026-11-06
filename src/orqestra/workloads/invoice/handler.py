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
  retry runs the whole batch again (DEC-009, DEC-053);
- each ``handle()`` is one attempt: one ``ProviderCalls`` for ``context.attempt_id``
  numbers every provider call of the batch, across items and routes, and holds the
  attempt to ``max_provider_calls_per_attempt``. An invoice that would exceed the
  remaining cap is ``needs_review`` (``provider_call_cap_reached``) without a call;
- with a ``recorder``, every call is recorded (DEC-057). A billable provider
  requires one. Whatever the recorder raises fails the task like any handler error.
  The worker's durable sink (``orqestra.worker.usage``, DEC-059) raises only for an
  invalid record (``ValueError``) or a cancellation (``HandlerCancelled``): a ledger
  failure ends the worker process and never fails the task.

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

from orqestra.ai.mock import MockProvider
from orqestra.ai.provider import Provider
from orqestra.ai.recording import ProviderCallRecorder
from orqestra.ai.routing import RouteOption, check_routes, check_units
from orqestra.domain.errors import PermanentError
from orqestra.domain.jobs import JsonValue
from orqestra.worker.handlers import Handler, HandlerCancelled, TaskContext
from orqestra.workloads.invoice.verify import (
    ProviderCalls,
    VerificationCancelled,
    VerificationOutcome,
    check_call_cap,
    check_recorder,
    check_wiring,
    verify_invoice,
)

JOB_KIND = "invoice_verification"
BATCH_PAYLOAD_INVALID = "batch_payload_invalid"


def billable_routes(
    providers: Mapping[str, Provider], non_billable_routes: object
) -> frozenset[str]:
    """The provider keys treated as billable: every key except those explicitly marked.

    Billability is deny-by-default: a provider is billable unless the wiring
    marks its key in ``non_billable_routes``, and only the offline
    ``MockProvider`` itself may be marked (``type(p) is MockProvider``, so a
    subclass does not count). Nothing is read from the provider objects, so a
    provider cannot declare itself free.

    ``non_billable_routes`` must be a ``frozenset`` of ``str``, each naming a key
    of ``providers``; otherwise this raises ``TypeError`` or ``ValueError``.
    """
    if not isinstance(non_billable_routes, frozenset):
        msg = f"non_billable_routes must be a frozenset, got {type(non_billable_routes).__name__}"
        raise TypeError(msg)
    for key in non_billable_routes:
        if not isinstance(key, str):
            msg = f"non_billable_routes entries must be str, got {type(key).__name__}"
            raise TypeError(msg)
    for key in sorted(non_billable_routes):
        if key not in providers:
            msg = f"non-billable route {key!r} has no provider"
            raise ValueError(msg)
        if type(providers[key]) is not MockProvider:
            msg = f"non-billable route {key!r} is not the offline mock provider"
            raise ValueError(msg)
    return frozenset(key for key in providers if key not in non_billable_routes)


def invoice_verification_handler(
    providers: Mapping[str, Provider],
    routes: Sequence[RouteOption],
    timeout: timedelta,
    max_cost_units: int | None = None,
    *,
    max_provider_calls_per_attempt: int,
    recorder: ProviderCallRecorder | None = None,
    non_billable_routes: frozenset[str] = frozenset(),
) -> Handler:
    """The handler for ``JOB_KIND``.

    Every route needs a provider, ``timeout`` must be a positive duration,
    ``max_cost_units``, if given, a non-negative integer, and
    ``non_billable_routes`` must pass ``billable_routes``.
    ``max_provider_calls_per_attempt`` is required: an ``int`` (not a ``bool``)
    from 0 to ``MAX_CALL_SEQ``, where 0 allows deterministic results only.
    ``recorder``, if given, must be a ``ProviderCallRecorder``, and it is required
    when any provider is billable. Otherwise this raises now, not when a task
    runs. The routes and providers are copied.

    Whether the cap, the per-call timeout and the recorder's write bounds fit in
    the worker's execution timeout is not checked here: that needs the worker's
    settings and is left to the worker's wiring (DEC-057), which can check it with
    ``orqestra.worker.usage.check_provider_call_budget`` (DEC-059).
    """
    checked_routes = check_routes(routes)
    checked_providers: Mapping[str, Provider] = MappingProxyType(dict(providers))
    check_wiring(checked_routes, checked_providers, timeout)
    if max_cost_units is not None:
        check_units("max_cost_units", max_cost_units)
    cap = check_call_cap("max_provider_calls_per_attempt", max_provider_calls_per_attempt)
    billable = billable_routes(checked_providers, non_billable_routes)
    recorder = check_recorder(recorder)
    if billable and recorder is None:
        msg = "a billable provider needs a recorder: pass recorder, or mark the offline mock"
        raise ValueError(msg)

    def handle(context: TaskContext, payload: JsonValue) -> JsonValue:
        offset, items = _batch(payload)
        calls = ProviderCalls(attempt_id=context.attempt_id, cap=cap, recorder=recorder)
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
                    calls=calls,
                    item_index=offset + position,
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
