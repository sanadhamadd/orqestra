"""Cost-aware route selection: a pure function, no I/O, no state.

A route is a way to reach a provider. The caller describes each one with a
``RouteOption`` whose ``estimated_cost_units`` is an opaque, non-negative
integer chosen by the caller: there is no currency, price table or vendor
assumption here, and an estimate is an input, not accounting.

``choose_route`` returns the eligible route with the lowest
``(estimated_cost_units, route_id)``, with ``route_id`` in code-point order, so
the choice does not depend on the order of the options. A route is eligible
when it declares the required capability, has an estimate, and the estimate
does not exceed ``max_cost_units`` (equal is allowed). A missing estimate is
never treated as zero. No eligible route is a result, not an error.
"""

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

_ROUTE_ID = re.compile(r"[A-Za-z0-9._-]{1,64}", re.ASCII)


class ExclusionReason(StrEnum):
    """Why a route was not eligible. Values are stable identifiers."""

    MISSING_CAPABILITY = "missing_capability"
    ESTIMATE_MISSING = "estimate_missing"
    OVER_BUDGET = "over_budget"


def check_units(name: str, value: object) -> int:
    """Return ``value`` if it is an ``int`` (not a ``bool``) that is not negative."""
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{name} must be an integer, got {type(value).__name__}"
        raise TypeError(msg)
    if value < 0:
        msg = f"{name} must not be negative, got {value}"
        raise ValueError(msg)
    return value


@dataclass(frozen=True, slots=True)
class RouteOption:
    """One candidate route. ``estimated_cost_units`` is ``None`` when unknown."""

    route_id: str
    capabilities: frozenset[str]
    estimated_cost_units: int | None

    def __post_init__(self) -> None:
        _check_route_id(self.route_id)
        _check_capabilities(self.capabilities)
        if self.estimated_cost_units is not None:
            check_units("estimated_cost_units", self.estimated_cost_units)


def _check_route_id(route_id: object) -> None:
    if not isinstance(route_id, str):
        msg = f"route_id must be a str, got {type(route_id).__name__}"
        raise TypeError(msg)
    if _ROUTE_ID.fullmatch(route_id) is None:
        msg = "route_id must be 1 to 64 characters of letters, digits, '.', '_' or '-'"
        raise ValueError(msg)


def _check_capabilities(capabilities: object) -> None:
    if not isinstance(capabilities, frozenset):
        msg = f"capabilities must be a frozenset, got {type(capabilities).__name__}"
        raise TypeError(msg)
    for capability in capabilities:
        _check_capability(capability)


def _check_capability(capability: object) -> None:
    if not isinstance(capability, str) or not capability.strip():
        msg = "a capability must be a non-blank str"
        raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    """The chosen route, or ``None``, and the excluded routes with why, by ``route_id``."""

    chosen: RouteOption | None
    rejected: tuple[tuple[str, ExclusionReason], ...]


def check_routes(options: object) -> tuple[RouteOption, ...]:
    """The routes as a tuple, checked: each a ``RouteOption``, with unique ``route_id``s."""
    if isinstance(options, str | bytes) or not isinstance(options, Iterable):
        msg = f"routes must be an iterable of RouteOption, got {type(options).__name__}"
        raise TypeError(msg)
    routes = tuple(options)
    seen: set[str] = set()
    for route in routes:
        if not isinstance(route, RouteOption):
            msg = f"each route must be a RouteOption, got {type(route).__name__}"
            raise TypeError(msg)
        if route.route_id in seen:
            msg = "route ids must be unique"
            raise ValueError(msg)
        seen.add(route.route_id)
    return routes


def choose_route(
    options: Iterable[RouteOption],
    *,
    capability: str,
    max_cost_units: int | None = None,
) -> RoutingDecision:
    """Choose the cheapest eligible route; ties go to the lowest ``route_id``."""
    routes = check_routes(options)
    _check_capability(capability)
    limit = None if max_cost_units is None else check_units("max_cost_units", max_cost_units)
    eligible: list[tuple[int, str, RouteOption]] = []
    rejected: list[tuple[str, ExclusionReason]] = []
    for route in routes:
        estimate = route.estimated_cost_units
        if capability not in route.capabilities:
            rejected.append((route.route_id, ExclusionReason.MISSING_CAPABILITY))
        elif estimate is None:
            rejected.append((route.route_id, ExclusionReason.ESTIMATE_MISSING))
        elif limit is not None and estimate > limit:
            rejected.append((route.route_id, ExclusionReason.OVER_BUDGET))
        else:
            eligible.append((estimate, route.route_id, route))
    chosen = min(eligible, key=lambda entry: (entry[0], entry[1]))[2] if eligible else None
    return RoutingDecision(chosen, tuple(sorted(rejected)))
