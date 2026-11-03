import itertools
from collections.abc import Iterator
from typing import Any

import pytest

from orqestra.ai.routing import (
    ExclusionReason,
    RouteOption,
    check_routes,
    check_units,
    choose_route,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

CAP = "interpret_amount"
CAPS = frozenset({CAP})


def option(route_id: str, cost: int | None, caps: frozenset[str] = CAPS) -> RouteOption:
    return RouteOption(route_id, caps, cost)


def chosen_id(options: list[RouteOption], max_cost_units: int | None = None) -> str | None:
    decision = choose_route(options, capability=CAP, max_cost_units=max_cost_units)
    return None if decision.chosen is None else decision.chosen.route_id


def test_the_cheapest_eligible_route_wins() -> None:
    assert chosen_id([option("a", 5), option("b", 2), option("c", 9)]) == "b"


def test_equal_costs_go_to_the_lowest_route_id_in_code_point_order() -> None:
    assert chosen_id([option("b", 2), option("a", 2), option("B", 2)]) == "B"


def test_a_zero_estimate_is_a_real_estimate() -> None:
    assert chosen_id([option("a", 1), option("z", 0)]) == "z"


def test_a_missing_estimate_is_excluded_and_never_treated_as_zero() -> None:
    decision = choose_route([option("a", None), option("b", 7)], capability=CAP)
    assert decision.chosen is not None
    assert decision.chosen.route_id == "b"
    assert decision.rejected == (("a", ExclusionReason.ESTIMATE_MISSING),)


def test_a_route_without_the_capability_is_excluded() -> None:
    other = frozenset({"something_else"})
    decision = choose_route([option("a", 0, other), option("b", 9)], capability=CAP)
    assert decision.chosen is not None
    assert decision.chosen.route_id == "b"
    assert decision.rejected == (("a", ExclusionReason.MISSING_CAPABILITY),)


def test_a_route_may_declare_more_capabilities_than_required() -> None:
    assert chosen_id([option("a", 1, frozenset({CAP, "other"}))]) == "a"


def test_the_budget_boundary_equal_is_eligible_and_one_above_is_not() -> None:
    assert chosen_id([option("a", 5)], max_cost_units=5) == "a"
    decision = choose_route([option("a", 6)], capability=CAP, max_cost_units=5)
    assert decision.chosen is None
    assert decision.rejected == (("a", ExclusionReason.OVER_BUDGET),)


def test_a_zero_budget_admits_only_free_routes() -> None:
    assert chosen_id([option("a", 1), option("b", 0)], max_cost_units=0) == "b"


def test_the_reason_for_an_exclusion_prefers_capability_then_estimate_then_budget() -> None:
    options = [
        option("a", None, frozenset()),
        option("b", None),
        option("c", 99),
    ]
    decision = choose_route(options, capability=CAP, max_cost_units=1)
    assert decision.rejected == (
        ("a", ExclusionReason.MISSING_CAPABILITY),
        ("b", ExclusionReason.ESTIMATE_MISSING),
        ("c", ExclusionReason.OVER_BUDGET),
    )


def test_no_options_and_no_eligible_route_are_results_not_errors() -> None:
    assert choose_route([], capability=CAP).chosen is None
    assert choose_route([option("a", None)], capability=CAP).chosen is None


def test_options_may_be_any_iterable() -> None:
    assert choose_route((option("a", 1) for _ in range(1)), capability=CAP).chosen is not None


@pytest.mark.parametrize("estimate", [True, False, 1.5, "1", b"1", [1]])
def test_an_estimate_that_is_not_an_integer_is_refused(estimate: object) -> None:
    value: Any = estimate
    with pytest.raises(TypeError, match="estimated_cost_units"):
        RouteOption("a", frozenset({CAP}), value)


def test_a_negative_estimate_is_refused() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        RouteOption("a", frozenset({CAP}), -1)


@pytest.mark.parametrize("route_id", ["", " ", "a b", "a@b", "x" * 65, "é", "a\n"])
def test_a_route_id_that_is_not_an_identifier_is_refused(route_id: str) -> None:
    with pytest.raises(ValueError, match="route_id"):
        RouteOption(route_id, frozenset({CAP}), 1)


def test_a_route_id_must_be_a_string() -> None:
    value: Any = 5
    with pytest.raises(TypeError, match="route_id"):
        RouteOption(value, frozenset({CAP}), 1)


def test_the_longest_route_id_is_accepted() -> None:
    assert RouteOption("x" * 64, frozenset({CAP}), 1).route_id == "x" * 64


def test_capabilities_must_be_a_frozenset_of_non_blank_strings() -> None:
    listed: Any = [CAP]
    with pytest.raises(TypeError, match="frozenset"):
        RouteOption("a", listed, 1)
    with pytest.raises(ValueError, match="capability"):
        RouteOption("a", frozenset({" "}), 1)
    mixed: Any = frozenset({1})
    with pytest.raises(ValueError, match="capability"):
        RouteOption("a", mixed, 1)


def test_duplicate_route_ids_are_refused() -> None:
    with pytest.raises(ValueError, match="unique"):
        choose_route([option("a", 1), option("a", 2)], capability=CAP)


def test_routes_must_be_route_options_in_an_iterable() -> None:
    wrong: Any = ["a"]
    with pytest.raises(TypeError, match="RouteOption"):
        check_routes(wrong)
    for not_routes in ("abc", b"abc", 5, None):
        with pytest.raises(TypeError, match="iterable"):
            check_routes(not_routes)


@pytest.mark.parametrize("capability", ["", "  "])
def test_the_required_capability_must_not_be_blank(capability: str) -> None:
    with pytest.raises(ValueError, match="capability"):
        choose_route([option("a", 1)], capability=capability)


@pytest.mark.parametrize("budget", [True, 1.5, "1"])
def test_a_budget_that_is_not_an_integer_is_refused(budget: object) -> None:
    value: Any = budget
    with pytest.raises(TypeError, match="max_cost_units"):
        choose_route([option("a", 1)], capability=CAP, max_cost_units=value)


def test_a_negative_budget_is_refused() -> None:
    with pytest.raises(ValueError, match="max_cost_units"):
        choose_route([option("a", 1)], capability=CAP, max_cost_units=-1)


def test_check_units_returns_a_valid_value() -> None:
    assert check_units("n", 0) == 0


def test_the_choice_does_not_depend_on_the_order_of_the_options() -> None:
    options = [option("b", 2), option("a", 2), option("c", 2), option("d", 1), option("e", None)]
    results = {
        chosen_id(list(order), max_cost_units=2) for order in itertools.permutations(options)
    }
    assert results == {"d"}
    ties = [option("b", 2), option("a", 2), option("c", 2)]
    assert {chosen_id(list(order)) for order in itertools.permutations(ties)} == {"a"}
    reasons = {
        choose_route(list(order), capability=CAP).rejected
        for order in itertools.permutations(options)
    }
    assert len(reasons) == 1


def _every_table() -> Iterator[list[RouteOption]]:
    capability_sets = [frozenset[str](), frozenset({CAP}), frozenset({CAP, "other"})]
    costs = [None, 0, 1, 2]
    shapes = list(itertools.product(capability_sets, costs))
    for first, second in itertools.product(shapes, repeat=2):
        yield [
            RouteOption("a", first[0], first[1]),
            RouteOption("b", second[0], second[1]),
        ]


def _restated(options: list[RouteOption], limit: int | None) -> str | None:
    """The rule stated again without the module's code: sort the eligible, take the first."""
    eligible = sorted(
        (cost, o.route_id)
        for o in options
        if (cost := o.estimated_cost_units) is not None
        and CAP in o.capabilities
        and (limit is None or cost <= limit)
    )
    return eligible[0][1] if eligible else None


def test_choose_route_agrees_with_an_independent_restatement_over_a_small_space() -> None:
    checked = 0
    for table in _every_table():
        for limit in (None, 0, 1, 2):
            assert chosen_id(table, limit) == _restated(table, limit)
            assert chosen_id(table[::-1], limit) == _restated(table, limit)
            checked += 1
    assert checked == 12 * 12 * 4
