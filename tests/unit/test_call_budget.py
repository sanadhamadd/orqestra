"""The call budget (DEC-065): the bound, its argument checks and every verdict.

Reports are built through the public ``UsageReportBuilder``, from hand-made
attempt and task rows (the usage report tests' helpers), so each one has the
finality the real report rules give it.
"""

import dataclasses

import pytest

from orqestra.aggregation.budget import (
    BudgetVerdict,
    CallBudgetCheck,
    call_bound,
    check_call_budget,
)
from orqestra.aggregation.usage import AttemptCapture, JobUsageReport, UsageFinality
from orqestra.calls.recording import MAX_CALL_SEQ
from orqestra.domain.states import AttemptState, TaskState
from tests.unit.test_usage_report import attempt, build, closed, task

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


def closed_report(calls: int) -> JobUsageReport:
    """A finished job whose one attempt recorded ``calls`` calls and closed them all."""
    report = build([closed(answered=calls)], [task()])
    assert (report.finality, report.calls) == (UsageFinality.CLOSED, calls)
    return report


def open_report(calls: int) -> JobUsageReport:
    """A job whose one attempt is still running with ``calls`` calls recorded."""
    report = build(
        [attempt(state=AttemptState.RUNNING, answered=calls)], [task(state=TaskState.RUNNING)]
    )
    assert (report.finality, report.calls) == (UsageFinality.OPEN, calls)
    return report


def lower_bound_report(calls: int) -> JobUsageReport:
    """A finished job whose revoked attempt left an unresolved call and no closure."""
    report = build(
        [attempt(state=AttemptState.REVOKED, answered=calls - 1, unresolved=1)],
        [task(state=TaskState.FAILED)],
    )
    assert (report.finality, report.calls) == (UsageFinality.LOWER_BOUND, calls)
    return report


# --- the bound -------------------------------------------------------------------


def test_the_bound_is_the_cap_times_the_retry_limits() -> None:
    assert call_bound(7, 3) == 21
    assert call_bound(0, 3) == 0
    assert call_bound(7, 0) == 0
    assert call_bound(2, MAX_CALL_SEQ) == 2 * MAX_CALL_SEQ


@pytest.mark.parametrize(
    ("max_attempts_total", "cap", "bad", "error"),
    [
        pytest.param(1, True, "cap", TypeError, id="bool-cap"),
        pytest.param(1, 1.5, "cap", TypeError, id="float-cap"),
        pytest.param(1, -7, "cap", ValueError, id="negative-cap"),
        pytest.param(1, MAX_CALL_SEQ + 1, "cap", ValueError, id="cap-above-max-call-seq"),
        pytest.param(True, 1, "max_attempts_total", TypeError, id="bool-total"),
        pytest.param("9", 1, "max_attempts_total", TypeError, id="text-total"),
        pytest.param(-7, 1, "max_attempts_total", ValueError, id="negative-total"),
    ],
)
def test_invalid_arguments_are_refused_without_their_values(
    max_attempts_total: object, cap: object, bad: str, error: type[Exception]
) -> None:
    with pytest.raises(error, match=bad) as raised:
        call_bound(max_attempts_total, cap)  # type: ignore[arg-type]
    value = cap if bad == "cap" else max_attempts_total
    assert str(value) not in str(raised.value)


def test_the_check_refuses_invalid_arguments_too() -> None:
    with pytest.raises(ValueError, match="cap"):
        check_call_budget(closed_report(1), max_attempts_total=1, cap=-1)
    with pytest.raises(TypeError, match="max_attempts_total"):
        check_call_budget(closed_report(1), max_attempts_total=False, cap=1)


# --- verdicts --------------------------------------------------------------------


def test_a_closed_report_at_the_bound_is_within() -> None:
    check = check_call_budget(closed_report(6), max_attempts_total=3, cap=2)
    assert check == CallBudgetCheck(
        cap=2,
        max_attempts_total=3,
        bound=6,
        calls=6,
        finality=UsageFinality.CLOSED,
        verdict=BudgetVerdict.WITHIN,
        unknown_reason=None,
    )
    assert check.cap_source == "operator"


def test_a_closed_report_one_call_over_the_bound_is_exceeded() -> None:
    check = check_call_budget(closed_report(7), max_attempts_total=3, cap=2)
    assert (check.bound, check.calls, check.verdict) == (6, 7, BudgetVerdict.EXCEEDED)
    assert check.unknown_reason is None


def test_a_closed_report_with_no_call_is_within_even_at_cap_zero() -> None:
    check = check_call_budget(closed_report(0), max_attempts_total=1, cap=0)
    assert (check.bound, check.verdict) == (0, BudgetVerdict.WITHIN)


def test_a_lower_bound_over_the_bound_is_exceeded() -> None:
    # Recorded calls only grow, so more than the bound already proves the overrun.
    check = check_call_budget(lower_bound_report(5), max_attempts_total=2, cap=2)
    assert (check.finality, check.verdict) == (UsageFinality.LOWER_BOUND, BudgetVerdict.EXCEEDED)
    assert check.unknown_reason is None


def test_an_open_report_over_the_bound_is_exceeded() -> None:
    check = check_call_budget(open_report(5), max_attempts_total=2, cap=2)
    assert (check.finality, check.verdict) == (UsageFinality.OPEN, BudgetVerdict.EXCEEDED)


@pytest.mark.parametrize("calls", [3, 4], ids=["under", "at"])
def test_a_lower_bound_within_the_bound_is_unknown(calls: int) -> None:
    check = check_call_budget(lower_bound_report(calls), max_attempts_total=2, cap=2)
    assert (check.verdict, check.unknown_reason) == (
        BudgetVerdict.UNKNOWN,
        UsageFinality.LOWER_BOUND,
    )


@pytest.mark.parametrize("calls", [0, 4], ids=["none-yet", "at"])
def test_an_open_report_within_the_bound_is_unknown(calls: int) -> None:
    check = check_call_budget(open_report(calls), max_attempts_total=2, cap=2)
    assert (check.verdict, check.unknown_reason) == (BudgetVerdict.UNKNOWN, UsageFinality.OPEN)


def test_attempts_that_captured_nothing_are_unknown_never_within() -> None:
    # A finished attempt with no call and no closure is NOT_CAPTURED: absent capture, not zero.
    report = build([attempt(state=AttemptState.SUCCEEDED)], [task()])
    assert dict(report.attempts_by_capture)[AttemptCapture.NOT_CAPTURED] == 1
    assert (report.finality, report.calls) == (UsageFinality.LOWER_BOUND, 0)

    check = check_call_budget(report, max_attempts_total=1, cap=4)
    assert (check.verdict, check.unknown_reason) == (
        BudgetVerdict.UNKNOWN,
        UsageFinality.LOWER_BOUND,
    )


def test_within_needs_closed_whatever_the_counts() -> None:
    for finality in UsageFinality:
        report = dataclasses.replace(closed_report(1), finality=finality)
        verdict = check_call_budget(report, max_attempts_total=1, cap=1).verdict
        assert (verdict is BudgetVerdict.WITHIN) == (finality is UsageFinality.CLOSED)


def test_reported_amounts_and_exposure_never_enter_the_check() -> None:
    report = dataclasses.replace(closed_report(2), estimated_exposure_units=10**6)
    check = check_call_budget(report, max_attempts_total=1, cap=2)
    assert (check.calls, check.bound, check.verdict) == (2, 2, BudgetVerdict.WITHIN)
    assert {field.name for field in dataclasses.fields(check)} == {
        "cap",
        "max_attempts_total",
        "bound",
        "calls",
        "finality",
        "verdict",
        "unknown_reason",
        "cap_source",
    }
