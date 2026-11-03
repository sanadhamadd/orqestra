import itertools

import pytest

from orqestra.workloads.invoice.checks import Finding, Rule, Verdict, run_checks
from orqestra.workloads.invoice.model import MAX_AMOUNT, Amount, Invoice, Known, Line, Raw

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


def invoice(lines: list[tuple[Amount, Amount]], total: Amount) -> Invoice:
    return Invoice(
        "INV",
        tuple(Line(f"L{i}", quantity, unit) for i, (quantity, unit) in enumerate(lines)),
        total,
    )


def verdicts(inv: Invoice) -> dict[str, Verdict]:
    return {f"{f.rule}:{f.path}": f.verdict for f in run_checks(inv)}


@pytest.mark.parametrize(
    ("quantity", "expected"),
    [
        (Known(1), Verdict.PASS),
        (Known(2), Verdict.PASS),
        (Known(0), Verdict.FAIL),
        (Raw("two"), Verdict.INDETERMINATE),
    ],
)
def test_quantity_positive_truth_table(quantity: Amount, expected: Verdict) -> None:
    (finding, _) = run_checks(invoice([(quantity, Known(1))], Known(1)))
    assert finding == Finding(Rule.QUANTITY_POSITIVE, "lines[0].quantity", expected)


@pytest.mark.parametrize(
    ("lines", "total", "expected"),
    [
        ([(Known(2), Known(5)), (Known(1), Known(3))], Known(13), Verdict.PASS),
        ([(Known(2), Known(5)), (Known(1), Known(3))], Known(12), Verdict.FAIL),
        ([(Known(2), Known(5)), (Known(1), Known(3))], Known(14), Verdict.FAIL),
        ([(Known(0), Known(5))], Known(0), Verdict.PASS),
        ([(Known(2), Raw("five"))], Known(10), Verdict.INDETERMINATE),
        ([(Raw("two"), Known(5))], Known(10), Verdict.INDETERMINATE),
        ([(Known(2), Known(5))], Raw("ten"), Verdict.INDETERMINATE),
        ([(Known(1), Known(1)), (Known(1), Raw("x"))], Known(99), Verdict.INDETERMINATE),
    ],
)
def test_total_matches_lines_truth_table(
    lines: list[tuple[Amount, Amount]], total: Amount, expected: Verdict
) -> None:
    assert run_checks(invoice(lines, total))[-1] == Finding(
        Rule.TOTAL_MATCHES_LINES, "total", expected
    )


def test_a_total_one_unit_off_fails_in_either_direction() -> None:
    key = f"{Rule.TOTAL_MATCHES_LINES}:total"
    assert verdicts(invoice([(Known(3), Known(7))], Known(21)))[key] is Verdict.PASS
    assert verdicts(invoice([(Known(3), Known(7))], Known(22)))[key] is Verdict.FAIL
    assert verdicts(invoice([(Known(3), Known(7))], Known(20)))[key] is Verdict.FAIL


def test_huge_amounts_are_exact() -> None:
    big = MAX_AMOUNT
    exact = invoice([(Known(big), Known(big))], Known(big * big))
    assert run_checks(exact)[-1].verdict is Verdict.PASS
    off_by_one = invoice([(Known(big), Known(big))], Known(big * big - 1))
    assert run_checks(off_by_one)[-1].verdict is Verdict.FAIL


def test_findings_come_in_a_fixed_order_one_per_line_then_the_total() -> None:
    findings = run_checks(invoice([(Known(1), Known(1)), (Known(0), Known(1))], Known(1)))
    assert findings == (
        Finding(Rule.QUANTITY_POSITIVE, "lines[0].quantity", Verdict.PASS),
        Finding(Rule.QUANTITY_POSITIVE, "lines[1].quantity", Verdict.FAIL),
        Finding(Rule.TOTAL_MATCHES_LINES, "total", Verdict.PASS),
    )


def test_the_total_verdict_does_not_depend_on_line_order() -> None:
    lines: list[tuple[Amount, Amount]] = [
        (Known(2), Known(5)),
        (Known(1), Known(3)),
        (Known(4), Known(0)),
    ]
    for total, expected in [(Known(13), Verdict.PASS), (Known(12), Verdict.FAIL)]:
        results = {
            run_checks(invoice(list(order), total))[-1].verdict
            for order in itertools.permutations(lines)
        }
        assert results == {expected}


def test_checks_are_repeatable() -> None:
    inv = invoice([(Known(2), Raw("five"))], Known(10))
    assert run_checks(inv) == run_checks(inv)
