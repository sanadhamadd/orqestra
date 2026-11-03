"""Deterministic invoice rules. Pure, no I/O, no provider.

Each rule gives one finding per subject, in a fixed order: ``quantity_positive``
for each line in order, then ``total_matches_lines``. A verdict is:

- ``PASS`` or ``FAIL`` when every amount the rule involves is known;
- ``INDETERMINATE`` when an involved amount is still an ambiguous ``Raw`` string
  and the rule cannot decide without it.

The rules check only the structural and arithmetic consistency of this synthetic
schema: a line's quantity is not zero, and the total is the sum of
``quantity x unit_amount`` over the lines. No tax, discount or rounding is implied.
"""

from dataclasses import dataclass
from enum import StrEnum

from orqestra.workloads.invoice.model import Invoice, Known


class Verdict(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    INDETERMINATE = "indeterminate"


class Rule(StrEnum):
    QUANTITY_POSITIVE = "quantity_positive"
    TOTAL_MATCHES_LINES = "total_matches_lines"


@dataclass(frozen=True, slots=True)
class Finding:
    """The verdict of one rule on one subject, located by a positional path."""

    rule: Rule
    path: str
    verdict: Verdict


def run_checks(invoice: Invoice) -> tuple[Finding, ...]:
    """Every finding for ``invoice``, in a fixed order."""
    findings: list[Finding] = []
    for index, line in enumerate(invoice.lines):
        path = f"lines[{index}].quantity"
        if not isinstance(line.quantity, Known):
            verdict = Verdict.INDETERMINATE
        else:
            verdict = Verdict.FAIL if line.quantity.value == 0 else Verdict.PASS
        findings.append(Finding(Rule.QUANTITY_POSITIVE, path, verdict))
    findings.append(Finding(Rule.TOTAL_MATCHES_LINES, "total", _total_verdict(invoice)))
    return tuple(findings)


def _total_verdict(invoice: Invoice) -> Verdict:
    expected = 0
    for line in invoice.lines:
        if not isinstance(line.quantity, Known) or not isinstance(line.unit_amount, Known):
            return Verdict.INDETERMINATE
        expected += line.quantity.value * line.unit_amount.value
    if not isinstance(invoice.total, Known):
        return Verdict.INDETERMINATE
    return Verdict.PASS if invoice.total.value == expected else Verdict.FAIL
