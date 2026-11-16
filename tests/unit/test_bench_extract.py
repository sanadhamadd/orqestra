"""Strict reply parsing, judging and scoring of the extraction workload (``benchmarks.extract``)."""

import json
from typing import Any

import pytest

from benchmarks.documents import Kind, generate_documents
from benchmarks.extract import (
    KEYS,
    SCORED_FIELDS,
    TOTAL_RULE,
    Reading,
    Reason,
    Score,
    Unusable,
    canonical_reply,
    judge,
    parse_reply,
    score,
    score_all,
)
from orqestra.workloads.invoice.verify import Status

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

DOCUMENTS = generate_documents(400)
CLEAN = next(d for d in DOCUMENTS if d.kind is Kind.CLEAN)


def reply(**changes: Any) -> str:
    """The clean document's perfect reply with ``changes`` applied (JSON values)."""
    value = json.loads(canonical_reply(CLEAN.expected))
    value.update(changes)
    return json.dumps(value)


def line(**changes: Any) -> dict[str, Any]:
    value = {"description": "Compact LED panels", "quantity": 2, "unit_price": "10.00"}
    value.update(changes)
    return value


def reason_of(content: str | None) -> Reason | None:
    return judge(parse_reply(content)).reason


# --- a perfect reader -----------------------------------------------------------------------


def test_a_perfect_reply_reaches_the_oracle_status_and_scores_one() -> None:
    total = Score()
    for document in DOCUMENTS:
        read = parse_reply(canonical_reply(document.expected))
        assert isinstance(read, Reading)
        assert judge(read).status is document.expected_status
        total.add(score(document.expected, read.as_json()))
    assert total.overall() == 1.0
    assert total.exact_documents == total.documents == len(DOCUMENTS)


def test_one_surrounding_code_fence_is_the_only_wrapper_allowed() -> None:
    plain = parse_reply(reply())
    assert parse_reply(f"```json\n{reply()}\n```") == plain
    assert parse_reply(f"```\n{reply()}\n```") == plain
    assert parse_reply(f"\n  {reply()}  \n") == plain
    assert reason_of(f"Here is the JSON:\n{reply()}") is Reason.REPLY_NOT_JSON
    assert reason_of(f"{reply()}\nHope this helps.") is Reason.REPLY_NOT_JSON
    assert reason_of(f"```json\n{reply()}\n```\nDone.") is Reason.REPLY_NOT_JSON
    assert reason_of(f"```json\n```json\n{reply()}\n```\n```") is Reason.REPLY_NOT_JSON


# --- needs review, never a guess ------------------------------------------------------------


def test_no_reply_is_incomplete() -> None:
    assert reason_of(None) is Reason.REPLY_INCOMPLETE


@pytest.mark.parametrize(
    "content",
    [
        "",
        "not json",
        '{"total": "1.00"',
        reply()[:-1],
        '{"vendor_name": NaN}',
        reply().replace('"vendor_name"', '"total": "1.00", "vendor_name"', 1),  # duplicate key
    ],
)
def test_what_is_not_json_needs_review(content: str) -> None:
    assert reason_of(content) is Reason.REPLY_NOT_JSON


@pytest.mark.parametrize(
    "content",
    [
        "[]",
        '"text"',
        "12",
        json.dumps({key: None for key in KEYS - {"tax"}}),
        json.dumps({**{key: None for key in KEYS}, "notes": None}),
        reply(subtotal=1234.5),
        reply(subtotal=1234),
        reply(subtotal="1,234.50"),
        reply(subtotal="1234.5"),
        reply(subtotal="1234"),
        reply(subtotal="-1.00"),
        reply(subtotal="01.00"),
        reply(subtotal="1e3"),
        reply(subtotal=""),
        reply(issue_date="16/11/2026"),
        reply(issue_date="2026-02-30"),
        reply(issue_date="2026-1-5"),
        reply(due_date=20261116),
        reply(currency="usd"),
        reply(currency="US$"),
        reply(currency="EURO"),
        reply(vendor_name=""),
        reply(vendor_name="   "),
        reply(vendor_name="x" * 201),
        reply(invoice_number=42),
        reply(line_items=[]),
        reply(line_items="none"),
        reply(line_items=[line()] * 51),
        reply(line_items=[line(extra=1)]),
        reply(line_items=[{"description": "x", "quantity": 1}]),
        reply(line_items=[line(quantity=True)]),
        reply(line_items=[line(quantity=1.0)]),
        reply(line_items=[line(quantity="2")]),
        reply(line_items=[line(quantity=-1)]),
        reply(line_items=[line(quantity=10**6 + 1)]),
        reply(line_items=[line(unit_price=10)]),
        reply(line_items=["Compact LED panels"]),
    ],
)
def test_what_has_the_wrong_shape_needs_review(content: str) -> None:
    assert reason_of(content) is Reason.REPLY_SCHEMA_INVALID


@pytest.mark.parametrize(
    "content",
    [
        reply(invoice_number=None),
        reply(due_date=None),
        reply(vendor_name=None),
        reply(total=None),
        reply(line_items=None),
        reply(line_items=[line(quantity=None)]),
        reply(line_items=[line(), line(unit_price=None)]),
    ],
)
def test_a_null_field_needs_review(content: str) -> None:
    judgement = judge(parse_reply(content))
    assert judgement.status is Status.NEEDS_REVIEW
    assert judgement.reason is Reason.FIELD_MISSING


def test_every_needs_review_reason_is_a_closed_code() -> None:
    assert {reason.value for reason in Reason} == {
        "reply_incomplete",
        "reply_not_json",
        "reply_schema_invalid",
        "field_missing",
        "provider_call_cap_reached",
    }


# --- the rules ------------------------------------------------------------------------------


def amounts(lines: list[dict[str, Any]], subtotal: str, tax: str, total: str) -> str:
    return reply(line_items=lines, subtotal=subtotal, tax=tax, total=total)


def test_consistent_amounts_are_verified() -> None:
    content = amounts([line(), line(quantity=1, unit_price="5.50")], "25.50", "2.55", "28.05")
    assert judge(parse_reply(content)).status is Status.VERIFIED


def test_a_zero_quantity_is_rejected_by_the_existing_rule() -> None:
    judgement = judge(parse_reply(amounts([line(quantity=0)], "0.00", "0.00", "0.00")))
    assert judgement.status is Status.REJECTED
    assert judgement.failed_rules == ("quantity_positive",)


def test_lines_that_do_not_sum_to_the_subtotal_are_rejected() -> None:
    judgement = judge(parse_reply(amounts([line()], "21.00", "0.00", "21.00")))
    assert judgement.status is Status.REJECTED
    assert judgement.failed_rules == ("total_matches_lines",)


def test_a_total_that_is_not_subtotal_plus_tax_is_rejected() -> None:
    judgement = judge(parse_reply(amounts([line()], "20.00", "2.00", "22.01")))
    assert judgement.status is Status.REJECTED
    assert judgement.failed_rules == (TOTAL_RULE,)


def test_amounts_are_read_as_minor_units() -> None:
    read = parse_reply(amounts([line(unit_price="0.05")], "0.10", "0.00", "0.10"))
    assert isinstance(read, Reading)
    assert read.line_items is not None
    assert read.line_items[0].unit_price == 5
    assert (read.subtotal, read.total) == (10, 10)


# --- scoring --------------------------------------------------------------------------------


def stored(content: str) -> dict[str, Any] | None:
    read = parse_reply(content)
    return read.as_json() if isinstance(read, Reading) else None


def test_nothing_read_scores_nothing() -> None:
    result = score(CLEAN.expected, None)
    assert result.overall() == 0.0
    assert result.exact_documents == 0
    assert sum(result.expected.values()) == 8 + 3 * len(CLEAN.expected.lines)


def test_text_fields_ignore_case_and_spacing() -> None:
    name = CLEAN.expected.vendor_name
    result = score(
        CLEAN.expected, stored(reply(vendor_name="  " + name.upper().replace(" ", "  ")))
    )
    assert result.overall() == 1.0


def test_one_wrong_amount_costs_one_field() -> None:
    wrong = f"{CLEAN.expected.tax // 100 + 1}.{CLEAN.expected.tax % 100:02d}"
    result = score(CLEAN.expected, stored(reply(tax=wrong)))
    assert result.per_field()["tax"] == 0.0
    assert result.matched["total"] == 1
    assert result.exact_documents == 0
    expected = sum(result.expected.values())
    assert result.overall() == pytest.approx((expected - 1) / expected)


def test_a_null_for_a_field_the_document_leaves_out_is_correct() -> None:
    missing = next(d for d in DOCUMENTS if d.kind is Kind.MISSING_FIELD)
    result = score(missing.expected, stored(canonical_reply(missing.expected)))
    assert result.overall() == 1.0
    assert judge(parse_reply(canonical_reply(missing.expected))).reason is Reason.FIELD_MISSING


def test_extra_or_missing_lines_do_not_count_as_matches() -> None:
    value = json.loads(canonical_reply(CLEAN.expected))
    extra = value["line_items"] + [line()]
    with_extra = score(CLEAN.expected, stored(reply(line_items=extra)))
    assert with_extra.overall() == 1.0
    assert with_extra.exact_documents == 0
    fewer = score(CLEAN.expected, stored(reply(line_items=value["line_items"][:1])))
    if len(CLEAN.expected.lines) > 1:
        assert fewer.per_field()["line_items.description"] == pytest.approx(
            1 / len(CLEAN.expected.lines)
        )


def test_scores_add_up_over_documents() -> None:
    expected = [d.expected for d in DOCUMENTS[:10]]
    readings = [stored(canonical_reply(f)) for f in expected[:5]] + [None] * 5
    total = score_all(expected, readings)
    assert total.documents == 10
    assert total.exact_documents == 5
    assert set(total.per_field()) == set(SCORED_FIELDS)
    with pytest.raises(ValueError, match="one reading per document"):
        score_all(expected, readings[:9])


def test_an_unusable_reply_is_its_reason() -> None:
    assert parse_reply(None) == Unusable(Reason.REPLY_INCOMPLETE)
