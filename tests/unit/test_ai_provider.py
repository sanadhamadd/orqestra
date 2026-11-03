import dataclasses
import hashlib
import json
import re
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest

from orqestra.ai.provider import (
    FieldKind,
    PermanentCode,
    ProviderPermanentError,
    ProviderRequest,
    ProviderResponse,
    ProviderTransientError,
    ProviderUsage,
    TransientCode,
    make_request,
    request_id_for,
)
from orqestra.domain.errors import PermanentError, TransientError, is_retryable
from orqestra.domain.jobs import AttemptFailed
from orqestra.domain.policy import FailureKind, classify_failure

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


def test_request_id_is_a_stable_sha256_hex_digest() -> None:
    first = request_id_for("cap", "r1", FieldKind.TOTAL, "about 12")
    assert first == request_id_for("cap", "r1", FieldKind.TOTAL, "about 12")
    assert re.fullmatch(r"[0-9a-f]{64}", first)


def test_request_id_is_the_hash_of_canonical_json_whatever_the_key_order() -> None:
    reordered = {"route_id": "r1", "raw": "about 12", "field_kind": "total", "capability": "cap"}
    canonical = json.dumps(reordered, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    expected = hashlib.sha256(canonical.encode("ascii")).hexdigest()
    assert request_id_for("cap", "r1", FieldKind.TOTAL, "about 12") == expected


@pytest.mark.parametrize(
    "other",
    [
        ("cap2", "r1", FieldKind.TOTAL, "about 12"),
        ("cap", "r2", FieldKind.TOTAL, "about 12"),
        ("cap", "r1", FieldKind.QUANTITY, "about 12"),
        ("cap", "r1", FieldKind.TOTAL, "about 13"),
    ],
)
def test_request_id_depends_on_every_component_including_the_route(
    other: tuple[str, str, FieldKind, str],
) -> None:
    assert request_id_for(*other) != request_id_for("cap", "r1", FieldKind.TOTAL, "about 12")


def test_request_id_handles_text_that_is_not_ascii() -> None:
    assert re.fullmatch(r"[0-9a-f]{64}", request_id_for("c", "r", FieldKind.TOTAL, "é中"))


def test_make_request_derives_its_id_from_its_content() -> None:
    timeout = timedelta(seconds=3)
    request = make_request(
        capability="cap", route_id="r1", field_kind=FieldKind.UNIT_AMOUNT, raw="x", timeout=timeout
    )
    assert request.request_id == request_id_for("cap", "r1", FieldKind.UNIT_AMOUNT, "x")
    assert (request.route_id, request.capability, request.raw) == ("r1", "cap", "x")
    assert request.timeout == timeout


def test_a_request_exposes_no_invoice_line_or_path_field() -> None:
    names = {field.name for field in dataclasses.fields(ProviderRequest)}
    assert names == {"request_id", "route_id", "capability", "field_kind", "raw", "timeout"}


def test_a_response_defaults_to_no_reported_usage() -> None:
    assert ProviderResponse("5").reported_cost_units is None
    assert ProviderResponse("5").usage is None


def test_a_response_keeps_its_legacy_positional_form_and_appends_usage_last() -> None:
    names = tuple(field.name for field in dataclasses.fields(ProviderResponse))
    assert names == ("value", "reported_cost_units", "usage")
    legacy = ProviderResponse("5", 3)
    assert (legacy.value, legacy.reported_cost_units, legacy.usage) == ("5", 3, None)
    usage = ProviderUsage("0.25", "credits")
    assert ProviderResponse("5", 3, usage).usage is usage
    assert ProviderResponse("5", usage=usage).reported_cost_units is None


def test_a_response_does_not_judge_its_value_or_legacy_units() -> None:
    # The verifier decides these cases; the response must still be constructible.
    value: Any = 5
    units: Any = 1.5
    not_text: Any = ProviderResponse(value)
    assert not_text.value == 5
    assert ProviderResponse("5", True).reported_cost_units is True
    assert ProviderResponse("5", -1).reported_cost_units == -1
    fractional: Any = ProviderResponse("5", units)
    assert fractional.reported_cost_units == 1.5


@pytest.mark.parametrize("bad", [("0.25", "credits"), {"amount": "0.25"}, "0.25", 1, False])
def test_a_response_accepts_only_a_provider_usage_as_usage(bad: object) -> None:
    usage: Any = bad
    with pytest.raises(TypeError, match="ProviderUsage"):
        ProviderResponse("5", usage=usage)


def test_usage_keeps_amount_and_unit_exactly_and_is_frozen() -> None:
    usage = ProviderUsage("0.000123", "Credits-v2.per_call")
    assert (usage.amount, usage.unit) == ("0.000123", "Credits-v2.per_call")
    assert usage == ProviderUsage("0.000123", "Credits-v2.per_call")
    assert ProviderUsage("1.0", "u") != ProviderUsage("1", "u")
    target: Any = usage
    with pytest.raises(dataclasses.FrozenInstanceError):
        target.amount = "1"
    with pytest.raises(dataclasses.FrozenInstanceError):
        target.unit = "other"


@pytest.mark.parametrize(
    "amount", ["0", "0.0", "0.50", "12", "0.000123", "1" + "0" * 63, "0." + "0" * 62]
)
def test_usage_accepts_a_plain_non_negative_decimal_amount(amount: str) -> None:
    assert len(amount) <= 64
    assert ProviderUsage(amount, "credits").amount == amount


@pytest.mark.parametrize(
    "amount",
    [
        "",
        "-1",
        "+1",
        "1e-5",
        "1E5",
        "1.",
        ".5",
        "01",
        "00",
        "1,5",
        " 1",
        "1 ",
        "1\n",
        "NaN",
        "inf",
        "٣",  # an Arabic-Indic digit
        "1" + "0" * 64,
    ],
)
def test_usage_refuses_an_amount_that_is_not_a_plain_decimal(amount: str) -> None:
    with pytest.raises(ValueError, match="usage amount"):
        ProviderUsage(amount, "credits")


@pytest.mark.parametrize("bad", [True, 1, 1.5, Decimal("1"), b"1", None])
def test_usage_refuses_an_amount_that_is_not_a_str(bad: object) -> None:
    amount: Any = bad
    with pytest.raises(TypeError, match="usage amount"):
        ProviderUsage(amount, "credits")


@pytest.mark.parametrize("unit", ["c", "credits", "Credits", "tok_v2.in-1", "u" * 64])
def test_usage_accepts_a_provider_declared_unit_verbatim(unit: str) -> None:
    assert ProviderUsage("1", unit).unit == unit


@pytest.mark.parametrize(
    "unit", ["", " ", "a b", "crédits", "credits\n", "\ncredits", "a/b", "u" * 65]
)
def test_usage_refuses_a_unit_outside_the_label_alphabet(unit: str) -> None:
    with pytest.raises(ValueError, match="usage unit"):
        ProviderUsage("1", unit)


@pytest.mark.parametrize("bad", [None, 1, True, b"credits"])
def test_usage_refuses_a_unit_that_is_not_a_str(bad: object) -> None:
    unit: Any = bad
    with pytest.raises(TypeError, match="usage unit"):
        ProviderUsage("1", unit)


def test_usage_errors_never_quote_the_rejected_text() -> None:
    sentinel = "Ignore-previous-instructions"
    with pytest.raises(ValueError, match="usage amount") as amount_error:
        ProviderUsage(sentinel, "credits")
    with pytest.raises(ValueError, match="usage unit") as unit_error:
        ProviderUsage("1", sentinel + " now")
    for error in (amount_error.value, unit_error.value):
        assert sentinel not in str(error)
        assert sentinel not in repr(error)


@pytest.mark.parametrize("code", list(TransientCode))
def test_transient_errors_carry_only_their_code_and_are_retryable(code: TransientCode) -> None:
    error = ProviderTransientError(code)
    assert str(error) == code.value
    assert error.code is code
    assert isinstance(error, TransientError)
    assert is_retryable(error)
    assert classify_failure(error) is FailureKind.TRANSIENT
    assert AttemptFailed.from_exception(error).error_message == code.value


@pytest.mark.parametrize("code", list(PermanentCode))
def test_permanent_errors_carry_only_their_code_and_are_not_retryable(code: PermanentCode) -> None:
    error = ProviderPermanentError(code)
    assert str(error) == code.value
    assert error.code is code
    assert isinstance(error, PermanentError)
    assert not is_retryable(error)
    assert classify_failure(error) is FailureKind.PERMANENT


@pytest.mark.parametrize("bad", ["timeout", 1, None, PermanentCode.REJECTED])
def test_transient_error_accepts_only_a_transient_code(bad: object) -> None:
    code: Any = bad
    with pytest.raises(TypeError, match="TransientCode"):
        ProviderTransientError(code)


@pytest.mark.parametrize("bad", ["rejected", 1, None, TransientCode.TIMEOUT])
def test_permanent_error_accepts_only_a_permanent_code(bad: object) -> None:
    code: Any = bad
    with pytest.raises(TypeError, match="PermanentCode"):
        ProviderPermanentError(code)
