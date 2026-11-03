import dataclasses
import hashlib
import json
import re
from datetime import timedelta
from typing import Any

import pytest

from orqestra.ai.provider import (
    FieldKind,
    PermanentCode,
    ProviderPermanentError,
    ProviderRequest,
    ProviderResponse,
    ProviderTransientError,
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
