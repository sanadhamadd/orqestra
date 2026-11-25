import dataclasses
import uuid
from decimal import Decimal
from typing import Any

import pytest

from orqestra.calls.provider import FieldKind, PermanentCode, ProviderUsage, TransientCode
from orqestra.calls.recording import (
    MAX_CALL_SEQ,
    MAX_COST_UNITS,
    CallOutcome,
    InMemoryCallRecorder,
    ProviderCallIntent,
    ProviderCallRecorder,
    ProviderCallResult,
)
from orqestra.persistence.provider_usage import BIGINT_MAX, INTEGER_MAX
from orqestra.persistence.provider_usage import CallOutcome as LedgerOutcome
from orqestra.persistence.provider_usage import CallResult as LedgerResult

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

A = uuid.UUID(int=1)
B = uuid.UUID(int=2)
SECRET = "SENTINEL-recorded-value-9b2e"


def intent(call_seq: int = 0, attempt_id: uuid.UUID = A, **fields: Any) -> ProviderCallIntent:
    values: dict[str, Any] = {
        "attempt_id": attempt_id,
        "call_seq": call_seq,
        "item_index": 0,
        "route_id": "r1",
        "capability": "interpret_amount",
        "field_kind": FieldKind.TOTAL,
        "estimated_cost_units": 3,
    }
    values.update(fields)
    return ProviderCallIntent(**values)


def result(call_seq: int = 0, attempt_id: uuid.UUID = A, **fields: Any) -> ProviderCallResult:
    values: dict[str, Any] = {
        "attempt_id": attempt_id,
        "call_seq": call_seq,
        "outcome": CallOutcome.ANSWERED,
    }
    values.update(fields)
    return ProviderCallResult(**values)


def test_the_outcomes_and_the_ranges_are_the_ledgers() -> None:
    assert [o.value for o in CallOutcome] == [o.value for o in LedgerOutcome]
    assert MAX_CALL_SEQ == INTEGER_MAX == 2**31 - 1
    assert MAX_COST_UNITS == BIGINT_MAX == 2**63 - 1


def test_records_are_frozen_keyword_only_and_carry_no_raw_text_or_reply() -> None:
    names = {f.name for f in dataclasses.fields(ProviderCallIntent)}
    names |= {f.name for f in dataclasses.fields(ProviderCallResult)}
    for forbidden in ("raw", "request_id", "value", "prompt", "reported_cost_units"):
        assert forbidden not in names
    with pytest.raises(dataclasses.FrozenInstanceError):
        intent().call_seq = 1  # type: ignore[misc]
    positional: Any = ProviderCallIntent
    with pytest.raises(TypeError):
        positional(A, 0, 0, "r1", "interpret_amount", FieldKind.TOTAL, 3)


def test_valid_records_are_accepted_at_their_bounds() -> None:
    upper = intent(MAX_CALL_SEQ, item_index=MAX_CALL_SEQ, estimated_cost_units=MAX_COST_UNITS)
    assert (upper.call_seq, upper.item_index) == (MAX_CALL_SEQ, MAX_CALL_SEQ)
    assert upper.estimated_cost_units == 2**63 - 1
    lower = intent(0, item_index=0, estimated_cost_units=0)
    assert (lower.call_seq, lower.item_index, lower.estimated_cost_units) == (0, 0, 0)
    usage = ProviderUsage("0.5", "credits")
    assert result(MAX_CALL_SEQ, usage=usage).usage == usage
    for code in (*TransientCode, *PermanentCode):
        failed = result(outcome=CallOutcome.FAILED, failure_code=code)
        assert failed.failure_code is code


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"attempt_id": str(A)}, TypeError),
        ({"call_seq": True}, TypeError),
        ({"call_seq": 1.0}, TypeError),
        ({"call_seq": "0"}, TypeError),
        ({"call_seq": -1}, ValueError),
        ({"call_seq": MAX_CALL_SEQ + 1}, ValueError),
        ({"item_index": False}, TypeError),
        ({"item_index": -1}, ValueError),
        ({"item_index": 2**31}, ValueError),
        ({"estimated_cost_units": None}, TypeError),
        ({"estimated_cost_units": -1}, ValueError),
        ({"estimated_cost_units": 2**63}, ValueError),
        ({"route_id": None}, TypeError),
        ({"capability": 5}, TypeError),
        ({"field_kind": "total"}, TypeError),
    ],
)
def test_a_bad_intent_is_refused(fields: dict[str, Any], error: type[Exception]) -> None:
    with pytest.raises(error):
        intent(**fields)


@pytest.mark.parametrize("amount", ["1" + "0" * 20, "0." + "1" * 19])
def test_a_result_keeps_usage_that_the_ledger_cannot_store(amount: str) -> None:
    """The documented gap: ``usage`` is bounded by ``ProviderUsage`` only, not by the ledger."""
    usage = ProviderUsage(amount, "credits")
    assert result(usage=usage).usage == usage
    with pytest.raises(ValueError, match="reported_amount"):
        LedgerResult(
            attempt_id=A,
            call_seq=0,
            outcome=LedgerOutcome.ANSWERED,
            reported_amount=Decimal(amount),
            reported_unit="credits",
        )


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"attempt_id": None}, TypeError),
        ({"call_seq": True}, TypeError),
        ({"call_seq": MAX_CALL_SEQ + 1}, ValueError),
        ({"outcome": "answered"}, TypeError),
        ({"outcome": CallOutcome.FAILED}, ValueError),
        ({"failure_code": TransientCode.TIMEOUT}, ValueError),
        ({"outcome": CallOutcome.FAILED, "failure_code": "timeout"}, TypeError),
        ({"usage": {"amount": "1", "unit": "u"}}, TypeError),
    ],
)
def test_a_bad_result_is_refused(fields: dict[str, Any], error: type[Exception]) -> None:
    with pytest.raises(error):
        result(**fields)


@pytest.mark.parametrize(
    "build",
    [
        lambda: intent(call_seq=SECRET),  # type: ignore[arg-type]
        lambda: intent(route_id=SECRET.encode()),
        lambda: intent(field_kind=SECRET),
        lambda: intent(item_index=2**31 + 987654321),
        lambda: intent(estimated_cost_units=2**63 + 987654321),
        lambda: result(outcome=SECRET),
        lambda: result(outcome=CallOutcome.FAILED, failure_code=SECRET),
        lambda: result(usage=SECRET),
    ],
)
def test_no_refusal_message_quotes_the_value(build: Any) -> None:
    with pytest.raises((TypeError, ValueError)) as raised:
        build()
    assert SECRET not in str(raised.value)
    assert "987654321" not in str(raised.value)


def test_the_in_memory_recorder_is_a_recorder() -> None:
    assert isinstance(InMemoryCallRecorder(), ProviderCallRecorder)
    assert not isinstance(object(), ProviderCallRecorder)


def test_the_in_memory_recorder_keeps_every_record_in_order() -> None:
    recorder = InMemoryCallRecorder()
    recorder.record_intent(intent(0))
    recorder.record_intent(intent(0, B))
    recorder.record_result(result(0))
    recorder.record_intent(intent(1))
    assert recorder.events == (intent(0), intent(0, B), result(0), intent(1))
    assert recorder.intents == (intent(0), intent(0, B), intent(1))
    assert recorder.results == (result(0),)
    assert recorder.unresolved() == (intent(0, B), intent(1))


@pytest.mark.parametrize("first", [1, 5])
def test_the_first_call_of_an_attempt_must_be_zero(first: int) -> None:
    recorder = InMemoryCallRecorder()
    with pytest.raises(ValueError, match="start at 0"):
        recorder.record_intent(intent(first))
    assert recorder.events == ()


@pytest.mark.parametrize("next_seq", [0, 2])
def test_call_seq_goes_up_by_one(next_seq: int) -> None:
    recorder = InMemoryCallRecorder()
    recorder.record_intent(intent(0))
    with pytest.raises(ValueError, match="go up by 1"):
        recorder.record_intent(intent(next_seq))
    assert recorder.events == (intent(0),)


def test_a_result_needs_a_recorded_intent() -> None:
    recorder = InMemoryCallRecorder()
    with pytest.raises(ValueError, match="needs a recorded intent"):
        recorder.record_result(result(0))
    recorder.record_intent(intent(0))
    with pytest.raises(ValueError, match="needs a recorded intent"):
        recorder.record_result(result(1))
    with pytest.raises(ValueError, match="needs a recorded intent"):
        recorder.record_result(result(0, B))
    assert recorder.events == (intent(0),)


def test_a_call_has_at_most_one_result() -> None:
    recorder = InMemoryCallRecorder()
    recorder.record_intent(intent(0))
    recorder.record_result(result(0))
    failed = result(0, outcome=CallOutcome.FAILED, failure_code=PermanentCode.REJECTED)
    with pytest.raises(ValueError, match="at most one result"):
        recorder.record_result(failed)
    assert recorder.results == (result(0),)


def test_the_in_memory_recorder_refuses_what_is_not_a_record() -> None:
    recorder = InMemoryCallRecorder()
    wrong: Any = result(0)
    with pytest.raises(TypeError, match="intent"):
        recorder.record_intent(wrong)
    wrong = intent(0)
    with pytest.raises(TypeError, match="result"):
        recorder.record_result(wrong)


# --- a whole-document call, and what a result says about the call -----------------------


def test_an_intent_may_carry_no_field_kind_for_a_whole_document() -> None:
    whole = intent(field_kind=None, capability="extract_invoice")
    assert whole.field_kind is None
    with pytest.raises(TypeError, match="field_kind"):
        intent(field_kind="total")


def test_the_call_metadata_limits_are_the_ledgers() -> None:
    from orqestra.calls import recording
    from orqestra.persistence import provider_usage

    assert recording.MODEL_MAX_LENGTH == provider_usage.MODEL_MAX_LENGTH
    assert recording.GENERATION_ID_MAX_LENGTH == provider_usage.GENERATION_ID_MAX_LENGTH
    assert recording.BIGINT_MAX == provider_usage.BIGINT_MAX
    assert recording.INTEGER_MAX == provider_usage.INTEGER_MAX


def test_a_result_without_call_metadata_is_the_old_result() -> None:
    plain = result()
    assert (
        plain.model_served,
        plain.provider_generation_id,
        plain.input_tokens,
        plain.output_tokens,
        plain.latency_ms,
    ) == (None, None, None, None, None)
    fields = [f.name for f in dataclasses.fields(ProviderCallResult)]
    assert fields[:5] == ["attempt_id", "call_seq", "outcome", "failure_code", "usage"]


def test_call_metadata_is_accepted_at_its_limits() -> None:
    upper = result(
        model_served="m" * 128,
        provider_generation_id="g" * 128,
        input_tokens=BIGINT_MAX,
        output_tokens=BIGINT_MAX,
        latency_ms=INTEGER_MAX,
    )
    assert (upper.input_tokens, upper.latency_ms) == (BIGINT_MAX, INTEGER_MAX)
    lower = result(input_tokens=0, output_tokens=0, latency_ms=0, model_served="m")
    assert (lower.input_tokens, lower.output_tokens, lower.latency_ms) == (0, 0, 0)


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"model_served": "m" * 129}, ValueError),
        ({"model_served": "  "}, ValueError),
        ({"model_served": "a\x00b"}, ValueError),
        ({"model_served": "\ud800"}, ValueError),
        ({"model_served": 7}, TypeError),
        ({"provider_generation_id": "g" * 129}, ValueError),
        ({"provider_generation_id": ""}, ValueError),
        ({"provider_generation_id": b"gen"}, TypeError),
        ({"input_tokens": BIGINT_MAX + 1}, ValueError),
        ({"input_tokens": -1}, ValueError),
        ({"input_tokens": True}, TypeError),
        ({"input_tokens": 1.0}, TypeError),
        ({"output_tokens": BIGINT_MAX + 1}, ValueError),
        ({"output_tokens": "3"}, TypeError),
        ({"latency_ms": INTEGER_MAX + 1}, ValueError),
        ({"latency_ms": -1}, ValueError),
        ({"latency_ms": False}, TypeError),
    ],
)
def test_call_metadata_outside_the_ledgers_rules_is_refused_when_built(
    fields: dict[str, Any], error: type[Exception]
) -> None:
    with pytest.raises(error) as raised:
        result(**fields)
    assert SECRET not in str(raised.value)


def test_metadata_never_echoes_the_value() -> None:
    with pytest.raises(ValueError, match="model_served") as raised:
        result(model_served=SECRET + "x" * 128)
    assert SECRET not in str(raised.value)


def test_the_invoice_verifier_always_names_the_field_kind() -> None:
    from datetime import timedelta

    from benchmarks.generate import generate_invoices
    from benchmarks.provider import SeparatorReader
    from orqestra.calls.routing import RouteOption
    from orqestra.workloads.invoice.verify import (
        REQUIRED_CAPABILITY,
        ProviderCalls,
        verify_invoice,
    )

    recorder = InMemoryCallRecorder()
    routes = (RouteOption("r1", frozenset({REQUIRED_CAPABILITY}), estimated_cost_units=1),)
    calls = ProviderCalls(attempt_id=A, cap=1000, recorder=recorder)
    for index, invoice in enumerate(generate_invoices(200)):
        verify_invoice(
            invoice,
            routes=routes,
            providers={"r1": SeparatorReader()},
            timeout=timedelta(seconds=1),
            calls=calls,
            item_index=index,
        )
    assert len(recorder.intents) == 10  # the ambiguous invoices: 5 % of 200
    assert all(isinstance(made.field_kind, FieldKind) for made in recorder.intents)
