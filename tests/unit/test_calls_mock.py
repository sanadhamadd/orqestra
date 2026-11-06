from datetime import timedelta

import pytest

from orqestra.calls.mock import MockProvider, ScriptExhaustedError
from orqestra.calls.provider import (
    FieldKind,
    Provider,
    ProviderRequest,
    ProviderResponse,
    ProviderTransientError,
    TransientCode,
    make_request,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


def ask(raw: str, kind: FieldKind = FieldKind.TOTAL) -> ProviderRequest:
    return make_request(
        capability="cap", route_id="r1", field_kind=kind, raw=raw, timeout=timedelta(seconds=1)
    )


def test_a_string_step_becomes_a_response() -> None:
    assert MockProvider(["12"]).interpret(ask("twelve")) == ProviderResponse("12")


def test_a_response_step_is_returned_as_it_is() -> None:
    reply = ProviderResponse("12", 3)
    assert MockProvider([reply]).interpret(ask("twelve")) is reply


def test_an_exception_step_is_raised() -> None:
    error = ProviderTransientError(TransientCode.TIMEOUT)
    with pytest.raises(ProviderTransientError) as raised:
        MockProvider([error]).interpret(ask("x"))
    assert raised.value is error


def test_any_other_step_is_returned_unchanged_to_simulate_a_broken_provider() -> None:
    marker = object()
    assert MockProvider([marker]).interpret(ask("x")) is marker


def test_steps_are_used_in_order_by_distinct_requests() -> None:
    mock = MockProvider(["1", "2"])
    assert mock.interpret(ask("a")).value == "1"
    assert mock.interpret(ask("b")).value == "2"
    assert mock.steps_used == 2


def test_a_repeated_request_id_gets_the_identical_answer_without_using_a_step() -> None:
    mock = MockProvider(["1", "2"])
    first = mock.interpret(ask("a"))
    assert mock.interpret(ask("a")) is first
    assert mock.steps_used == 1
    assert mock.interpret(ask("b")).value == "2"


def test_the_same_raw_text_of_another_field_kind_is_a_new_request() -> None:
    mock = MockProvider(["1", "2"])
    assert mock.interpret(ask("a", FieldKind.TOTAL)).value == "1"
    assert mock.interpret(ask("a", FieldKind.QUANTITY)).value == "2"


def test_a_raised_exception_is_not_remembered() -> None:
    mock = MockProvider([RuntimeError("boom"), "7"])
    with pytest.raises(RuntimeError, match="boom"):
        mock.interpret(ask("a"))
    assert mock.interpret(ask("a")).value == "7"


def test_every_request_is_recorded_repeats_included() -> None:
    mock = MockProvider(["1"])
    first, second = ask("a"), ask("a")
    mock.interpret(first)
    mock.interpret(second)
    assert mock.requests == (first, second)


def test_an_exhausted_script_raises_a_distinct_error() -> None:
    with pytest.raises(ScriptExhaustedError, match="exhausted"):
        MockProvider().interpret(ask("a"))


def test_the_mock_satisfies_the_provider_protocol() -> None:
    provider: Provider = MockProvider(["1"])
    assert provider.interpret(ask("a")).value == "1"
