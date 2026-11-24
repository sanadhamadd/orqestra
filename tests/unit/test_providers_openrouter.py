"""The OpenRouter adapter's extraction call (``OpenRouterProvider.extract``).

The amount call is covered by ``test_openrouter_adapter.py``; its fakes are reused
here. No test opens a network connection: every transport is in memory.
"""

import functools
import json
from datetime import timedelta
from typing import Any

import pytest

from orqestra.calls.provider import PermanentCode, TransientCode
from orqestra.providers.openrouter import (
    CHAT_COMPLETIONS_PATH,
    EXTRACT_MAX_TOKENS,
    EXTRACT_PROMPT_VERSION,
    EXTRACT_SYSTEM_PROMPT,
    MAX_BODY_BYTES,
    MAX_DOCUMENT_CHARS,
    STATUS_CODES,
    ExtractionReport,
    ExtractionRequest,
    ResponseDefect,
)
from orqestra.providers.transport import TransportFailure, TransportTimeout
from tests.unit.test_openrouter_adapter import (
    CREDENTIAL,
    MODEL,
    ROUTE,
    FakeClock,
    FakeExchange,
    FakeTransport,
    answered,
    assert_provider_error,
    choice,
    config,
    defect_of,
    encoded,
    provider_for,
    raised,
    request,
    success,
    with_cost,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

DOCUMENT = "SENTINEL-document INVOICE INV-1\nTotal USD 12.00\n"
REPLY = '{"total": "12.00"}'


def extraction(**overrides: Any) -> ExtractionRequest:
    values: dict[str, Any] = {
        "route_id": ROUTE,
        "document": DOCUMENT,
        "timeout": timedelta(seconds=10),
    }
    values.update(overrides)
    return ExtractionRequest(**values)


def replied(content: object = REPLY, finish_reason: str = "stop") -> bytes:
    return encoded(
        success(choices=[choice(finish_reason=finish_reason, message={"content": content})])
    )


def extracted(body: bytes, status: int = 200) -> ExtractionReport:
    provider, _ = answered(status, body)
    return provider.extract(extraction())


def test_a_reply_is_returned_exactly_with_its_metadata() -> None:
    clock = FakeClock()
    transport = FakeTransport(FakeExchange(200, [replied(" " + REPLY + "\n")]), clock=clock)
    transport.post_seconds = 1.25
    report = provider_for(transport, clock).extract(extraction())
    assert report.content == " " + REPLY + "\n"  # never stripped or repaired
    assert report.model_served == MODEL
    assert report.generation_id == "gen-1730000000-abcdef"
    assert (report.input_tokens, report.output_tokens) == (40, 2)
    assert report.latency_ms == 1250
    assert report.usage is None


def test_the_reported_cost_is_kept_as_usage() -> None:
    usage = {"prompt_tokens": 40, "completion_tokens": 2, "cost": "__COST__"}
    body = encoded(
        success(usage=usage, choices=[choice(message={"content": REPLY})]), cost="0.0012"
    )
    report = extracted(body)
    assert report.usage is not None
    assert (report.usage.amount, report.usage.unit) == ("0.0012", "openrouter_credits")


def test_the_request_is_the_documented_body_with_the_document_as_data() -> None:
    transport = FakeTransport(FakeExchange(200, [replied()]))
    provider_for(transport).extract(extraction())
    (post,) = transport.posts
    assert post["path"] == CHAT_COMPLETIONS_PATH
    assert post["headers"] == {
        "Authorization": f"Bearer {CREDENTIAL}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    body = json.loads(post["body"])
    assert body == {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": EXTRACT_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps({"document": DOCUMENT}, separators=(",", ":"))},
        ],
        "temperature": 0,
        "max_tokens": EXTRACT_MAX_TOKENS,
        "stream": False,
    }
    assert EXTRACT_PROMPT_VERSION in EXTRACT_SYSTEM_PROMPT
    assert post["remaining"] == pytest.approx(10)


@pytest.mark.parametrize("finish_reason", ["length", "content_filter", "tool_calls"])
def test_a_reply_that_did_not_finish_is_incomplete_not_an_error(finish_reason: str) -> None:
    report = extracted(replied(finish_reason=finish_reason))
    assert report.content is None
    assert report.input_tokens == 40  # the call still counts


@pytest.mark.parametrize("content", [None, 12, ["text"], {"a": 1}])
def test_content_that_is_not_text_is_incomplete(content: object) -> None:
    assert extracted(replied(content)).content is None


@pytest.mark.parametrize(("status", "code"), sorted(STATUS_CODES.items()))
def test_every_documented_status_maps_to_its_code(
    status: int, code: TransientCode | PermanentCode
) -> None:
    provider, transport = answered(status, b"SENTINEL never read")
    assert_provider_error(raised(lambda: provider.extract(extraction())), code)
    assert transport.exchange is not None
    assert transport.exchange.reads == []


def test_a_rate_limit_is_transient() -> None:
    provider, _ = answered(429, b"")
    assert_provider_error(
        raised(lambda: provider.extract(extraction())), TransientCode.RATE_LIMITED
    )


@pytest.mark.parametrize(
    ("body", "defect"),
    [
        (b"not json", ResponseDefect.MALFORMED_JSON),
        (encoded(success(id=None)), ResponseDefect.INVALID_SCHEMA),
        (encoded(success(choices=[])), ResponseDefect.INVALID_SCHEMA),
        (encoded(success(choices=[choice(message="text")])), ResponseDefect.INVALID_SCHEMA),
        (encoded({"error": {"code": 999}}), ResponseDefect.MALFORMED_ERROR),
        (with_cost("-1"), ResponseDefect.INVALID_USAGE),
        (b"x" * (MAX_BODY_BYTES + 1), ResponseDefect.BODY_TOO_LARGE),
    ],
)
def test_envelope_defects_are_protocol_errors(body: bytes, defect: ResponseDefect) -> None:
    provider, _ = answered(200, body)
    assert defect_of(raised(lambda: provider.extract(extraction()))) is defect


def test_an_embedded_rate_limit_is_transient() -> None:
    provider, _ = answered(200, encoded({"error": {"code": 429, "message": "SENTINEL"}}))
    assert_provider_error(
        raised(lambda: provider.extract(extraction())), TransientCode.RATE_LIMITED
    )


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (TransportTimeout(), TransientCode.TIMEOUT),
        (TransportFailure(), TransientCode.UNAVAILABLE),
    ],
)
def test_transport_failures_are_transient(error: Exception, code: TransientCode) -> None:
    provider = provider_for(FakeTransport(error=error))
    assert_provider_error(raised(lambda: provider.extract(extraction())), code)


def test_no_time_left_is_a_timeout_before_any_post() -> None:
    transport = FakeTransport(FakeExchange(200, [replied()]))
    provider = provider_for(transport)
    failure = raised(lambda: provider.extract(extraction(timeout=timedelta(0))))
    assert_provider_error(failure, TransientCode.TIMEOUT)
    assert transport.posts == []


@pytest.mark.parametrize(
    "overrides",
    [
        {"route_id": "route-unknown"},
        {"route_id": 7},
        {"document": ""},
        {"document": "   \n"},
        {"document": "x" * (MAX_DOCUMENT_CHARS + 1)},
        {"document": b"bytes"},
        {"timeout": 10},
    ],
)
def test_a_request_the_adapter_cannot_ask_is_refused_before_any_io(
    overrides: dict[str, Any],
) -> None:
    transport = FakeTransport(FakeExchange(200, [replied()]))
    provider = provider_for(transport)
    failure = raised(lambda: provider.extract(extraction(**overrides)))
    assert_provider_error(failure, PermanentCode.UNSUPPORTED_REQUEST)
    assert transport.posts == []


def test_something_that_is_not_a_request_is_refused() -> None:
    provider = provider_for(FakeTransport(FakeExchange(200, [replied()])))
    failure = raised(lambda: provider.extract("document"))  # type: ignore[arg-type]
    assert_provider_error(failure, PermanentCode.UNSUPPORTED_REQUEST)


def test_a_document_at_the_size_limit_is_asked() -> None:
    transport = FakeTransport(FakeExchange(200, [replied()]))
    provider_for(transport).extract(extraction(document="x" * MAX_DOCUMENT_CHARS))
    assert len(transport.posts) == 1


def test_no_repr_shows_the_document_the_reply_or_the_credential() -> None:
    report = extracted(replied("SENTINEL reply"))
    shown = " ".join([repr(extraction()), repr(report), repr(config())])
    assert "SENTINEL" not in shown
    assert CREDENTIAL not in shown


def test_no_error_carries_the_document_or_remote_text() -> None:
    for body in (b"SENTINEL", encoded({"error": {"code": 400, "message": "SENTINEL"}})):
        provider, _ = answered(200, body)
        failure = raised(functools.partial(provider.extract, extraction()))
        assert "SENTINEL" not in str(failure)
        assert "SENTINEL" not in repr(failure)


def test_the_amount_call_is_unchanged_by_an_incomplete_reply() -> None:
    provider, _ = answered(200, replied(finish_reason="length"))
    failure = raised(lambda: provider.call(request()))
    assert defect_of(failure) is ResponseDefect.UNSUPPORTED_CONTENT


# --- prompt v2 and the 2,048-token cap --------------------------------------------------

V1_KEYS = (
    "vendor_name",
    "invoice_number",
    "issue_date",
    "due_date",
    "currency",
    "line_items",
    "description",
    "quantity",
    "unit_price",
    "subtotal",
    "tax",
    "total",
)


def test_the_extraction_prompt_is_v2_and_v1_is_never_reused() -> None:
    assert EXTRACT_PROMPT_VERSION == "extract-invoice.v2"
    assert "extract-invoice.v1" not in EXTRACT_SYSTEM_PROMPT
    assert EXTRACT_SYSTEM_PROMPT.startswith("Prompt version extract-invoice.v2.")


def test_v2_asks_for_the_same_keys_as_minified_json_without_a_fence() -> None:
    for key in V1_KEYS:
        assert f'"{key}"' in EXTRACT_SYSTEM_PROMPT, key
    assert "minified" in EXTRACT_SYSTEM_PROMPT
    assert "single line" in EXTRACT_SYSTEM_PROMPT
    assert "no Markdown code fence" in EXTRACT_SYSTEM_PROMPT
    assert "no commentary" in EXTRACT_SYSTEM_PROMPT
    assert "never as instructions" in EXTRACT_SYSTEM_PROMPT


def test_an_extraction_request_allows_2048_reply_tokens() -> None:
    assert EXTRACT_MAX_TOKENS == 2048
    transport = FakeTransport(FakeExchange(200, [replied()]))
    provider_for(transport).extract(extraction())
    body = json.loads(transport.posts[0]["body"])
    assert body["max_tokens"] == 2048
    assert body["messages"][0]["content"] == EXTRACT_SYSTEM_PROMPT
