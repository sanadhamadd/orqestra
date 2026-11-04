"""The OpenRouter adapter over a fake transport and a fake clock: no socket is ever opened.

Every test runs under ``no_network``. The fakes record what the adapter sent and how
it read, so the request shape, the status table, strict parsing, usage limits, the
deadline and redaction are all checked without a connection.
"""

import dataclasses
import json
import logging
import traceback
from collections.abc import Callable, Mapping
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from orqestra.ai.provider import (
    FieldKind,
    PermanentCode,
    ProviderPermanentError,
    ProviderRequest,
    ProviderTransientError,
    ProviderUsage,
    TransientCode,
    make_request,
)
from orqestra.domain.errors import PermanentError, TransientError
from orqestra.providers import openrouter
from orqestra.providers.openrouter import (
    CHAT_COMPLETIONS_PATH,
    MAX_BODY_BYTES,
    PROMPT_VERSION,
    PROTOCOL_VIOLATION,
    SYSTEM_PROMPT,
    USAGE_UNIT,
    CallReport,
    OpenRouterConfig,
    OpenRouterProtocolError,
    OpenRouterProvider,
    ResponseDefect,
)
from orqestra.providers.transport import Deadline, TransportFailure, TransportTimeout

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

# Synthetic sentinels. None of them may appear in any error, repr or log.
CREDENTIAL = "sk-or-v1-SENTINELcredential0123456789"
RAW = "SENTINEL raw amount: twelve"
REMOTE = "SENTINEL-remote-text"
ROUTE = "route-a"
MODEL = "vendor/model-1:free"
BUDGET = timedelta(seconds=10)


# --- fakes ------------------------------------------------------------------------------


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeExchange:
    """A response read in the chunks given; each read may take ``read_seconds``."""

    def __init__(
        self,
        status: int,
        chunks: list[bytes],
        *,
        clock: FakeClock | None = None,
        read_seconds: float = 0.0,
        read_error: Exception | None = None,
    ) -> None:
        self.status = status
        self._chunks = list(chunks)
        self._clock = clock
        self._read_seconds = read_seconds
        self._read_error = read_error
        self.reads: list[tuple[int, float]] = []  # (max_bytes, time left at the read)
        self.closed = 0

    def read(self, max_bytes: int, deadline: Deadline) -> bytes:
        remaining = deadline.remaining()  # as the real transport does, before blocking
        self.reads.append((max_bytes, remaining))
        if self._clock is not None:
            self._clock.advance(self._read_seconds)
        if self._read_error is not None:
            raise self._read_error
        if not self._chunks:
            return b""
        chunk = self._chunks.pop(0)
        if len(chunk) > max_bytes:
            self._chunks.insert(0, chunk[max_bytes:])
            chunk = chunk[:max_bytes]
        return chunk

    def close(self) -> None:
        self.closed += 1


class FakeTransport:
    def __init__(
        self,
        exchange: FakeExchange | None = None,
        *,
        error: Exception | None = None,
        clock: FakeClock | None = None,
        post_seconds: float = 0.0,
    ) -> None:
        self.exchange = exchange
        self.error = error
        self.clock = clock
        self.post_seconds = post_seconds
        self.posts: list[dict[str, Any]] = []

    def post(
        self, *, path: str, headers: Mapping[str, str], body: bytes, deadline: Deadline
    ) -> FakeExchange:
        record = {
            "path": path,
            "headers": dict(headers),
            "body": body,
            "remaining": deadline.remaining(),
        }
        self.posts.append(record)
        if self.clock is not None:
            self.clock.advance(self.post_seconds)
        if self.error is not None:
            raise self.error
        assert self.exchange is not None
        return self.exchange


class PlainSecret:
    def __init__(self, value: str) -> None:
        self._value = value

    def get_secret_value(self) -> str:
        return self._value


class Revealing:
    """Reveals a value of any type, to exercise the credential's type check."""

    def __init__(self, value: object) -> None:
        self._value = value

    def get_secret_value(self) -> object:
        return self._value


# --- builders ---------------------------------------------------------------------------


def config(**overrides: Any) -> OpenRouterConfig:
    values: dict[str, Any] = {
        "credential": SecretStr(CREDENTIAL),
        "route_models": {ROUTE: MODEL},
    }
    values.update(overrides)
    return OpenRouterConfig(**values)


def request(**overrides: Any) -> ProviderRequest:
    values: dict[str, Any] = {
        "capability": "interpret_amount",
        "route_id": ROUTE,
        "field_kind": FieldKind.TOTAL,
        "raw": RAW,
        "timeout": BUDGET,
    }
    values.update(overrides)
    return make_request(**values)


def choice(**overrides: Any) -> dict[str, Any]:
    value: dict[str, Any] = {"finish_reason": "stop", "message": {"content": " 12\n"}}
    value.update(overrides)
    return value


def success(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "id": "gen-1730000000-abcdef",
        "model": MODEL,
        "choices": [choice()],
        "usage": {"prompt_tokens": 40, "completion_tokens": 2, "total_tokens": 42},
    }
    document.update(overrides)
    return document


def encoded(document: object, *, cost: str | None = None) -> bytes:
    """``document`` as JSON; ``cost`` is spliced in as literal JSON number text."""
    text = json.dumps(document)
    if cost is not None:
        text = text.replace('"__COST__"', cost)
    return text.encode("utf-8")


def with_cost(cost: str) -> bytes:
    usage = {"prompt_tokens": 40, "completion_tokens": 2, "cost": "__COST__"}
    return encoded(success(usage=usage), cost=cost)


def provider_for(
    transport: FakeTransport,
    clock: FakeClock | None = None,
    cfg: OpenRouterConfig | None = None,
) -> OpenRouterProvider:
    return OpenRouterProvider(cfg or config(), transport, clock=clock or FakeClock())


def answered(status: int, body: bytes, **kwargs: Any) -> tuple[OpenRouterProvider, FakeTransport]:
    transport = FakeTransport(FakeExchange(status, [body], **kwargs))
    return provider_for(transport), transport


def raised(call: Callable[[], object]) -> Exception:
    """The exception ``call`` raised, checked to carry no chain."""
    failure: Exception | None = None
    try:
        call()
    except Exception as exc:
        failure = exc
    assert failure is not None, "nothing was raised"
    assert failure.__cause__ is None
    assert failure.__context__ is None
    return failure


def defect_of(exc: Exception) -> ResponseDefect:
    assert type(exc) is OpenRouterProtocolError
    assert isinstance(exc, OpenRouterProtocolError)
    assert isinstance(exc, PermanentError)
    assert str(exc) == PROTOCOL_VIOLATION
    return exc.defect


def assert_provider_error(exc: Exception, code: TransientCode | PermanentCode) -> None:
    transient = isinstance(code, TransientCode)
    assert type(exc) is (ProviderTransientError if transient else ProviderPermanentError)
    assert isinstance(exc, ProviderTransientError | ProviderPermanentError)
    assert exc.code is code
    assert str(exc) == code.value


def failure_of(status: int, body: bytes) -> Exception:
    provider, _ = answered(status, body)
    return raised(lambda: provider.call(request()))


# --- success and the request ------------------------------------------------------------


def test_a_valid_answer_is_reported_with_its_metadata() -> None:
    clock = FakeClock()
    exchange = FakeExchange(200, [with_cost("0.000123")], clock=clock, read_seconds=0.1)
    transport = FakeTransport(exchange, clock=clock, post_seconds=0.25)
    report = provider_for(transport, clock).call(request())
    assert isinstance(report, CallReport)
    assert report.response.value == "12"  # surrounding whitespace stripped
    assert report.response.reported_cost_units is None
    assert report.response.usage == ProviderUsage("0.000123", USAGE_UNIT)
    assert report.model_served == MODEL
    assert report.generation_id == "gen-1730000000-abcdef"
    assert (report.input_tokens, report.output_tokens) == (40, 2)
    assert report.latency_ms == 450  # the POST (250 ms) and two reads (100 ms each)
    assert exchange.closed == 1
    assert len(transport.posts) == 1  # one POST, never retried


def test_interpret_returns_the_calls_response() -> None:
    provider, _ = answered(200, encoded(success()))
    response = provider.interpret(request())
    assert response.value == "12"
    assert response.usage is None


def test_the_request_is_exactly_the_documented_body_and_headers() -> None:
    provider, transport = answered(200, encoded(success()))
    provider.call(request(field_kind=FieldKind.UNIT_AMOUNT))
    (post,) = transport.posts
    assert post["path"] == CHAT_COMPLETIONS_PATH == "/api/v1/chat/completions"
    assert post["headers"] == {
        "Authorization": f"Bearer {CREDENTIAL}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    document = json.loads(post["body"])
    assert list(document) == ["model", "messages", "stream"]
    assert document["model"] == MODEL
    assert document["stream"] is False
    system, user = document["messages"]
    assert system == {"role": "system", "content": SYSTEM_PROMPT}
    assert PROMPT_VERSION in SYSTEM_PROMPT
    assert user["role"] == "user"
    question = {"field_kind": "unit_amount", "raw": RAW}
    assert user["content"] == json.dumps(question, sort_keys=True, separators=(",", ":"))
    assert post["body"].isascii()


def test_the_credential_reaches_only_the_authorization_header() -> None:
    provider, transport = answered(200, encoded(success()))
    provider.call(request())
    (post,) = transport.posts
    assert CREDENTIAL.encode() not in post["body"]
    assert CREDENTIAL not in post["path"]
    for name, value in post["headers"].items():
        if name != "Authorization":
            assert CREDENTIAL not in name + value
    assert post["headers"]["Authorization"] == f"Bearer {CREDENTIAL}"


def test_each_route_uses_its_own_model() -> None:
    cfg = config(route_models={"route-a": "vendor/a", "route-b": "vendor/b"})
    transport = FakeTransport(FakeExchange(200, [encoded(success(model="vendor/b"))]))
    report = provider_for(transport, cfg=cfg).call(request(route_id="route-b"))
    assert json.loads(transport.posts[0]["body"])["model"] == "vendor/b"
    assert report.model_served == "vendor/b"


# --- before any I/O ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"capability": "summarize"},
        {"capability": "Interpret_amount"},
        {"route_id": "route-unmapped"},
    ],
)
def test_an_unsupported_request_touches_no_transport(overrides: dict[str, Any]) -> None:
    transport = FakeTransport(error=AssertionError("the transport was used"))
    exc = raised(lambda: provider_for(transport).call(request(**overrides)))
    assert_provider_error(exc, PermanentCode.UNSUPPORTED_REQUEST)
    assert transport.posts == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("field_kind", "total"),
        ("raw", 12),
        ("timeout", 10),
    ],
)
def test_a_request_that_breaks_its_types_touches_no_transport(field: str, value: object) -> None:
    changes: dict[str, Any] = {field: value}
    bad: ProviderRequest = dataclasses.replace(request(), **changes)
    transport = FakeTransport(error=AssertionError("the transport was used"))
    exc = raised(lambda: provider_for(transport).call(bad))
    assert_provider_error(exc, PermanentCode.UNSUPPORTED_REQUEST)
    assert transport.posts == []


# --- status table -----------------------------------------------------------------------

DOCUMENTED = [
    (400, PermanentCode.UNSUPPORTED_REQUEST),
    (401, PermanentCode.REJECTED),
    (402, PermanentCode.REJECTED),
    (403, PermanentCode.REJECTED),
    (408, TransientCode.TIMEOUT),
    (429, TransientCode.RATE_LIMITED),
    (502, TransientCode.UNAVAILABLE),
    (503, TransientCode.UNAVAILABLE),
]


@pytest.mark.parametrize(("status", "code"), DOCUMENTED)
def test_a_documented_status_maps_without_reading_the_body(
    status: int, code: TransientCode | PermanentCode
) -> None:
    body = encoded({"error": {"code": status, "message": REMOTE}})
    provider, transport = answered(status, body)
    assert_provider_error(raised(lambda: provider.call(request())), code)
    assert transport.exchange is not None
    assert transport.exchange.reads == []
    assert transport.exchange.closed == 1


def test_the_status_table_is_exactly_the_documented_one() -> None:
    assert dict(openrouter.STATUS_CODES) == dict(DOCUMENTED)
    assert isinstance(ProviderTransientError(TransientCode.TIMEOUT), TransientError)


@pytest.mark.parametrize("status", [100, 201, 204, 301, 302, 404, 418, 500, 504, 599])
def test_any_other_status_is_a_protocol_violation_without_reading(status: int) -> None:
    provider, transport = answered(status, encoded(success()))
    assert defect_of(raised(lambda: provider.call(request()))) is ResponseDefect.UNKNOWN_STATUS
    assert transport.exchange is not None
    assert transport.exchange.reads == []
    assert transport.exchange.closed == 1


# --- embedded errors --------------------------------------------------------------------


@pytest.mark.parametrize(("status", "code"), DOCUMENTED)
def test_an_embedded_error_maps_like_its_status(
    status: int, code: TransientCode | PermanentCode
) -> None:
    envelope = {"code": status, "message": REMOTE, "metadata": {"raw": REMOTE}}
    assert_provider_error(failure_of(200, encoded(success(error=envelope))), code)


@pytest.mark.parametrize(
    "envelope",
    [
        {"code": 999, "message": REMOTE},
        {"code": 200},
        {"code": 500},
        {"code": "429"},
        {"code": True},
        {"code": 429.0},
        {"message": REMOTE},
        REMOTE,
        [429],
        None,
    ],
)
def test_an_unknown_or_malformed_embedded_error_is_a_protocol_violation(envelope: object) -> None:
    exc = failure_of(200, encoded(success(error=envelope)))
    assert defect_of(exc) is ResponseDefect.MALFORMED_ERROR


# --- malformed bodies -------------------------------------------------------------------

ARRAY_DEPTH = 100_000
OBJECT_DEPTH = 40_000  # five bytes a level, so the body stays under the size cap


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"\xff\xfe{}", id="bad-utf8"),
        pytest.param(b'{"id": "\xc3"}', id="truncated-utf8"),
        pytest.param(b"\xef\xbb\xbf{}", id="bom"),
        pytest.param(b"", id="empty"),
        pytest.param(b"not json", id="not-json"),
        pytest.param(b'{"id": NaN}', id="nan"),
        pytest.param(b'{"id": Infinity}', id="infinity"),
        pytest.param(b'{"id": -Infinity}', id="negative-infinity"),
        pytest.param(b'{"id": "a", "id": "b"}', id="duplicate-key"),
        pytest.param(b'{"usage": {"cost": 1, "cost": 2}}', id="nested-duplicate-key"),
        pytest.param(b"[]", id="top-level-array"),
        pytest.param(b'"text"', id="top-level-string"),
        pytest.param(b"null", id="top-level-null"),
        pytest.param(b"[" * ARRAY_DEPTH + b"]" * ARRAY_DEPTH, id="deep-array"),
        pytest.param(b'{"a":' * OBJECT_DEPTH + b"1" + b"}" * OBJECT_DEPTH, id="deep-object"),
        pytest.param(b'{"id": ' + b"7" * 5000 + b"}", id="int-over-digit-limit"),
    ],
)
def test_a_body_that_is_not_one_strict_json_object_is_malformed(body: bytes) -> None:
    assert len(body) <= MAX_BODY_BYTES
    assert defect_of(failure_of(200, body)) is ResponseDefect.MALFORMED_JSON


@pytest.mark.parametrize(
    ("overrides", "defect"),
    [
        ({"choices": []}, ResponseDefect.INVALID_SCHEMA),
        ({"choices": [choice(), choice()]}, ResponseDefect.INVALID_SCHEMA),
        ({"choices": None}, ResponseDefect.INVALID_SCHEMA),
        ({"choices": choice()}, ResponseDefect.INVALID_SCHEMA),
        ({"choices": ["12"]}, ResponseDefect.INVALID_SCHEMA),
        ({"choices": [{"finish_reason": "stop"}]}, ResponseDefect.INVALID_SCHEMA),
        ({"choices": [choice(message="12")]}, ResponseDefect.INVALID_SCHEMA),
        ({"choices": [choice(message={"content": ["12"]})]}, ResponseDefect.UNSUPPORTED_CONTENT),
        ({"choices": [choice(message={"content": None})]}, ResponseDefect.UNSUPPORTED_CONTENT),
        ({"choices": [choice(message={"content": 12})]}, ResponseDefect.UNSUPPORTED_CONTENT),
        ({"choices": [choice(message={"role": "assistant"})]}, ResponseDefect.UNSUPPORTED_CONTENT),
        ({"choices": [choice(finish_reason="length")]}, ResponseDefect.UNSUPPORTED_CONTENT),
        ({"choices": [choice(finish_reason=None)]}, ResponseDefect.UNSUPPORTED_CONTENT),
        ({"choices": [{"message": {"content": "12"}}]}, ResponseDefect.UNSUPPORTED_CONTENT),
        ({"id": None}, ResponseDefect.INVALID_SCHEMA),
        ({"id": ""}, ResponseDefect.INVALID_SCHEMA),
        ({"id": "   "}, ResponseDefect.INVALID_SCHEMA),
        ({"id": "g" * 129}, ResponseDefect.INVALID_SCHEMA),
        ({"id": "gen\x00x"}, ResponseDefect.INVALID_SCHEMA),
        ({"id": "gen 1"}, ResponseDefect.INVALID_SCHEMA),
        ({"id": "gen\t1"}, ResponseDefect.INVALID_SCHEMA),
        ({"id": "gen-é"}, ResponseDefect.INVALID_SCHEMA),
        ({"id": 123}, ResponseDefect.INVALID_SCHEMA),
        ({"model": None}, ResponseDefect.INVALID_SCHEMA),
        ({"model": ""}, ResponseDefect.INVALID_SCHEMA),
        ({"model": " "}, ResponseDefect.INVALID_SCHEMA),
        ({"model": "m" * 129}, ResponseDefect.INVALID_SCHEMA),
        ({"model": "vendor/\x00model"}, ResponseDefect.INVALID_SCHEMA),
        ({"model": "vendor model"}, ResponseDefect.INVALID_SCHEMA),
        ({"model": "/vendor/model"}, ResponseDefect.INVALID_SCHEMA),
        ({"model": ["vendor/model"]}, ResponseDefect.INVALID_SCHEMA),
    ],
)
def test_a_response_outside_the_schema_is_a_protocol_violation(
    overrides: dict[str, Any], defect: ResponseDefect
) -> None:
    assert defect_of(failure_of(200, encoded(success(**overrides)))) is defect


@pytest.mark.parametrize("field", ["id", "model", "choices"])
def test_a_missing_required_field_is_a_protocol_violation(field: str) -> None:
    document = success()
    del document[field]
    assert defect_of(failure_of(200, encoded(document))) is ResponseDefect.INVALID_SCHEMA


def test_identifiers_at_their_limits_are_accepted() -> None:
    long_id, long_model = "g" * 128, "m" * 128
    provider, _ = answered(200, encoded(success(id=long_id, model=long_model)))
    report = provider.call(request())
    assert (report.generation_id, report.model_served) == (long_id, long_model)


def test_only_the_surrounding_whitespace_of_the_content_is_removed() -> None:
    body = encoded(success(choices=[choice(message={"content": "\n 1 2 \t"})]))
    provider, _ = answered(200, body)
    assert provider.call(request()).response.value == "1 2"  # the verifier judges the digits


# --- body size --------------------------------------------------------------------------


def test_a_body_over_the_cap_is_rejected_and_reading_stops() -> None:
    exchange = FakeExchange(200, [b" " * 65536] * 5)  # 320 KiB in total
    exc = raised(lambda: provider_for(FakeTransport(exchange)).call(request()))
    assert defect_of(exc) is ResponseDefect.BODY_TOO_LARGE
    assert sum(size for size, _ in exchange.reads) == MAX_BODY_BYTES + 1
    assert exchange.closed == 1


def test_a_body_exactly_at_the_cap_is_read() -> None:
    body = encoded(success())
    padded = body + b" " * (MAX_BODY_BYTES - len(body))
    assert len(padded) == MAX_BODY_BYTES
    provider, _ = answered(200, padded)
    assert provider.call(request()).response.value == "12"


# --- usage ------------------------------------------------------------------------------


def test_absent_usage_is_unknown_never_zero() -> None:
    document = success()
    del document["usage"]
    provider, _ = answered(200, encoded(document))
    report = provider.call(request())
    assert report.response.usage is None
    assert report.input_tokens is None
    assert report.output_tokens is None


def test_usage_without_cost_or_tokens_reports_none_for_each() -> None:
    provider, _ = answered(200, encoded(success(usage={})))
    report = provider.call(request())
    assert (report.response.usage, report.input_tokens, report.output_tokens) == (None, None, None)


@pytest.mark.parametrize("usage", [None, [], "40", 40])
def test_usage_that_is_not_an_object_is_invalid(usage: object) -> None:
    exc = failure_of(200, encoded(success(usage=usage)))
    assert defect_of(exc) is ResponseDefect.INVALID_USAGE


@pytest.mark.parametrize("key", ["prompt_tokens", "completion_tokens", "total_tokens"])
@pytest.mark.parametrize("value", ["true", "-1", "-0", "3.0", "3e0", str(2**63), '"3"', "null"])
def test_an_invalid_token_count_is_invalid_usage(key: str, value: str) -> None:
    usage = {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2, key: "__COST__"}
    exc = failure_of(200, encoded(success(usage=usage), cost=value))
    assert defect_of(exc) is ResponseDefect.INVALID_USAGE


def test_token_counts_up_to_the_ledgers_maximum_are_accepted() -> None:
    top = 2**63 - 1
    usage = {"prompt_tokens": top, "completion_tokens": 0, "total_tokens": top}
    provider, _ = answered(200, encoded(success(usage=usage)))
    report = provider.call(request())
    assert (report.input_tokens, report.output_tokens) == (top, 0)


@pytest.mark.parametrize(
    "cost",
    [
        pytest.param("true", id="bool"),
        pytest.param('"0.5"', id="string"),
        pytest.param("null", id="null"),
        pytest.param("[1]", id="array"),
        pytest.param("-1", id="negative-int"),
        pytest.param("-0.5", id="negative"),
        pytest.param("-0", id="negative-zero-int"),
        pytest.param("-0.0", id="negative-zero"),
        pytest.param("1e400", id="huge-exponent"),
        pytest.param("0.0000000000000000001", id="scale-19"),
        pytest.param("1E+20", id="at-limit"),
        pytest.param("100000000000000000000", id="int-at-limit"),
        pytest.param("1" * 60, id="sixty-digits"),
        pytest.param("1" * 30 + "." + "1" * 30, id="sixty-one-characters"),
    ],
)
def test_an_unrecordable_cost_is_invalid_usage(cost: str) -> None:
    assert defect_of(failure_of(200, with_cost(cost))) is ResponseDefect.INVALID_USAGE


@pytest.mark.parametrize(
    ("cost", "amount"),
    [
        ("0", "0"),
        ("7", "7"),
        ("1.50", "1.50"),
        ("0.0", "0.0"),
        ("1.5e-3", "0.0015"),
        ("1.50E-3", "0.00150"),
        ("1E+2", "100"),
        ("0.000000000000000001", "0.000000000000000001"),
        ("99999999999999999999", "99999999999999999999"),
        ("99999999999999999999.999999999999999999", "99999999999999999999.999999999999999999"),
    ],
)
def test_a_cost_keeps_its_value_and_scale_in_fixed_point(cost: str, amount: str) -> None:
    provider, _ = answered(200, with_cost(cost))
    usage = provider.call(request()).response.usage
    assert usage == ProviderUsage(amount, USAGE_UNIT)
    assert usage is not None
    assert Decimal(usage.amount) == Decimal(cost)


def test_a_cost_provider_usage_refuses_is_invalid_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    # Widen the mirrored limit so only ProviderUsage's own 64-character rule can refuse.
    monkeypatch.setattr(openrouter, "AMOUNT_LIMIT", Decimal("1E+100"))
    assert defect_of(failure_of(200, with_cost("1" * 70))) is ResponseDefect.INVALID_USAGE


# --- deadline ---------------------------------------------------------------------------


def test_a_slow_chunked_body_times_out_and_nothing_is_read_after_expiry() -> None:
    clock = FakeClock()
    start = clock()
    body = encoded(success())
    chunks = [body[i : i + 8] for i in range(0, len(body), 8)]
    exchange = FakeExchange(200, chunks, clock=clock, read_seconds=1.5)
    provider = provider_for(FakeTransport(exchange, clock=clock, post_seconds=0.5), clock)
    exc = raised(lambda: provider.call(request(timeout=BUDGET)))
    assert_provider_error(exc, TransientCode.TIMEOUT)
    # Reads at 0.5 s, 2.0 s, ... 9.5 s each started with time left; none after 10 s.
    lefts = [left for _, left in exchange.reads]
    assert lefts == pytest.approx([9.5, 8.0, 6.5, 5.0, 3.5, 2.0, 0.5])
    assert len(exchange.reads) < len(chunks)
    assert clock() >= start + BUDGET.total_seconds()
    assert exchange.closed == 1


def test_the_transport_is_given_only_the_time_left() -> None:
    clock = FakeClock()
    exchange = FakeExchange(200, [encoded(success())], clock=clock, read_seconds=2.0)
    transport = FakeTransport(exchange, clock=clock, post_seconds=3.0)
    provider_for(transport, clock).call(request(timeout=BUDGET))
    assert transport.posts[0]["remaining"] == pytest.approx(10.0)
    assert [left for _, left in exchange.reads] == pytest.approx([7.0, 5.0])


@pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
def test_no_time_at_all_is_a_timeout_without_a_transport_call(timeout: timedelta) -> None:
    transport = FakeTransport(error=AssertionError("the transport was used"))
    exc = raised(lambda: provider_for(transport).call(request(timeout=timeout)))
    assert_provider_error(exc, TransientCode.TIMEOUT)
    assert transport.posts == []


def test_a_budget_spent_before_the_post_never_calls_the_transport() -> None:
    ticks = iter([0.0, 0.0, 1.0])  # the start, the deadline, the check before the POST

    def clock() -> float:
        return next(ticks)

    transport = FakeTransport(error=AssertionError("the transport was used"))
    provider = OpenRouterProvider(config(), transport, clock=clock)
    exc = raised(lambda: provider.call(request(timeout=timedelta(seconds=1))))
    assert_provider_error(exc, TransientCode.TIMEOUT)
    assert transport.posts == []


def test_a_post_that_uses_up_the_budget_reads_nothing() -> None:
    clock = FakeClock()
    exchange = FakeExchange(200, [encoded(success())])
    transport = FakeTransport(exchange, clock=clock, post_seconds=BUDGET.total_seconds())
    exc = raised(lambda: provider_for(transport, clock).call(request()))
    assert_provider_error(exc, TransientCode.TIMEOUT)
    assert exchange.reads == []
    assert exchange.closed == 1


def test_a_deadline_counts_down_and_refuses_at_zero() -> None:
    clock = FakeClock()
    deadline = Deadline(clock, timedelta(seconds=2))
    assert deadline.remaining() == 2
    clock.advance(1.5)
    assert deadline.remaining() == pytest.approx(0.5)
    clock.advance(0.5)
    with pytest.raises(TransportTimeout):
        deadline.remaining()
    with pytest.raises(TypeError, match="timedelta"):
        Deadline(clock, 2)  # type: ignore[arg-type]


# --- transport failures -----------------------------------------------------------------

TRANSPORT_ERRORS = [
    (TransportTimeout(), TransientCode.TIMEOUT),
    (TransportFailure(), TransientCode.UNAVAILABLE),
]


@pytest.mark.parametrize(("error", "code"), TRANSPORT_ERRORS)
def test_a_failed_post_maps_to_a_transient_code(error: Exception, code: TransientCode) -> None:
    transport = FakeTransport(error=error)
    assert_provider_error(raised(lambda: provider_for(transport).call(request())), code)
    assert len(transport.posts) == 1


@pytest.mark.parametrize(("error", "code"), TRANSPORT_ERRORS)
def test_a_failed_read_maps_to_a_transient_code_and_closes(
    error: Exception, code: TransientCode
) -> None:
    exchange = FakeExchange(200, [encoded(success())], read_error=error)
    provider = provider_for(FakeTransport(exchange))
    assert_provider_error(raised(lambda: provider.call(request())), code)
    assert exchange.closed == 1


# --- configuration ----------------------------------------------------------------------


def test_config_copies_the_route_map_into_an_immutable_mapping() -> None:
    source = {ROUTE: MODEL}
    cfg = config(route_models=source)
    source["route-b"] = "vendor/b"
    assert dict(cfg.route_models) == {ROUTE: MODEL}
    with pytest.raises(TypeError):
        cfg.route_models["route-c"] = "vendor/c"  # type: ignore[index]


def test_config_accepts_any_secret_value_at_the_limits() -> None:
    cfg = OpenRouterConfig(credential=PlainSecret("k" * 512), route_models={"r" * 64: "m" * 128})
    assert cfg.credential.get_secret_value() == "k" * 512
    assert dict(cfg.route_models) == {"r" * 64: "m" * 128}


def test_config_has_no_defaults() -> None:
    with pytest.raises(TypeError):
        OpenRouterConfig(credential=SecretStr(CREDENTIAL))  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        OpenRouterConfig(route_models={ROUTE: MODEL})  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("route_models", "error"),
    [
        ({}, ValueError),
        ([(ROUTE, MODEL)], TypeError),
        ("route-a", TypeError),
        ({"": MODEL}, ValueError),
        ({"route a": MODEL}, ValueError),
        ({"route/a": MODEL}, ValueError),
        ({"r" * 65: MODEL}, ValueError),
        ({1: MODEL}, ValueError),
        ({ROUTE: ""}, ValueError),
        ({ROUTE: "/vendor/model"}, ValueError),
        ({ROUTE: "vendor model"}, ValueError),
        ({ROUTE: "vendor/model\n"}, ValueError),
        ({ROUTE: "m" * 129}, ValueError),
        ({ROUTE: "vendor/modèle"}, ValueError),
        ({ROUTE: None}, ValueError),
    ],
)
def test_config_rejects_a_bad_route_map(route_models: object, error: type[Exception]) -> None:
    with pytest.raises(error):
        config(route_models=route_models)


@pytest.mark.parametrize(
    "secret",
    ["", "sk x", "sk\rx", "sk\nx", "sk\tx", "sk\x00x", "sk-é", "k" * 513, "sk\x7f"],
)
def test_config_rejects_a_bad_credential_without_quoting_it(secret: str) -> None:
    with pytest.raises(ValueError, match="visible ASCII") as info:
        config(credential=SecretStr(secret))
    if secret:
        assert secret not in str(info.value)
        assert secret not in repr(info.value)


@pytest.mark.parametrize("credential", [CREDENTIAL, object(), Revealing(12), Revealing(None)])
def test_config_rejects_what_is_not_a_secret_value(credential: object) -> None:
    with pytest.raises(TypeError) as info:
        config(credential=credential)
    assert CREDENTIAL not in str(info.value)


def test_config_and_provider_reprs_hide_the_credential() -> None:
    cfg = config()
    provider = provider_for(FakeTransport(), cfg=cfg)
    for text in (repr(cfg), str(cfg), repr(provider)):
        assert CREDENTIAL not in text
    assert "credential=***" in repr(cfg)


def test_the_provider_needs_a_config() -> None:
    not_a_config: Any = {"route_models": {ROUTE: MODEL}}
    with pytest.raises(TypeError, match="OpenRouterConfig"):
        OpenRouterProvider(not_a_config, FakeTransport())


def test_a_protocol_error_needs_a_defect_code() -> None:
    with pytest.raises(TypeError, match="ResponseDefect"):
        OpenRouterProtocolError("invalid_schema")  # type: ignore[arg-type]


# --- redaction --------------------------------------------------------------------------

SENTINEL_ERROR = {"code": 429, "message": REMOTE, "metadata": {"raw": RAW}}
TRUNCATED = choice(finish_reason="length", message={"content": REMOTE})
REDACTION_CASES = [
    pytest.param(401, encoded({"error": SENTINEL_ERROR}), id="status-401"),
    pytest.param(500, REMOTE.encode(), id="status-500"),
    pytest.param(200, encoded(success(error=SENTINEL_ERROR)), id="embedded-error"),
    pytest.param(200, encoded(success(error={"code": 9, "message": REMOTE})), id="unknown-error"),
    pytest.param(200, b"{" + REMOTE.encode(), id="malformed-json"),
    pytest.param(200, encoded(success(choices=[TRUNCATED])), id="truncated-content"),
    pytest.param(200, encoded(success(model=f"{REMOTE} x")), id="bad-model"),
    pytest.param(200, encoded(success(id=f"{REMOTE} x")), id="bad-id"),
    pytest.param(200, encoded(success(usage={"cost": REMOTE})), id="bad-usage"),
]


def assert_redacted(exc: BaseException) -> None:
    rendered = [
        str(exc),
        repr(exc),
        repr(exc.args),
        repr(vars(exc)),
        "".join(traceback.format_exception(exc)),
    ]
    for sentinel in (CREDENTIAL, RAW, REMOTE):
        for text in rendered:
            assert sentinel not in text


@pytest.mark.parametrize(("status", "body"), REDACTION_CASES)
def test_no_error_or_log_carries_the_credential_prompt_or_response(
    status: int, body: bytes, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    exc = failure_of(status, body)
    assert isinstance(exc, PermanentError | TransientError)
    assert_redacted(exc)
    assert caplog.records == []


@pytest.mark.parametrize("error", [TransportTimeout(), TransportFailure()])
def test_transport_failures_are_redacted_too(
    error: Exception, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    exc = raised(lambda: provider_for(FakeTransport(error=error)).call(request()))
    assert_redacted(exc)
    assert caplog.records == []


def test_a_successful_call_logs_nothing(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)
    provider, _ = answered(200, with_cost("0.5"))
    report = provider.call(request())
    assert caplog.records == []
    assert CREDENTIAL not in repr(report)
    assert RAW not in repr(report)
