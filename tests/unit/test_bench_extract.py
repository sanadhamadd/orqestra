"""Strict reply parsing, judging and scoring of the extraction workload (``benchmarks.extract``)."""

import json
import threading
import uuid
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest

from benchmarks import extract
from benchmarks.documents import Kind, generate_documents
from benchmarks.extract import (
    CANNED_PRICES,
    JOB_KIND,
    KEYS,
    SCORED_FIELDS,
    TOTAL_RULE,
    AttemptTime,
    DocumentRetry,
    Reading,
    Reason,
    Score,
    Unusable,
    canonical_reply,
    extraction_block,
    extraction_gate,
    extraction_handler,
    fixed_point_text,
    judge,
    parse_reply,
    request_text,
    score,
    score_all,
)
from benchmarks.ratelimit import LIMITER_MAX_WAIT
from orqestra.calls.provider import (
    PermanentCode,
    ProviderPermanentError,
    ProviderTransientError,
    ProviderUsage,
    TransientCode,
)
from orqestra.calls.recording import CallOutcome, InMemoryCallRecorder
from orqestra.domain.errors import PermanentError
from orqestra.domain.jobs import JsonValue
from orqestra.providers.openrouter import ExtractionReport, ExtractionRequest
from orqestra.worker.handlers import CancellationSignal, HandlerCancelled, TaskContext
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


# --- fixed-point text, the request text, the largest call ---------------------------


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (Decimal("3E-7"), "0.0000003"),
        (Decimal("0.000001") * 40, "0.000040"),
        (Decimal("1E+2"), "100"),
        (Decimal("12.50"), "12.50"),
    ],
)
def test_fixed_point_text_is_never_in_exponent_form(value: Decimal, text: str) -> None:
    assert fixed_point_text(value) == text
    assert "E" not in fixed_point_text(value).upper()


def test_the_request_text_is_exactly_what_the_adapter_sends() -> None:
    from orqestra.providers.openrouter import ExtractionRequest
    from tests.unit.test_openrouter_adapter import FakeExchange, FakeTransport, provider_for
    from tests.unit.test_providers_openrouter import replied

    document = DOCUMENTS[3].text + "\\u00e9 é"  # escapes and a non-ASCII character
    transport = FakeTransport(FakeExchange(200, [replied()]))
    provider_for(transport).extract(
        ExtractionRequest(route_id="route-a", document=document, timeout=timedelta(seconds=5))
    )
    (post,) = transport.posts
    sent = json.loads(post["body"])["messages"][-1]["content"]
    assert sent == request_text(document)


def _call(state: str, outcome: str | None, tokens: tuple[int | None, int | None]) -> Any:
    from benchmarks.extract import _Call

    return _Call(state, outcome, None, tokens[0], tokens[1], 10, None, None)


def test_the_largest_call_is_over_every_answered_call_of_every_attempt() -> None:
    from benchmarks.extract import _max_per_call

    answered = [
        _call("succeeded", "answered", (300, 90)),
        _call("revoked", "answered", (420, 60)),
        _call("succeeded", "answered", (310, 130)),
    ]
    assert _max_per_call(answered, 0) == {"input": 420, "output": 130}
    assert _max_per_call([], 0) == {"input": None, "output": None}
    assert _max_per_call(answered, 1) == {"input": None, "output": None}


# --- the per-document retry and the pace (DEC-073) -----------------------------------------

ROUTE = "route-a"
ATTEMPT = uuid.UUID(int=7)
CALL_TIMEOUT = timedelta(seconds=30)
ROOMY = AttemptTime(budget=timedelta(hours=1), per_call=timedelta(seconds=1))
THROTTLED = ProviderTransientError(TransientCode.RATE_LIMITED)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class Waits(threading.Event):
    """A cancellation event that records each wait and moves ``clock`` on, without sleeping."""

    def __init__(self, clock: Clock, cancel_on_wait: bool = False) -> None:
        super().__init__()
        self.clock = clock
        self.cancel_on_wait = cancel_on_wait
        self.waits: list[float] = []

    def wait(self, timeout: float | None = None) -> bool:
        assert timeout is not None
        self.waits.append(timeout)
        self.clock.now += timeout
        if self.cancel_on_wait:
            self.set()
        return self.is_set()


class Scripted:
    """Answers from a script, one step per request: a report, an exception, or ``perfect``."""

    def __init__(self, *steps: object) -> None:
        self.steps = list(steps)
        self.requests: list[ExtractionRequest] = []

    def extract(self, request: ExtractionRequest) -> ExtractionReport:
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        assert step == "perfect"
        document = next(d for d in DOCUMENTS if d.text == request.document)
        return ExtractionReport(
            content=canonical_reply(document.expected),
            usage=ProviderUsage("0.0002", "openrouter_credits"),
            model_served="vendor/model-a",
            generation_id="gen-1",
            input_tokens=300,
            output_tokens=90,
            latency_ms=800,
        )


class Pace:
    def __init__(self, *answers: object) -> None:
        self.answers = list(answers)
        self.asked: list[float] = []

    def reserve(self, max_wait: float) -> float | None:
        self.asked.append(max_wait)
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer  # type: ignore[return-value]


def run_batch(
    extractor: Scripted,
    count: int,
    *,
    cap: int = 100,
    retry: DocumentRetry | None = None,
    pace: Pace | None = None,
    attempt_time: AttemptTime | None = ROOMY,
    cancel_on_wait: bool = False,
    recorder: InMemoryCallRecorder | None = None,
    event: Waits | None = None,
) -> tuple[Any, InMemoryCallRecorder, Waits]:
    """Run one batch of the first ``count`` documents; the result (or the exception raised)."""
    event = event or Waits(Clock(), cancel_on_wait)
    clock = event.clock
    recorder = recorder or InMemoryCallRecorder()
    handle = extraction_handler(
        extractor,
        ROUTE,
        CALL_TIMEOUT,
        max_provider_calls_per_attempt=cap,
        recorder=recorder,
        retry=retry,
        pace=pace,
        attempt_time=attempt_time,
        clock=clock,
    )
    context = TaskContext(uuid.UUID(int=1), ATTEMPT, 1, JOB_KIND, CancellationSignal(event))
    payload: JsonValue = {"offset": 0, "items": [d.text for d in DOCUMENTS[:count]]}
    try:
        outcome: Any = handle(context, payload)
    except BaseException as exc:  # the test inspects it
        outcome = exc
    return outcome, recorder, event


def test_a_throttled_document_is_called_again_and_no_answered_one_is() -> None:
    extractor = Scripted("perfect", THROTTLED, THROTTLED, "perfect", "perfect")
    result, recorder, event = run_batch(extractor, 3, cap=5, retry=DocumentRetry())
    assert [item["status"] for item in result["results"]] == [
        d.expected_status.value for d in DOCUMENTS[:3]
    ]
    called = [r.document for r in extractor.requests]
    assert called == [DOCUMENTS[i].text for i in (0, 1, 1, 1, 2)]
    assert [(i.call_seq, i.item_index) for i in recorder.intents] == [
        (0, 0),
        (1, 1),
        (2, 1),
        (3, 1),
        (4, 2),
    ]
    assert [(r.outcome, r.failure_code) for r in recorder.results] == [
        (CallOutcome.ANSWERED, None),
        (CallOutcome.FAILED, TransientCode.RATE_LIMITED),
        (CallOutcome.FAILED, TransientCode.RATE_LIMITED),
        (CallOutcome.ANSWERED, None),
        (CallOutcome.ANSWERED, None),
    ]
    first, second = event.waits  # DEC-053's shape: [capped / 2, capped], 1 s then 2 s
    assert 0.5 <= first <= 1.0
    assert 1.0 <= second <= 2.0


def test_an_unavailable_provider_is_retried_too() -> None:
    extractor = Scripted(ProviderTransientError(TransientCode.UNAVAILABLE), "perfect")
    result, recorder, _ = run_batch(extractor, 1, cap=2, retry=DocumentRetry())
    assert result["results"][0]["status"] == DOCUMENTS[0].expected_status.value
    assert len(recorder.intents) == 2


@pytest.mark.parametrize(
    ("error", "raised"),
    [
        (ProviderTransientError(TransientCode.TIMEOUT), ProviderTransientError),
        (ProviderPermanentError(PermanentCode.REJECTED), ProviderPermanentError),
        (ValueError("anything else"), PermanentError),
    ],
)
def test_other_failures_end_the_attempt_as_before(
    error: Exception, raised: type[Exception]
) -> None:
    extractor = Scripted(error, "perfect")
    outcome, recorder, event = run_batch(extractor, 1, cap=5, retry=DocumentRetry())
    assert type(outcome) is raised
    assert len(recorder.intents) == 1
    assert event.waits == []


def test_without_a_retry_a_throttle_ends_the_attempt_as_before() -> None:
    outcome, recorder, _ = run_batch(Scripted(THROTTLED), 2, cap=5, retry=None)
    assert isinstance(outcome, ProviderTransientError)
    assert outcome.code is TransientCode.RATE_LIMITED
    assert len(recorder.intents) == 1


def test_a_retry_never_takes_the_slot_a_later_document_needs() -> None:
    # Three documents, cap 4: one spare slot. The first retry fits; the second would
    # leave a later document without a call, so the throttle ends the attempt instead.
    extractor = Scripted(THROTTLED, THROTTLED, "perfect")
    outcome, recorder, _ = run_batch(extractor, 3, cap=4, retry=DocumentRetry())
    assert isinstance(outcome, ProviderTransientError)
    assert len(recorder.intents) == 2
    # With the cap at the batch size there is no spare slot at all: no retry.
    outcome, recorder, _ = run_batch(Scripted(THROTTLED), 3, cap=3, retry=DocumentRetry())
    assert isinstance(outcome, ProviderTransientError)
    assert len(recorder.intents) == 1


def test_a_document_is_tried_at_most_the_retry_s_tries() -> None:
    extractor = Scripted(THROTTLED, THROTTLED, THROTTLED)
    outcome, recorder, event = run_batch(extractor, 1, cap=10, retry=DocumentRetry(tries=2))
    assert isinstance(outcome, ProviderTransientError)
    assert len(recorder.intents) == 2
    assert len(event.waits) == 1


def test_a_wait_that_would_not_leave_time_for_the_later_documents_is_not_taken() -> None:
    # 2 s of budget, 1 s per call: two documents fill it, so no backoff fits.
    tight = AttemptTime(budget=timedelta(seconds=2), per_call=timedelta(seconds=1))
    extractor = Scripted(THROTTLED, "perfect", "perfect")
    outcome, _, event = run_batch(extractor, 2, cap=10, retry=DocumentRetry(), attempt_time=tight)
    assert isinstance(outcome, ProviderTransientError)
    assert event.waits == []
    # And with no attempt time given, a retry never happens at all.
    outcome, _, event = run_batch(
        Scripted(THROTTLED), 1, cap=10, retry=DocumentRetry(), attempt_time=None
    )
    assert isinstance(outcome, ProviderTransientError)
    assert event.waits == []


def test_a_cancellation_during_the_backoff_stops_at_once() -> None:
    extractor = Scripted(THROTTLED, "perfect")
    outcome, recorder, _ = run_batch(
        extractor, 1, cap=5, retry=DocumentRetry(), cancel_on_wait=True
    )
    assert isinstance(outcome, HandlerCancelled)
    assert len(recorder.intents) == 1


def test_the_backoff_doubles_up_to_its_maximum_within_the_jitter() -> None:
    retry = DocumentRetry()
    for index in range(20):
        for tries in range(1, 9):
            capped = min(16.0, 2.0 ** (tries - 1))
            assert capped / 2 <= retry.wait(ATTEMPT, index, tries) <= capped
    assert retry.wait(ATTEMPT, 3, 2) == retry.wait(ATTEMPT, 3, 2)  # deterministic
    with pytest.raises(ValueError, match="tries"):
        DocumentRetry(tries=0)
    with pytest.raises(ValueError):  # noqa: PT011 - RetryPolicy's own message
        DocumentRetry(base=timedelta(seconds=5), maximum=timedelta(seconds=1))


class Logged(InMemoryCallRecorder):
    """Records into ``log`` too, beside the waits, so their order is visible."""

    def __init__(self, log: list[str]) -> None:
        super().__init__()
        self.log = log

    def record_intent(self, intent: Any) -> None:
        self.log.append("intent")
        super().record_intent(intent)


class LoggedWaits(Waits):
    def __init__(self, log: list[str]) -> None:
        super().__init__(Clock())
        self.log = log

    def wait(self, timeout: float | None = None) -> bool:
        self.log.append(f"wait {timeout}")
        return super().wait(timeout)


def test_a_paced_try_waits_for_its_slot_before_the_intent() -> None:
    # The order is pinned: the slot is waited first, then the intent (which reserves one
    # call from the cell's counter), then the call.
    log: list[str] = []
    pace = Pace(0.25, 0.0)
    result, recorder, _ = run_batch(
        Scripted("perfect", "perfect"),
        2,
        cap=5,
        retry=DocumentRetry(),
        pace=pace,
        recorder=Logged(log),
        event=LoggedWaits(log),
    )
    assert len(result["results"]) == 2
    assert log == ["wait 0.25", "intent", "intent"]  # a zero delay is not waited
    assert pace.asked == [LIMITER_MAX_WAIT, LIMITER_MAX_WAIT]
    assert len(recorder.intents) == 2


def test_no_slot_in_time_is_a_429_with_no_intent_and_no_call() -> None:
    pace = Pace(None, RuntimeError("the pace file is gone"), 0.0)
    extractor = Scripted("perfect")
    result, recorder, event = run_batch(extractor, 1, cap=5, retry=DocumentRetry(), pace=pace)
    assert result["results"][0]["status"] == DOCUMENTS[0].expected_status.value
    assert len(extractor.requests) == len(recorder.intents) == 1
    assert len(event.waits) == 2  # two backoffs, after the two missing slots
    # With no retry left, a missing slot ends the attempt as a 429 would.
    outcome, recorder, _ = run_batch(Scripted(), 1, cap=5, retry=None, pace=Pace(None))
    assert isinstance(outcome, ProviderTransientError)
    assert outcome.code is TransientCode.RATE_LIMITED
    assert recorder.intents == ()


def test_pacing_waits_inside_its_own_allowance_and_never_fails_the_attempt() -> None:
    # The execution time is sized as the harness sizes a paced cell: a call per cap slot
    # (no spare slot here) plus LIMITER_MAX_WAIT per document. Each slot is 9.5 s away.
    sized = AttemptTime(
        budget=timedelta(seconds=3 * 1 + 3 * LIMITER_MAX_WAIT), per_call=timedelta(seconds=1)
    )
    pace = Pace(9.5, 9.5, 9.5)
    result, recorder, event = run_batch(
        Scripted("perfect", "perfect", "perfect"),
        3,
        cap=3,
        retry=DocumentRetry(),
        pace=pace,
        attempt_time=sized,
    )
    assert [item["status"] for item in result["results"]] == [
        d.expected_status.value for d in DOCUMENTS[:3]
    ]
    assert event.waits == [9.5, 9.5, 9.5]
    assert all(asked >= 9.5 for asked in pace.asked)
    assert len(recorder.intents) == 3


def test_the_pace_s_wait_is_bounded_by_the_attempt_s_time() -> None:
    tight = AttemptTime(budget=timedelta(seconds=5), per_call=timedelta(seconds=1))
    pace = Pace(0.0, 0.0)
    run_batch(Scripted("perfect", "perfect"), 2, pace=pace, attempt_time=tight)
    # The first document's slot may use only what is left after two calls and the later
    # document's own allowance: nothing. The last keeps 5 s less its call.
    assert pace.asked == [0.0, 4.0]
    with pytest.raises(ValueError, match="attempt_time"):
        extraction_handler(
            Scripted(),
            ROUTE,
            CALL_TIMEOUT,
            max_provider_calls_per_attempt=5,
            recorder=InMemoryCallRecorder(),
            pace=pace,
        )


# --- the gate and the block, in partial and retried cells -----------------------------------


class Rows:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def __iter__(self) -> Any:
        return iter(self.rows)

    def fetchone(self) -> tuple[Any, ...]:
        return self.rows[0]


class FakeConn:
    """Answers the block's and the gate's queries from fixed rows."""

    def __init__(
        self,
        items: list[dict[str, Any]],
        calls: list[tuple[Any, ...]],
        attempts: int,
        tasks_attempted: int,
    ) -> None:
        self.answers = {
            extract._ITEM_RESULTS: [({"offset": 0, "results": items},)],
            extract._CALLS: calls,
            extract._ATTEMPTS: [(attempts, 10.0)],
            extract._TASKS_ATTEMPTED: [(tasks_attempted,)],
            extract._OPEN_CLOSURES: [(0,)],
        }

    def execute(self, query: str, params: Any = None) -> Rows:
        return Rows(self.answers[query])


def call(
    state: str, outcome: str | None, code: str | None = None, *, again: bool = False
) -> tuple[Any, ...]:
    tokens = (300, 90) if outcome == "answered" else (None, None)
    return (state, outcome, code, *tokens, 800 if outcome else None, None, None, again)


def item(index: int, fields: Any = None, status: str | None = None) -> dict[str, Any]:
    document = DOCUMENTS[index]
    return {
        "index": index,
        "status": status or document.expected_status.value,
        "reason": None,
        "failed_rules": [],
        "fields": fields if fields is not None else stored(canonical_reply(document.expected)),
    }


def test_a_partial_cell_is_measured_per_processed_document() -> None:
    expected = [d.expected for d in DOCUMENTS[:4]]
    statuses = [d.expected_status for d in DOCUMENTS[:4]]
    # Two of four documents ran, in one task of two; one call was a retried 429.
    calls = [
        call("succeeded", "answered"),
        call("succeeded", "failed", "rate_limited", again=True),
        call("succeeded", "answered"),
    ]
    conn = FakeConn([item(0), item(1)], calls, attempts=1, tasks_attempted=1)
    block = extraction_block(conn, uuid.UUID(int=3), expected, statuses, CANNED_PRICES)
    assert block["documents"] == 4
    assert block["documents_processed"] == 2
    assert block["accuracy"]["overall"] == 1.0
    assert block["accuracy"]["exact_documents"] == 2
    assert block["retries"] == 0  # never negative: the unclaimed task is not counted
    assert block["calls"]["retried_in_attempt"] == 1
    cost = Decimal(block["cost"]["total"])
    assert Decimal(block["cost"]["per_1000_invoices"]) == cost * 1000 / 2


def test_a_cell_with_nothing_processed_shows_its_spend_over_every_document() -> None:
    expected = [d.expected for d in DOCUMENTS[:4]]
    statuses = [d.expected_status for d in DOCUMENTS[:4]]
    conn = FakeConn([], [call("revoked", "answered")], attempts=1, tasks_attempted=1)
    block = extraction_block(conn, uuid.UUID(int=3), expected, statuses, CANNED_PRICES)
    assert block["documents_processed"] == 0
    assert block["accuracy"]["overall"] is None
    cost = Decimal(block["cost"]["total"])
    assert Decimal(block["cost"]["per_1000_invoices"]) == cost * 1000 / 4


def gate_of(calls: list[tuple[Any, ...]]) -> dict[str, Any]:
    conn = FakeConn([item(0), item(1)], calls, attempts=1, tasks_attempted=1)
    checks = extraction_gate(conn, uuid.UUID(int=3), 2, clean=False)
    return next(check for check in checks if check["name"] == "extraction_usage")


def test_the_gate_accepts_retried_throttles_and_nothing_else() -> None:
    retried = call("succeeded", "failed", "rate_limited", again=True)
    passed = gate_of([call("succeeded", "answered"), retried, call("succeeded", "answered")])
    assert passed["passed"]
    assert passed["detail"]["succeeded_retried"] == 1
    for other in (
        call("succeeded", "failed", "timeout", again=True),  # never retried
        call("succeeded", "failed", "rate_limited"),  # not followed by another try
        call("succeeded", None),  # unresolved
    ):
        check = gate_of([call("succeeded", "answered"), other, call("succeeded", "answered")])
        assert not check["passed"]
