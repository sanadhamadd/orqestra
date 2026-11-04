import json
import threading
import uuid
from typing import Any

import pytest

from orqestra.ai.mock import MockProvider
from orqestra.ai.provider import (
    PermanentCode,
    Provider,
    ProviderPermanentError,
    ProviderRequest,
    ProviderResponse,
    ProviderTransientError,
    ProviderUsage,
    TransientCode,
)
from orqestra.ai.recording import (
    MAX_CALL_SEQ,
    MAX_COST_UNITS,
    InMemoryCallRecorder,
    ProviderCallIntent,
    ProviderCallResult,
)
from orqestra.ai.routing import RouteOption
from orqestra.domain.jobs import AttemptCancelled, AttemptFailed, AttemptSucceeded, JsonValue
from orqestra.domain.policy import FailureKind
from orqestra.worker.handlers import (
    CancellationSignal,
    Handler,
    HandlerRegistry,
    TaskContext,
    run_handler,
)
from orqestra.workloads.invoice.handler import (
    JOB_KIND,
    billable_routes,
    invoice_verification_handler,
)
from orqestra.workloads.invoice.verify import REQUIRED_CAPABILITY
from tests.unit.test_invoice_verify import (
    ROUTE,
    SENTINEL,
    TIMEOUT,
    ambiguous_quantity_and_unit,
    ambiguous_unit,
    known,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]


ATTEMPT = uuid.UUID(int=2)


def context(cancel: threading.Event | None = None, attempt_id: uuid.UUID = ATTEMPT) -> TaskContext:
    return TaskContext(
        uuid.UUID(int=1),
        attempt_id,
        1,
        JOB_KIND,
        CancellationSignal(cancel or threading.Event()),
    )


PERMANENT_ERROR = "orqestra.domain.errors.PermanentError"
PROVIDER_TRANSIENT = "orqestra.ai.provider.ProviderTransientError"
PROVIDER_PERMANENT = "orqestra.ai.provider.ProviderPermanentError"
CAP = 64  # more calls than any batch here needs


def handler_for(provider: Provider, **kwargs: Any) -> Handler:
    """The handler with a sufficient cap and, unless a test chooses, an in-memory recorder."""
    kwargs.setdefault("max_provider_calls_per_attempt", CAP)
    if "non_billable_routes" not in kwargs:
        kwargs.setdefault("recorder", InMemoryCallRecorder())
    return invoice_verification_handler({"r1": provider}, [ROUTE], TIMEOUT, **kwargs)


def batch(offset: int, *items: Any) -> dict[str, JsonValue]:
    return {"offset": offset, "items": list(items)}


def rejected_invoice() -> dict[str, Any]:
    raw = known()
    raw["total"] = 14
    return raw


def quantity_only_ambiguous() -> dict[str, Any]:
    raw = known()
    raw["lines"][0]["quantity"] = "two"
    return raw


def finding(rule: str, path: str, verdict: str) -> dict[str, str]:
    return {"rule": rule, "verdict": verdict, "path": path}


def quantity(index: int, verdict: str) -> dict[str, str]:
    return finding("quantity_positive", f"lines[{index}].quantity", verdict)


def total(verdict: str) -> dict[str, str]:
    return finding("total_matches_lines", "total", verdict)


def item(index: int, status: str, **fields: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "index": index,
        "status": status,
        "provider_assisted": False,
        "findings": [],
        "issues": [],
        "reason": None,
        "routing": None,
    }
    result.update(fields)
    return result


# The stored result for the five items of ``golden_batch``; it is the contract.
GOLDEN: dict[str, Any] = {
    "offset": 10,
    "results": [
        item(10, "verified", findings=[quantity(0, "pass"), quantity(1, "pass"), total("pass")]),
        item(11, "rejected", findings=[quantity(0, "pass"), quantity(1, "pass"), total("fail")]),
        item(12, "malformed", issues=[{"code": "not_an_object", "path": "$"}]),
        item(
            13,
            "verified",
            provider_assisted=True,
            findings=[quantity(0, "pass"), quantity(1, "pass"), total("pass")],
            routing={"route_id": "r1"},
        ),
        item(
            14,
            "needs_review",
            findings=[quantity(0, "indeterminate"), quantity(1, "pass"), total("indeterminate")],
            reason="provider_response_invalid",
            routing={"route_id": "r1"},
        ),
    ],
}


def golden_batch() -> dict[str, JsonValue]:
    return batch(
        10,
        known(),
        rejected_invoice(),
        "not an invoice",
        ambiguous_unit(),
        quantity_only_ambiguous(),
    )


def run_golden() -> JsonValue:
    handler = handler_for(MockProvider(["5", "oops"]))
    outcome = run_handler(handler, context(), golden_batch())
    assert isinstance(outcome, AttemptSucceeded)
    return outcome.result


def test_the_stored_result_for_a_mixed_batch_is_the_golden_result() -> None:
    assert run_golden() == GOLDEN


def test_two_fresh_runs_give_byte_identical_json() -> None:
    first = json.dumps(run_golden(), sort_keys=True)
    assert first == json.dumps(run_golden(), sort_keys=True)
    assert first == json.dumps(GOLDEN, sort_keys=True)


def test_the_result_is_a_valid_json_task_result_and_the_registry_accepts_the_handler() -> None:
    handler = handler_for(MockProvider(["5", "oops"]))
    assert HandlerRegistry({JOB_KIND: handler}).kinds == (JOB_KIND,)
    assert isinstance(run_handler(handler, context(), golden_batch()), AttemptSucceeded)


def test_the_stored_result_has_no_usage_cost_count_identifier_or_provider_text() -> None:
    mock = MockProvider([ProviderResponse("5", 4242)])
    route = RouteOption("r1", frozenset({REQUIRED_CAPABILITY}), 777)
    handler = invoice_verification_handler(
        {"r1": mock},
        [route],
        TIMEOUT,
        max_provider_calls_per_attempt=CAP,
        recorder=InMemoryCallRecorder(),
    )
    outcome = run_handler(handler, context(), batch(0, ambiguous_unit()))
    assert isinstance(outcome, AttemptSucceeded)
    text = json.dumps(outcome.result)
    for forbidden in (
        "reported_cost_units",
        "estimated_cost_units",
        "provider_calls",
        "4242",
        "777",
        "INV-1",
        "L1",
        "five bucks",
    ):
        assert forbidden not in text
    assert isinstance(outcome.result, dict)
    results: Any = outcome.result["results"]
    (result,) = results
    assert result["routing"] == {"route_id": "r1"}
    assert set(result) == {
        "index",
        "status",
        "provider_assisted",
        "findings",
        "issues",
        "reason",
        "routing",
    }


def test_indices_follow_the_batch_offset() -> None:
    outcome = run_handler(handler_for(MockProvider()), context(), batch(7, known(), known()))
    assert isinstance(outcome, AttemptSucceeded)
    assert isinstance(outcome.result, dict)
    results: Any = outcome.result["results"]
    assert outcome.result["offset"] == 7
    assert [r["index"] for r in results] == [7, 8]


def test_an_empty_batch_has_no_results() -> None:
    outcome = run_handler(handler_for(MockProvider()), context(), batch(0))
    assert outcome == AttemptSucceeded({"offset": 0, "results": []})


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        "batch",
        7,
        {},
        {"offset": 0},
        {"items": []},
        {"offset": 0, "items": [], "extra": 1},
        {"offset": True, "items": []},
        {"offset": -1, "items": []},
        {"offset": 1.5, "items": []},
        {"offset": "0", "items": []},
        {"offset": 0, "items": "abc"},
        {"offset": 0, "items": {"a": 1}},
        {"offset": 0, "items": None},
    ],
)
def test_a_payload_that_is_not_a_batch_fails_permanently(payload: JsonValue) -> None:
    outcome = run_handler(handler_for(MockProvider()), context(), payload)
    assert outcome == AttemptFailed(FailureKind.PERMANENT, PERMANENT_ERROR, "batch_payload_invalid")


def test_a_bad_item_never_fails_the_batch() -> None:
    outcome = run_handler(
        handler_for(MockProvider()), context(), batch(0, None, 5, [], {"a": 1}, "x")
    )
    assert isinstance(outcome, AttemptSucceeded)
    assert isinstance(outcome.result, dict)
    results: Any = outcome.result["results"]
    assert [r["status"] for r in results] == ["malformed"] * 5


def test_cancellation_before_an_item_ends_the_attempt_as_cancelled() -> None:
    cancel = threading.Event()
    cancel.set()
    mock = MockProvider(["5"])
    outcome = run_handler(handler_for(mock), context(cancel), batch(0, ambiguous_unit()))
    assert outcome == AttemptCancelled()
    assert mock.requests == ()


class CancelsWhenAsked:
    """A provider that records that cancellation was requested while it was being called."""

    def __init__(self, event: threading.Event) -> None:
        self.event = event
        self.calls = 0

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        self.calls += 1
        self.event.set()
        return ProviderResponse("5")


def test_cancellation_between_items_stops_before_the_next_item() -> None:
    cancel = threading.Event()
    provider = CancelsWhenAsked(cancel)
    outcome = run_handler(
        handler_for(provider), context(cancel), batch(0, ambiguous_unit(), ambiguous_unit())
    )
    assert outcome == AttemptCancelled()
    assert provider.calls == 1


def test_cancellation_between_provider_calls_stops_before_the_next_call() -> None:
    cancel = threading.Event()
    provider = CancelsWhenAsked(cancel)
    outcome = run_handler(
        handler_for(provider), context(cancel), batch(0, ambiguous_quantity_and_unit())
    )
    assert outcome == AttemptCancelled()
    assert provider.calls == 1


def test_a_cancellation_before_the_first_item_records_nothing() -> None:
    cancel = threading.Event()
    cancel.set()
    mock = MockProvider(["5"])
    recorder = InMemoryCallRecorder()
    handler = handler_for(mock, recorder=recorder)
    outcome = run_handler(handler, context(cancel), batch(0, ambiguous_unit()))
    assert outcome == AttemptCancelled()
    assert mock.requests == ()
    assert recorder.events == ()


@pytest.mark.parametrize(
    "items",
    [
        (ambiguous_unit(), ambiguous_unit()),  # stopped before the next item
        (ambiguous_quantity_and_unit(),),  # stopped before the next call of the same item
    ],
)
def test_a_cancellation_leaves_earlier_calls_resolved_and_records_no_later_intent(
    items: tuple[dict[str, Any], ...],
) -> None:
    cancel = threading.Event()
    provider = CancelsWhenAsked(cancel)
    recorder = InMemoryCallRecorder()
    handler = handler_for(provider, recorder=recorder)
    outcome = run_handler(handler, context(cancel), batch(0, *items))
    assert outcome == AttemptCancelled()
    assert provider.calls == 1
    assert [(i.attempt_id, i.call_seq, i.item_index) for i in recorder.intents] == [
        (ATTEMPT, 0, 0),
    ]
    assert [r.call_seq for r in recorder.results] == [0]
    assert recorder.unresolved() == ()


def test_a_transient_provider_error_fails_the_batch_as_transient_with_only_its_code() -> None:
    mock = MockProvider([ProviderTransientError(TransientCode.UNAVAILABLE)])
    outcome = run_handler(handler_for(mock), context(), batch(0, known(), ambiguous_unit()))
    assert outcome == AttemptFailed(FailureKind.TRANSIENT, PROVIDER_TRANSIENT, "unavailable")


def test_a_permanent_provider_error_fails_the_batch_as_permanent_with_only_its_code() -> None:
    mock = MockProvider([ProviderPermanentError(PermanentCode.UNSUPPORTED_REQUEST)])
    outcome = run_handler(handler_for(mock), context(), batch(0, ambiguous_unit()))
    expected = AttemptFailed(FailureKind.PERMANENT, PROVIDER_PERMANENT, "unsupported_request")
    assert outcome == expected


def test_an_unexpected_provider_exception_never_reaches_the_stored_failure() -> None:
    mock = MockProvider([RuntimeError(SENTINEL)])
    outcome = run_handler(handler_for(mock), context(), batch(0, ambiguous_unit()))
    expected = AttemptFailed(FailureKind.PERMANENT, PERMANENT_ERROR, "provider_protocol_violation")
    assert outcome == expected
    assert SENTINEL not in repr(outcome)


def test_the_cost_ceiling_reaches_the_routing_decision() -> None:
    mock = MockProvider(["5"])
    outcome = run_handler(
        handler_for(mock, max_cost_units=2), context(), batch(0, ambiguous_unit())
    )
    assert isinstance(outcome, AttemptSucceeded)
    assert isinstance(outcome.result, dict)
    results: Any = outcome.result["results"]
    (result,) = results
    assert (result["status"], result["reason"], result["routing"]) == (
        "needs_review",
        "no_eligible_route",
        None,
    )
    assert mock.requests == ()


def test_the_handler_keeps_its_own_copy_of_the_providers() -> None:
    providers = {"r1": MockProvider(["5"])}
    handler = invoice_verification_handler(
        providers,
        [ROUTE],
        TIMEOUT,
        max_provider_calls_per_attempt=CAP,
        recorder=InMemoryCallRecorder(),
    )
    providers.clear()
    outcome = run_handler(handler, context(), batch(0, ambiguous_unit()))
    assert isinstance(outcome, AttemptSucceeded)


def test_the_factory_refuses_bad_wiring_at_once() -> None:
    mock = MockProvider()
    wired: dict[str, Any] = {
        "max_provider_calls_per_attempt": CAP,
        "recorder": InMemoryCallRecorder(),
    }
    with pytest.raises(ValueError, match="has no provider"):
        invoice_verification_handler({}, [ROUTE], TIMEOUT, **wired)
    with pytest.raises(ValueError, match="positive"):
        invoice_verification_handler({"r1": mock}, [ROUTE], TIMEOUT * 0, **wired)
    with pytest.raises(ValueError, match="max_cost_units"):
        invoice_verification_handler({"r1": mock}, [ROUTE], TIMEOUT, max_cost_units=-1, **wired)
    not_routes: Any = ["r1"]
    with pytest.raises(TypeError, match="RouteOption"):
        invoice_verification_handler({"r1": mock}, not_routes, TIMEOUT, **wired)


class ClaimsToBeFree:
    """A provider that declares itself non-billable; the declaration must be ignored."""

    billable = False

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        return ProviderResponse("5")


class ScriptedSubclass(MockProvider):
    """Not the offline mock itself, so it may not be marked non-billable."""


def test_every_provider_is_billable_by_default() -> None:
    providers: dict[str, Provider] = {
        "r1": MockProvider(),
        "r2": ClaimsToBeFree(),
        "r3": ScriptedSubclass(),
    }
    assert billable_routes(providers, frozenset()) == frozenset({"r1", "r2", "r3"})


def test_only_a_marked_offline_mock_is_non_billable() -> None:
    providers: dict[str, Provider] = {"r1": MockProvider(), "r2": MockProvider()}
    assert billable_routes(providers, frozenset({"r1"})) == frozenset({"r2"})
    assert billable_routes(providers, frozenset({"r1", "r2"})) == frozenset()


@pytest.mark.parametrize(
    ("provider", "match"),
    [
        (ClaimsToBeFree(), "not the offline mock"),
        (CancelsWhenAsked(threading.Event()), "not the offline mock"),
        (ScriptedSubclass(), "not the offline mock"),
    ],
)
def test_the_factory_refuses_to_mark_anything_but_the_offline_mock(
    provider: Provider, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        handler_for(provider, non_billable_routes=frozenset({"r1"}))
    with pytest.raises(ValueError, match=match):
        billable_routes({"r1": provider}, frozenset({"r1"}))


def test_the_factory_refuses_an_unknown_non_billable_route() -> None:
    with pytest.raises(ValueError, match="has no provider"):
        handler_for(MockProvider(), non_billable_routes=frozenset({"r9"}))


@pytest.mark.parametrize(
    ("marked", "match"),
    [
        ("r1", "frozenset"),
        ({"r1"}, "frozenset"),
        (["r1"], "frozenset"),
        (None, "frozenset"),
        (frozenset({1}), "entries must be str"),
        (frozenset({"r1", 1}), "entries must be str"),
    ],
)
def test_the_factory_refuses_non_billable_routes_of_the_wrong_type(
    marked: object, match: str
) -> None:
    with pytest.raises(TypeError, match=match):
        handler_for(MockProvider(), non_billable_routes=marked)


def test_a_marked_mock_still_gives_the_golden_result() -> None:
    handler = handler_for(MockProvider(["5", "oops"]), non_billable_routes=frozenset({"r1"}))
    outcome = run_handler(handler, context(), golden_batch())
    assert outcome == AttemptSucceeded(GOLDEN)


def test_provider_reported_usage_never_changes_or_reaches_the_stored_result() -> None:
    usage = ProviderUsage("0.25", "credits")
    with_usage = run_handler(
        handler_for(MockProvider([ProviderResponse("5", usage=usage)])),
        context(),
        batch(0, ambiguous_unit()),
    )
    without_usage = run_handler(
        handler_for(MockProvider([ProviderResponse("5")])),
        context(),
        batch(0, ambiguous_unit()),
    )
    assert isinstance(with_usage, AttemptSucceeded)
    assert isinstance(without_usage, AttemptSucceeded)
    text = json.dumps(with_usage.result, sort_keys=True)
    assert text == json.dumps(without_usage.result, sort_keys=True)
    for forbidden in ("0.25", "credits", "usage"):
        assert forbidden not in text


# --- the per-attempt cap and the recorder ------------------------------------------------


def results_of(outcome: object) -> list[Any]:
    assert isinstance(outcome, AttemptSucceeded)
    assert isinstance(outcome.result, dict)
    results: Any = outcome.result["results"]
    return list(results)


def test_a_marked_mock_gives_byte_identical_results_with_and_without_a_recorder() -> None:
    marked = frozenset({"r1"})
    recorder = InMemoryCallRecorder()
    texts: list[str] = []
    for chosen in (None, recorder):
        handler = handler_for(
            MockProvider(["5", "oops"]), non_billable_routes=marked, recorder=chosen
        )
        outcome = run_handler(handler, context(), golden_batch())
        assert isinstance(outcome, AttemptSucceeded)
        texts.append(json.dumps(outcome.result, sort_keys=True))
    assert texts[0] == texts[1] == json.dumps(GOLDEN, sort_keys=True)
    assert len(recorder.intents) == 2  # a marked mock is still recorded when a recorder is given


def test_every_call_of_a_batch_is_recorded_under_the_attempt_with_absolute_indices() -> None:
    recorder = InMemoryCallRecorder()
    handler = handler_for(MockProvider(["5", "oops"]), recorder=recorder)
    run_handler(handler, context(), golden_batch())
    assert [(i.attempt_id, i.call_seq, i.item_index) for i in recorder.intents] == [
        (ATTEMPT, 0, 13),
        (ATTEMPT, 1, 14),
    ]
    assert [r.call_seq for r in recorder.results] == [0, 1]
    assert recorder.unresolved() == ()


def test_the_cap_spans_the_batch_and_is_checked_per_invoice() -> None:
    recorder = InMemoryCallRecorder()
    mock = MockProvider(["2", "5", "5"])
    handler = handler_for(mock, max_provider_calls_per_attempt=3, recorder=recorder)
    items = (ambiguous_quantity_and_unit(), ambiguous_quantity_and_unit(), ambiguous_unit())
    results = results_of(run_handler(handler, context(), batch(20, *items)))
    routed = {"route_id": "r1"}
    assert [(r["index"], r["status"], r["reason"], r["routing"]) for r in results] == [
        (20, "verified", None, routed),
        (21, "needs_review", "provider_call_cap_reached", routed),
        (22, "verified", None, routed),
    ]
    assert not results[1]["provider_assisted"]
    assert [(i.call_seq, i.item_index) for i in recorder.intents] == [(0, 20), (1, 20), (2, 22)]
    assert len(mock.requests) == 3


def test_each_run_is_one_attempt_with_its_own_cap_and_numbering() -> None:
    recorder = InMemoryCallRecorder()
    mock = MockProvider(["5"])
    handler = handler_for(mock, max_provider_calls_per_attempt=1, recorder=recorder)
    other = uuid.UUID(int=3)
    for attempt_id in (ATTEMPT, other):
        outcome = run_handler(handler, context(attempt_id=attempt_id), batch(0, ambiguous_unit()))
        assert [r["status"] for r in results_of(outcome)] == ["verified"]
    assert [(i.attempt_id, i.call_seq) for i in recorder.intents] == [(ATTEMPT, 0), (other, 0)]


def test_a_cap_of_zero_allows_deterministic_results_only() -> None:
    mock = MockProvider(["5"])
    handler = handler_for(mock, max_provider_calls_per_attempt=0)
    items = (known(), rejected_invoice(), ambiguous_unit())
    results = results_of(run_handler(handler, context(), batch(0, *items)))
    assert [(r["status"], r["reason"]) for r in results] == [
        ("verified", None),
        ("rejected", None),
        ("needs_review", "provider_call_cap_reached"),
    ]
    assert mock.requests == ()


@pytest.mark.parametrize(
    ("cap", "error"),
    [
        (True, TypeError),
        (False, TypeError),
        (1.0, TypeError),
        ("1", TypeError),
        (None, TypeError),
        (-1, ValueError),
        (MAX_CALL_SEQ + 1, ValueError),
    ],
)
def test_the_factory_refuses_a_bad_cap(cap: object, error: type[Exception]) -> None:
    with pytest.raises(error, match="max_provider_calls_per_attempt"):
        handler_for(MockProvider(), max_provider_calls_per_attempt=cap)


def test_the_cap_is_required_and_its_whole_range_is_accepted() -> None:
    factory: Any = invoice_verification_handler
    with pytest.raises(TypeError, match="max_provider_calls_per_attempt"):
        factory({"r1": MockProvider()}, [ROUTE], TIMEOUT, recorder=InMemoryCallRecorder())
    handler_for(MockProvider(), max_provider_calls_per_attempt=0)
    handler_for(MockProvider(), max_provider_calls_per_attempt=MAX_CALL_SEQ)


@pytest.mark.parametrize("provider", [MockProvider(), ClaimsToBeFree(), ScriptedSubclass()])
def test_a_billable_provider_without_a_recorder_is_refused(provider: Provider) -> None:
    with pytest.raises(ValueError, match="needs a recorder"):
        invoice_verification_handler(
            {"r1": provider}, [ROUTE], TIMEOUT, max_provider_calls_per_attempt=CAP
        )


def test_one_unmarked_provider_is_enough_to_require_a_recorder() -> None:
    providers: dict[str, Provider] = {"r1": MockProvider(), "r2": MockProvider()}
    routes = [ROUTE, RouteOption("r2", frozenset({REQUIRED_CAPABILITY}), 9)]
    with pytest.raises(ValueError, match="needs a recorder"):
        invoice_verification_handler(
            providers,
            routes,
            TIMEOUT,
            max_provider_calls_per_attempt=CAP,
            non_billable_routes=frozenset({"r1"}),
        )


@pytest.mark.parametrize("recorder", [object(), [], "recorder"])
def test_the_factory_refuses_a_recorder_of_the_wrong_type(recorder: object) -> None:
    with pytest.raises(TypeError, match="ProviderCallRecorder"):
        handler_for(MockProvider(), recorder=recorder)
    with pytest.raises(TypeError, match="ProviderCallRecorder"):
        handler_for(MockProvider(), recorder=recorder, non_billable_routes=frozenset({"r1"}))


class RecorderDownError(Exception):
    pass


class Refuses:
    """A recorder that cannot record an intent."""

    def record_intent(self, intent: ProviderCallIntent) -> None:
        raise RecorderDownError("recorder_down")

    def record_result(self, result: ProviderCallResult) -> None:
        raise AssertionError("no result without an intent")


def test_a_recorder_failure_fails_the_task_as_the_recorders_error_and_no_call_is_made() -> None:
    mock = MockProvider(["5"])
    outcome = run_handler(
        handler_for(mock, recorder=Refuses()), context(), batch(0, ambiguous_unit())
    )
    error_type = f"{RecorderDownError.__module__}.RecorderDownError"
    assert outcome == AttemptFailed(FailureKind.UNCLASSIFIED, error_type, "recorder_down")
    assert mock.requests == ()


def test_an_estimate_beyond_the_recordable_range_fails_the_task_before_any_call() -> None:
    mock = MockProvider(["5"])
    recorder = InMemoryCallRecorder()
    route = RouteOption("r1", frozenset({REQUIRED_CAPABILITY}), MAX_COST_UNITS + 1)
    handler = invoice_verification_handler(
        {"r1": mock},
        [route],
        TIMEOUT,
        max_provider_calls_per_attempt=CAP,
        recorder=recorder,
    )
    outcome = run_handler(handler, context(), batch(0, ambiguous_unit()))
    assert isinstance(outcome, AttemptFailed)
    assert (outcome.kind, outcome.error_type) == (FailureKind.UNCLASSIFIED, "builtins.ValueError")
    assert mock.requests == ()
    assert recorder.events == ()


def test_an_item_index_beyond_the_recordable_range_fails_the_task_before_any_call() -> None:
    mock = MockProvider(["5"])
    recorder = InMemoryCallRecorder()
    handler = handler_for(mock, recorder=recorder)
    outcome = run_handler(handler, context(), batch(2**31, ambiguous_unit()))
    assert isinstance(outcome, AttemptFailed)
    assert (outcome.kind, outcome.error_type) == (FailureKind.UNCLASSIFIED, "builtins.ValueError")
    assert mock.requests == ()
    assert recorder.events == ()
