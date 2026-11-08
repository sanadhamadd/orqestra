"""The worker's durable usage sink (DEC-059): scheduling, handoff, conversions and limits.

Offline: the supervisor's ledger writes are injected, and the handler's side runs
on real threads against a fake clock. The PostgreSQL behaviour is in
``tests/integration/test_worker_usage_pg.py``.
"""

import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg import errors

from orqestra.calls.provider import FieldKind, PermanentCode, ProviderUsage, TransientCode
from orqestra.calls.recording import (
    MAX_CALL_SEQ,
    CallOutcome,
    ProviderCallIntent,
    ProviderCallRecorder,
    ProviderCallResult,
)
from orqestra.domain.jobs import AttemptCancelled, AttemptSucceeded, JsonValue
from orqestra.domain.states import AttemptState
from orqestra.logging import JsonFormatter
from orqestra.persistence.lifecycle import CancellationRequestedError, NotFoundError
from orqestra.persistence.provider_usage import (
    CallFailure,
    CallIntent,
    CallIntentNotFoundError,
    CallResult,
    CaptureNotCompleteError,
    CaptureState,
    CaptureTier,
    ClosedBy,
    Recorded,
    StoredCall,
    StoredResult,
    UsageClosure,
    UsageReplayConflictError,
)
from orqestra.worker import runtime
from orqestra.worker.fence import ExitCode, Expiry, Watchdog
from orqestra.worker.handlers import HandlerCancelled, HandlerRegistry, TaskContext
from orqestra.worker.runtime import Step, Timing, Worker, next_step
from orqestra.worker.usage import (
    Reply,
    UsageHandoffExpiredError,
    UsageSink,
    check_provider_call_budget,
    ledger_result,
    representable_amount,
    usage_handoff_bound,
)
from tests.unit.test_worker_runtime import (
    SETTINGS,
    T0,
    FakeClock,
    FakeConn,
    Terminated,
    attempt,
    raising,
    terminate,
    timing,
)

pytestmark = pytest.mark.unit

ATTEMPT = uuid.UUID(int=1)  # attempt()'s id
CONTEXT = {"worker_id": "worker-1", "task_id": str(uuid.UUID(int=2)), "attempt_id": str(ATTEMPT)}
WAIT = 10.0


def intent(
    seq: int = 0,
    *,
    attempt_id: uuid.UUID = ATTEMPT,
    route: str = "route-a",
    capability: str = "interpret_amount",
) -> ProviderCallIntent:
    return ProviderCallIntent(
        attempt_id=attempt_id,
        call_seq=seq,
        item_index=10 + seq,
        route_id=route,
        capability=capability,
        field_kind=FieldKind.TOTAL,
        estimated_cost_units=5,
    )


def result(
    seq: int = 0, *, usage: ProviderUsage | None = None, attempt_id: uuid.UUID = ATTEMPT
) -> ProviderCallResult:
    return ProviderCallResult(
        attempt_id=attempt_id, call_seq=seq, outcome=CallOutcome.ANSWERED, usage=usage
    )


def new_sink(kinds: tuple[str, ...] = ("k",)) -> UsageSink:
    return UsageSink(kinds, {"route-a": "provider-a"}, {"route-a": "model-a"}, "prompt-v1")


# --- scheduling ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        # a due heartbeat comes first
        ({"running": True, "heartbeat_due": 100.0, "usage_due": 100.0}, (Step.HEARTBEAT, 0.0)),
        # usage waits inside the heartbeat's reserve, like a sweep
        ({"running": True, "heartbeat_due": 101.9, "usage_due": 100.0}, (Step.WAIT, 1.9)),
        ({"running": True, "heartbeat_due": 102.0, "usage_due": 100.0}, (Step.USAGE, 0.0)),
        # renewals stopped after a confirmed cancellation: usage is never held back (O3)
        ({"running": True, "heartbeat_due": None, "usage_due": 100.0}, (Step.USAGE, 0.0)),
        # usage outranks sweeps
        (
            {"running": True, "heartbeat_due": 110.0, "usage_due": 100.0, "sweep_due": 90.0},
            (Step.USAGE, 0.0),
        ),
        # a retry spaced by the bound waits; a due sweep may run meanwhile
        ({"running": True, "heartbeat_due": 110.0, "usage_due": 100.5}, (Step.WAIT, 0.5)),
        (
            {"running": True, "heartbeat_due": 110.0, "usage_due": 101.0, "sweep_due": 100.0},
            (Step.SWEEP, 0.0),
        ),
        # draining: usage goes on, sweeps do not
        (
            {
                "running": True,
                "stopping": True,
                "drain_due": 130.0,
                "heartbeat_due": 110.0,
                "usage_due": 100.0,
                "sweep_due": 0.0,
            },
            (Step.USAGE, 0.0),
        ),
        (
            {
                "running": True,
                "stopping": True,
                "drain_due": 130.0,
                "heartbeat_due": None,
                "usage_due": 104.0,
            },
            (Step.WAIT, 4.0),
        ),
        # the drain deadline still comes first; a finished handler's completion too
        (
            {"running": True, "stopping": True, "drain_due": 100.0, "usage_due": 100.0},
            (Step.ABANDON, 0.0),
        ),
        ({"completing": True, "completion_due": 100.0, "usage_due": 50.0}, (Step.COMPLETE, 0.0)),
    ],
)
def test_next_step_with_a_waiting_usage_record(
    changes: dict[str, Any], expected: tuple[Step, float]
) -> None:
    assert next_step(timing(**changes)) == pytest.approx(expected)


def test_a_timing_built_the_old_way_has_no_usage_record() -> None:
    old = Timing(
        now=100.0,
        running=True,
        completing=False,
        stopping=False,
        drain_due=None,
        heartbeat_due=110.0,
        completion_due=0.0,
        sweep_due=200.0,
        claim_due=200.0,
        sweep_reserve=2.0,
    )
    assert old.usage_due is None
    assert next_step(old) == (Step.WAIT, 10.0)


@pytest.mark.parametrize("stopping", [False, True])
@pytest.mark.parametrize("heartbeat_due", [None, 99.0, 100.0, 101.0, 150.0])
@pytest.mark.parametrize("usage_due", [None, 50.0, 100.0, 100.5, 300.0])
def test_every_wait_with_usage_is_finite_and_positive(
    stopping: bool, heartbeat_due: float | None, usage_due: float | None
) -> None:
    step, seconds = next_step(
        timing(
            running=True,
            stopping=stopping,
            drain_due=160.0 if stopping else None,
            heartbeat_due=heartbeat_due,
            usage_due=usage_due,
            sweep_due=120.0,
        )
    )
    if step is Step.WAIT:
        assert 0.0 < seconds < float("inf")


def test_usage_never_delays_a_heartbeat_and_always_progresses() -> None:
    # A handler that posts a new record the moment the last is answered, every
    # write taking its full bound: heartbeats still run exactly when due, and
    # records are written between every two heartbeats.
    interval, reserve = 5.0, 2.0
    now, heartbeat_due = 0.0, 5.0
    lateness: list[float] = []
    writes_between: list[int] = []
    writes = 0
    while now < 1_000.0:
        step, seconds = next_step(
            timing(
                now=now,
                running=True,
                heartbeat_due=heartbeat_due,
                usage_due=now,
                sweep_due=now,
                sweep_reserve=reserve,
            )
        )
        if step is Step.HEARTBEAT:
            lateness.append(now - heartbeat_due)
            writes_between.append(writes)
            writes = 0
            heartbeat_due = now + interval
            now += reserve
        elif step is Step.USAGE:
            writes += 1
            now += reserve
        else:
            assert step is Step.WAIT
            assert 0.0 < seconds < float("inf")
            now += seconds
    assert len(lateness) > 100
    assert max(lateness) == 0.0
    assert min(writes_between[1:]) >= 1


# --- the handoff, from the handler's side ----------------------------------------------


class Call(threading.Thread):
    """Runs ``fn`` as a handler would, and records how it ended."""

    def __init__(self, fn: Callable[[], None]) -> None:
        super().__init__(daemon=True)
        self.fn = fn
        self.returned = False
        self.raised: BaseException | None = None

    def run(self) -> None:
        try:
            self.fn()
            self.returned = True
        except BaseException as exc:
            self.raised = exc

    def ended(self) -> "Call":
        self.join(WAIT)
        assert not self.is_alive()
        return self


class Bound:
    """A sink bound to a fake clock, serving ``ATTEMPT``; the test plays the supervisor."""

    def __init__(self, backstop: float = 1_000.0) -> None:
        self.clock = FakeClock()
        self.wakes = 0
        self.cancel = threading.Event()
        self.sink = new_sink()
        self.sink.bind(clock=self.clock, wake=self.wake)
        self.sink.begin(ATTEMPT, cancel=self.cancel, backstop=backstop, context=CONTEXT)

    def wake(self) -> None:
        self.wakes += 1

    def posted(self) -> Any:
        deadline = time.monotonic() + WAIT
        while (request := self.sink.pending()) is None:
            assert time.monotonic() < deadline, "nothing was posted"
            time.sleep(0.001)
        return request

    def intent_then(self, reply: Reply, seq: int = 0) -> Call:
        call = Call(lambda: self.sink.recorder.record_intent(intent(seq)))
        call.start()
        self.sink.answer(self.posted(), reply)
        return call.ended()

    def acked_intent(self, seq: int = 0) -> None:
        assert self.intent_then(Reply.ACKED, seq).returned


def test_the_recorder_is_a_provider_call_recorder() -> None:
    assert isinstance(new_sink().recorder, ProviderCallRecorder)


def test_an_acknowledged_intent_returns_and_its_result_waits_for_its_row() -> None:
    bound = Bound()
    call = Call(lambda: bound.sink.recorder.record_intent(intent(0)))
    call.start()
    request = bound.posted()
    assert bound.wakes == 1  # the supervisor is woken
    assert request.is_intent
    assert request.due == 100.0
    assert request.record == bound.sink.ledger_intent(intent(0))
    time.sleep(0.05)
    assert call.is_alive()  # waits until the row has committed
    bound.sink.answer(request, Reply.ACKED)
    assert call.ended().returned

    done = Call(lambda: bound.sink.recorder.record_result(result(0)))
    done.start()
    posted = bound.posted()
    assert not posted.is_intent
    bound.sink.answer(posted, Reply.ACKED)
    assert done.ended().returned
    bound.acked_intent(1)  # numbering goes on


def test_a_waiting_intent_is_withdrawn_on_cancellation() -> None:
    bound = Bound()
    call = Call(lambda: bound.sink.recorder.record_intent(intent(0)))
    call.start()
    request = bound.posted()
    bound.cancel.set()
    assert bound.sink.withdraw() is request
    assert isinstance(call.ended().raised, HandlerCancelled)  # no call is made
    assert request is not None
    assert request.reply is Reply.CANCELLED
    assert bound.sink.pending() is None


def test_an_intent_after_cancellation_is_never_posted() -> None:
    bound = Bound()
    bound.cancel.set()
    with pytest.raises(HandlerCancelled):
        bound.sink.recorder.record_intent(intent(0))
    assert bound.wakes == 0
    assert bound.sink.pending() is None


def test_after_an_ack_the_call_goes_ahead_and_its_result_is_never_withdrawn() -> None:
    bound = Bound()
    bound.acked_intent(0)
    bound.cancel.set()  # cancellation confirmed while the call is in flight
    call = Call(lambda: bound.sink.recorder.record_result(result(0)))
    call.start()
    request = bound.posted()
    assert bound.sink.withdraw() is None  # a result is never withdrawn
    with pytest.raises(RuntimeError, match="never withdrawn"):
        bound.sink.answer(request, Reply.CANCELLED)
    bound.sink.answer(request, Reply.ACKED)
    assert call.ended().returned


def test_a_second_post_while_one_waits_is_refused() -> None:
    bound = Bound()
    call = Call(lambda: bound.sink.recorder.record_intent(intent(0)))
    call.start()
    request = bound.posted()
    with pytest.raises(RuntimeError, match="still waiting"):
        bound.sink.recorder.record_intent(intent(0))
    bound.sink.answer(request, Reply.ACKED)
    assert call.ended().returned


def test_the_backstop_ends_a_wait_the_supervisor_never_answers() -> None:
    bound = Bound(backstop=110.0)
    call = Call(lambda: bound.sink.recorder.record_intent(intent(0)))
    call.start()
    bound.posted()
    bound.clock.now = 110.0
    raised = call.ended().raised
    assert isinstance(raised, UsageHandoffExpiredError)
    assert not isinstance(raised, Exception)  # never classified: nothing is recorded (exit 5)


@pytest.mark.parametrize(
    ("record", "match"),
    [
        (intent(0, route="route-b"), "no provider"),  # unmapped
        (intent(0, attempt_id=uuid.UUID(int=9)), "not the one this worker runs"),
        (intent(1), "start at 0 and go up by 1"),
        (intent(0, capability=" "), "capability must not be blank"),  # the ledger's text rule
        (intent(0, capability="x" * 257), "at most 256"),
    ],
    ids=["unmapped-route", "other-attempt", "not-contiguous", "blank", "too-long"],
)
def test_invalid_intents_are_refused_before_anything_is_posted(
    record: ProviderCallIntent, match: str
) -> None:
    bound = Bound()
    with pytest.raises(ValueError, match=match):
        bound.sink.recorder.record_intent(record)
    assert bound.wakes == 0
    assert bound.sink.pending() is None


def test_records_are_refused_without_a_running_attempt() -> None:
    sink = new_sink()
    sink.bind(clock=FakeClock(), wake=lambda: None)
    with pytest.raises(ValueError, match="not the one this worker runs"):
        sink.recorder.record_intent(intent(0))
    bound = Bound()
    bound.acked_intent(0)
    bound.sink.end()  # settled
    with pytest.raises(ValueError, match="not the one this worker runs"):
        bound.sink.recorder.record_result(result(0))


@pytest.mark.parametrize("seq", [0, 1])
def test_a_result_needs_an_acknowledged_intent_without_a_result(seq: int) -> None:
    bound = Bound()
    bound.acked_intent(0)
    if seq == 0:  # the first result is accepted; a second one is not
        call = Call(lambda: bound.sink.recorder.record_result(result(0)))
        call.start()
        bound.sink.answer(bound.posted(), Reply.ACKED)
        assert call.ended().returned
    with pytest.raises(ValueError, match="acknowledged intent"):
        bound.sink.recorder.record_result(result(seq))


def test_wrong_record_types_are_refused() -> None:
    bound = Bound()
    with pytest.raises(TypeError):
        bound.sink.recorder.record_intent(result(0))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        bound.sink.recorder.record_result(intent(0))  # type: ignore[arg-type]


def test_a_sink_binds_once_and_serves_one_attempt_at_a_time() -> None:
    bound = Bound()
    with pytest.raises(RuntimeError, match="already bound"):
        bound.sink.bind(clock=bound.clock, wake=bound.wake)
    with pytest.raises(RuntimeError, match="already serves"):
        bound.sink.begin(ATTEMPT, cancel=threading.Event(), backstop=1.0, context={})


# --- conversions ----------------------------------------------------------------------------


def test_an_intent_gets_the_wiring_and_keeps_its_fields() -> None:
    assert new_sink().ledger_intent(intent(3)) == CallIntent(
        attempt_id=ATTEMPT,
        call_seq=3,
        provider="provider-a",
        route_id="route-a",
        capability="interpret_amount",
        prompt_version="prompt-v1",
        model="model-a",
        field_kind=FieldKind.TOTAL,
        item_index=13,
        estimated_cost_units=5,
    )


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (TransientCode.TIMEOUT, CallFailure.TIMEOUT),
        (TransientCode.RATE_LIMITED, CallFailure.RATE_LIMITED),
        (TransientCode.UNAVAILABLE, CallFailure.UNAVAILABLE),
        (PermanentCode.REJECTED, CallFailure.REJECTED),
        (PermanentCode.UNSUPPORTED_REQUEST, CallFailure.UNSUPPORTED_REQUEST),
    ],
)
def test_failure_codes_map_by_value(
    code: TransientCode | PermanentCode, expected: CallFailure
) -> None:
    failed = ProviderCallResult(
        attempt_id=ATTEMPT, call_seq=0, outcome=CallOutcome.FAILED, failure_code=code
    )
    converted = ledger_result(failed)
    assert converted is not None
    assert (converted.outcome.value, converted.failure_code) == ("failed", expected)
    assert converted.reported_amount is None


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        ("0", Decimal("0")),
        ("12.50", Decimal("12.50")),  # the scale is kept
        ("99999999999999999999.999999999999999999", None),  # 18 places, below 1E+20
        ("0.000000000000000001", None),  # 18 places
    ],
)
def test_an_amount_is_converted_exactly(amount: str, expected: Decimal | None) -> None:
    converted = ledger_result(result(0, usage=ProviderUsage(amount, "tok.v1")))
    assert converted is not None
    assert converted.reported_amount is not None
    # Exact, never rounded: the same digits and the same scale.
    assert converted.reported_amount.as_tuple() == Decimal(amount).as_tuple()
    assert converted.reported_unit == "tok.v1"
    if expected is not None:
        assert converted.reported_amount == expected


@pytest.mark.parametrize(
    "amount",
    [
        "100000000000000000000",  # 1E+20
        "123456789012345678901234",
        "0.0000000000000000001",  # 19 decimal places
        "1.1234567890123456789",
    ],
)
def test_an_amount_the_ledger_cannot_store_is_not_converted(amount: str) -> None:
    assert ledger_result(result(0, usage=ProviderUsage(amount, "tokens"))) is None
    assert not representable_amount(Decimal(amount))


def test_an_unrepresentable_amount_leaves_the_call_unresolved_and_logs_ids_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="orqestra.worker")
    bound = Bound()
    bound.acked_intent(0)
    huge = "100000000000000000000"
    bound.sink.recorder.record_result(result(0, usage=ProviderUsage(huge, "secret-unit")))
    assert bound.wakes == 1  # only the intent was posted: no result row
    assert bound.sink.pending() is None
    (record,) = [
        r for r in caplog.records if getattr(r, "event", "") == "usage_result_unrepresentable"
    ]
    assert (record.attempt_id, record.call_seq) == (str(ATTEMPT), "0")  # type: ignore[attr-defined]
    line = JsonFormatter().format(record)  # the line a worker writes (DEC-059)
    assert (json.loads(line)["attempt_id"], json.loads(line)["call_seq"]) == (str(ATTEMPT), "0")
    assert huge not in line
    assert "secret-unit" not in line
    text = record.getMessage() + repr(record.__dict__)
    assert huge not in text
    assert "secret-unit" not in text
    with pytest.raises(ValueError, match="acknowledged intent"):  # no second try
        bound.sink.recorder.record_result(result(0))
    bound.acked_intent(1)  # the handler goes on


# --- wiring and limits ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("args", "error"),
    [
        (((), {"r": "p"}), ValueError),  # no kind
        ((("k",), {}), ValueError),  # no route
        ((("k",), ["r"]), TypeError),
        ((("k",), {"r": 1}), TypeError),
        ((("k",), {"secret route": "p"}), ValueError),  # the ledger's identifier rule
        ((("k",), {"r": "secret provider"}), ValueError),
        ((("k",), {"r": "p"}, {"other": "m"}), ValueError),  # a model for an unmapped route
        ((("k",), {"r": "p"}, {"r": " "}), ValueError),  # the ledger's text rule
        ((("k",), {"r": "p"}, None, "secret version"), ValueError),
    ],
)
def test_the_wiring_is_checked_against_the_ledgers_rules(
    args: tuple[Any, ...], error: type[Exception]
) -> None:
    with pytest.raises(error) as info:
        UsageSink(*args)
    assert "secret" not in str(info.value)


def test_a_sink_maps_its_kinds_to_t3() -> None:
    sink = UsageSink(["b", "a", "a"], {"r": "p"})
    assert sink.kinds == ("a", "b")
    assert dict(sink.tiers) == {"a": CaptureTier.T3, "b": CaptureTier.T3}


def test_the_handoff_bound_is_three_database_bounds() -> None:
    assert usage_handoff_bound(SETTINGS) == 3 * SETTINGS.db_operation_timeout


# SETTINGS: execution 600 s, margin 3 s, handoff bound 6 s: 597 s for cap * (timeout + 12 s).
@pytest.mark.parametrize(
    ("cap", "timeout", "fits"),
    [
        (0, timedelta(days=1), True),
        (3, timedelta(seconds=187), True),  # 3 * 199 s = 597 s exactly
        (3, timedelta(seconds=187, microseconds=1), False),
        (29, timedelta(seconds=8), True),
        (30, timedelta(seconds=8), False),
        (MAX_CALL_SEQ, timedelta(microseconds=1), False),  # no overflow
    ],
)
def test_the_call_budget_must_fit_the_execution_time(
    cap: int, timeout: timedelta, fits: bool
) -> None:
    if fits:
        check_provider_call_budget(
            SETTINGS, max_provider_calls_per_attempt=cap, provider_call_timeout=timeout
        )
    else:
        with pytest.raises(ValueError, match="must not exceed"):
            check_provider_call_budget(
                SETTINGS, max_provider_calls_per_attempt=cap, provider_call_timeout=timeout
            )


@pytest.mark.parametrize(
    ("cap", "timeout", "error"),
    [
        (True, timedelta(seconds=1), TypeError),
        (False, timedelta(seconds=1), TypeError),
        (1.0, timedelta(seconds=1), TypeError),
        ("1", timedelta(seconds=1), TypeError),
        (None, timedelta(seconds=1), TypeError),
        (-1, timedelta(seconds=1), ValueError),
        (MAX_CALL_SEQ + 1, timedelta(seconds=1), ValueError),
        (1, timedelta(0), ValueError),
        (1, timedelta(seconds=-1), ValueError),
        (1, 1.0, TypeError),
    ],
)
def test_the_call_budget_refuses_bad_arguments(
    cap: Any, timeout: Any, error: type[Exception]
) -> None:
    with pytest.raises(error):
        check_provider_call_budget(
            SETTINGS, max_provider_calls_per_attempt=cap, provider_call_timeout=timeout
        )


def test_a_worker_refuses_a_sink_whose_kinds_it_cannot_run() -> None:
    registry = HandlerRegistry({"k": lambda ctx, payload: None})
    with pytest.raises(ValueError, match="registered handler"):
        Worker(
            registry=registry,
            settings=SETTINGS,
            connect=FakeConn,  # type: ignore[arg-type]
            usage=new_sink(("other",)),
        )
    with pytest.raises(TypeError, match="UsageSink"):
        Worker(
            registry=registry,
            settings=SETTINGS,
            connect=FakeConn,  # type: ignore[arg-type]
            usage=object(),  # type: ignore[arg-type]
        )
    sink = new_sink()
    Worker(
        registry=registry,
        settings=SETTINGS,
        connect=FakeConn,  # type: ignore[arg-type]
        usage=sink,
    )
    with pytest.raises(RuntimeError, match="already bound"):
        Worker(
            registry=registry,
            settings=SETTINGS,
            connect=FakeConn,  # type: ignore[arg-type]
            usage=sink,
        )


# --- the supervisor's side, with injected ledger writes ---------------------------------------


class Metered:
    """A handler that records ``calls`` calls through the sink, then returns."""

    def __init__(self, sink: UsageSink, calls: int = 1, usage: ProviderUsage | None = None) -> None:
        self.sink, self.calls, self.usage = sink, calls, usage
        self.made: list[int] = []

    def handler(self, ctx: TaskContext, payload: JsonValue) -> JsonValue:
        for seq in range(self.calls):
            self.sink.recorder.record_intent(intent(seq, attempt_id=ctx.attempt_id))
            self.made.append(seq)  # the call
            self.sink.recorder.record_result(
                result(seq, usage=self.usage, attempt_id=ctx.attempt_id)
            )
        return {"calls": len(self.made)}


def stored_call(record: CallIntent, state: AttemptState = AttemptState.RUNNING) -> StoredCall:
    return StoredCall(record, uuid.UUID(int=3), uuid.UUID(int=2), state, T0)


class Ledger:
    """Fake ``record_call_intent`` and ``record_call_result`` playing scripted answers."""

    def __init__(self, harness: "SinkHarness", answers: list[Any] | None = None) -> None:
        self.harness, self.answers = harness, list(answers or [])
        self.intents: list[CallIntent] = []
        self.results: list[CallResult] = []
        harness.monkeypatch.setattr(runtime, "record_call_intent", self.record_intent)
        harness.monkeypatch.setattr(runtime, "record_call_result", self.record_result)

    def _play(self) -> Any:
        answer = self.answers.pop(0) if self.answers else "created"
        if answer == "lost":
            self.harness.conn.closed = True
            raise psycopg.OperationalError("server closed the connection")
        if isinstance(answer, BaseException):
            raise answer
        return answer

    def record_intent(self, conn: Any, record: CallIntent) -> Recorded[StoredCall]:
        self.intents.append(record)
        answer = self._play()
        if answer == "late":
            return Recorded(stored_call(record, AttemptState.REVOKED), True)
        return Recorded(stored_call(record), answer != "replayed")

    def record_result(self, conn: Any, record: CallResult) -> Recorded[StoredResult]:
        self.results.append(record)
        answer = self._play()
        return Recorded(StoredResult(record, AttemptState.RUNNING, T0), answer != "replayed")


class SinkHarness:
    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, calls: int = 1, usage: ProviderUsage | None = None
    ) -> None:
        self.monkeypatch = monkeypatch
        self.clock = FakeClock()
        self.expiries: list[Expiry] = []
        self.conn = FakeConn()
        self.sink = new_sink()
        self.metered = Metered(self.sink, calls, usage)

        def connect() -> Any:
            self.conn = FakeConn()
            return self.conn

        self.worker = Worker(
            registry=HandlerRegistry({"k": self.metered.handler}),
            settings=SETTINGS,
            connect=connect,
            worker_id="worker-1",
            clock=self.clock,
            watchdog=Watchdog(clock=self.clock, terminate=self.expiries.append),
            terminate=terminate,
            usage=self.sink,
        )

    def start(self) -> runtime._Running:
        self.worker._start(attempt(), 100.0, CONTEXT, {}, "k")
        running = self.worker._schedule.running
        assert running is not None
        return running

    def posted(self) -> Any:
        deadline = time.monotonic() + WAIT
        while (request := self.sink.pending()) is None:
            assert time.monotonic() < deadline, "nothing was posted"
            time.sleep(0.001)
        return request

    def serve(self, running: runtime._Running, count: int = 1) -> None:
        for _ in range(count):
            request = self.posted()
            self.clock.now = max(self.clock.now, request.due)
            self.worker._serve_usage(running)

    def finished(self, running: runtime._Running) -> None:
        assert running.done.wait(WAIT)
        running.thread.join(WAIT)


@pytest.fixture
def make(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., SinkHarness]]:
    made: list[SinkHarness] = []

    def build(**kwargs: Any) -> SinkHarness:
        harness = SinkHarness(monkeypatch, **kwargs)
        made.append(harness)
        return harness

    yield build
    for harness in made:
        harness.sink.end()  # wakes any handler still waiting; it then hits its backstop
        harness.clock.now = 10_000.0


def test_every_call_is_recorded_in_order_and_closed_complete(make: Any) -> None:
    harness = make(calls=2)
    ledger = Ledger(harness)
    running = harness.start()
    harness.serve(running, 4)
    harness.finished(running)
    assert harness.metered.made == [0, 1]
    assert [r.call_seq for r in ledger.intents] == [0, 1]
    assert [r.call_seq for r in ledger.results] == [0, 1]
    assert running.usage is not None
    assert running.usage.closure(ATTEMPT) == UsageClosure(
        attempt_id=ATTEMPT,
        capture=CaptureState.COMPLETE,
        closed_by=ClosedBy.WORKER,
        capture_tier=CaptureTier.T3,
        call_count=2,
    )


def test_the_completion_carries_the_closure_and_the_sweep_the_tiers(make: Any) -> None:
    harness = make(calls=1)
    Ledger(harness)
    running = harness.start()
    harness.serve(running, 2)
    harness.finished(running)
    closures: list[Any] = []

    def complete(conn: Any, attempt_id: uuid.UUID, **kwargs: Any) -> Any:
        closures.append(kwargs["usage_closure"])
        raise CaptureNotCompleteError(attempt_id, 1, 1, 0)  # a defect of the count

    harness.monkeypatch.setattr(runtime, "complete_attempt", complete)
    with pytest.raises(Terminated) as info:
        harness.worker._complete(running)
    assert (info.value.code, info.value.event) == (ExitCode.DEFECT, "completion_refused")
    assert closures[0].capture is CaptureState.COMPLETE
    assert closures[0].call_count == 1

    tiers: list[Any] = []

    def revoke(conn: Any, *, retry_policy: Any, usage_capture: Any) -> Any:
        tiers.append(dict(usage_capture))

    harness.monkeypatch.setattr(runtime, "revoke_next_expired", revoke)
    harness.worker._sweep()
    assert tiers == [{"k": CaptureTier.T3}]


def test_a_conflicting_closure_ends_the_completion_as_a_defect(make: Any) -> None:
    harness = make(calls=0)
    running = harness.start()
    harness.finished(running)
    harness.monkeypatch.setattr(
        runtime, "complete_attempt", raising(UsageReplayConflictError("usage closure", ATTEMPT))
    )
    with pytest.raises(Terminated) as info:
        harness.worker._complete(running)
    assert info.value.code is ExitCode.DEFECT


def test_a_retryable_write_is_retried_after_the_bound_until_the_deadline(make: Any) -> None:
    harness = make()
    Ledger(harness, [errors.DeadlockDetected(), errors.QueryCanceled()])
    running = harness.start()
    harness.serve(running)
    request = harness.sink.pending()
    assert request is not None
    assert request.due == 102.0  # spaced by the database bound
    harness.clock.now = 102.0
    harness.worker._serve_usage(running)
    assert request.due == 104.0
    harness.serve(running, 2)  # acknowledged; then its result
    harness.finished(running)
    assert harness.metered.made == [0]


@pytest.mark.parametrize(("calls_before", "event"), [(0, "usage_intent_unrecorded"), (1, None)])
def test_a_write_never_confirmed_by_the_deadline_ends_the_process_as_unrecorded(
    make: Any, calls_before: int, event: str | None
) -> None:
    harness = make()
    # calls_before=1: the intent is recorded, the result never is.
    Ledger(harness, ["created"] * calls_before + [errors.DeadlockDetected()] * 3)
    running = harness.start()
    harness.serve(running, calls_before)
    harness.serve(running)
    request = harness.sink.pending()
    assert request is not None
    harness.clock.now = 100.0 + 599.0
    harness.worker._serve_usage(running)
    assert request.due == 100.0 + 600.0  # the next try is at the deadline, not after it
    harness.clock.now = request.due
    with pytest.raises(Terminated) as info:
        harness.worker._serve_usage(running)
    expected = event or "usage_result_unrecorded"
    assert (info.value.code, info.value.event) == (ExitCode.UNRECORDED, expected)
    assert harness.metered.made == ([] if calls_before == 0 else [0])


def test_the_handlers_backstop_is_later_than_the_supervisors_deadline(make: Any) -> None:
    harness = make()
    harness.start()
    binding = harness.sink._binding
    assert binding is not None
    deadline = 100.0 + 600.0  # the attempt's deadline on the local clock
    bound = 2.0  # db_operation_timeout
    assert binding.backstop == deadline + 3 * bound + 2 * bound  # D + 5 bounds
    assert binding.backstop - deadline > 4 * bound  # the supervisor's worst case


def test_the_supervisors_worst_case_ends_before_the_handlers_backstop(make: Any) -> None:
    harness = make()
    Ledger(harness, [errors.DeadlockDetected()])
    running = harness.start()
    binding = harness.sink._binding
    assert binding is not None
    deadline, bound = 100.0 + 600.0, 2.0
    request = harness.posted()
    harness.clock.now = deadline - 1.0
    harness.worker._serve_usage(running)  # retryable: the next try is due at D
    assert request.due == deadline
    harness.clock.now = request.due + bound  # 1. a supervisor step already in progress
    harness.clock.now += bound  # 2. a heartbeat that comes first
    answers: list[Any] = ["lost", errors.SerializationFailure()]

    def slow_write(conn: Any, record: CallIntent) -> Any:
        harness.clock.now += bound  # each write takes its whole bound
        answer = answers.pop(0)
        if answer == "lost":
            harness.conn.closed = True
            raise psycopg.OperationalError("server closed the connection")
        raise answer

    harness.monkeypatch.setattr(runtime, "record_call_intent", slow_write)
    harness.worker._serve_usage(running)  # 3. its answer is lost: replayed at once
    assert request.uncertain
    assert request.due == deadline + 3 * bound
    with pytest.raises(Terminated) as info:
        harness.worker._serve_usage(running)  # 4. the replay fails, retryable
    assert (info.value.code, info.value.event) == (ExitCode.UNRECORDED, "usage_intent_unrecorded")
    assert not answers
    assert harness.clock.now == deadline + 4 * bound
    assert harness.clock.now < binding.backstop
    assert harness.metered.made == []


@pytest.mark.parametrize("committed", [True, False], ids=["committed", "rolled-back"])
def test_a_lost_answer_is_replayed_at_once_on_a_fresh_connection(
    make: Any, committed: bool
) -> None:
    harness = make()
    replay = "replayed" if committed else "created"
    ledger = Ledger(harness, ["lost", replay, "lost", replay])
    running = harness.start()
    harness.serve(running)
    request = harness.sink.pending()
    assert request is not None
    assert request.uncertain
    assert request.due == 100.0  # at once
    assert harness.worker._conn is None  # closed
    harness.serve(running, 3)
    harness.finished(running)
    assert harness.metered.made == [0]  # exactly one call
    assert [r.call_seq for r in ledger.intents] == [0, 0]  # the same keyed write
    assert ledger.intents[0] == ledger.intents[1]
    assert running.usage is not None
    assert running.usage.closure(ATTEMPT).call_count == 1


def test_a_second_lost_answer_is_replayed_after_the_bound(make: Any) -> None:
    harness = make()
    Ledger(harness, ["lost", "lost"])
    running = harness.start()
    harness.serve(running)
    harness.worker._serve_usage(running)
    request = harness.sink.pending()
    assert request is not None
    assert request.due == 102.0


@pytest.mark.parametrize(
    "answer",
    [
        errors.CheckViolation(),
        NotFoundError("attempt", ATTEMPT),
        UsageReplayConflictError("call intent", ATTEMPT, 0),
        "replayed",  # a first, non-replay intent found a row: the numbering is shared
        RuntimeError("transaction already open"),
    ],
    ids=["rejected", "not-found", "conflict", "first-not-created", "runtime-error"],
)
def test_a_refused_intent_is_a_defect_and_no_call(make: Any, answer: Any) -> None:
    harness = make()
    Ledger(harness, [answer])
    running = harness.start()
    with pytest.raises(Terminated) as info:
        harness.serve(running)
    assert (info.value.code, info.value.event) == (ExitCode.DEFECT, "usage_write_rejected")
    assert harness.metered.made == []


def test_a_result_without_its_intent_row_is_a_defect(make: Any) -> None:
    harness = make()
    Ledger(harness, ["created", CallIntentNotFoundError(ATTEMPT, 0)])
    running = harness.start()
    harness.serve(running)
    with pytest.raises(Terminated) as info:
        harness.serve(running)
    assert (info.value.code, info.value.event) == (ExitCode.DEFECT, "usage_write_rejected")


def test_a_result_write_raising_runtime_error_is_a_defect(make: Any) -> None:
    harness = make()
    Ledger(harness, ["created", RuntimeError("transaction already open")])
    running = harness.start()
    harness.serve(running)
    with pytest.raises(Terminated) as info:
        harness.serve(running)
    assert (info.value.code, info.value.event) == (ExitCode.DEFECT, "usage_write_rejected")


def test_a_late_intent_fences_the_process_before_the_call(make: Any) -> None:
    harness = make()
    Ledger(harness, ["late"])
    running = harness.start()
    with pytest.raises(Terminated) as info:
        harness.serve(running)
    assert (info.value.code, info.value.event) == (ExitCode.FENCED, "usage_intent_late")
    assert harness.metered.made == []


def test_a_confirmed_cancellation_withdraws_the_waiting_intent(make: Any) -> None:
    harness = make()
    ledger = Ledger(harness)
    running = harness.start()
    harness.posted()
    harness.monkeypatch.setattr(
        runtime,
        "renew_lease",
        raising(CancellationRequestedError(ATTEMPT, T0, T0 + timedelta(seconds=12))),
    )
    harness.worker._heartbeat(running)
    harness.finished(running)
    assert running.outcome == [AttemptCancelled()]
    assert ledger.intents == []  # no row
    assert harness.metered.made == []  # no call
    assert running.usage is not None
    assert running.usage.closure(ATTEMPT).capture is CaptureState.COMPLETE  # complete(0)


def test_an_intent_that_may_have_committed_and_is_withdrawn_closes_incomplete(make: Any) -> None:
    harness = make()
    Ledger(harness, ["lost"])
    running = harness.start()
    harness.serve(running)  # lost: it may have committed
    running.cancel.set()
    harness.worker._serve_usage(running)  # never taken once cancellation is known
    harness.finished(running)
    assert running.outcome == [AttemptCancelled()]
    assert running.usage is not None
    assert running.usage.closure(ATTEMPT).capture is CaptureState.INCOMPLETE


def test_an_unrepresentable_amount_closes_the_capture_incomplete(make: Any) -> None:
    harness = make(usage=ProviderUsage("100000000000000000000", "tokens"))
    ledger = Ledger(harness)
    running = harness.start()
    harness.serve(running)
    harness.finished(running)
    assert running.outcome == [AttemptSucceeded({"calls": 1})]  # the task still succeeds
    assert ledger.results == []
    assert running.usage is not None
    assert running.usage.closure(ATTEMPT).capture is CaptureState.INCOMPLETE


def test_usage_diagnostics_carry_ids_only(make: Any, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="orqestra.worker")
    harness = make()
    Ledger(harness, [errors.DeadlockDetected(), "lost"])
    running = harness.start()
    harness.serve(running, 4)
    harness.finished(running)
    usage_records = [r for r in caplog.records if str(getattr(r, "event", "")).startswith("usage_")]
    assert {getattr(r, "event", "") for r in usage_records} == {
        "usage_write_retry",
        "usage_write_ambiguous",
    }
    for record in usage_records:
        assert record.call_seq == "0"  # type: ignore[attr-defined]
        assert json.loads(JsonFormatter().format(record))["call_seq"] == "0"
        text = record.getMessage()
        for secret in ("route-a", "provider-a", "model-a", "prompt-v1", "interpret_amount"):
            assert secret not in text
