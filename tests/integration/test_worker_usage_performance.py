"""Per-call overhead of the worker's usage sink (DEC-059). Measures; asserts correctness only.

Marked ``performance``, so neither ``make check`` nor ``make verify`` runs it. Run it
by hand against the test database and record what it prints::

    uv run pytest -m performance -s tests/integration/test_worker_usage_performance.py

It compares one provider call made through ``ProviderCalls`` (with an instant,
synthetic provider) under three recorders: the in-memory recorder, direct ledger
writes on one autocommit connection (no worker), and the worker's sink, whose two
records per call each wait for a serialized supervisor round trip. It prints the
PostgreSQL version, the platform, Python, the sample sizes, the raw samples and their
percentiles. No threshold is asserted: the numbers depend on the machine.
"""

import functools
import json
import platform
import statistics
import sys
import threading
import time
import uuid
from datetime import timedelta
from typing import Any, NoReturn

import psycopg
import pytest
from psycopg.rows import TupleRow

from orqestra.ai.provider import ProviderRequest, ProviderResponse
from orqestra.ai.recording import (
    MAX_CALL_SEQ,
    InMemoryCallRecorder,
    ProviderCallIntent,
    ProviderCallRecorder,
    ProviderCallResult,
)
from orqestra.domain.jobs import JsonValue
from orqestra.domain.leases import LeasePolicy
from orqestra.persistence.lifecycle import start_attempt
from orqestra.persistence.provider_usage import (
    CaptureState,
    read_attempt_usage,
    record_call_intent,
    record_call_result,
)
from orqestra.worker.fence import ExitCode, Expiry, Watchdog
from orqestra.worker.handlers import HandlerRegistry, TaskContext
from orqestra.worker.runtime import Worker, connect_supervisor
from orqestra.worker.usage import UsageSink, ledger_result
from orqestra.workloads.invoice.verify import ProviderCalls
from tests.integration.test_worker_runtime import (
    WAIT,
    Terminated,
    eventually,
    submit,
    task_states,
)
from tests.integration.test_worker_usage_pg import KIND, REQUEST, SETTINGS, new_sink

pytestmark = [pytest.mark.performance, pytest.mark.integration]

Conn = psycopg.Connection[TupleRow]
CALLS = 300  # per mode; a fixed test profile, not a recommendation


class Instant:
    """A synthetic provider that answers at once."""

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        return ProviderResponse(value="12")


class DirectLedger:
    """A recorder that writes straight to the ledger on one connection: no worker, no handoff."""

    def __init__(self, conn: Conn, sink: UsageSink) -> None:
        self.conn, self.sink = conn, sink

    def record_intent(self, intent: ProviderCallIntent) -> None:
        record_call_intent(self.conn, self.sink.ledger_intent(intent))

    def record_result(self, result: ProviderCallResult) -> None:
        converted = ledger_result(result)
        assert converted is not None
        record_call_result(self.conn, converted)


def timed_calls(attempt_id: uuid.UUID, recorder: ProviderCallRecorder) -> list[float]:
    """Seconds per call, ``CALLS`` calls, through ``ProviderCalls`` and ``recorder``."""
    calls = ProviderCalls(attempt_id=attempt_id, cap=MAX_CALL_SEQ, recorder=recorder)
    provider = Instant()
    samples: list[float] = []
    for _ in range(CALLS):
        began = time.perf_counter()
        calls.call(provider, REQUEST, item_index=calls.made, estimated_cost_units=1)
        samples.append(time.perf_counter() - began)
    return samples


def summary(samples: list[float]) -> dict[str, object]:
    ordered = sorted(samples)
    cuts = statistics.quantiles(ordered, n=100, method="inclusive")
    return {
        "n": len(ordered),
        "mean_ms": statistics.fmean(ordered) * 1e3,
        "p50_ms": cuts[49] * 1e3,
        "p90_ms": cuts[89] * 1e3,
        "p99_ms": cuts[98] * 1e3,
        "max_ms": ordered[-1] * 1e3,
        "raw_ms": [round(s * 1e3, 4) for s in samples],
    }


def test_the_sinks_per_call_overhead_is_measured(schema_url: str, db: Conn) -> None:
    # 1. In memory: the floor.
    memory = timed_calls(uuid.uuid4(), InMemoryCallRecorder())

    # 2. Direct ledger writes, on an attempt of a kind no worker runs.
    policy = LeasePolicy(lease=timedelta(minutes=5), execution_timeout=timedelta(minutes=5))
    (direct_task,) = submit(db, "direct", {})
    direct_attempt = start_attempt(db, direct_task.id, "measuring", policy=policy)
    with psycopg.connect(schema_url, autocommit=True) as conn:
        direct = timed_calls(direct_attempt.id, DirectLedger(conn, new_sink()))
    direct_usage = read_attempt_usage(db, direct_attempt.id)
    assert direct_usage is not None
    assert len(direct_usage.calls) == CALLS

    # 3. Through a running worker's sink.
    sink = new_sink()
    sink_samples: list[float] = []

    def handler(ctx: TaskContext, payload: JsonValue) -> JsonValue:
        sink_samples.extend(timed_calls(ctx.attempt_id, sink.recorder))
        return {"calls": CALLS}

    def terminate(code: ExitCode, event: str, message: str, context: Any) -> NoReturn:
        raise Terminated(event)

    (task,) = submit(db, KIND, {})
    expiries: list[Expiry] = []
    worker = Worker(
        registry=HandlerRegistry({KIND: handler}),
        settings=SETTINGS,
        connect=functools.partial(connect_supervisor, schema_url, SETTINGS),
        watchdog=Watchdog(terminate=expiries.append),  # never ends the test process
        terminate=terminate,
        usage=sink,
    )
    runner = threading.Thread(target=worker.run, daemon=True)
    runner.start()
    try:
        eventually(
            lambda: task_states(db, [task]) == [("succeeded", 1)], "the measured task", WAIT * 6
        )
    finally:
        worker.request_stop()
        runner.join(WAIT)
    ((attempt_id,),) = db.execute(
        "SELECT id FROM attempts WHERE task_id = %s", (task.id,)
    ).fetchall()
    usage = read_attempt_usage(db, attempt_id)
    assert usage is not None
    assert len(usage.calls) == CALLS
    assert all(call.result is not None for call in usage.calls)
    assert usage.closure is not None
    assert (usage.closure.closure.capture, usage.closure.closure.call_count) == (
        CaptureState.COMPLETE,
        CALLS,
    )

    ((server_version,),) = db.execute("SHOW server_version").fetchall()
    report = {
        "postgresql": server_version,
        "platform": platform.platform(),
        "python": sys.version,
        "settings": {
            "heartbeat_interval_s": SETTINGS.heartbeat_interval.total_seconds(),
            "db_operation_timeout_s": SETTINGS.db_operation_timeout.total_seconds(),
        },
        "guard_expiries": len(expiries),
        "in_memory": summary(memory),
        "direct_ledger": summary(direct),
        "worker_sink": summary(sink_samples),
    }
    print(f"OBSERVATION {json.dumps(report, indent=2)}")  # the measurement is the output
