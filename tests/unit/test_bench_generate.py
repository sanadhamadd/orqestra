"""The seeded invoice generator and its oracle, against the real handler and summary.

The oracle decides statuses from the index rule alone. These tests run every
generated invoice through the real batch handler, with the benchmark provider
double and an in-memory recorder, and fold the stored-result shape through the
real ``SummaryBuilder``: a disagreement anywhere is a defect in the generator,
the oracle or the code they describe.
"""

import threading
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

import pytest

from benchmarks.generate import (
    COMPOSITION_PERIOD,
    POISON_KEY_PREFIX,
    Expected,
    Kind,
    build_tasks,
    encode_tasks,
    expected_outcome,
    generate_invoices,
    kind_of,
)
from benchmarks.provider import SeparatorReader
from orqestra.calls.recording import CallOutcome, InMemoryCallRecorder
from orqestra.calls.routing import RouteOption
from orqestra.domain.jobs import JobStatus, JsonValue
from orqestra.domain.states import TaskState
from orqestra.worker.handlers import CancellationSignal, TaskContext
from orqestra.workloads.invoice.handler import JOB_KIND, invoice_verification_handler
from orqestra.workloads.invoice.summary import AnomalyCode, InvoiceJobSummary, SummaryBuilder
from orqestra.workloads.invoice.verify import REQUIRED_CAPABILITY, Status
from tests.unit.test_invoice_summary import JOB, NOW, row

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SEEDS = (1, 7, 20261107)
BATCH_SIZES = (10, 100, 1000)
INVOICES = 2000
CAP = 16
MAX_ATTEMPTS = 3
ROUTE = RouteOption("route-a", frozenset({REQUIRED_CAPABILITY}), estimated_cost_units=1)


@dataclass(frozen=True)
class BatchRun:
    task: dict[str, JsonValue]
    result: JsonValue
    calls: int
    tokens: int


def run_batch(number: int, task: dict[str, JsonValue]) -> BatchRun:
    """One attempt of the real handler on one task, with a fresh recorder."""
    recorder = InMemoryCallRecorder()
    handle = invoice_verification_handler(
        {"route-a": SeparatorReader()},
        [ROUTE],
        timedelta(seconds=1),
        max_provider_calls_per_attempt=CAP,
        recorder=recorder,
    )
    context = TaskContext(
        uuid.UUID(int=number + 1),
        uuid.UUID(int=10_000 + number),
        1,
        JOB_KIND,
        CancellationSignal(threading.Event()),
    )
    result = handle(context, task["payload"])
    answered = [r for r in recorder.results if r.outcome is CallOutcome.ANSWERED]
    assert len(answered) == len(recorder.intents)  # the double never fails
    tokens = sum(int(r.usage.amount) for r in answered if r.usage is not None)
    return BatchRun(task, result, len(recorder.intents), tokens)


def run_all(invoices: list[dict[str, JsonValue]], batch_size: int) -> list[BatchRun]:
    tasks = build_tasks(invoices, batch_size, MAX_ATTEMPTS)
    return [run_batch(number, task) for number, task in enumerate(tasks)]


def entries(runs: list[BatchRun]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for run in runs:
        result = cast(dict[str, Any], run.result)
        found.extend(result["results"])
    return found


def summarize(rows: list[Any]) -> InvoiceJobSummary:
    builder = SummaryBuilder()
    for task_row in rows:
        builder.add(task_row)
    return builder.finish(JOB, NOW)


def succeeded(run: BatchRun) -> Any:
    return row(
        cast(str, run.task["task_key"]), TaskState.SUCCEEDED, run.task["payload"], run.result
    )


def failed(task: dict[str, JsonValue]) -> Any:
    return row(cast(str, task["task_key"]), TaskState.FAILED, task["payload"])


def assert_counts(summary: InvoiceJobSummary, expected: Expected) -> None:
    assert summary.verified == expected.verified
    assert summary.rejected == expected.rejected
    assert summary.needs_review == expected.needs_review
    assert summary.malformed == expected.malformed
    assert summary.provider_assisted_items == expected.provider_assisted
    assert summary.processed_items == expected.processed_items


# --- the oracle against the real handler ---------------------------------------


@pytest.mark.parametrize("batch_size", BATCH_SIZES)
@pytest.mark.parametrize("seed", SEEDS)
def test_the_oracle_matches_the_handler_and_the_summary(seed: int, batch_size: int) -> None:
    invoices = generate_invoices(INVOICES, seed)
    expected = expected_outcome(invoices, batch_size, CAP)
    runs = run_all(invoices, batch_size)

    results = entries(runs)
    assert [entry["index"] for entry in results] == list(range(INVOICES))
    assert tuple(Status(entry["status"]) for entry in results) == expected.statuses
    assert sum(entry["provider_assisted"] for entry in results) == expected.provider_assisted
    assert sum(run.calls for run in runs) == expected.calls
    assert sum(run.tokens for run in runs) == expected.tokens

    summary = summarize([succeeded(run) for run in runs])
    assert summary.complete
    assert summary.integrity_ok
    assert summary.expected_items == expected.expected_items == INVOICES
    assert_counts(summary, expected)


def test_the_cap_is_modelled_where_it_binds() -> None:
    invoices = generate_invoices(INVOICES)
    small = expected_outcome(invoices, 100, CAP)
    large = expected_outcome(invoices, 1000, CAP)
    assert small.needs_review == 0  # 5 ambiguous invoices per batch of 100
    # 50 ambiguous invoices per batch of 1,000: the first 16 are called, 34 are not.
    assert large.needs_review == 2 * (50 - CAP)
    assert large.calls == 2 * CAP
    assert expected_outcome(invoices, 100, 0).calls == 0


# --- composition and determinism -----------------------------------------------


def test_composition_shares_are_exact() -> None:
    count = 10 * COMPOSITION_PERIOD
    kinds = Counter(kind_of(index) for index in range(count))
    assert kinds == {
        Kind.MALFORMED: count // 200,
        Kind.ZERO_QUANTITY: count // 100,
        Kind.WRONG_TOTAL: count // 50,
        Kind.AMBIGUOUS: count // 20,
        Kind.VERIFIED: count - count // 200 - count // 100 - count // 50 - count // 20,
    }


def test_the_same_seed_gives_the_same_bytes_and_another_seed_the_same_answers() -> None:
    first = encode_tasks(build_tasks(generate_invoices(INVOICES, 7), 100, MAX_ATTEMPTS))
    again = encode_tasks(build_tasks(generate_invoices(INVOICES, 7), 100, MAX_ATTEMPTS))
    other = generate_invoices(INVOICES, 8)
    assert first == again
    assert first != encode_tasks(build_tasks(other, 100, MAX_ATTEMPTS))
    assert first.endswith(b"\n")
    assert first.isascii()
    assert (
        expected_outcome(other, 100, CAP).statuses
        == expected_outcome(generate_invoices(INVOICES, 7), 100, CAP).statuses
    )


def test_ambiguous_totals_are_grouped_and_every_other_amount_is_an_integer() -> None:
    for index, invoice in enumerate(generate_invoices(400)):
        total = invoice["total"]
        if kind_of(index) is Kind.AMBIGUOUS:
            assert isinstance(total, str)
            assert any(separator in total for separator in ",_ ")
        else:
            assert isinstance(total, int)


def test_the_tasks_are_the_partition_in_the_submit_command_format() -> None:
    tasks = build_tasks(generate_invoices(250), 100, MAX_ATTEMPTS)
    assert [set(task) for task in tasks] == [{"task_key", "payload", "max_attempts"}] * 3
    payloads = [cast(dict[str, Any], task["payload"]) for task in tasks]
    assert [payload["offset"] for payload in payloads] == [0, 100, 200]
    assert [len(payload["items"]) for payload in payloads] == [100, 100, 50]


# --- drill adjustments -----------------------------------------------------------


def test_a_failed_batch_is_left_out_of_the_counts() -> None:
    invoices = generate_invoices(300)
    whole = expected_outcome(invoices, 100, CAP)
    adjusted = expected_outcome(invoices, 100, CAP, failed_batches=frozenset({1}))
    assert adjusted.failed_batch_items == 100
    assert adjusted.processed_items == 200
    assert adjusted.expected_items == 300
    assert adjusted.statuses == whole.statuses
    assert adjusted.calls == whole.calls - 5
    with pytest.raises(ValueError, match="failed_batches"):
        expected_outcome(invoices, 100, CAP, failed_batches=frozenset({3}))


def test_the_summary_of_a_job_with_a_failed_batch_matches_the_adjusted_oracle() -> None:
    invoices = generate_invoices(300)
    runs = run_all(invoices, 100)
    expected = expected_outcome(invoices, 100, CAP, failed_batches=frozenset({1}))

    summary = summarize([succeeded(runs[0]), failed(runs[1].task), succeeded(runs[2])])

    assert summary.job_status is JobStatus.FAILED
    assert not summary.complete
    assert summary.integrity_ok
    assert_counts(summary, expected)
    assert (
        summary.unprocessed_items + summary.missing_items + summary.invalid_items
        == expected.failed_batch_items
    )
    assert sum(run.calls for i, run in enumerate(runs) if i != 1) == expected.calls


def test_extra_invalid_payload_tasks_follow_the_partition() -> None:
    invoices = generate_invoices(300)
    tasks = build_tasks(invoices, 100, MAX_ATTEMPTS, invalid_payload_tasks=2)
    keys = [cast(str, task["task_key"]) for task in tasks]
    assert keys[3:] == [f"{POISON_KEY_PREFIX}-0", f"{POISON_KEY_PREFIX}-1"]
    assert not set(keys[:3]) & set(keys[3:])
    expected = expected_outcome(invoices, 100, CAP, invalid_payload_tasks=2)
    assert expected.expected_items is None
    assert expected.processed_items == 300


def test_an_extra_invalid_task_reports_only_payload_invalid() -> None:
    invoices = generate_invoices(300)
    tasks = build_tasks(invoices, 100, MAX_ATTEMPTS, invalid_payload_tasks=1)
    runs = [run_batch(number, task) for number, task in enumerate(tasks[:3])]
    expected = expected_outcome(invoices, 100, CAP, invalid_payload_tasks=1)

    summary = summarize([*(succeeded(run) for run in runs), failed(tasks[3])])

    assert summary.job_status is JobStatus.FAILED
    assert summary.expected_items is expected.expected_items is None
    assert summary.anomalies[AnomalyCode.PAYLOAD_INVALID] == expected.invalid_payload_tasks == 1
    assert sum(summary.anomalies.values()) == 1
    assert_counts(summary, expected)


def test_a_replaced_middle_batch_breaks_the_partition() -> None:
    """Why the payload drill adds a task instead of replacing one: the gap is reported."""
    invoices = generate_invoices(300)
    tasks = build_tasks(invoices, 100, MAX_ATTEMPTS)
    runs = [run_batch(number, task) for number, task in enumerate(tasks)]
    replaced = row(cast(str, tasks[1]["task_key"]), TaskState.FAILED, {"not_a_batch": 0})

    summary = summarize([succeeded(runs[0]), replaced, succeeded(runs[2])])

    assert summary.anomalies[AnomalyCode.PAYLOAD_INVALID] == 1
    assert summary.anomalies[AnomalyCode.PARTITION_NOT_CONTIGUOUS] == 1


# --- argument checks -----------------------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        lambda: generate_invoices(0),
        lambda: expected_outcome([], 10, CAP),
        lambda: expected_outcome(generate_invoices(10), 0, CAP),
        lambda: expected_outcome(generate_invoices(10), 10, -1),
        lambda: expected_outcome(generate_invoices(10), 10, CAP, invalid_payload_tasks=-1),
        lambda: build_tasks(generate_invoices(10), 10, 1, invalid_payload_tasks=-1),
    ],
)
def test_invalid_arguments_are_refused(call: Any) -> None:
    with pytest.raises(ValueError, match="must be"):
        call()
