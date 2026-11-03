"""The pure invoice-job summary: index mapping, batch and entry rules, completeness.

Rows are built by hand with ``project``, which gives what the snapshot's SQL
projection returns for a JSON payload. The integration tests check that the SQL
agrees with it on payloads read back from PostgreSQL.
"""

import ast
import dataclasses
import itertools
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import orqestra.persistence.snapshots as snapshots_module
import orqestra.workloads.invoice.summary as summary_module
from orqestra.domain.errors import PermanentError
from orqestra.domain.jobs import Job, JobStatus, JsonValue, derive_job_status
from orqestra.domain.states import TaskState
from orqestra.workloads.invoice.handler import JOB_KIND, _batch
from orqestra.workloads.invoice.summary import (
    MAX_SAMPLES,
    Anomaly,
    AnomalyCode,
    InvoiceJobSummary,
    SummaryBuilder,
)
from orqestra.workloads.invoice.verify import Status
from tests.unit.test_invoice_isolation import violations

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

NOW = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
JOB = Job(uuid.UUID(int=1), JOB_KIND, None, NOW)
FINISHED = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}

Obj = dict[str, JsonValue]


@dataclasses.dataclass(frozen=True)
class Row:
    task_key: str
    state: TaskState
    payload_type: str
    key_count: int | None
    has_offset: bool | None
    has_items: bool | None
    offset_text: str | None
    items_length: int | None
    result: JsonValue


def type_name(value: JsonValue) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, str):
        return "string"
    return "array" if isinstance(value, list) else "object"


def project(payload: JsonValue) -> dict[str, Any]:
    """What the snapshot projection returns for ``payload``: a bounded description of its shape."""
    if not isinstance(payload, dict):
        return {
            "payload_type": type_name(payload),
            "key_count": None,
            "has_offset": None,
            "has_items": None,
            "offset_text": None,
            "items_length": None,
        }
    offset = payload.get("offset")
    items = payload.get("items")
    return {
        "payload_type": "object",
        "key_count": len(payload),
        "has_offset": "offset" in payload,
        "has_items": "items" in payload,
        "offset_text": json.dumps(offset) if type_name(offset) == "number" else None,
        "items_length": len(items) if isinstance(items, list) else None,
    }


def row(key: str, state: TaskState, payload: JsonValue, result: JsonValue = None) -> Row:
    return Row(key, state, result=result, **project(payload))


def entry(index: int, status: str = "verified", assisted: bool = False) -> Obj:
    return {
        "index": index,
        "status": status,
        "provider_assisted": assisted,
        "findings": [],
        "issues": [],
        "reason": None,
        "routing": None,
    }


def envelope(offset: int, entries: list[JsonValue]) -> Obj:
    return {"offset": offset, "results": entries}


def payload_of(offset: int, count: int) -> Obj:
    return {"offset": offset, "items": [{} for _ in range(count)]}


def good(offset: int, count: int) -> Obj:
    return envelope(offset, [entry(offset + i) for i in range(count)])


def done(key: str, offset: int, count: int, result: JsonValue) -> Row:
    """A succeeded batch task with exactly this stored result."""
    return row(key, TaskState.SUCCEEDED, payload_of(offset, count), result)


def ok(key: str, offset: int, count: int) -> Row:
    return done(key, offset, count, good(offset, count))


def pending(key: str, offset: int, count: int, state: TaskState) -> Row:
    return row(key, state, payload_of(offset, count))


def at(offset: int, count: int, result: JsonValue) -> list[Row]:
    """The rows of a job whose batch ``t-b`` at ``offset`` follows a complete batch before it."""
    target = done("t-b", offset, count, result)
    return [ok("t-a", 0, offset), target] if offset else [target]


def summarize(rows: list[Row]) -> InvoiceJobSummary:
    builder = SummaryBuilder()
    for r in rows:
        builder.add(r)
    return builder.finish(JOB, NOW)


def raised(summary: InvoiceJobSummary) -> dict[AnomalyCode, int]:
    return {code: n for code, n in summary.anomalies.items() if n}


def assert_partition(summary: InvoiceJobSummary) -> None:
    if summary.expected_items is not None:
        total = (
            summary.processed_items
            + summary.unprocessed_items
            + summary.missing_items
            + summary.invalid_items
        )
        assert total == summary.expected_items


def test_a_job_whose_every_item_has_one_valid_outcome_is_complete() -> None:
    first = envelope(0, [entry(0, "verified"), entry(1, "rejected")])
    second = envelope(2, [entry(2, "needs_review", True), entry(3, "malformed")])
    summary = summarize([done("t-0", 0, 2, first), done("t-1", 2, 2, second)])

    assert summary.job == JOB
    assert summary.observed_at == NOW
    assert summary.job_status is JobStatus.SUCCEEDED
    assert summary.finished
    assert summary.expected_items == 4
    assert (summary.verified, summary.rejected) == (1, 1)
    assert (summary.needs_review, summary.malformed) == (1, 1)
    assert summary.processed_items == 4
    assert summary.provider_assisted_items == 1
    assert summary.unprocessed_items == summary.missing_items == summary.invalid_items == 0
    assert summary.integrity_ok
    assert summary.anomaly_samples == ()
    assert summary.complete
    assert summary.task_counts[TaskState.SUCCEEDED] == 2


@pytest.mark.parametrize("status", list(Status))
def test_each_status_is_counted_in_its_own_field(status: Status) -> None:
    result = envelope(0, [entry(0, status.value, True)])
    summary = summarize([done("t-0", 0, 1, result)])

    counts = {
        Status.VERIFIED: summary.verified,
        Status.REJECTED: summary.rejected,
        Status.NEEDS_REVIEW: summary.needs_review,
        Status.MALFORMED: summary.malformed,
    }
    assert counts == {s: int(s is status) for s in Status}
    assert summary.provider_assisted_items == 1
    assert summary.complete


def test_a_job_of_only_rejected_items_is_complete_and_succeeded() -> None:
    result = envelope(0, [entry(0, "rejected"), entry(1, "rejected"), entry(2, "rejected")])
    summary = summarize([done("t-0", 0, 3, result)])

    assert summary.job_status is JobStatus.SUCCEEDED
    assert summary.rejected == summary.expected_items == 3
    assert summary.complete


@pytest.mark.parametrize("offset", [0, 3, 100])
def test_result_indices_are_absolute_and_the_offset_is_never_added(offset: int) -> None:
    summary = summarize(at(offset, 3, good(offset, 3)))

    assert summary.verified == offset + 3
    assert raised(summary) == {}
    assert summary.complete

    shifted = envelope(offset, [entry(offset + offset + i) for i in range(3)])
    regression = summarize(at(offset, 3, shifted))
    if offset == 0:  # adding a zero offset changes nothing
        assert regression.complete
    else:
        assert regression.verified == offset
        assert raised(regression) == {AnomalyCode.INDEX_OUT_OF_RANGE: 3}
        assert regression.missing_items == 3
        assert not regression.complete


@pytest.mark.parametrize("offset", [3, 100])
def test_relative_indices_at_a_positive_offset_are_out_of_range(offset: int) -> None:
    relative = envelope(offset, [entry(i) for i in range(3)])
    summary = summarize(at(offset, 3, relative))

    assert summary.processed_items == offset  # only the batch before it
    assert raised(summary) == {AnomalyCode.INDEX_OUT_OF_RANGE: 3}
    assert summary.missing_items == 3
    assert not summary.complete
    assert not summary.integrity_ok


def test_an_index_in_another_tasks_range_is_out_of_range_for_this_task() -> None:
    stolen = envelope(0, [entry(2), entry(3)])
    summary = summarize([done("t-0", 0, 2, stolen), ok("t-1", 2, 2)])

    assert summary.verified == 2  # only the second task's own entries
    assert summary.missing_items == 2
    assert raised(summary) == {AnomalyCode.INDEX_OUT_OF_RANGE: 2}
    assert_partition(summary)


# A batch of three at offset 4 (indices 4, 5 and 6): entries, anomalies raised,
# items processed, invalid and missing.
ENTRY_CASES = [
    pytest.param(
        [entry(4), entry(5), "x"],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        0,
        1,
        id="not-an-object",
    ),
    pytest.param(
        [entry(4), entry(5), {"status": "verified", "provider_assisted": False}],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        0,
        1,
        id="no-index",
    ),
    pytest.param(
        [entry(4), entry(5), {**entry(6), "index": True}],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        0,
        1,
        id="bool-index",
    ),
    pytest.param(
        [entry(4), entry(5), {**entry(6), "index": 6.0}],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        0,
        1,
        id="float-index",
    ),
    pytest.param(
        [entry(4), entry(5), entry(3)],
        {AnomalyCode.INDEX_OUT_OF_RANGE: 1},
        2,
        0,
        1,
        id="below-range",
    ),
    pytest.param(
        [entry(4), entry(5), entry(7)],
        {AnomalyCode.INDEX_OUT_OF_RANGE: 1},
        2,
        0,
        1,
        id="above-range",
    ),
    pytest.param(
        [entry(4), entry(5), entry(-1)],
        {AnomalyCode.INDEX_OUT_OF_RANGE: 1},
        2,
        0,
        1,
        id="negative",
    ),
    pytest.param(
        [entry(4), entry(5), entry(5, "rejected")],
        {AnomalyCode.INDEX_DUPLICATE: 1},
        1,
        1,
        1,
        id="duplicate-counts-no-occurrence",
    ),
    pytest.param(
        [entry(4), entry(5), entry(6, "weird")],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        1,
        0,
        id="unknown-status",
    ),
    pytest.param(
        [entry(4), entry(5), {**entry(6), "status": {"a": 1}}],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        1,
        0,
        id="unhashable-status",
    ),
    pytest.param(
        [entry(4), entry(5), {**entry(6), "provider_assisted": "yes"}],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        1,
        0,
        id="assisted-not-a-bool",
    ),
    pytest.param(
        [entry(4), entry(5), {"index": 6, "status": "verified"}],
        {AnomalyCode.ITEM_INVALID: 1},
        2,
        1,
        0,
        id="assisted-missing",
    ),
    pytest.param(
        [entry(5), entry(4), entry(6)],
        {AnomalyCode.INDEX_OUT_OF_ORDER: 2},
        3,
        0,
        0,
        id="out-of-order-is-counted-and-flagged",
    ),
    pytest.param(
        [{**entry(4), "extra": 1, "findings": "ignored"}, entry(5), entry(6)],
        {},
        3,
        0,
        0,
        id="extra-keys-tolerated",
    ),
]


@pytest.mark.parametrize(("entries", "anomalies", "processed", "invalid", "missing"), ENTRY_CASES)
def test_entries_are_judged_by_their_claimed_index(
    entries: list[JsonValue],
    anomalies: dict[AnomalyCode, int],
    processed: int,
    invalid: int,
    missing: int,
) -> None:
    summary = summarize(at(4, 3, envelope(4, entries)))

    assert raised(summary) == anomalies
    assert summary.processed_items == 4 + processed
    assert summary.invalid_items == invalid
    assert summary.missing_items == missing
    assert summary.expected_items == 7
    assert_partition(summary)
    assert summary.complete is (not anomalies and processed == 3)

    backwards = summarize(at(4, 3, envelope(4, list(reversed(entries)))))
    # The outcome never depends on the order of the entries (out-of-order flags aside).
    assert backwards.processed_items == summary.processed_items
    assert backwards.invalid_items == invalid
    assert backwards.missing_items == missing


# Stored results that do not match a batch of three at offset 4, and the code each earns.
ENVELOPE_CASES = [
    pytest.param(None, AnomalyCode.RESULT_INVALID, id="null"),
    pytest.param([], AnomalyCode.RESULT_INVALID, id="array"),
    pytest.param("x", AnomalyCode.RESULT_INVALID, id="string"),
    pytest.param({"offset": 4}, AnomalyCode.RESULT_INVALID, id="no-results"),
    pytest.param({"results": [entry(4)]}, AnomalyCode.RESULT_INVALID, id="no-offset"),
    pytest.param(
        {**good(4, 3), "extra": 1},
        AnomalyCode.RESULT_INVALID,
        id="extra-envelope-key",
    ),
    pytest.param({"offset": 4, "results": "x"}, AnomalyCode.RESULT_INVALID, id="not-a-list"),
    pytest.param({"offset": 4, "results": {}}, AnomalyCode.RESULT_INVALID, id="an-object"),
    pytest.param(
        {**good(4, 3), "offset": 3},
        AnomalyCode.RESULT_OFFSET_MISMATCH,
        id="offset-differs",
    ),
    pytest.param(
        {**good(4, 3), "offset": True},
        AnomalyCode.RESULT_OFFSET_MISMATCH,
        id="offset-bool",
    ),
    pytest.param(
        {**good(4, 3), "offset": 4.0},
        AnomalyCode.RESULT_OFFSET_MISMATCH,
        id="offset-float",
    ),
    pytest.param(
        {**good(4, 3), "offset": "4"},
        AnomalyCode.RESULT_OFFSET_MISMATCH,
        id="offset-string",
    ),
    pytest.param(
        envelope(4, [entry(4), entry(5)]),
        AnomalyCode.RESULT_LENGTH_MISMATCH,
        id="too-short",
    ),
    pytest.param(
        envelope(4, [entry(4), entry(5), entry(6), entry(6)]),
        AnomalyCode.RESULT_LENGTH_MISMATCH,
        id="too-long",
    ),
    pytest.param(envelope(4, []), AnomalyCode.RESULT_LENGTH_MISMATCH, id="empty"),
]


@pytest.mark.parametrize(("result", "code"), ENVELOPE_CASES)
def test_a_result_envelope_that_does_not_match_the_payload_rejects_the_whole_batch(
    result: JsonValue, code: AnomalyCode
) -> None:
    summary = summarize(at(4, 3, result))

    assert raised(summary) == {code: 1}
    assert summary.anomaly_samples == (Anomaly(code, "t-b", None),)
    assert summary.invalid_items == 3
    assert summary.processed_items == 4  # only the batch before it
    assert summary.missing_items == summary.unprocessed_items == 0
    assert summary.expected_items == 7
    assert not summary.complete
    assert_partition(summary)


def test_a_rejected_batch_does_not_examine_its_entries() -> None:
    bad_entries: list[JsonValue] = ["x", entry(99), entry(4), entry(4)]
    summary = summarize(at(4, 3, envelope(4, bad_entries)))

    assert raised(summary) == {AnomalyCode.RESULT_LENGTH_MISMATCH: 1}
    assert summary.verified == 4  # only the batch before it


def test_an_empty_batch_is_valid_but_never_makes_a_job_complete() -> None:
    summary = summarize([done("t-0", 0, 0, envelope(0, []))])

    assert summary.expected_items == 0
    assert raised(summary) == {}
    assert summary.job_status is JobStatus.SUCCEEDED
    assert not summary.complete


def test_an_empty_batch_next_to_a_real_one_is_contiguous() -> None:
    summary = summarize([done("a", 0, 0, envelope(0, [])), ok("b", 0, 3)])

    assert raised(summary) == {}
    assert summary.complete


@pytest.mark.parametrize(
    "state", [TaskState.QUEUED, TaskState.RUNNING, TaskState.FAILED, TaskState.CANCELLED]
)
def test_a_task_that_did_not_succeed_counts_its_items_as_unprocessed(state: TaskState) -> None:
    unfinished = row("t-1", state, payload_of(2, 3), result=good(2, 3))  # never read
    summary = summarize([ok("t-0", 0, 2), unfinished])

    assert summary.unprocessed_items == 3
    assert summary.processed_items == 2
    assert summary.expected_items == 5
    assert summary.job_status is derive_job_status({TaskState.SUCCEEDED: 1, state: 1})
    assert not summary.complete
    assert summary.integrity_ok
    assert_partition(summary)


INVALID_PAYLOADS: list[JsonValue] = [
    5,
    "x",
    None,
    True,
    [1],
    [],
    {},
    {"invoice_id": "INV-1"},
    {"offset": 1.5, "items": []},
    {"offset": 0, "items": "x"},
    {"offset": True, "items": []},
    {"offset": -1, "items": []},
    {"offset": 0, "items": [], "extra": 1},
    {"offset": 0},
    {"items": []},
    {"offset": "0", "items": []},
    {"offset": None, "items": []},
    {"offset": 1.0, "items": []},
    {"offset": 0, "items": {}},
    {"offset": 0, "items": None},
]


@pytest.mark.parametrize("payload", INVALID_PAYLOADS, ids=repr)
def test_a_payload_that_is_not_a_batch_is_reported_and_hides_the_expected_count(
    payload: JsonValue,
) -> None:
    perfect = good(0, 1)
    summary = summarize([row("t-0", TaskState.SUCCEEDED, payload, perfect), ok("t-1", 0, 2)])

    assert raised(summary) == {AnomalyCode.PAYLOAD_INVALID: 1}
    assert summary.anomaly_samples == (Anomaly(AnomalyCode.PAYLOAD_INVALID, "t-0", None),)
    assert summary.expected_items is None
    assert summary.verified == 2  # the invalid task's result is ignored
    assert not summary.integrity_ok
    assert not summary.complete


PARITY_PAYLOADS: list[JsonValue] = [
    *INVALID_PAYLOADS,
    {"offset": 0, "items": []},
    {"offset": 7, "items": [1, 2]},
    {"offset": 10**30, "items": [{}]},
]


@pytest.mark.parametrize("payload", PARITY_PAYLOADS, ids=repr)
def test_payload_validity_is_the_handlers_rule(payload: JsonValue) -> None:
    try:
        _, items = _batch(payload)
        handler_accepts = True
    except PermanentError:
        handler_accepts = False

    summary = summarize([row("t-0", TaskState.QUEUED, payload)])

    assert (summary.anomalies[AnomalyCode.PAYLOAD_INVALID] == 0) is handler_accepts
    if handler_accepts:
        assert summary.expected_items == len(items)
        assert summary.unprocessed_items == len(items)


def test_a_huge_offset_is_a_valid_batch_not_an_error() -> None:
    offset = 10**30
    summary = summarize([done("t-0", offset, 1, good(offset, 1))])

    assert summary.verified == 1
    assert raised(summary) == {AnomalyCode.PARTITION_NOT_CONTIGUOUS: 1}  # it does not start at 0


def test_an_offset_with_too_many_digits_to_convert_is_an_invalid_payload() -> None:
    limit = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(4300)
    try:
        absurd = Row("t-0", TaskState.QUEUED, "object", 2, True, True, "9" * 6000, 1, None)
        summary = summarize([absurd])
    finally:
        sys.set_int_max_str_digits(limit)

    assert raised(summary) == {AnomalyCode.PAYLOAD_INVALID: 1}


@pytest.mark.parametrize(
    ("layout", "flagged"),
    [
        pytest.param([(0, 2), (3, 2)], "t-1", id="gap"),
        pytest.param([(0, 3), (2, 2)], "t-1", id="overlap"),
        pytest.param([(1, 2), (3, 2)], "t-0", id="does-not-start-at-zero"),
        pytest.param([(0, 2), (2, 2), (5, 1)], "t-2", id="gap-at-the-end"),
    ],
)
def test_ranges_that_do_not_partition_from_zero_are_reported(
    layout: list[tuple[int, int]], flagged: str
) -> None:
    rows = [ok(f"t-{i}", offset, count) for i, (offset, count) in enumerate(layout)]
    summary = summarize(rows)

    flag = Anomaly(AnomalyCode.PARTITION_NOT_CONTIGUOUS, flagged, None)
    assert raised(summary) == {AnomalyCode.PARTITION_NOT_CONTIGUOUS: 1}
    assert summary.anomaly_samples == (flag,)
    assert summary.processed_items == summary.expected_items  # every item still has an outcome
    assert not summary.complete
    assert not summary.integrity_ok
    assert_partition(summary)


def test_task_key_text_is_never_parsed_to_find_the_order() -> None:
    rows = [ok("zzz", 0, 2), ok("aaa", 2, 2), ok("mmm", 4, 1)]

    assert summarize(rows).complete
    assert summarize(list(reversed(rows))).complete


def test_a_null_result_is_never_complete() -> None:
    summary = summarize([done("t-0", 0, 2, None)])

    assert summary.job_status is JobStatus.SUCCEEDED
    assert raised(summary) == {AnomalyCode.RESULT_INVALID: 1}
    assert not summary.complete


def test_complete_needs_a_succeeded_job_and_nothing_left_over() -> None:
    assert summarize([ok("t-0", 0, 2)]).complete
    for state in (TaskState.QUEUED, TaskState.RUNNING, TaskState.FAILED, TaskState.CANCELLED):
        assert not summarize([ok("t-0", 0, 2), pending("t-1", 2, 2, state)]).complete
        assert not summarize([pending("t-0", 0, 2, state)]).complete


def test_a_failed_job_is_finished_and_not_complete() -> None:
    summary = summarize([ok("t-0", 0, 2), pending("t-1", 2, 2, TaskState.FAILED)])

    assert summary.job_status is JobStatus.FAILED
    assert summary.finished
    assert not summary.complete


STATE_MIXES: list[dict[TaskState, int]] = [
    {TaskState.QUEUED: 2},
    {TaskState.QUEUED: 1, TaskState.SUCCEEDED: 1},
    {TaskState.RUNNING: 1},
    {TaskState.FAILED: 1, TaskState.SUCCEEDED: 1},
    {TaskState.CANCELLED: 1, TaskState.SUCCEEDED: 1},
    {TaskState.SUCCEEDED: 3},
]


@pytest.mark.parametrize("counts", STATE_MIXES, ids=repr)
def test_the_job_status_is_derived_from_the_rows_it_was_given(
    counts: dict[TaskState, int],
) -> None:
    rows: list[Row] = []
    for state, n in counts.items():
        for _ in range(n):
            key = f"t-{len(rows)}"
            offset = len(rows)
            if state is TaskState.SUCCEEDED:
                rows.append(ok(key, offset, 1))
            else:
                rows.append(pending(key, offset, 1, state))
    summary = summarize(rows)

    assert summary.job_status is derive_job_status(counts)
    assert dict(summary.task_counts) == {s: counts.get(s, 0) for s in TaskState}
    assert summary.finished is (summary.job_status in FINISHED)


def test_the_output_is_frozen_and_zero_filled() -> None:
    summary = summarize([ok("t-0", 0, 1)])

    assert set(summary.task_counts) == set(TaskState)
    assert set(summary.anomalies) == set(AnomalyCode)
    assert not any(summary.anomalies.values())
    with pytest.raises(dataclasses.FrozenInstanceError):
        summary.verified = 9  # type: ignore[misc]
    with pytest.raises(TypeError):
        summary.task_counts[TaskState.QUEUED] = 9  # type: ignore[index]
    with pytest.raises(TypeError):
        summary.anomalies[AnomalyCode.ITEM_INVALID] = 9  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        Anomaly(AnomalyCode.ITEM_INVALID, "t-0", None).index = 1  # type: ignore[misc]


def test_a_summary_never_carries_a_value_from_the_data() -> None:
    secret = "INV-SECRET-42"
    hostile: Obj = {**entry(0, status=secret), "findings": [{"rule": secret, "path": secret}]}
    reasoned: Obj = {**entry(1), "reason": secret}
    entries: list[JsonValue] = [hostile, reasoned, secret, {"index": secret}]
    items: list[JsonValue] = [{"invoice_id": secret} for _ in range(4)]
    payload: Obj = {"offset": 0, "items": items}
    summary = summarize(
        [
            row("t-0", TaskState.SUCCEEDED, payload, envelope(0, entries)),
            row("t-1", TaskState.SUCCEEDED, secret, None),
        ]
    )

    assert summary.anomaly_samples
    assert secret not in repr(summary)
    assert {a.index for a in summary.anomaly_samples} <= {None, 0, 1, 2, 3}


def test_anomaly_samples_are_bounded_and_independent_of_the_order() -> None:
    count = MAX_SAMPLES * 3
    entries: list[JsonValue] = [entry(i, "weird") for i in range(count)]

    forward = summarize([done("t-0", 0, count, envelope(0, entries))])
    backward = summarize([done("t-0", 0, count, envelope(0, list(reversed(entries))))])

    assert forward.anomalies[AnomalyCode.ITEM_INVALID] == count
    assert forward.invalid_items == count
    assert len(forward.anomaly_samples) == MAX_SAMPLES
    assert [a.index for a in forward.anomaly_samples] == list(range(MAX_SAMPLES))
    assert backward.anomaly_samples == forward.anomaly_samples
    assert_partition(forward)


def test_samples_are_ordered_by_task_key_then_index_then_code() -> None:
    rows = [
        done("b", 0, 1, envelope(0, [entry(0, "weird")])),
        row("a", TaskState.QUEUED, "x"),
        done("c", 1, 1, None),
    ]
    summary = summarize(rows)

    assert [(a.task_key, a.code) for a in summary.anomaly_samples] == [
        ("a", AnomalyCode.PAYLOAD_INVALID),
        ("b", AnomalyCode.ITEM_INVALID),
        ("c", AnomalyCode.RESULT_INVALID),
    ]


def test_provider_assisted_counts_only_accepted_items() -> None:
    entries: list[JsonValue] = [entry(0, assisted=True), entry(1, assisted=True), entry(1)]
    summary = summarize([done("t-0", 0, 3, envelope(0, entries))])

    assert summary.provider_assisted_items == 1  # the duplicated index counts for nothing
    assert summary.verified == 1


def test_a_builder_is_used_once_and_a_job_needs_tasks() -> None:
    with pytest.raises(ValueError, match="at least one task"):
        SummaryBuilder().finish(JOB, NOW)

    builder = SummaryBuilder()
    builder.add(ok("t-0", 0, 1))
    builder.finish(JOB, NOW)
    with pytest.raises(RuntimeError, match="finished"):
        builder.add(ok("t-1", 1, 1))
    with pytest.raises(RuntimeError, match="finished"):
        builder.finish(JOB, NOW)


def task_variants(position: int) -> dict[str, Row]:
    """Every shape this test space gives the task at ``position``: two items at its own offset."""
    offset = position * 2
    key = f"t-{position}"

    def result(entries: list[JsonValue]) -> Row:
        return done(key, offset, 2, envelope(offset, entries))

    return {
        "queued": pending(key, offset, 2, TaskState.QUEUED),
        "running": pending(key, offset, 2, TaskState.RUNNING),
        "failed": pending(key, offset, 2, TaskState.FAILED),
        "cancelled": pending(key, offset, 2, TaskState.CANCELLED),
        "ok": ok(key, offset, 2),
        "duplicate": result([entry(offset), entry(offset)]),
        "junk": result([entry(offset), "junk"]),
        "bad-status": result([entry(offset, "weird"), entry(offset + 1)]),
        "out-of-range": result([entry(offset), entry(offset + 9)]),
        "null": done(key, offset, 2, None),
        "short": result([entry(offset)]),
        "bad-payload": row(key, TaskState.SUCCEEDED, "x", good(offset, 2)),
    }


def test_items_are_conserved_and_order_does_not_matter_over_a_small_space() -> None:
    kinds = list(task_variants(0))
    checked = 0
    for size in range(1, 4):
        for combination in itertools.product(kinds, repeat=size):
            rows = [task_variants(i)[kind] for i, kind in enumerate(combination)]
            summary = summarize(rows)

            assert_partition(summary)
            assert (summary.expected_items is None) is ("bad-payload" in combination)
            assert summary.complete is all(kind == "ok" for kind in combination)
            states = dict.fromkeys(TaskState, 0)
            for r in rows:
                states[r.state] += 1
            assert summary.job_status is derive_job_status(states)
            assert dict(summary.task_counts) == states
            assert summarize(list(reversed(rows))) == summary
            checked += 1
    assert checked == len(kinds) + len(kinds) ** 2 + len(kinds) ** 3


def source_of(module: Any) -> str:
    return Path(module.__file__).read_text(encoding="utf-8")


def imported_modules(source: str) -> set[str]:
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            found.add(node.module or "")
    return found


def test_the_builder_module_is_pure() -> None:
    source = source_of(summary_module)

    assert violations(source) == []
    modules = imported_modules(source)
    assert not {m for m in modules if m.startswith(("orqestra.persistence", "psycopg"))}


def test_persistence_does_not_import_workloads() -> None:
    modules = imported_modules(source_of(snapshots_module))

    assert not {m for m in modules if m.startswith(("orqestra.workloads", "orqestra.aggregation"))}
