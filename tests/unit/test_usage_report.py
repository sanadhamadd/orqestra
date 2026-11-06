"""The pure usage report: capture states, job finality, totals, the cross-check.

Attempt rows are built by hand as ``AttemptUsageRow`` values, and task rows as
``BatchTaskRow`` values through the summary tests' ``project``. The integration
tests check that the snapshot's SQL produces the same rows from a real ledger.
"""

import dataclasses
import itertools
import random
import uuid
from datetime import UTC, datetime
from decimal import Context, Decimal, Inexact, localcontext
from typing import Any

import pytest

from orqestra.aggregation.usage import (
    AttemptCapture,
    JobUsageReport,
    ProvenanceUsage,
    UnitTotal,
    UsageAnomaly,
    UsageAnomalyCode,
    UsageFinality,
    UsageReportBuilder,
    attempt_capture,
    closure_contradicted,
)
from orqestra.domain.jobs import Job, JobStatus, JsonValue
from orqestra.domain.states import TERMINAL_ATTEMPT_STATES, AttemptState, TaskState
from orqestra.persistence.provider_usage import CaptureState, CaptureTier, ClosedBy
from orqestra.persistence.snapshots import AttemptUsageRow, BatchTaskRow, UsageTotalRow
from orqestra.workloads.invoice.handler import JOB_KIND
from orqestra.workloads.invoice.summary import MAX_SAMPLES, SummaryBuilder
from tests.unit.test_invoice_summary import entry, envelope, good, payload_of, project

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
JOB = Job(uuid.UUID(int=7), JOB_KIND, None, NOW)
TERMINAL = sorted(TERMINAL_ATTEMPT_STATES)
COMPLETE = CaptureState.COMPLETE
INCOMPLETE = CaptureState.INCOMPLETE


# --- helpers -----------------------------------------------------------------


def task_id_of(key: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"task/{key}")


def attempt(
    key: str = "t-0",
    number: int = 1,
    state: AttemptState = AttemptState.SUCCEEDED,
    *,
    answered: int = 0,
    failed: int = 0,
    unresolved: int = 0,
    closure: CaptureState | None = None,
    call_count: int | None = None,
    settled_without_amount: int = 0,
    late_intents: int = 0,
    late_results: int = 0,
    estimate: int = 0,
    without_estimate: int = 0,
) -> AttemptUsageRow:
    """An attempt row; a complete closure counts the attempt's intents unless told otherwise."""
    intents = answered + failed + unresolved
    if closure is COMPLETE and call_count is None:
        call_count = intents
    return AttemptUsageRow(
        attempt_id=uuid.uuid5(uuid.NAMESPACE_URL, f"attempt/{key}/{number}"),
        task_id=task_id_of(key),
        task_key=key,
        attempt_number=number,
        state=state,
        intents=intents,
        answered=answered,
        failed=failed,
        unresolved=unresolved,
        settled_without_amount=settled_without_amount,
        late_intents=late_intents,
        late_results=late_results,
        unresolved_estimate_units=estimate,
        unresolved_without_estimate=without_estimate,
        closure_capture=closure,
        closure_call_count=call_count,
        closure_closed_by=None if closure is None else ClosedBy.WORKER,
        closure_tier=None if closure is None else CaptureTier.T1,
    )


def closed(
    key: str = "t-0", number: int = 1, state: AttemptState = AttemptState.SUCCEEDED, **counts: Any
) -> AttemptUsageRow:
    return attempt(key, number, state, closure=COMPLETE, **counts)


def task(
    key: str = "t-0",
    state: TaskState = TaskState.SUCCEEDED,
    result: JsonValue = None,
    payload: JsonValue = None,
) -> BatchTaskRow:
    """A task row; a succeeded one defaults to one verified item without a provider."""
    payload = payload_of(0, 1) if payload is None else payload
    if state is TaskState.SUCCEEDED and result is None:
        result = good(0, 1)
    return BatchTaskRow(
        task_id=task_id_of(key),
        task_key=key,
        state=state,
        result=result if state is TaskState.SUCCEEDED else None,
        **project(payload),
    )


def assisted(count: int) -> JsonValue:
    """A result whose ``count`` items all say a provider read an amount."""
    return envelope(0, [entry(i, assisted=True) for i in range(count)])


def assisted_task(key: str, count: int) -> BatchTaskRow:
    """A succeeded task whose payload has ``count`` items, all assisted in its result."""
    return task(key, result=assisted(count), payload=payload_of(0, count))


def total(state: AttemptState | None, unit: str, calls: int, amount: str) -> UsageTotalRow:
    return UsageTotalRow(state, unit, calls, Decimal(amount))


def build(
    attempts: list[AttemptUsageRow],
    tasks: list[BatchTaskRow],
    totals: list[UsageTotalRow] | None = None,
) -> JobUsageReport:
    builder = UsageReportBuilder(JOB, NOW)
    builder.add_totals(totals or [])
    for attempt_row in attempts:
        builder.add_attempt(attempt_row)
    for task_row in tasks:
        builder.add_task(task_row)
    return builder.finish()


def raised(report: JobUsageReport) -> dict[UsageAnomalyCode, int]:
    return {code: n for code, n in report.anomalies.items() if n}


def captures(report: JobUsageReport) -> dict[AttemptCapture, int]:
    return {capture: n for capture, n in report.attempts_by_capture.items() if n}


def assert_conserved(report: JobUsageReport) -> None:
    """Counts add up, and the per-state figures add up to the job's, unit by unit.

    Amounts are added in an exact context: the default 28 digits would round a
    total the database kept exactly, and any rounding here raises ``Inexact``.
    """
    assert report.calls == report.answered + report.failed + report.unresolved
    states = report.by_attempt_state.values()
    assert sum(s.attempts for s in states) == sum(report.attempts_by_capture.values())
    assert sum(s.calls for s in states) == report.calls
    assert sum(s.answered for s in states) == report.answered
    assert sum(s.failed for s in states) == report.failed
    assert sum(s.unresolved for s in states) == report.unresolved
    for s in states:
        assert s.calls == s.answered + s.failed + s.unresolved
    units = {unit for s in states for unit in s.reported_totals}
    assert units == set(report.reported_totals)
    for unit, job_total in report.reported_totals.items():
        parts = [s.reported_totals[unit] for s in states if unit in s.reported_totals]
        assert sum(p.calls for p in parts) == job_total.calls
        with localcontext(Context(prec=200, traps=[Inexact])):
            assert sum((p.amount for p in parts), Decimal(0)) == job_total.amount


# --- the capture of one attempt ----------------------------------------------

# (attempt row for a finished attempt, its capture, whether its closure is contradicted)
FINISHED_CASES = [
    pytest.param({}, AttemptCapture.NOT_CAPTURED, False, id="nothing-recorded"),
    pytest.param({"answered": 1}, AttemptCapture.LOWER_BOUND, False, id="calls-no-closure"),
    pytest.param({"unresolved": 1}, AttemptCapture.LOWER_BOUND, False, id="unresolved-no-closure"),
    pytest.param({"closure": COMPLETE}, AttemptCapture.CLOSED, False, id="complete-zero"),
    pytest.param(
        {"closure": COMPLETE, "answered": 2, "failed": 1},
        AttemptCapture.CLOSED,
        False,
        id="complete-matching",
    ),
    pytest.param(
        {"closure": COMPLETE, "answered": 2, "call_count": 1},
        AttemptCapture.LOWER_BOUND,
        True,
        id="complete-then-a-later-intent-answered",
    ),
    pytest.param(
        {"closure": COMPLETE, "answered": 1, "unresolved": 1, "call_count": 1},
        AttemptCapture.LOWER_BOUND,
        True,
        id="complete-then-a-later-intent-unresolved",
    ),
    pytest.param(
        {"closure": COMPLETE, "answered": 1, "unresolved": 1, "call_count": 2},
        AttemptCapture.LOWER_BOUND,
        True,
        id="complete-count-matches-but-unresolved",
    ),
    pytest.param({"closure": INCOMPLETE}, AttemptCapture.LOWER_BOUND, False, id="incomplete-zero"),
    pytest.param(
        {"closure": INCOMPLETE, "answered": 1, "unresolved": 1},
        AttemptCapture.LOWER_BOUND,
        False,
        id="incomplete-with-calls",
    ),
]


@pytest.mark.parametrize("state", TERMINAL)
@pytest.mark.parametrize(("fields", "expected", "contradicted"), FINISHED_CASES)
def test_the_capture_of_a_finished_attempt_follows_its_closure_and_its_calls(
    state: AttemptState, fields: dict[str, Any], expected: AttemptCapture, contradicted: bool
) -> None:
    row = attempt(state=state, **fields)

    assert attempt_capture(row) is expected
    assert closure_contradicted(row) is contradicted


@pytest.mark.parametrize(("fields", "expected", "contradicted"), FINISHED_CASES)
def test_a_running_attempt_is_open_whatever_its_ledger_says(
    fields: dict[str, Any], expected: AttemptCapture, contradicted: bool
) -> None:
    row = attempt(state=AttemptState.RUNNING, **fields)

    assert attempt_capture(row) is AttemptCapture.OPEN
    assert closure_contradicted(row) is contradicted  # a ledger fact, whatever the state


def test_only_a_confirmed_complete_closure_is_closed() -> None:
    closures = [None, COMPLETE, INCOMPLETE]
    for state, closure, answered, unresolved, count in itertools.product(
        AttemptState, closures, range(3), range(2), [None, 0, 1, 2, 3]
    ):
        call_count = count if closure is COMPLETE else None
        row = attempt(
            state=state,
            answered=answered,
            unresolved=unresolved,
            closure=closure,
            call_count=call_count,
        )
        capture = attempt_capture(row)
        if capture is AttemptCapture.CLOSED:
            assert state is not AttemptState.RUNNING
            assert closure is COMPLETE
            assert row.closure_call_count == row.intents
            assert row.unresolved == 0
        if capture is AttemptCapture.NOT_CAPTURED:
            assert (closure, row.intents) == (None, 0)
        assert (capture is AttemptCapture.OPEN) is (state is AttemptState.RUNNING)


# --- job finality -------------------------------------------------------------


def test_a_finished_job_whose_every_attempt_is_closed_is_closed() -> None:
    report = build(
        [
            closed("t-0", 1, AttemptState.REVOKED, answered=1),
            closed("t-0", 2, AttemptState.FAILED, failed=1),
            closed("t-0", 3, answered=2),
            closed("t-1", 1, AttemptState.FAILED),
        ],
        [task("t-0"), task("t-1", TaskState.FAILED)],
    )

    assert report.job_status is JobStatus.FAILED
    assert report.finished
    assert report.finality is UsageFinality.CLOSED
    assert captures(report) == {AttemptCapture.CLOSED: 4}
    assert report.unclosed_attempts == 0
    assert_conserved(report)


def test_a_finished_job_without_attempts_is_a_closed_zero() -> None:
    report = build([], [task("t-0", TaskState.CANCELLED), task("t-1", TaskState.CANCELLED)])

    assert report.job_status is JobStatus.CANCELLED
    assert report.finality is UsageFinality.CLOSED
    assert report.calls == 0
    assert dict(report.reported_totals) == {}


@pytest.mark.parametrize(
    "other",
    [
        pytest.param(attempt("t-1"), id="not-captured"),
        pytest.param(attempt("t-1", answered=1), id="unclosed"),
        pytest.param(attempt("t-1", closure=INCOMPLETE), id="incomplete"),
        pytest.param(closed("t-1", call_count=0, answered=1), id="contradicted"),
    ],
)
def test_a_finished_job_with_any_attempt_not_closed_is_a_lower_bound(
    other: AttemptUsageRow,
) -> None:
    report = build([closed("t-0", answered=1), other], [task("t-0"), task("t-1")])

    assert report.job_status is JobStatus.SUCCEEDED
    assert report.finality is UsageFinality.LOWER_BOUND


@pytest.mark.parametrize(
    ("attempts", "tasks"),
    [
        pytest.param(
            [attempt("t-0", state=AttemptState.RUNNING, answered=1)],
            [task("t-0", TaskState.RUNNING)],
            id="running-attempt",
        ),
        pytest.param(
            [closed("t-0")],
            [task("t-0"), task("t-1", TaskState.QUEUED)],
            id="queued-task",
        ),
        pytest.param(
            [attempt("t-0", 1, AttemptState.REVOKED, closure=INCOMPLETE)],
            [task("t-0", TaskState.QUEUED)],
            id="requeued-task",
        ),
        pytest.param(
            [closed("t-0"), attempt("t-1", state=AttemptState.RUNNING)],
            [task("t-0"), task("t-1")],
            id="running-attempt-in-a-finished-job",
        ),
    ],
)
def test_an_unfinished_job_or_a_running_attempt_is_open(
    attempts: list[AttemptUsageRow], tasks: list[BatchTaskRow]
) -> None:
    report = build(attempts, tasks)

    assert report.finality is UsageFinality.OPEN


def test_absent_capture_is_not_a_zero() -> None:
    zero = build([closed("t-0")], [task("t-0")])
    absent = build([attempt("t-0")], [task("t-0")])

    for report in (zero, absent):
        assert (report.calls, dict(report.reported_totals)) == (0, {})
    assert zero.finality is UsageFinality.CLOSED
    assert captures(zero) == {AttemptCapture.CLOSED: 1}
    assert absent.finality is UsageFinality.LOWER_BOUND
    assert captures(absent) == {AttemptCapture.NOT_CAPTURED: 1}
    assert absent.unclosed_attempts == 0  # nothing recorded, so nothing is unclosed


def test_a_finished_attempt_with_calls_but_no_closure_is_unclosed() -> None:
    report = build(
        [
            attempt("t-0", 1, AttemptState.REVOKED, answered=1, unresolved=1),
            attempt("t-0", 2, AttemptState.RUNNING, answered=1),
        ],
        [task("t-0", TaskState.RUNNING)],
    )

    assert report.unclosed_attempts == 1  # a running attempt is not counted
    assert captures(report) == {AttemptCapture.LOWER_BOUND: 1, AttemptCapture.OPEN: 1}


def test_a_contradicted_closure_is_reported_and_makes_a_lower_bound() -> None:
    report = build([closed("t-0", 2, call_count=1, answered=1, unresolved=1)], [task("t-0")])

    assert raised(report) == {UsageAnomalyCode.CLOSURE_CONTRADICTED: 1}
    assert report.anomaly_samples == (
        UsageAnomaly(UsageAnomalyCode.CLOSURE_CONTRADICTED, "t-0", 2),
    )
    assert report.finality is UsageFinality.LOWER_BOUND


# --- counts, totals and exposure ----------------------------------------------


def test_counts_and_totals_are_split_by_the_current_attempt_state() -> None:
    attempts = [
        attempt(
            "t-0",
            1,
            AttemptState.REVOKED,
            answered=1,
            unresolved=2,
            closure=INCOMPLETE,
            late_results=1,
            estimate=7,
            without_estimate=1,
        ),
        closed("t-0", 2, AttemptState.FAILED, answered=1, failed=1, settled_without_amount=1),
        closed("t-0", 3, answered=1, late_intents=0),
    ]
    totals = [
        total(None, "credits", 3, "2.00"),
        total(None, "Credits", 1, "1"),
        total(AttemptState.REVOKED, "credits", 1, "0.25"),
        total(AttemptState.FAILED, "credits", 1, "1.50"),
        total(AttemptState.FAILED, "Credits", 1, "1"),
        total(AttemptState.SUCCEEDED, "credits", 1, "0.25"),
    ]

    report = build(attempts, [task("t-0")], totals)

    assert (report.calls, report.answered, report.failed, report.unresolved) == (6, 3, 1, 2)
    assert report.settled_without_reported_usage == 1
    assert (report.late_intents, report.late_results) == (0, 1)
    assert (report.estimated_exposure_units, report.unresolved_without_estimate) == (7, 1)
    assert dict(report.reported_totals) == {
        "Credits": UnitTotal(Decimal(1), 1),
        "credits": UnitTotal(Decimal("2.00"), 3),
    }
    assert list(report.reported_totals) == ["Credits", "credits"]  # by code point
    revoked = report.by_attempt_state[AttemptState.REVOKED]
    assert revoked == ProvenanceUsage(1, 3, 1, 0, 2, revoked.reported_totals)
    assert dict(revoked.reported_totals) == {"credits": UnitTotal(Decimal("0.25"), 1)}
    failed = report.by_attempt_state[AttemptState.FAILED]
    assert (failed.attempts, failed.calls, failed.answered, failed.failed) == (1, 2, 1, 1)
    assert set(failed.reported_totals) == {"credits", "Credits"}
    succeeded = report.by_attempt_state[AttemptState.SUCCEEDED]
    assert (succeeded.attempts, succeeded.calls) == (1, 1)
    for state in (AttemptState.RUNNING, AttemptState.CANCELLED):
        assert report.by_attempt_state[state] == ProvenanceUsage(0, 0, 0, 0, 0, {})
    assert report.finality is UsageFinality.LOWER_BOUND
    assert_conserved(report)


def test_a_unit_is_kept_exactly_as_given() -> None:
    units = ["credits", "Credits", "\N{MICRO SIGN}-credits", " credits"]
    totals = [total(None, unit, 1, "1") for unit in units]
    totals += [total(AttemptState.SUCCEEDED, unit, 1, "1") for unit in units]

    report = build([closed("t-0", answered=4)], [task("t-0")], totals)

    assert set(report.reported_totals) == set(units)
    assert_conserved(report)


def test_an_amount_is_kept_as_given_with_its_scale() -> None:
    big = "199999999999999999999.999999999999999998"  # 39 digits
    totals = [total(None, "credits", 2, big), total(AttemptState.SUCCEEDED, "credits", 2, big)]

    report = build([closed("t-0", answered=2)], [task("t-0")], totals)

    assert str(report.reported_totals["credits"].amount) == big
    scaled = build(
        [closed("t-0", answered=2)],
        [task("t-0")],
        [total(None, "u", 2, "1.75"), total(AttemptState.SUCCEEDED, "u", 2, "1.75")],
    )
    assert str(scaled.reported_totals["u"].amount) == "1.75"


@pytest.mark.parametrize("state", [None, AttemptState.FAILED])
def test_a_total_given_twice_is_refused(state: AttemptState | None) -> None:
    builder = UsageReportBuilder(JOB, NOW)
    builder.add_total(total(state, "credits", 1, "1"))
    builder.add_total(total(state, "Credits", 1, "1"))

    with pytest.raises(ValueError, match="twice"):
        builder.add_total(total(state, "credits", 1, "1"))


# --- the cross-check against the summary --------------------------------------


def test_a_closed_succeeded_attempt_with_enough_answers_is_checked() -> None:
    report = build([closed("t-0", answered=2)], [assisted_task("t-0", 2)])

    assert report.assisted_items == 2
    assert (report.assisted_checked_tasks, report.assisted_unchecked_tasks) == (1, 0)
    assert raised(report) == {}


def test_more_answers_than_assisted_items_are_not_an_anomaly() -> None:
    report = build([closed("t-0", answered=3, failed=1)], [task("t-0", result=assisted(1))])

    assert report.assisted_checked_tasks == 1
    assert raised(report) == {}


def test_fewer_answers_than_assisted_items_are_reported_not_raised() -> None:
    report = build(
        [closed("t-0", 1, AttemptState.REVOKED, answered=5), closed("t-0", 2, answered=1)],
        [assisted_task("t-0", 2)],
    )

    assert raised(report) == {UsageAnomalyCode.ASSISTED_EXCEEDS_ANSWERED: 1}
    assert report.anomaly_samples == (
        UsageAnomaly(UsageAnomalyCode.ASSISTED_EXCEEDS_ANSWERED, "t-0", 2),
    )
    assert report.assisted_checked_tasks == 1  # the earlier attempt's answers do not count


@pytest.mark.parametrize(
    "attempts",
    [
        pytest.param([attempt("t-0", answered=0)], id="not-captured"),
        pytest.param([attempt("t-0", answered=1)], id="unclosed"),
        pytest.param([attempt("t-0", closure=INCOMPLETE)], id="incomplete"),
        pytest.param([closed("t-0", call_count=0, answered=1)], id="contradicted"),
        pytest.param([], id="no-succeeded-attempt"),
        pytest.param([closed("t-0", 1, AttemptState.FAILED)], id="only-a-failed-attempt"),
    ],
)
def test_a_succeeded_task_whose_attempt_is_not_closed_is_unchecked(
    attempts: list[AttemptUsageRow],
) -> None:
    report = build(attempts, [assisted_task("t-0", 3)])

    assert report.assisted_items == 3
    assert (report.assisted_checked_tasks, report.assisted_unchecked_tasks) == (0, 1)
    assert UsageAnomalyCode.ASSISTED_EXCEEDS_ANSWERED not in raised(report)


def test_unfinished_tasks_are_neither_checked_nor_unchecked() -> None:
    tasks = [
        task("t-0", TaskState.FAILED),
        task("t-1", TaskState.RUNNING),
        task("t-2", TaskState.QUEUED),
    ]
    report = build(
        [closed("t-0", 1, AttemptState.FAILED), attempt("t-1", state=AttemptState.RUNNING)], tasks
    )

    assert (report.assisted_items, report.assisted_checked_tasks) == (0, 0)
    assert report.assisted_unchecked_tasks == 0


def test_assisted_items_are_counted_by_the_summary_rules() -> None:
    hostile = envelope(0, [entry(0, assisted=True), entry(0, assisted=True)])  # a duplicate
    tasks = [
        task("t-0", result=assisted(2), payload=payload_of(0, 2)),
        task("t-1", result=hostile, payload=payload_of(0, 2)),
        task("t-2", result=assisted(1), payload="not a batch"),
        task("t-3", result="not an envelope"),
        task("t-4", result=envelope(0, [{**entry(0), "provider_assisted": "yes"}])),
        task("t-5", TaskState.FAILED),
    ]
    attempts = [closed(t.task_key, answered=2) for t in tasks if t.state is TaskState.SUCCEEDED]
    summary = SummaryBuilder()
    for row in tasks:
        summary.add(row)

    report = build(attempts, tasks)

    assert report.assisted_items == summary.finish(JOB, NOW).provider_assisted_items == 2


def test_every_attempt_must_come_before_the_first_task() -> None:
    builder = UsageReportBuilder(JOB, NOW)
    builder.add_attempt(closed("t-0"))
    builder.add_task(task("t-0"))

    with pytest.raises(RuntimeError, match="before the first task"):
        builder.add_attempt(closed("t-1"))


# --- samples, output shape, misuse ---------------------------------------------


def test_anomaly_samples_are_bounded_and_independent_of_the_order() -> None:
    count = MAX_SAMPLES * 3
    attempts = [closed(f"t-{i:03d}", 1, call_count=0, answered=1) for i in range(count)]
    tasks = [assisted_task(f"t-{i:03d}", 2) for i in range(count)]
    code = UsageAnomalyCode.CLOSURE_CONTRADICTED
    expected = tuple(UsageAnomaly(code, f"t-{i:03d}", 1) for i in range(MAX_SAMPLES))

    reference = build(attempts, tasks)
    assert reference.anomalies[UsageAnomalyCode.CLOSURE_CONTRADICTED] == count
    assert reference.anomaly_samples == expected
    shuffle = random.Random(20261104)
    for _ in range(5):
        shuffle.shuffle(attempts)
        shuffle.shuffle(tasks)
        assert build(attempts, tasks) == reference


def test_samples_of_both_codes_are_ordered_by_task_attempt_and_code() -> None:
    report = build(
        [
            closed("t-1", 1, AttemptState.REVOKED, call_count=0, answered=1),
            closed("t-1", 2, answered=0),
            closed("t-0", 3, call_count=0, answered=1),
        ],
        [task("t-1", result=assisted(1)), task("t-0", result=assisted(1))],
    )

    assert [(a.task_key, a.attempt_number, a.code) for a in report.anomaly_samples] == [
        ("t-0", 3, UsageAnomalyCode.CLOSURE_CONTRADICTED),
        ("t-1", 1, UsageAnomalyCode.CLOSURE_CONTRADICTED),
        ("t-1", 2, UsageAnomalyCode.ASSISTED_EXCEEDS_ANSWERED),
    ]


def test_the_output_is_frozen_and_zero_filled() -> None:
    report = build([closed("t-0")], [task("t-0")], [total(None, "u", 1, "1")])

    assert set(report.attempts_by_capture) == set(AttemptCapture)
    assert set(report.anomalies) == set(UsageAnomalyCode)
    assert set(report.by_attempt_state) == set(AttemptState)
    assert not any(report.anomalies.values())
    with pytest.raises(dataclasses.FrozenInstanceError):
        report.calls = 9  # type: ignore[misc]
    with pytest.raises(TypeError):
        report.attempts_by_capture[AttemptCapture.OPEN] = 9  # type: ignore[index]
    with pytest.raises(TypeError):
        report.reported_totals["u"] = UnitTotal(Decimal(0), 0)  # type: ignore[index]
    with pytest.raises(TypeError):
        report.by_attempt_state[AttemptState.RUNNING] = None  # type: ignore[index]
    with pytest.raises(TypeError):
        report.anomalies[UsageAnomalyCode.CLOSURE_CONTRADICTED] = 9  # type: ignore[index]
    with pytest.raises(dataclasses.FrozenInstanceError):
        report.reported_totals["u"].calls = 2  # type: ignore[misc]
    succeeded = report.by_attempt_state[AttemptState.SUCCEEDED]
    with pytest.raises(TypeError):
        succeeded.reported_totals["v"] = UnitTotal(Decimal(0), 0)  # type: ignore[index]


def test_anomalies_never_carry_a_value_from_the_data() -> None:
    secret = "INV-SECRET-42"
    hostile = envelope(0, [{**entry(0, assisted=True), "reason": secret, "findings": [secret]}])
    report = build(
        [closed("t-0", call_count=0, answered=0, unresolved=1)],
        [task("t-0", result=hostile)],
    )

    assert report.anomaly_samples
    assert secret not in repr(report.anomaly_samples)
    assert secret not in repr(report.anomalies)
    assert {f.name for f in dataclasses.fields(UsageAnomaly)} == {
        "code",
        "task_key",
        "attempt_number",
    }


def test_a_job_without_tasks_is_a_value_error() -> None:
    with pytest.raises(ValueError, match="at least one task"):
        build([], [])


def test_a_finished_report_accepts_nothing_more() -> None:
    builder = UsageReportBuilder(JOB, NOW)
    builder.add_task(task("t-0"))
    builder.finish()

    with pytest.raises(RuntimeError, match="already finished"):
        builder.finish()
    with pytest.raises(RuntimeError, match="already finished"):
        builder.add_task(task("t-1"))
    with pytest.raises(RuntimeError, match="already finished"):
        builder.add_attempt(closed("t-1"))
    with pytest.raises(RuntimeError, match="already finished"):
        builder.add_total(total(None, "u", 1, "1"))


def test_the_report_names_its_job_and_snapshot_time() -> None:
    report = build([closed("t-0")], [task("t-0")])

    assert (report.job, report.observed_at) == (JOB, NOW)
    assert report.job_status is JobStatus.SUCCEEDED
