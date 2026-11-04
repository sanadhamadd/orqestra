"""Report one invoice job's provider usage from its ledger, next to its summary.

``report_job_usage`` reads, in one consistent snapshot (``job_batch_snapshot``),
the job row, its exact usage totals, one usage row per attempt and the task rows,
and folds them with the pure ``UsageReportBuilder``. The snapshot ends before
the report is built.

What a report says, and what it does not:

- **Reported figures only.** Amounts are what providers reported, summed by
  PostgreSQL per exact unit string: no unit is normalized or converted, and
  no currency, price or cost is derived. ``credits`` and ``Credits`` are two
  units.
- **Finality.** Each attempt's capture is ``OPEN`` (still running),
  ``CLOSED`` (a complete closure that this snapshot confirms),
  ``LOWER_BOUND`` (calls may be missing) or ``NOT_CAPTURED`` (finished with
  nothing recorded: absent capture, never a zero). A job's figures are
  ``CLOSED`` only when every attempt is, and ``OPEN`` while the job or any
  attempt is unfinished. A ``CLOSED`` report is a statement about this
  snapshot: the ledger accepts late writes, and a later one turns the
  attempt's closure into a contradiction.
- **Exposure.** ``estimated_exposure_units`` sums the route estimates of
  calls that have no result yet. Estimates are opaque route units, never added
  to reported amounts; a call without an estimate is counted apart, never read
  as zero.
- **Cross-check.** For each succeeded task, the items its result marks
  ``provider_assisted`` (by the summary's own rules, ``SummaryBuilder``) are
  compared with the answered calls of its succeeded attempt, when that
  attempt is ``CLOSED``. Fewer answered calls than assisted items is reported
  as an anomaly, never raised. ``assisted_items`` equals the summary's
  ``provider_assisted_items`` for the same database state.

Anomalies carry a code, a task key and an attempt number, never a value from
the data.
"""

import bisect
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from orqestra.aggregation.invoice import WrongJobKindError
from orqestra.domain.jobs import Job, JobStatus, derive_job_status
from orqestra.domain.states import AttemptState, TaskState
from orqestra.persistence._shared import Conn
from orqestra.persistence.provider_usage import CaptureState
from orqestra.persistence.snapshots import (
    DEFAULT_FETCH_SIZE,
    AttemptUsageRow,
    JobBatchSnapshot,
    UsageTotalRow,
    job_batch_snapshot,
)
from orqestra.workloads.invoice.handler import JOB_KIND
from orqestra.workloads.invoice.summary import MAX_SAMPLES, SummaryBuilder, TaskRowLike

_FINISHED = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED})


class AttemptCapture(StrEnum):
    """How much of one attempt's provider usage the ledger is known to hold."""

    OPEN = "open"
    CLOSED = "closed"
    LOWER_BOUND = "lower_bound"
    NOT_CAPTURED = "not_captured"


class UsageFinality(StrEnum):
    """Whether a job's figures are final as of the snapshot, a lower bound, or still open."""

    OPEN = "open"
    CLOSED = "closed"
    LOWER_BOUND = "lower_bound"


class UsageAnomalyCode(StrEnum):
    """The closed set of ledger inconsistencies a report names. Values are stable identifiers."""

    CLOSURE_CONTRADICTED = "closure_contradicted"
    ASSISTED_EXCEEDS_ANSWERED = "assisted_exceeds_answered"


@dataclass(frozen=True, slots=True)
class UsageAnomaly:
    """One reported inconsistency: a code, the task key and the attempt number."""

    code: UsageAnomalyCode
    task_key: str
    attempt_number: int

    def sort_key(self) -> tuple[str, int, str]:
        return (self.task_key, self.attempt_number, self.code.value)


@dataclass(frozen=True, slots=True)
class UnitTotal:
    """The exact sum of the amounts reported in one unit, and how many calls reported one."""

    amount: Decimal
    calls: int


@dataclass(frozen=True, slots=True)
class ProvenanceUsage:
    """The usage of the attempts that are currently in one state.

    ``calls == answered + failed + unresolved``. ``reported_totals`` is keyed by
    the exact unit string.
    """

    attempts: int
    calls: int
    answered: int
    failed: int
    unresolved: int
    reported_totals: Mapping[str, UnitTotal]


@dataclass(frozen=True, slots=True)
class JobUsageReport:
    """What a job's provider usage ledger says, as of one consistent read.

    ``observed_at`` is the database ``statement_timestamp()`` of the snapshot's
    first query: approximate, not the snapshot instant. ``job_status`` is
    derived from this snapshot's own task rows.

    ``calls == answered + failed + unresolved``, and the per-state figures in
    ``by_attempt_state`` add up to the job's counts and, unit by unit, to
    ``reported_totals``. ``settled_without_reported_usage`` counts answered or
    failed calls that reported no amount. A true zero is ``CLOSED`` with no
    call; a job whose attempts recorded nothing and closed nothing is
    ``LOWER_BOUND`` with ``NOT_CAPTURED`` attempts.

    ``unclosed_attempts`` counts finished attempts that recorded calls but no
    closure. ``late_intents`` and ``late_results`` are advisory.
    ``assisted_checked_tasks`` counts succeeded tasks whose succeeded attempt
    is ``CLOSED`` and was compared; ``assisted_unchecked_tasks`` counts the other
    succeeded tasks.
    """

    job: Job
    observed_at: datetime
    job_status: JobStatus
    finality: UsageFinality
    attempts_by_capture: Mapping[AttemptCapture, int]
    unclosed_attempts: int
    calls: int
    answered: int
    failed: int
    unresolved: int
    settled_without_reported_usage: int
    reported_totals: Mapping[str, UnitTotal]
    by_attempt_state: Mapping[AttemptState, ProvenanceUsage]
    late_intents: int
    late_results: int
    estimated_exposure_units: int
    unresolved_without_estimate: int
    assisted_items: int
    assisted_checked_tasks: int
    assisted_unchecked_tasks: int
    anomalies: Mapping[UsageAnomalyCode, int]
    anomaly_samples: tuple[UsageAnomaly, ...]

    @property
    def finished(self) -> bool:
        return self.job_status in _FINISHED


class UsageTaskRowLike(TaskRowLike, Protocol):
    """A task row as the summary reads it, with the task's id."""

    @property
    def task_id(self) -> uuid.UUID: ...


def closure_contradicted(row: AttemptUsageRow) -> bool:
    """A complete closure that no longer counts every intent, each with its result.

    The ledger accepts an intent or a result after a complete closure, so a
    report re-checks it against the calls in its own snapshot.
    """
    return row.closure_capture is CaptureState.COMPLETE and (
        row.closure_call_count != row.intents or row.unresolved != 0
    )


def attempt_capture(row: AttemptUsageRow) -> AttemptCapture:
    """The capture state of one attempt, as of the snapshot it was read from."""
    if row.state is AttemptState.RUNNING:
        return AttemptCapture.OPEN
    if row.closure_capture is None:
        return AttemptCapture.NOT_CAPTURED if row.intents == 0 else AttemptCapture.LOWER_BOUND
    if row.closure_capture is CaptureState.COMPLETE and not closure_contradicted(row):
        return AttemptCapture.CLOSED
    return AttemptCapture.LOWER_BOUND


@dataclass(slots=True)
class _StateTally:
    attempts: int = 0
    calls: int = 0
    answered: int = 0
    failed: int = 0
    unresolved: int = 0
    totals: dict[str, UnitTotal] = field(default_factory=dict)

    def freeze(self) -> ProvenanceUsage:
        return ProvenanceUsage(
            attempts=self.attempts,
            calls=self.calls,
            answered=self.answered,
            failed=self.failed,
            unresolved=self.unresolved,
            reported_totals=_frozen_totals(self.totals),
        )


@dataclass(slots=True)
class UsageReportBuilder:
    """Fold one snapshot's usage rows into a ``JobUsageReport``.

    Pure, with no I/O. Add the totals, then every attempt row, then every task
    row, each once, and finish once. Attempts must all be added before the
    first task, because a task is cross-checked against its succeeded attempt.
    Memory is the counters, one ``(attempt_number, answered, capture)`` entry per
    succeeded attempt until its task is read, and at most ``MAX_SAMPLES`` anomaly
    samples.
    """

    job: Job
    observed_at: datetime
    _job_totals: dict[str, UnitTotal] = field(default_factory=dict)
    _states: dict[AttemptState, _StateTally] = field(
        default_factory=lambda: {state: _StateTally() for state in AttemptState}
    )
    _captures: dict[AttemptCapture, int] = field(
        default_factory=lambda: dict.fromkeys(AttemptCapture, 0)
    )
    _task_states: dict[TaskState, int] = field(default_factory=lambda: dict.fromkeys(TaskState, 0))
    _anomalies: dict[UsageAnomalyCode, int] = field(
        default_factory=lambda: dict.fromkeys(UsageAnomalyCode, 0)
    )
    _samples: list[UsageAnomaly] = field(default_factory=list)
    _succeeded: dict[uuid.UUID, tuple[int, int, AttemptCapture]] = field(default_factory=dict)
    _unclosed: int = 0
    _settled_without_amount: int = 0
    _late_intents: int = 0
    _late_results: int = 0
    _exposure: int = 0
    _without_estimate: int = 0
    _assisted: int = 0
    _checked: int = 0
    _unchecked: int = 0
    _tasks_started: bool = False
    _finished: bool = False

    def add_total(self, row: UsageTotalRow) -> None:
        """Record one exact total; each (state, unit) pair is given once, else ``ValueError``."""
        self._check_open()
        state = row.attempt_state
        totals = self._job_totals if state is None else self._states[state].totals
        if row.unit in totals:
            msg = "a usage total was given twice for the same unit and state"
            raise ValueError(msg)
        totals[row.unit] = UnitTotal(row.amount, row.calls)

    def add_totals(self, rows: Iterable[UsageTotalRow]) -> None:
        for row in rows:
            self.add_total(row)

    def add_attempt(self, row: AttemptUsageRow) -> None:
        """Account for one attempt. Inconsistencies are reported as anomalies, never raised."""
        self._check_open()
        if self._tasks_started:
            msg = "every attempt must be added before the first task"
            raise RuntimeError(msg)
        capture = attempt_capture(row)
        self._captures[capture] += 1
        tally = self._states[row.state]
        tally.attempts += 1
        tally.calls += row.intents
        tally.answered += row.answered
        tally.failed += row.failed
        tally.unresolved += row.unresolved
        self._settled_without_amount += row.settled_without_amount
        self._late_intents += row.late_intents
        self._late_results += row.late_results
        self._exposure += row.unresolved_estimate_units
        self._without_estimate += row.unresolved_without_estimate
        if row.state is not AttemptState.RUNNING and row.closure_capture is None and row.intents:
            self._unclosed += 1
        if closure_contradicted(row):
            self._report(UsageAnomalyCode.CLOSURE_CONTRADICTED, row.task_key, row.attempt_number)
        if row.state is AttemptState.SUCCEEDED:
            self._succeeded[row.task_id] = (row.attempt_number, row.answered, capture)

    def add_task(self, row: UsageTaskRowLike) -> None:
        """Account for one task: its state, and the cross-check of a succeeded one."""
        self._check_open()
        self._tasks_started = True
        self._task_states[row.state] += 1
        if row.state is not TaskState.SUCCEEDED:
            return
        assisted = _assisted_items(row, self.job, self.observed_at)
        self._assisted += assisted
        attempt = self._succeeded.pop(row.task_id, None)
        if attempt is None or attempt[2] is not AttemptCapture.CLOSED:
            self._unchecked += 1
            return
        attempt_number, answered, _ = attempt
        self._checked += 1
        if answered < assisted:
            self._report(UsageAnomalyCode.ASSISTED_EXCEEDS_ANSWERED, row.task_key, attempt_number)

    def finish(self) -> JobUsageReport:
        """The report. Raises ``ValueError`` for a job without tasks (``derive_job_status``)."""
        self._check_open()
        status = derive_job_status(self._task_states)
        self._finished = True
        tallies = self._states.values()
        return JobUsageReport(
            job=self.job,
            observed_at=self.observed_at,
            job_status=status,
            finality=self._finality(status),
            attempts_by_capture=MappingProxyType(dict(self._captures)),
            unclosed_attempts=self._unclosed,
            calls=sum(t.calls for t in tallies),
            answered=sum(t.answered for t in tallies),
            failed=sum(t.failed for t in tallies),
            unresolved=sum(t.unresolved for t in tallies),
            settled_without_reported_usage=self._settled_without_amount,
            reported_totals=_frozen_totals(self._job_totals),
            by_attempt_state=MappingProxyType(
                {state: tally.freeze() for state, tally in self._states.items()}
            ),
            late_intents=self._late_intents,
            late_results=self._late_results,
            estimated_exposure_units=self._exposure,
            unresolved_without_estimate=self._without_estimate,
            assisted_items=self._assisted,
            assisted_checked_tasks=self._checked,
            assisted_unchecked_tasks=self._unchecked,
            anomalies=MappingProxyType(dict(self._anomalies)),
            anomaly_samples=tuple(self._samples),
        )

    def _finality(self, status: JobStatus) -> UsageFinality:
        if status not in _FINISHED or self._captures[AttemptCapture.OPEN]:
            return UsageFinality.OPEN
        not_closed = sum(
            n for capture, n in self._captures.items() if capture is not AttemptCapture.CLOSED
        )
        return UsageFinality.LOWER_BOUND if not_closed else UsageFinality.CLOSED

    def _check_open(self) -> None:
        if self._finished:
            msg = "the report is already finished"
            raise RuntimeError(msg)

    def _report(self, code: UsageAnomalyCode, task_key: str, attempt_number: int) -> None:
        self._anomalies[code] += 1
        # Keep the smallest by (task_key, attempt_number, code): the same samples
        # whatever the order in which the anomalies were found.
        sample = UsageAnomaly(code, task_key, attempt_number)
        bisect.insort(self._samples, sample, key=UsageAnomaly.sort_key)
        del self._samples[MAX_SAMPLES:]


def _assisted_items(row: TaskRowLike, job: Job, observed_at: datetime) -> int:
    """The task's ``provider_assisted`` items, by exactly the summary's rules."""
    builder = SummaryBuilder()
    builder.add(row)
    return builder.finish(job, observed_at).provider_assisted_items


def _frozen_totals(totals: dict[str, UnitTotal]) -> Mapping[str, UnitTotal]:
    """Totals ordered by unit code point, so equal reports read the same."""
    return MappingProxyType({unit: totals[unit] for unit in sorted(totals)})


def report_job_usage(
    conn: Conn, job_id: uuid.UUID, *, fetch_size: int = DEFAULT_FETCH_SIZE
) -> JobUsageReport | None:
    """Job ``job_id``'s usage report as of one snapshot, or ``None`` if there is no such job.

    Raises ``WrongJobKindError`` (before any ledger row or task is read) for
    another kind of job, ``ValueError`` for a job without tasks and
    ``RuntimeError`` if ``conn`` is already inside a transaction. Inconsistencies
    in the ledger are reported in the report and never raised. Database and
    connection errors propagate and no partial report is returned.
    """
    with job_batch_snapshot(conn, job_id, fetch_size=fetch_size) as snapshot:
        if snapshot is None:
            return None
        builder = _read(snapshot)
    return builder.finish()


def _read(snapshot: JobBatchSnapshot) -> UsageReportBuilder:
    job = snapshot.job
    if job.kind != JOB_KIND:
        raise WrongJobKindError(job.id, job.kind)
    builder = UsageReportBuilder(job, snapshot.observed_at)
    builder.add_totals(snapshot.usage_totals())
    for attempt in snapshot.attempt_usage():
        builder.add_attempt(attempt)
    for task in snapshot.tasks():
        builder.add_task(task)
    return builder
