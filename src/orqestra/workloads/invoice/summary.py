"""Aggregate the stored results of one invoice-verification job into a summary.

Pure rules and no I/O: a caller streams one bounded row per task into
``SummaryBuilder.add`` and then calls ``finish``. The rows come from a consistent
database snapshot (``orqestra.persistence.snapshots``); this module only reads
what the rows say and reports it.

Index mapping. A task whose payload is a valid batch, ``{"offset": n, "items":
[...]}`` (the handler's rule), owns the absolute range ``[n, n + len(items))``.
Item results carry absolute indices already, so ``offset`` is never added to
them.

What is counted. An outcome is counted only from an entry that is valid, sits in
the range of its own task, is claimed once, and belongs to a succeeded task whose
result envelope matches the payload exactly. Nothing is inferred from the job's
status, from a task's state or from a missing result. Counts are per-task
tallies, not a final job tally unless ``complete`` is true. Overlapping ranges
can count one absolute index in multiple tasks, while also making the summary
incomplete.

Anomalies name a code, a task key and, where there is one, an index. They never
carry an invoice value, an identifier or a payload.

``provider_assisted_items`` counts items whose result says a provider read an
amount. It is not a count of provider calls and not a spend figure: a retried
batch repeats its calls, and no usage is stored.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from orqestra.domain.jobs import Job, JobStatus, JsonValue, derive_job_status
from orqestra.domain.states import TaskState
from orqestra.workloads.invoice.verify import Status

MAX_SAMPLES = 20

_FINISHED = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED})
_OFFSET_TEXT = re.compile(r"0|[1-9][0-9]*")
_STATUS_BY_VALUE = {status.value: status for status in Status}
_ENVELOPE_KEYS = frozenset({"offset", "results"})


class AnomalyCode(StrEnum):
    """The closed set of things a summary can report as wrong. Values are stable identifiers."""

    PAYLOAD_INVALID = "payload_invalid"
    PARTITION_NOT_CONTIGUOUS = "partition_not_contiguous"
    RESULT_INVALID = "result_invalid"
    RESULT_OFFSET_MISMATCH = "result_offset_mismatch"
    RESULT_LENGTH_MISMATCH = "result_length_mismatch"
    ITEM_INVALID = "item_invalid"
    INDEX_OUT_OF_RANGE = "index_out_of_range"
    INDEX_DUPLICATE = "index_duplicate"
    INDEX_OUT_OF_ORDER = "index_out_of_order"


@dataclass(frozen=True, slots=True)
class Anomaly:
    """One reported problem: a code, the task key and, if it concerns one item, its index."""

    code: AnomalyCode
    task_key: str
    index: int | None

    def sort_key(self) -> tuple[str, int, str]:
        return (self.task_key, -1 if self.index is None else self.index, self.code.value)


class TaskRowLike(Protocol):
    """The bounded projection of one task that the builder reads.

    ``payload_type`` is the JSON type name of the payload (``object``, ``array``,
    ``string``, ``number``, ``boolean`` or ``null``). The four fields after it are
    set only for an object payload, ``offset_text`` only when its ``offset`` is a
    JSON number, and ``items_length`` only when its ``items`` is an array.
    ``result`` is set only for a succeeded task.
    """

    @property
    def task_key(self) -> str: ...

    @property
    def state(self) -> TaskState: ...

    @property
    def payload_type(self) -> str: ...

    @property
    def key_count(self) -> int | None: ...

    @property
    def has_offset(self) -> bool | None: ...

    @property
    def has_items(self) -> bool | None: ...

    @property
    def offset_text(self) -> str | None: ...

    @property
    def items_length(self) -> int | None: ...

    @property
    def result(self) -> JsonValue: ...


@dataclass(frozen=True, slots=True)
class InvoiceJobSummary:
    """What a job's stored results say, as of one consistent read.

    ``observed_at`` is the database ``statement_timestamp()`` of the snapshot's
    first query: approximate, not the snapshot instant. ``job_status`` is derived
    from this snapshot's own task rows.

    ``expected_items`` is the sum of the valid payload lengths, or ``None`` when
    any payload is invalid. When it is not ``None``::

        processed_items + unprocessed_items + missing_items + invalid_items == expected_items

    The outcome counts and ``provider_assisted_items`` are per-task tallies, not
    a final job tally unless ``complete`` holds. A ``complete`` summary does not change later if the
    engine's contracts hold (``SUCCEEDED`` has no outgoing transition and only a
    completion writes a result); the schema does not enforce that, so a manual
    edit could break it. A summary that is not complete is a point-in-time
    observation.
    """

    job: Job
    observed_at: datetime
    job_status: JobStatus
    task_counts: Mapping[TaskState, int]
    expected_items: int | None
    verified: int
    rejected: int
    needs_review: int
    malformed: int
    provider_assisted_items: int
    unprocessed_items: int
    missing_items: int
    invalid_items: int
    anomalies: Mapping[AnomalyCode, int]
    anomaly_samples: tuple[Anomaly, ...]

    @property
    def processed_items(self) -> int:
        return self.verified + self.rejected + self.needs_review + self.malformed

    @property
    def finished(self) -> bool:
        return self.job_status in _FINISHED

    @property
    def integrity_ok(self) -> bool:
        return not any(self.anomalies.values())

    @property
    def complete(self) -> bool:
        """Every item has exactly one valid outcome, in a job that succeeded, with no anomaly."""
        return (
            self.job_status is JobStatus.SUCCEEDED
            and self.expected_items is not None
            and self.expected_items >= 1
            and self.processed_items == self.expected_items
            and self.unprocessed_items == 0
            and self.missing_items == 0
            and self.invalid_items == 0
            and self.integrity_ok
        )


@dataclass(slots=True)
class SummaryBuilder:
    """Fold task rows into an ``InvoiceJobSummary``; add every row once, then finish once.

    Memory is the counters, one ``(offset, length, task_key)`` triple per task and
    at most ``MAX_SAMPLES`` anomaly samples, plus the one result being read.
    """

    _states: dict[TaskState, int] = field(default_factory=lambda: dict.fromkeys(TaskState, 0))
    _anomalies: dict[AnomalyCode, int] = field(
        default_factory=lambda: dict.fromkeys(AnomalyCode, 0)
    )
    _samples: list[Anomaly] = field(default_factory=list)
    _ranges: list[tuple[int, int, str]] = field(default_factory=list)
    _outcomes: dict[Status, int] = field(default_factory=lambda: dict.fromkeys(Status, 0))
    _expected: int = 0
    _payload_invalid: bool = False
    _assisted: int = 0
    _unprocessed: int = 0
    _missing: int = 0
    _invalid: int = 0
    _finished: bool = False

    def add(self, row: TaskRowLike) -> None:
        """Account for one task. Content problems are reported as anomalies, never raised."""
        if self._finished:
            msg = "the summary is already finished"
            raise RuntimeError(msg)
        self._states[row.state] += 1
        batch = _payload_range(row)
        if batch is None:
            # The range is unknown, so no item count is known: the result, if any,
            # cannot be mapped and is ignored apart from this anomaly.
            self._payload_invalid = True
            self._report(AnomalyCode.PAYLOAD_INVALID, row.task_key, None)
            return
        offset, count = batch
        self._ranges.append((offset, count, row.task_key))
        self._expected += count
        if row.state is not TaskState.SUCCEEDED:
            self._unprocessed += count
            return
        self._add_result(row.task_key, offset, count, row.result)

    def finish(self, job: Job, observed_at: datetime) -> InvoiceJobSummary:
        """The summary. Raises ``ValueError`` for a job without tasks (``derive_job_status``)."""
        if self._finished:
            msg = "the summary is already finished"
            raise RuntimeError(msg)
        status = derive_job_status(self._states)
        self._finished = True
        self._check_partition()
        return InvoiceJobSummary(
            job=job,
            observed_at=observed_at,
            job_status=status,
            task_counts=MappingProxyType(dict(self._states)),
            expected_items=None if self._payload_invalid else self._expected,
            verified=self._outcomes[Status.VERIFIED],
            rejected=self._outcomes[Status.REJECTED],
            needs_review=self._outcomes[Status.NEEDS_REVIEW],
            malformed=self._outcomes[Status.MALFORMED],
            provider_assisted_items=self._assisted,
            unprocessed_items=self._unprocessed,
            missing_items=self._missing,
            invalid_items=self._invalid,
            anomalies=MappingProxyType(dict(self._anomalies)),
            anomaly_samples=tuple(self._samples),
        )

    def _report(self, code: AnomalyCode, task_key: str, index: int | None) -> None:
        self._anomalies[code] += 1
        self._samples.append(Anomaly(code, task_key, index))
        # Keep the smallest by (task_key, index, code): the same samples whatever the
        # order in which the anomalies were found.
        self._samples.sort(key=Anomaly.sort_key)
        del self._samples[MAX_SAMPLES:]

    def _reject_batch(self, task_key: str, count: int, code: AnomalyCode) -> None:
        self._invalid += count
        self._report(code, task_key, None)

    def _add_result(self, task_key: str, offset: int, count: int, result: JsonValue) -> None:
        if not isinstance(result, dict) or set(result) != _ENVELOPE_KEYS:
            # Includes JSON null, which is a legal stored result.
            self._reject_batch(task_key, count, AnomalyCode.RESULT_INVALID)
            return
        claimed_offset = result["offset"]
        if (
            isinstance(claimed_offset, bool)
            or not isinstance(claimed_offset, int)
            or claimed_offset != offset
        ):
            self._reject_batch(task_key, count, AnomalyCode.RESULT_OFFSET_MISMATCH)
            return
        entries = result["results"]
        if not isinstance(entries, list):
            self._reject_batch(task_key, count, AnomalyCode.RESULT_INVALID)
            return
        if len(entries) != count:
            self._reject_batch(task_key, count, AnomalyCode.RESULT_LENGTH_MISMATCH)
            return

        # Entries are judged by the index they claim, not by where they stand, so
        # the outcome does not depend on their order. First collect the positions
        # that claim each in-range index.
        claims: dict[int, list[int]] = {}
        for position, entry in enumerate(entries):
            index = entry.get("index") if isinstance(entry, dict) else None
            if isinstance(index, bool) or not isinstance(index, int):
                self._report(AnomalyCode.ITEM_INVALID, task_key, None)
            elif not offset <= index < offset + count:
                self._report(AnomalyCode.INDEX_OUT_OF_RANGE, task_key, index)
            else:
                claims.setdefault(index, []).append(position)

        for index in sorted(claims):
            positions = claims[index]
            if len(positions) > 1:
                # No occurrence is counted, so a duplicate cannot pick a winner.
                self._invalid += 1
                self._report(AnomalyCode.INDEX_DUPLICATE, task_key, index)
                continue
            outcome = _item_outcome(entries[positions[0]])
            if outcome is None:
                self._invalid += 1
                self._report(AnomalyCode.ITEM_INVALID, task_key, index)
                continue
            status, assisted = outcome
            self._outcomes[status] += 1
            self._assisted += assisted
            if positions[0] != index - offset:
                self._report(AnomalyCode.INDEX_OUT_OF_ORDER, task_key, index)
        self._missing += count - len(claims)

    def _check_partition(self) -> None:
        """The valid ranges, by offset, must start at 0 and each begin where the last ended."""
        expected_start = 0
        for offset, count, task_key in sorted(self._ranges):
            if offset != expected_start:
                self._report(AnomalyCode.PARTITION_NOT_CONTIGUOUS, task_key, None)
            expected_start = offset + count


def _payload_range(row: TaskRowLike) -> tuple[int, int] | None:
    """``(offset, item count)`` of a valid batch payload, else ``None``.

    The same rule as the handler's ``_batch``: an object with exactly ``offset``
    and ``items``, the offset a non-negative integer (never a bool or a fraction)
    and the items an array.
    """
    if (
        row.payload_type != "object"
        or row.key_count != 2
        or not row.has_offset
        or not row.has_items
        or row.offset_text is None
        or row.items_length is None
        or _OFFSET_TEXT.fullmatch(row.offset_text) is None
    ):
        return None
    try:
        return int(row.offset_text), row.items_length
    except ValueError:  # more digits than Python converts: not a batch offset
        return None


def _item_outcome(entry: JsonValue) -> tuple[Status, bool] | None:
    """The status and ``provider_assisted`` flag of an item result, or ``None`` if unusable.

    Other fields are not validated and extra keys are tolerated: the summary
    reports outcomes and does not verify again.
    """
    if not isinstance(entry, dict):
        return None
    raw_status = entry.get("status")
    assisted = entry.get("provider_assisted")
    if not isinstance(raw_status, str) or not isinstance(assisted, bool):
        return None
    status = _STATUS_BY_VALUE.get(raw_status)
    return None if status is None else (status, assisted)
