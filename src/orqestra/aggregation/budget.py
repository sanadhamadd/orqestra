"""A provable bound on one invoice job's provider calls, checked against its ledger (DEC-065).

The bound is ``cap * sum(max_attempts)`` over the job's tasks: every attempt makes at
most ``cap`` calls (the per-attempt cap, DEC-057), and a task starts at most
``max_attempts`` attempts. It needs no price and no unit, so it stays inside
DEC-062, and it can be computed before the job is submitted.

What the check can and cannot say:

- **The cap is the operator's assumption.** It lives only in each worker's
  wiring, and the ledger checks nothing but ``call_seq >= 0``, so the bound holds
  only if every worker of the job used that cap. Nothing enforces that, as with
  the retry settings. Every check therefore carries ``cap_source = "operator"``.
- **``exceeded``** means more calls are recorded than the bound allows. Recorded
  calls only grow, so this holds at any finality, even for a lower bound: the
  assumed cap is wrong, a worker ran with a larger one, or there is a defect.
- **``within``** needs a ``CLOSED`` report, and it holds for that snapshot only:
  the ledger accepts late writes, which can contradict a closed report.
- **``unknown``** is every other case, with the report's finality as the reason.
  Unknown usage is never read as zero.

Nothing is enforced and nothing is priced. Reported amounts and estimated
exposure stay in the usage report and are never added to the check.
"""

import uuid
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from orqestra.aggregation.usage import JobUsageReport, UsageFinality, read_job_usage
from orqestra.persistence._shared import Conn
from orqestra.persistence.snapshots import DEFAULT_FETCH_SIZE, job_batch_snapshot
from orqestra.workloads.invoice.verify import check_call_cap


class BudgetVerdict(StrEnum):
    """How a job's recorded calls compare with its call bound."""

    WITHIN = "within"
    EXCEEDED = "exceeded"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CallBudgetCheck:
    """A job's recorded calls against ``bound = cap * max_attempts_total``.

    ``unknown_reason`` is the report's finality when the verdict is ``unknown``,
    and ``None`` otherwise.
    """

    cap: int
    max_attempts_total: int
    bound: int
    calls: int
    finality: UsageFinality
    verdict: BudgetVerdict
    unknown_reason: UsageFinality | None
    cap_source: Literal["operator"] = "operator"


def call_bound(max_attempts_total: int, cap: int) -> int:
    """The most provider calls a job's attempts can make: ``cap * max_attempts_total``.

    ``cap`` must be an ``int`` (not a ``bool``) from 0 to ``MAX_CALL_SEQ``, and
    ``max_attempts_total`` a non-negative ``int``; ``TypeError`` or ``ValueError``
    otherwise.
    """
    checked_cap = check_call_cap("cap", cap)
    if isinstance(max_attempts_total, bool) or not isinstance(max_attempts_total, int):
        msg = f"max_attempts_total must be an integer, got {type(max_attempts_total).__name__}"
        raise TypeError(msg)
    if max_attempts_total < 0:
        msg = "max_attempts_total must not be negative"
        raise ValueError(msg)
    return checked_cap * max_attempts_total


def check_call_budget(
    report: JobUsageReport, *, max_attempts_total: int, cap: int
) -> CallBudgetCheck:
    """Compare ``report``'s recorded calls with the job's call bound; see the module docstring."""
    bound = call_bound(max_attempts_total, cap)
    if report.calls > bound:
        verdict = BudgetVerdict.EXCEEDED
    elif report.finality is UsageFinality.CLOSED:
        verdict = BudgetVerdict.WITHIN
    else:
        verdict = BudgetVerdict.UNKNOWN
    return CallBudgetCheck(
        cap=cap,
        max_attempts_total=max_attempts_total,
        bound=bound,
        calls=report.calls,
        finality=report.finality,
        verdict=verdict,
        unknown_reason=report.finality if verdict is BudgetVerdict.UNKNOWN else None,
    )


def read_job_call_budget(
    conn: Conn, job_id: uuid.UUID, *, cap: int, fetch_size: int = DEFAULT_FETCH_SIZE
) -> tuple[JobUsageReport, CallBudgetCheck] | None:
    """Job ``job_id``'s usage report and call budget check, from one snapshot.

    Returns ``None`` if there is no such job. The usage is read first, so another
    kind of job raises ``WrongJobKindError`` before any ledger row or task is
    read. The retry limits are summed afterwards in the same snapshot. Otherwise
    it raises as ``report_job_usage`` does, and refuses an invalid ``cap`` before
    any read.
    """
    check_call_cap("cap", cap)
    with job_batch_snapshot(conn, job_id, fetch_size=fetch_size) as snapshot:
        if snapshot is None:
            return None
        builder = read_job_usage(snapshot)
        max_attempts_total = snapshot.max_attempts_total()
    report = builder.finish()
    return report, check_call_budget(report, max_attempts_total=max_attempts_total, cap=cap)
