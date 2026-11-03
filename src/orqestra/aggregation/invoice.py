"""Summarize one invoice-verification job from its stored task results.

``summarize_invoice_job`` reads the job and all of its tasks in one consistent
snapshot (``job_batch_snapshot``) and folds them with the pure ``SummaryBuilder``.
The snapshot ends before the summary is built. See ``InvoiceJobSummary`` for the
per-task counts and when they form a final job tally.
"""

import uuid

from orqestra.domain.errors import OrqestraError
from orqestra.persistence._shared import Conn
from orqestra.persistence.snapshots import DEFAULT_FETCH_SIZE, job_batch_snapshot
from orqestra.workloads.invoice.handler import JOB_KIND
from orqestra.workloads.invoice.summary import InvoiceJobSummary, SummaryBuilder


class WrongJobKindError(OrqestraError):
    """The job exists but is not an invoice-verification job."""

    def __init__(self, job_id: uuid.UUID, kind: str) -> None:
        super().__init__(f"job {job_id} has kind {kind!r}, not {JOB_KIND!r}")
        self.job_id = job_id
        self.kind = kind


def summarize_invoice_job(
    conn: Conn, job_id: uuid.UUID, *, fetch_size: int = DEFAULT_FETCH_SIZE
) -> InvoiceJobSummary | None:
    """The summary of job ``job_id`` as of one snapshot, or ``None`` if there is no such job.

    Raises ``WrongJobKindError`` (before any task is read) for another kind of
    job, ``ValueError`` for a job without tasks (which ``submit_job`` never
    creates) and ``RuntimeError`` if ``conn`` is already inside a transaction.
    Problems in the stored data, including payloads that are not batches and
    results that do not match them, are reported in the summary and never raised.
    Database and connection errors propagate and no partial summary is returned.
    """
    builder = SummaryBuilder()
    with job_batch_snapshot(conn, job_id, fetch_size=fetch_size) as snapshot:
        if snapshot is None:
            return None
        job = snapshot.job
        if job.kind != JOB_KIND:
            raise WrongJobKindError(job.id, job.kind)
        observed_at = snapshot.observed_at
        for row in snapshot.tasks():
            builder.add(row)
    return builder.finish(job, observed_at)
