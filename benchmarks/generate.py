"""Seeded synthetic invoices, and the outcome each one must have once verified.

What an invoice is meant to test depends on its index alone, through the first
rule it matches:

====================  =====  =============  ==========================================
index rule            share  kind           expected status
====================  =====  =============  ==========================================
``i % 200 == 31``     0.5 %  malformed      ``malformed`` (an empty ``unit_amount``)
``i % 100 == 23``     1 %    zero quantity  ``rejected`` by ``quantity_positive``
``i % 50 == 11``      2 %    wrong total    ``rejected`` by ``total_matches_lines``
``i % 20 == 7``       5 %    ambiguous      ``verified`` and provider assisted
otherwise             91.5 % verified       ``verified``
====================  =====  =============  ==========================================

The residues never coincide, so every share is exact for a count that is a
multiple of 200. The seed draws line counts (1 to 5), quantities (1 to 20) and
unit amounts (1,000 to 99,999 minor units), so it changes the bytes and never
an expected status. Every total is therefore at least 1,000, and an ambiguous
one is written with a grouping separator (``1,234``, ``1_234`` or ``1 234``),
which ``benchmarks.provider.SeparatorReader`` reads back as its digits.

``expected_outcome`` is the oracle. It decides each status from the index rule,
never by running the checks, and it models the per-attempt provider call cap
exactly as the verifier applies it: within one batch, an ambiguous invoice that
finds the cap used up is ``needs_review`` without a call. Drill adjustments
describe batches that are expected to fail and extra tasks whose payload is not
a batch.
"""

import json
import random
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

from orqestra.domain.jobs import JsonValue
from orqestra.domain.partitioning import partition
from orqestra.workloads.invoice.verify import Status

DEFAULT_SEED = 20261107
BATCH_KEY_PREFIX = "batch"
POISON_KEY_PREFIX = "poison"
COMPOSITION_PERIOD = 200  # every share is exact over a multiple of this many invoices

_SEPARATORS = (",", "_", " ")
_MIN_UNIT_AMOUNT = 1_000
_MAX_UNIT_AMOUNT = 99_999


class Kind(StrEnum):
    """What a generated invoice is built to exercise."""

    MALFORMED = "malformed"
    ZERO_QUANTITY = "zero_quantity"
    WRONG_TOTAL = "wrong_total"
    AMBIGUOUS = "ambiguous"
    VERIFIED = "verified"


_STATUS_OF_KIND = {
    Kind.MALFORMED: Status.MALFORMED,
    Kind.ZERO_QUANTITY: Status.REJECTED,
    Kind.WRONG_TOTAL: Status.REJECTED,
    Kind.AMBIGUOUS: Status.VERIFIED,
    Kind.VERIFIED: Status.VERIFIED,
}


def kind_of(index: int) -> Kind:
    """The kind of the invoice at ``index``: the first rule of the module table it matches."""
    if index % 200 == 31:
        return Kind.MALFORMED
    if index % 100 == 23:
        return Kind.ZERO_QUANTITY
    if index % 50 == 11:
        return Kind.WRONG_TOTAL
    if index % 20 == 7:
        return Kind.AMBIGUOUS
    return Kind.VERIFIED


def generate_invoices(count: int, seed: int = DEFAULT_SEED) -> list[dict[str, JsonValue]]:
    """``count`` invoices for ``seed``; the same arguments always give the same invoices."""
    _check_positive("count", count)
    rng = random.Random(seed)
    return [_invoice(index, kind_of(index), rng) for index in range(count)]


def _invoice(index: int, kind: Kind, rng: random.Random) -> dict[str, JsonValue]:
    lines: list[dict[str, JsonValue]] = [
        {
            "line_id": f"L{number}",
            "quantity": rng.randint(1, 20),
            "unit_amount": rng.randint(_MIN_UNIT_AMOUNT, _MAX_UNIT_AMOUNT),
        }
        for number in range(1, rng.randint(1, 5) + 1)
    ]
    if kind is Kind.ZERO_QUANTITY:
        lines[0]["quantity"] = 0
    total = sum(cast(int, line["quantity"]) * cast(int, line["unit_amount"]) for line in lines)
    written: JsonValue = total
    if kind is Kind.WRONG_TOTAL:
        written = total + rng.randint(1, 999)
    elif kind is Kind.AMBIGUOUS:
        written = f"{total:,}".replace(",", rng.choice(_SEPARATORS))
    elif kind is Kind.MALFORMED:
        lines[0]["unit_amount"] = ""
    return {
        "invoice_id": f"INV-{index:07d}",
        "lines": cast(list[JsonValue], lines),
        "total": written,
    }


def build_tasks(
    invoices: Sequence[JsonValue],
    batch_size: int,
    max_attempts: int,
    *,
    invalid_payload_tasks: int = 0,
) -> list[dict[str, JsonValue]]:
    """The task objects ``orqestra job submit`` accepts (DEC-064), in partition order.

    The invoices are partitioned whole (DEC-052). Each of the
    ``invalid_payload_tasks`` extra tasks follows, keyed ``poison-<n>`` (outside
    the partition's keys), with a payload that is not a batch, so the batches
    stay contiguous.
    """
    _check_not_negative("invalid_payload_tasks", invalid_payload_tasks)
    specs = partition(
        invoices, batch_size=batch_size, max_attempts=max_attempts, key_prefix=BATCH_KEY_PREFIX
    )
    tasks: list[dict[str, JsonValue]] = [
        {"task_key": spec.task_key, "payload": spec.payload, "max_attempts": spec.max_attempts}
        for spec in specs
    ]
    tasks.extend(
        {
            "task_key": f"{POISON_KEY_PREFIX}-{number}",
            "payload": {"not_a_batch": number},
            "max_attempts": max_attempts,
        }
        for number in range(invalid_payload_tasks)
    )
    return tasks


def encode_tasks(tasks: Sequence[JsonValue]) -> bytes:
    """``tasks`` as a canonical JSON array: sorted keys, no spaces, ASCII, one final newline."""
    text = json.dumps(list(tasks), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"{text}\n".encode("ascii")


@dataclass(frozen=True, slots=True)
class Expected:
    """What a correct run of the generated invoices must report.

    ``statuses`` holds every index's status as if its batch ran; the counts,
    ``provider_assisted``, ``calls`` and ``tokens`` cover only the batches that
    are expected to succeed. ``calls`` and ``tokens`` are those of the attempt
    that succeeded each batch. ``failed_batch_items`` counts the items of the
    batches expected to fail. ``expected_items`` is the summary's figure:
    ``None`` as soon as any task's payload is not a batch.
    """

    invoices: int
    batch_size: int
    cap: int
    statuses: tuple[Status, ...]
    verified: int
    rejected: int
    needs_review: int
    malformed: int
    provider_assisted: int
    calls: int
    tokens: int
    failed_batch_items: int
    invalid_payload_tasks: int

    @property
    def batches(self) -> int:
        return -(-self.invoices // self.batch_size)

    @property
    def processed_items(self) -> int:
        return self.verified + self.rejected + self.needs_review + self.malformed

    @property
    def expected_items(self) -> int | None:
        return None if self.invalid_payload_tasks else self.invoices


def expected_outcome(
    invoices: Sequence[JsonValue],
    batch_size: int,
    cap: int,
    *,
    failed_batches: frozenset[int] = frozenset(),
    invalid_payload_tasks: int = 0,
) -> Expected:
    """The oracle for ``invoices`` from ``generate_invoices``, split into batches of ``batch_size``.

    ``cap`` is the per-attempt provider call cap. ``failed_batches`` names, by
    position in the partition, the batches expected to end failed;
    ``invalid_payload_tasks`` counts the extra tasks of ``build_tasks``.
    """
    _check_positive("batch_size", batch_size)
    _check_not_negative("cap", cap)
    _check_not_negative("invalid_payload_tasks", invalid_payload_tasks)
    count = len(invoices)
    _check_positive("the number of invoices", count)
    batches = -(-count // batch_size)
    for batch in failed_batches:
        if isinstance(batch, bool) or not isinstance(batch, int) or not 0 <= batch < batches:
            msg = f"failed_batches must name batches 0 to {batches - 1}"
            raise ValueError(msg)

    statuses: list[Status] = []
    tally = dict.fromkeys(Status, 0)
    assisted = calls = tokens = failed_items = 0
    for batch in range(batches):
        start = batch * batch_size
        remaining = cap
        succeeds = batch not in failed_batches
        for index in range(start, min(start + batch_size, count)):
            kind = kind_of(index)
            status = _STATUS_OF_KIND[kind]
            called = False
            if kind is Kind.AMBIGUOUS:
                if remaining >= 1:
                    remaining -= 1
                    called = True
                else:
                    status = Status.NEEDS_REVIEW
            statuses.append(status)
            if not succeeds:
                failed_items += 1
                continue
            tally[status] += 1
            if called:
                assisted += 1
                calls += 1
                tokens += len(_ambiguous_total(invoices[index]))
    return Expected(
        invoices=count,
        batch_size=batch_size,
        cap=cap,
        statuses=tuple(statuses),
        verified=tally[Status.VERIFIED],
        rejected=tally[Status.REJECTED],
        needs_review=tally[Status.NEEDS_REVIEW],
        malformed=tally[Status.MALFORMED],
        provider_assisted=assisted,
        calls=calls,
        tokens=tokens,
        failed_batch_items=failed_items,
        invalid_payload_tasks=invalid_payload_tasks,
    )


def _ambiguous_total(invoice: JsonValue) -> str:
    if not isinstance(invoice, dict) or not isinstance(invoice.get("total"), str):
        msg = "an ambiguous invoice must carry its total as a string"
        raise ValueError(msg)
    return cast(str, invoice["total"])


def _check_positive(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        msg = f"{name} must be a positive integer"
        raise ValueError(msg)


def _check_not_negative(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        msg = f"{name} must be a non-negative integer"
        raise ValueError(msg)
