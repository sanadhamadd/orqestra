"""The extraction workload: a model reads each document, the rules judge what it read.

A reply is parsed strictly (``parse_reply``). It must be one JSON object with
exactly the keys the prompt names (``openrouter.EXTRACT_SYSTEM_PROMPT``), and each
value must have its stated form:

- ``vendor_name``, ``invoice_number``: 1 to 200 characters of text;
- ``issue_date``, ``due_date``: a real date written ``YYYY-MM-DD``;
- ``currency``: three upper-case ASCII letters;
- ``line_items``: a list of 1 to ``MAX_READ_LINES`` objects with exactly
  ``description`` (text), ``quantity`` (a whole number from 0 to 10**6, never a
  ``bool``) and ``unit_price``;
- ``unit_price``, ``subtotal``, ``tax``, ``total``: an amount string, digits, a
  point and exactly two decimals (``AMOUNT_PATTERN``), read as minor units.

Any value may be ``null``, which says the document does not state it. Duplicate
keys, ``NaN`` and ``Infinity`` are refused. The only text allowed around the
object is one surrounding Markdown code fence (```` ```json ```` ... ```` ``` ````):
removing it changes no value. Nothing is ever repaired or guessed.

The status of one document (``judge``) is decided in this order:

1. no reply, or a reply that is not a complete text: ``needs_review``
   (``reply_incomplete``);
2. not JSON: ``needs_review`` (``reply_not_json``); JSON of the wrong shape:
   ``needs_review`` (``reply_schema_invalid``);
3. any ``null`` field: ``needs_review`` (``field_missing``);
4. otherwise the existing invoice rules run on the lines and the subtotal
   (``quantity_positive``, ``total_matches_lines``), then
   ``total_matches_subtotal_plus_tax``. Any ``FAIL`` is ``rejected``; all
   ``PASS`` is ``verified``.

A reading is scored against the oracle field by field (``score``); a ``null`` read
for a field the document leaves out is correct.
"""

import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, cast

from benchmarks.documents import Fields
from benchmarks.metrics import percentiles
from orqestra.calls.provider import (
    PermanentCode,
    ProviderPermanentError,
    ProviderTransientError,
    TransientCode,
)
from orqestra.calls.recording import (
    CallOutcome,
    ProviderCallIntent,
    ProviderCallRecorder,
    ProviderCallResult,
)
from orqestra.domain.errors import PermanentError
from orqestra.domain.jobs import JsonValue
from orqestra.domain.leases import check_duration
from orqestra.providers.openrouter import (
    EXTRACT_CAPABILITY,
    EXTRACT_PROMPT_VERSION,
    PROTOCOL_VIOLATION,
    ExtractionReport,
    ExtractionRequest,
)
from orqestra.worker.handlers import Handler, HandlerCancelled, TaskContext
from orqestra.workloads.invoice.checks import Verdict, run_checks
from orqestra.workloads.invoice.model import Invoice, normalize_invoice
from orqestra.workloads.invoice.verify import Status, check_call_cap, check_recorder

JOB_KIND = "invoice_extraction"
# The extraction prompt's version, as the usage ledger records it.
PROMPT_VERSION = EXTRACT_PROMPT_VERSION
BATCH_PAYLOAD_INVALID = "batch_payload_invalid"
# The route estimate every intent carries (opaque units, not accounting).
ESTIMATED_COST_UNITS = 1
MAX_READ_LINES = 50
MAX_TEXT_CHARS = 200
MAX_QUANTITY = 10**6
AMOUNT_PATTERN = r"(?:0|[1-9][0-9]{0,14})\.[0-9]{2}"
TOTAL_RULE = "total_matches_subtotal_plus_tax"

_AMOUNT = re.compile(AMOUNT_PATTERN, re.ASCII)
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", re.ASCII)
_CURRENCY = re.compile(r"[A-Z]{3}", re.ASCII)
_FENCE = re.compile(r"\A```(?:json)?[ \t]*\n(.*)\n[ \t]*```\Z", re.DOTALL)

KEYS = frozenset(
    {
        "vendor_name",
        "invoice_number",
        "issue_date",
        "due_date",
        "currency",
        "line_items",
        "subtotal",
        "tax",
        "total",
    }
)
LINE_KEYS = frozenset({"description", "quantity", "unit_price"})
SCALAR_FIELDS = ("vendor_name", "invoice_number", "issue_date", "due_date", "currency")
AMOUNT_FIELDS = ("subtotal", "tax", "total")
LINE_FIELDS = ("description", "quantity", "unit_price")
SCORED_FIELDS = (*SCALAR_FIELDS, *(f"line_items.{name}" for name in LINE_FIELDS), *AMOUNT_FIELDS)


class Reason(StrEnum):
    """Why a document needs review. Values are stable identifiers."""

    REPLY_INCOMPLETE = "reply_incomplete"
    REPLY_NOT_JSON = "reply_not_json"
    REPLY_SCHEMA_INVALID = "reply_schema_invalid"
    FIELD_MISSING = "field_missing"
    PROVIDER_CALL_CAP_REACHED = "provider_call_cap_reached"


@dataclass(frozen=True, slots=True)
class ReadLine:
    description: str | None
    quantity: int | None
    unit_price: int | None


@dataclass(frozen=True, slots=True)
class Reading:
    """What a reply says, in canonical form; ``None`` where it says ``null``."""

    vendor_name: str | None
    invoice_number: str | None
    issue_date: date | None
    due_date: date | None
    currency: str | None
    line_items: tuple[ReadLine, ...] | None
    subtotal: int | None
    tax: int | None
    total: int | None

    def complete(self) -> bool:
        """Whether no field is ``null``, including those of every line."""
        values = [getattr(self, name) for name in KEYS]
        lines = self.line_items or ()
        values += [getattr(line, name) for line in lines for name in LINE_FIELDS]
        return all(value is not None for value in values)

    def as_json(self) -> dict[str, JsonValue]:
        lines: JsonValue = None
        if self.line_items is not None:
            lines = [
                {
                    "description": line.description,
                    "quantity": line.quantity,
                    "unit_price": line.unit_price,
                }
                for line in self.line_items
            ]
        return {
            "vendor_name": self.vendor_name,
            "invoice_number": self.invoice_number,
            "issue_date": None if self.issue_date is None else self.issue_date.isoformat(),
            "due_date": None if self.due_date is None else self.due_date.isoformat(),
            "currency": self.currency,
            "line_items": lines,
            "subtotal": self.subtotal,
            "tax": self.tax,
            "total": self.total,
        }


@dataclass(frozen=True, slots=True)
class Unusable:
    """A reply that cannot be read; ``reason`` says why."""

    reason: Reason


class _Refused(Exception):  # noqa: N818 - internal: carries a reason to one handler
    def __init__(self, reason: Reason) -> None:
        super().__init__(reason.value)
        self.reason = reason


# --- parsing --------------------------------------------------------------------------------


def parse_reply(content: str | None) -> Reading | Unusable:
    """``content`` read strictly; see the module docstring."""
    if content is None:
        return Unusable(Reason.REPLY_INCOMPLETE)
    try:
        return _reading(_decode(content))
    except _Refused as refused:
        return Unusable(refused.reason)


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    obj = dict(pairs)
    if len(obj) != len(pairs):
        msg = "duplicate key"
        raise ValueError(msg)
    return obj


def _no_constant(name: str) -> object:
    msg = "non-finite numbers are not JSON"
    raise ValueError(msg)


def _decode(content: str) -> dict[str, object]:
    text = content.strip()
    fenced = _FENCE.match(text)
    if fenced is not None:
        text = fenced.group(1).strip()
    try:
        document = json.loads(
            text, object_pairs_hook=_unique, parse_constant=_no_constant, parse_float=Decimal
        )
    except (ValueError, RecursionError):
        raise _Refused(Reason.REPLY_NOT_JSON) from None
    if not isinstance(document, dict):
        raise _Refused(Reason.REPLY_SCHEMA_INVALID)
    if set(document) != KEYS:
        raise _Refused(Reason.REPLY_SCHEMA_INVALID)
    return cast(dict[str, object], document)


def _invalid() -> _Refused:
    return _Refused(Reason.REPLY_SCHEMA_INVALID)


def _text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > MAX_TEXT_CHARS:
        raise _invalid()
    return value


def _date(value: object) -> date | None:
    if value is None:
        return None
    if not isinstance(value, str) or _DATE.fullmatch(value) is None:
        raise _invalid()
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise _invalid() from None


def _currency(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _CURRENCY.fullmatch(value) is None:
        raise _invalid()
    return value


def _amount(value: object) -> int | None:
    if value is None:
        return None
    if not isinstance(value, str) or _AMOUNT.fullmatch(value) is None:
        raise _invalid()
    units, cents = value.split(".")
    return int(units) * 100 + int(cents)


def _quantity(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_QUANTITY:
        raise _invalid()
    return value


def _lines(value: object) -> tuple[ReadLine, ...] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_READ_LINES:
        raise _invalid()
    lines: list[ReadLine] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != LINE_KEYS:
            raise _invalid()
        lines.append(
            ReadLine(
                description=_text(item["description"]),
                quantity=_quantity(item["quantity"]),
                unit_price=_amount(item["unit_price"]),
            )
        )
    return tuple(lines)


def _reading(document: Mapping[str, object]) -> Reading:
    return Reading(
        vendor_name=_text(document["vendor_name"]),
        invoice_number=_text(document["invoice_number"]),
        issue_date=_date(document["issue_date"]),
        due_date=_date(document["due_date"]),
        currency=_currency(document["currency"]),
        line_items=_lines(document["line_items"]),
        subtotal=_amount(document["subtotal"]),
        tax=_amount(document["tax"]),
        total=_amount(document["total"]),
    )


# --- the reply a correct reader gives -------------------------------------------------------


def _written(minor: int) -> str:
    units, cents = divmod(minor, 100)
    return f"{units}.{cents:02d}"


def canonical_reply(fields: Fields) -> str:
    """The reply the prompt asks for, for ``fields``: what a perfect reader answers."""
    reply = {
        "vendor_name": fields.vendor_name,
        "invoice_number": fields.invoice_number,
        "issue_date": fields.issue_date.isoformat(),
        "due_date": None if fields.due_date is None else fields.due_date.isoformat(),
        "currency": fields.currency,
        "line_items": [
            {
                "description": line.description,
                "quantity": line.quantity,
                "unit_price": _written(line.unit_price),
            }
            for line in fields.lines
        ],
        "subtotal": _written(fields.subtotal),
        "tax": _written(fields.tax),
        "total": _written(fields.total),
    }
    return json.dumps(reply, ensure_ascii=False, separators=(",", ":"))


# --- judging --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Judgement:
    """The status of one document and, for ``needs_review``, the reason."""

    status: Status
    reason: Reason | None = None
    failed_rules: tuple[str, ...] = field(default=())


def judge(read: Reading | Unusable) -> Judgement:
    """The document's status from what was read; see the module docstring."""
    if isinstance(read, Unusable):
        return Judgement(Status.NEEDS_REVIEW, read.reason)
    if not read.complete():
        return Judgement(Status.NEEDS_REVIEW, Reason.FIELD_MISSING)
    assert read.line_items is not None
    assert read.subtotal is not None
    assert read.tax is not None
    assert read.total is not None
    invoice = normalize_invoice(
        {
            "invoice_id": "document",
            "lines": [
                {"line_id": f"L{number}", "quantity": line.quantity, "unit_amount": line.unit_price}
                for number, line in enumerate(read.line_items, 1)
            ],
            "total": read.subtotal,
        }
    )
    assert isinstance(invoice, Invoice)  # every value was checked to fit the schema
    failed = [f.rule.value for f in run_checks(invoice) if f.verdict is Verdict.FAIL]
    if read.subtotal + read.tax != read.total:
        failed.append(TOTAL_RULE)
    if failed:
        return Judgement(Status.REJECTED, None, tuple(dict.fromkeys(failed)))
    return Judgement(Status.VERIFIED)


# --- scoring against the oracle -------------------------------------------------------------


def _same_text(expected: str | None, read: object) -> bool:
    if expected is None or not isinstance(read, str):
        return expected is None and read is None
    return " ".join(expected.split()).casefold() == " ".join(read.split()).casefold()


@dataclass(slots=True)
class Score:
    """Matched and expected counts per scored field, summed over documents."""

    matched: dict[str, int] = field(default_factory=lambda: dict.fromkeys(SCORED_FIELDS, 0))
    expected: dict[str, int] = field(default_factory=lambda: dict.fromkeys(SCORED_FIELDS, 0))
    documents: int = 0
    exact_documents: int = 0

    def add(self, other: "Score") -> None:
        for name in SCORED_FIELDS:
            self.matched[name] += other.matched[name]
            self.expected[name] += other.expected[name]
        self.documents += other.documents
        self.exact_documents += other.exact_documents

    def per_field(self) -> dict[str, float | None]:
        return {
            name: (self.matched[name] / self.expected[name] if self.expected[name] else None)
            for name in SCORED_FIELDS
        }

    def overall(self) -> float | None:
        expected = sum(self.expected.values())
        return sum(self.matched.values()) / expected if expected else None


def score(expected: Fields, read: Mapping[str, JsonValue] | None) -> Score:
    """How much of ``expected`` the stored reading ``read`` got right (``None``: nothing)."""
    result = Score(documents=1)
    got: Mapping[str, JsonValue] = read or {}
    oracle = expected.as_json()

    def tally(name: str, hit: bool) -> None:
        result.expected[name] += 1
        result.matched[name] += int(hit)

    for name in ("vendor_name", "invoice_number"):
        tally(name, read is not None and _same_text(cast(str | None, oracle[name]), got.get(name)))
    for name in ("issue_date", "due_date", "currency", *AMOUNT_FIELDS):
        tally(name, read is not None and got.get(name) == oracle[name])
    read_lines = got.get("line_items")
    lines = read_lines if isinstance(read_lines, list) else []
    for position, line in enumerate(expected.lines):
        other = lines[position] if position < len(lines) else None
        other_map = other if isinstance(other, dict) else {}
        tally(
            "line_items.description",
            other is not None and _same_text(line.description, other_map.get("description")),
        )
        tally(
            "line_items.quantity", other is not None and other_map.get("quantity") == line.quantity
        )
        tally(
            "line_items.unit_price",
            other is not None and other_map.get("unit_price") == line.unit_price,
        )
    exact = all(result.matched[name] == result.expected[name] for name in SCORED_FIELDS) and len(
        lines
    ) == len(expected.lines)
    result.exact_documents = int(exact)
    return result


def score_all(
    expected: Sequence[Fields], readings: Sequence[Mapping[str, JsonValue] | None]
) -> Score:
    """The summed score of each document's reading, in order."""
    if len(expected) != len(readings):
        msg = "one reading per document is required"
        raise ValueError(msg)
    total = Score()
    for fields, read in zip(expected, readings, strict=True):
        total.add(score(fields, read))
    return total


# --- the handler ----------------------------------------------------------------------------


class Extractor(Protocol):
    """What the handler asks: ``OpenRouterProvider.extract``'s contract."""

    def extract(self, request: ExtractionRequest) -> ExtractionReport: ...


def extraction_handler(
    extractor: Extractor,
    route_id: str,
    timeout: timedelta,
    *,
    max_provider_calls_per_attempt: int,
    recorder: ProviderCallRecorder,
) -> Handler:
    """The handler for ``JOB_KIND``: one call per document, every call recorded.

    The payload is a DEC-052 batch whose items are document texts; any other shape is
    ``PermanentError("batch_payload_invalid")``. For each document, in order: a
    cancellation check, then (within the attempt's cap) the intent, the call, its
    result, and the judgement. A document beyond the cap is ``needs_review``
    (``provider_call_cap_reached``) without a call. The item result is::

        {"index": offset + i, "status": ..., "reason": str | null,
         "failed_rules": [str], "fields": <Reading.as_json()> | null}

    ``fields`` holds what was read, so the harness can score it before it drops the
    run schema. No prompt, reply text or request id is ever stored or recorded.
    A provider error ends the attempt as ``verify_invoice``'s do: the same code,
    raised fresh, after its result is recorded; anything else the provider does
    wrong is ``PermanentError("provider_protocol_violation")`` and leaves the call
    unresolved. Whatever the recorder raises propagates unchanged.
    """
    cap = check_call_cap("max_provider_calls_per_attempt", max_provider_calls_per_attempt)
    checked = check_recorder(recorder)
    if checked is None:
        msg = "the extraction handler needs a recorder: every call is billable"
        raise ValueError(msg)
    check_duration("timeout", timeout)

    def handle(context: TaskContext, payload: JsonValue) -> JsonValue:
        offset, documents = _batch(payload)
        made = 0
        results: list[JsonValue] = []
        for position, document in enumerate(documents):
            if context.cancellation.is_set():
                raise HandlerCancelled
            index = offset + position
            if made >= cap:
                judgement = Judgement(Status.NEEDS_REVIEW, Reason.PROVIDER_CALL_CAP_REACHED)
                results.append(_item_result(index, judgement, None))
                continue
            request = ExtractionRequest(route_id=route_id, document=document, timeout=timeout)
            checked.record_intent(_intent(context.attempt_id, made, index, route_id))
            seq = made
            made += 1
            reply = _extract(extractor, request)  # a BaseException passes through: no result
            if isinstance(reply, ExtractionReport):
                checked.record_result(_answered(context.attempt_id, seq, reply))
            else:
                if isinstance(reply, ProviderTransientError | ProviderPermanentError):
                    checked.record_result(_failed(context.attempt_id, seq, reply.code))
                raise reply
            read = parse_reply(reply.content)
            fields = read.as_json() if isinstance(read, Reading) else None
            results.append(_item_result(index, judge(read), fields))
        return {"offset": offset, "results": results}

    return handle


def _batch(payload: JsonValue) -> tuple[int, list[str]]:
    """The offset and documents of a batch; ``PermanentError`` for any other shape."""
    if not isinstance(payload, dict) or set(payload) != {"offset", "items"}:
        raise PermanentError(BATCH_PAYLOAD_INVALID)
    offset = payload["offset"]
    items = payload["items"]
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise PermanentError(BATCH_PAYLOAD_INVALID)
    if not isinstance(items, list) or not all(isinstance(item, str) for item in items):
        raise PermanentError(BATCH_PAYLOAD_INVALID)
    return offset, cast(list[str], items)


def _extract(extractor: Extractor, request: ExtractionRequest) -> ExtractionReport | Exception:
    """The report, or the fixed-text error to raise in its place; nothing is chained."""
    try:
        reply: object = extractor.extract(request)
    except ProviderTransientError as exc:
        return ProviderTransientError(exc.code)
    except ProviderPermanentError as exc:
        return ProviderPermanentError(exc.code)
    except Exception:  # whatever it was, its text is not ours to store
        return PermanentError(PROTOCOL_VIOLATION)
    if isinstance(reply, ExtractionReport):
        return reply
    return PermanentError(PROTOCOL_VIOLATION)


def _intent(attempt_id: uuid.UUID, seq: int, index: int, route_id: str) -> ProviderCallIntent:
    return ProviderCallIntent(
        attempt_id=attempt_id,
        call_seq=seq,
        item_index=index,
        route_id=route_id,
        capability=EXTRACT_CAPABILITY,
        field_kind=None,  # a whole document, not one amount
        estimated_cost_units=ESTIMATED_COST_UNITS,
    )


def _answered(attempt_id: uuid.UUID, seq: int, report: ExtractionReport) -> ProviderCallResult:
    return ProviderCallResult(
        attempt_id=attempt_id,
        call_seq=seq,
        outcome=CallOutcome.ANSWERED,
        usage=report.usage,
        model_served=report.model_served,
        provider_generation_id=report.generation_id,
        input_tokens=report.input_tokens,
        output_tokens=report.output_tokens,
        latency_ms=report.latency_ms,
    )


def _failed(
    attempt_id: uuid.UUID, seq: int, code: TransientCode | PermanentCode
) -> ProviderCallResult:
    return ProviderCallResult(
        attempt_id=attempt_id, call_seq=seq, outcome=CallOutcome.FAILED, failure_code=code
    )


def _item_result(index: int, judgement: Judgement, fields: JsonValue) -> JsonValue:
    return {
        "index": index,
        "status": judgement.status.value,
        "reason": None if judgement.reason is None else judgement.reason.value,
        "failed_rules": list(judgement.failed_rules),
        "fields": fields,
    }


# --- what a cell measures -------------------------------------------------------------------

# Prices are given in US dollars per million tokens.
CURRENCY = "USD"
# Synthetic prices for a dry run, per million tokens: they say nothing about any real model.
CANNED_PRICE_IN_PER_MTOK = Decimal("1.00")
CANNED_PRICE_OUT_PER_MTOK = Decimal("5.00")
_MTOK = Decimal(1_000_000)


@dataclass(frozen=True, slots=True)
class Prices:
    """The model's price per million input and output tokens, as given to the harness."""

    in_per_mtok: Decimal
    out_per_mtok: Decimal
    canned: bool = False

    def __post_init__(self) -> None:
        for name in ("in_per_mtok", "out_per_mtok"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite() or value < 0:
                msg = f"{name} must be a finite, non-negative Decimal"
                raise ValueError(msg)


CANNED_PRICES = Prices(CANNED_PRICE_IN_PER_MTOK, CANNED_PRICE_OUT_PER_MTOK, canned=True)

_ITEM_RESULTS = """
SELECT t.result FROM tasks t WHERE t.job_id = %s AND t.state = 'succeeded' ORDER BY t.task_key
"""
_CALLS = """
SELECT a.state, r.outcome, r.failure_code, r.input_tokens, r.output_tokens, r.latency_ms,
       r.reported_amount, r.reported_unit
FROM provider_calls c
JOIN attempts a ON a.id = c.attempt_id
LEFT JOIN provider_call_results r ON r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq
WHERE c.job_id = %s
"""
_ATTEMPTS = """
SELECT count(*), coalesce(sum(extract(epoch FROM a.finished_at - a.started_at)), 0)
FROM attempts a JOIN tasks t ON t.id = a.task_id WHERE t.job_id = %s
"""
_TASKS = "SELECT count(*) FROM tasks WHERE job_id = %s"
_OPEN_CLOSURES = """
SELECT count(*) FROM attempts a JOIN tasks t ON t.id = a.task_id
LEFT JOIN attempt_usage_closures u ON u.attempt_id = a.id
WHERE t.job_id = %s AND a.state = 'succeeded' AND (u.attempt_id IS NULL OR u.capture <> 'complete')
"""


@dataclass(frozen=True, slots=True)
class _Call:
    attempt_state: str
    outcome: str | None
    failure_code: str | None
    input_tokens: int | None
    output_tokens: int | None
    latency_ms: int | None
    reported_amount: Decimal | None
    reported_unit: str | None


def _item_results(conn: Any, job_id: uuid.UUID) -> dict[int, list[Mapping[str, Any]]]:
    """Every stored item result by its index; an index may appear more than once (a defect)."""
    by_index: dict[int, list[Mapping[str, Any]]] = {}
    for (result,) in conn.execute(_ITEM_RESULTS, (job_id,)):
        for item in result["results"]:
            by_index.setdefault(int(item["index"]), []).append(item)
    return by_index


def _calls(conn: Any, job_id: uuid.UUID) -> list[_Call]:
    return [_Call(*row) for row in conn.execute(_CALLS, (job_id,))]


def _scalar(conn: Any, query: str, job_id: uuid.UUID) -> int:
    row = conn.execute(query, (job_id,)).fetchone()
    return int(row[0])


def _check(name: str, passed: bool, detail: Any) -> dict[str, Any]:
    return {"name": name, "passed": passed, "detail": detail}


def extraction_gate(
    conn: Any, job_id: uuid.UUID, documents: int, *, clean: bool
) -> list[dict[str, Any]]:
    """The extract profile's own gate checks, after the engine's (``run.run_gate``).

    ``extraction_complete``: each of the ``documents`` has exactly one item result.
    ``extraction_usage``: the succeeded attempts answered exactly one call per
    document they called, left none unresolved and closed their capture complete;
    when ``clean`` (no retry and no failed task) the job made no other call. A
    model's wrong reading is measured, never gated.
    """
    by_index = _item_results(conn, job_id)
    missing = [index for index in range(documents) if index not in by_index]
    repeated = sorted(index for index, items in by_index.items() if len(items) > 1)
    unknown = sorted(index for index in by_index if not 0 <= index < documents)
    complete = not missing and not repeated and not unknown
    checks = [
        _check(
            "extraction_complete",
            complete,
            {"missing": missing[:20], "repeated": repeated[:20], "unknown": unknown[:20]},
        )
    ]
    called = sum(
        1
        for items in by_index.values()
        for item in items
        if item["reason"] != Reason.PROVIDER_CALL_CAP_REACHED.value
    )
    calls = _calls(conn, job_id)
    succeeded = [call for call in calls if call.attempt_state == "succeeded"]
    answered = sum(1 for call in succeeded if call.outcome == "answered")
    unresolved = sum(1 for call in succeeded if call.outcome is None)
    open_closures = _scalar(conn, _OPEN_CLOSURES, job_id)
    passed = answered == called == len(succeeded) and unresolved == 0 and open_closures == 0
    if clean:
        passed = passed and len(calls) == len(succeeded)
    detail = {
        "documents_called": called,
        "succeeded_calls": len(succeeded),
        "succeeded_answered": answered,
        "succeeded_unresolved": unresolved,
        "open_closures": open_closures,
        "calls": len(calls),
    }
    checks.append(_check("extraction_usage", passed, detail))
    return checks


def _sum(values: Sequence[int | None]) -> int:
    return sum(value for value in values if value is not None)


def _cost(input_tokens: int, output_tokens: int, prices: Prices) -> Decimal:
    return (
        Decimal(input_tokens) * prices.in_per_mtok + Decimal(output_tokens) * prices.out_per_mtok
    ) / _MTOK


def _tokens_of(calls: Sequence[_Call]) -> dict[str, int]:
    return {
        "input": _sum([call.input_tokens for call in calls]),
        "output": _sum([call.output_tokens for call in calls]),
    }


def extraction_block(
    conn: Any,
    job_id: uuid.UUID,
    expected: Sequence[Fields],
    statuses: Sequence[Status],
    prices: Prices,
) -> dict[str, Any]:
    """The result's ``extraction`` block, in the shape the T1 plan pins; counts only."""
    documents = len(expected)
    by_index = _item_results(conn, job_id)
    items = [by_index.get(index, [None])[0] for index in range(documents)]
    accuracy = score_all(expected, [None if item is None else item["fields"] for item in items])
    status_counts = {status.value: 0 for status in Status}
    reasons = {reason.value: 0 for reason in Reason}
    status_matched = 0
    for item, wanted in zip(items, statuses, strict=True):
        if item is None:
            continue
        status_counts[item["status"]] += 1
        if item["reason"] is not None:
            reasons[item["reason"]] += 1
        status_matched += int(item["status"] == wanted.value)

    calls = _calls(conn, job_id)
    answered = [call for call in calls if call.outcome == "answered"]
    succeeded = [call for call in answered if call.attempt_state == "succeeded"]
    other = [call for call in answered if call.attempt_state != "succeeded"]
    latencies = [call.latency_ms for call in answered if call.latency_ms is not None]
    provider_s = sum(latencies) / 1000
    attempts, handler = conn.execute(_ATTEMPTS, (job_id,)).fetchone()
    handler_s = float(handler)
    tasks = _scalar(conn, _TASKS, job_id)

    tokens = _tokens_of(answered)
    succeeded_tokens = _tokens_of(succeeded)
    other_tokens = _tokens_of(other)
    total = _cost(tokens["input"], tokens["output"], prices)
    succeeded_cost = _cost(succeeded_tokens["input"], succeeded_tokens["output"], prices)
    other_cost = _cost(other_tokens["input"], other_tokens["output"], prices)
    reported: dict[str, Decimal] = {}
    for call in calls:
        if call.reported_amount is not None and call.reported_unit is not None:
            unit = call.reported_unit
            reported[unit] = reported.get(unit, Decimal(0)) + Decimal(call.reported_amount)
    transient = {code.value: 0 for code in TransientCode}
    permanent = {code.value: 0 for code in PermanentCode}
    for call in calls:
        if call.failure_code in transient:
            transient[call.failure_code] += 1
        elif call.failure_code in permanent:
            permanent[call.failure_code] += 1
    return {
        "documents": documents,
        "calls": {
            "total": len(calls),
            "answered": len(answered),
            "failed": sum(1 for call in calls if call.outcome == "failed"),
            "unresolved": sum(1 for call in calls if call.outcome is None),
            "succeeded_attempts": sum(1 for call in calls if call.attempt_state == "succeeded"),
            "other_attempts": sum(1 for call in calls if call.attempt_state != "succeeded"),
            "latency_ms": percentiles(latencies),
        },
        "time_split": {
            "provider_s": provider_s,
            "handler_s": handler_s,
            "provider_share": provider_s / handler_s if handler_s > 0 else None,
        },
        "accuracy": {
            "matched": sum(accuracy.matched.values()),
            "expected": sum(accuracy.expected.values()),
            "overall": accuracy.overall(),
            "per_field": {
                name: {"matched": accuracy.matched[name], "expected": accuracy.expected[name]}
                for name in SCORED_FIELDS
            },
            "exact_documents": accuracy.exact_documents,
            "status_matched": status_matched,
        },
        "statuses": status_counts,
        "needs_review": {"count": status_counts[Status.NEEDS_REVIEW.value], "by_reason": reasons},
        "tokens": {
            **tokens,
            "succeeded_attempts": succeeded_tokens,
            "other_attempts": other_tokens,
            "answered_without_counts": sum(
                1 for call in answered if call.input_tokens is None or call.output_tokens is None
            ),
        },
        "cost": {
            "currency": CURRENCY,
            "prices": {
                "in_per_mtok": str(prices.in_per_mtok),
                "out_per_mtok": str(prices.out_per_mtok),
                "canned": prices.canned,
            },
            "total": str(total),
            "succeeded_attempts": str(succeeded_cost),
            "other_attempts": str(other_cost),
            "per_1000_invoices": str(total * 1000 / documents),
            "per_1000_invoices_succeeded": str(succeeded_cost * 1000 / documents),
            "reported": {unit: str(amount) for unit, amount in sorted(reported.items())},
        },
        "retries": int(attempts) - tasks,
        "transient_errors": transient,
        "permanent_errors": permanent,
    }
