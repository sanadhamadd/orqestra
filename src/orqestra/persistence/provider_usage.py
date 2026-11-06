"""Provider usage ledger: append-only facts about the provider calls an attempt made.

Migration 0005 stores three kinds of fact, each written at most once:

- a call *intent* (``record_call_intent``), recorded before the call is made. Its
  job and task are copied from the attempt at that moment, so attribution survives
  fencing and recovery;
- a call *result* (``record_call_result``): what the provider answered or how it
  failed, with any amount and unit exactly as the provider reported them. A call
  whose outcome is unknown has no result;
- an attempt's usage *closure* (``record_usage_closure``): whether its capture is
  complete. A complete closure counts every recorded intent, each with its result,
  and recovery cannot close a capture as complete below tier ``t3``.

Every write is one short READ COMMITTED transaction it owns, and is idempotent
after a lost acknowledgement: its key is the caller's ``(attempt_id, call_seq)``,
or ``attempt_id`` for a closure, and a repeat with the same content returns the
stored record with ``created=False``. Different content under the same key raises
``UsageReplayConflictError``; nothing is ever overwritten. Identity compares only
the fields the caller supplies, never ``job_id``, ``task_id``,
``attempt_state_at_record`` or ``recorded_at``. An amount's identity includes its
scale (``amount_identity``), as PostgreSQL displays it: ``Decimal("1E+2")``
repeats a stored ``100``, but ``Decimal("1.5")`` conflicts with a stored ``1.50``.
A real replay resends the same ``Decimal``; two readings of one provider figure
that differ in scale point to a parsing defect, which is not hidden.

Writes are accepted whatever state the attempt is in: a provider that was called
was called. A fact recorded after its attempt stopped running is flagged ``late``.
An intent reads its attempt ``FOR KEY SHARE`` (the foreign key's own lock mode,
taken earlier), so an intent that waited behind a completion or a revocation reads
the state that transition committed and is flagged late accurately; a result's
flag is advisory. An intent or result committed after a complete closure is also
accepted, so a report must re-check completeness (``capture_of``). Nothing here
changes a task or an attempt. No write locks a task row; an intent waits only for
a lifecycle ``FOR UPDATE`` lock on its own attempt row.

Closures written by the lifecycle (DEC-057). ``complete_attempt`` can close the
capture with the worker's closure, and ``revoke_next_expired`` with a closure it
derives for the kinds it is told about, each in the transaction that ends the
attempt (``close_capture`` and ``close_capture_on_recovery``). That transaction
holds the attempt row ``FOR UPDATE``, so no intent can be added while the closure
is checked and written. ``record_late_usage`` records a result that arrives after
the attempt may have ended and says whether a closure already exists.

Text rule. ``capability``, ``model``, ``reported_unit``, ``model_served`` and
``provider_generation_id`` must be a ``str`` that is not blank after ``.strip()``
(exactly the routing rule for a capability), no longer than the column's limit in
code points, without ``NUL``, and encodable as UTF-8 (no lone surrogates). They are
stored exactly as given: pass the exact string given to
``choose_route(capability=...)``, and a unit exactly as the provider declared it.
The database's ``btrim`` checks are looser than this rule on purpose: they are a
backstop, not the gate. Non-ASCII text assumes a UTF-8 server encoding.

``read_attempt_usage`` reads one attempt's ledger in one ``REPEATABLE READ, READ
ONLY`` transaction, and ``capture_of`` classifies what it read. Neither computes
totals.

The module has no function that removes ledger rows, and logs carry ids only.
"""

import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from psycopg import errors
from psycopg.rows import TupleRow

from orqestra.calls.provider import FieldKind
from orqestra.domain.errors import OrqestraError
from orqestra.domain.states import AttemptState
from orqestra.persistence._shared import (
    _CALLER_TRANSACTION,
    Conn,
    Cursor,
    NotFoundError,
    owned_transaction,
    tuple_cursor,
)

logger = logging.getLogger(__name__)

INTEGER_MAX = 2**31 - 1
BIGINT_MAX = 2**63 - 1
CAPABILITY_MAX_LENGTH = 256
MODEL_MAX_LENGTH = 128
UNIT_MAX_LENGTH = 64
GENERATION_ID_MAX_LENGTH = 128
AMOUNT_MAX_SCALE = 18
AMOUNT_LIMIT = Decimal("1E+20")  # exclusive

# The routing route-id pattern (orqestra.calls.routing), also used for provider names
# and prompt versions.
_IDENTIFIER = re.compile(r"[A-Za-z0-9._-]{1,64}", re.ASCII)


class CallOutcome(StrEnum):
    """How a provider call ended, when that is known. Values are stable identifiers."""

    ANSWERED = "answered"
    FAILED = "failed"


class CallFailure(StrEnum):
    """Why a call failed: the provider error codes (``TransientCode`` and ``PermanentCode``)."""

    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    UNAVAILABLE = "unavailable"
    REJECTED = "rejected"
    UNSUPPORTED_REQUEST = "unsupported_request"


class CaptureState(StrEnum):
    """Whether an attempt's ledger holds every call it made."""

    COMPLETE = "complete"
    INCOMPLETE = "incomplete"


class ClosedBy(StrEnum):
    """Who closed an attempt's capture."""

    WORKER = "worker"
    RECOVERY = "recovery"


class CaptureTier(StrEnum):
    """How strong the evidence behind a closure is. Recovery may close complete only at T3."""

    T1 = "t1"
    T2 = "t2"
    T3 = "t3"


# --- validation -------------------------------------------------------------------------


def _check_uuid(name: str, value: object) -> None:
    if not isinstance(value, uuid.UUID):
        msg = f"{name} must be a UUID, got {type(value).__name__}"
        raise TypeError(msg)


def _check_int(name: str, value: object, maximum: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{name} must be an integer, got {type(value).__name__}"
        raise TypeError(msg)
    if not 0 <= value <= maximum:
        msg = f"{name} must be between 0 and {maximum}"
        raise ValueError(msg)


def _check_enum(name: str, value: object, kind: type[StrEnum]) -> None:
    if not isinstance(value, kind):
        msg = f"{name} must be a {kind.__name__}, got {type(value).__name__}"
        raise TypeError(msg)


def _check_identifier(name: str, value: object) -> None:
    if not isinstance(value, str):
        msg = f"{name} must be a str, got {type(value).__name__}"
        raise TypeError(msg)
    if _IDENTIFIER.fullmatch(value) is None:
        msg = f"{name} must be 1 to 64 characters of letters, digits, '.', '_' or '-'"
        raise ValueError(msg)


def _check_text(name: str, value: object, limit: int) -> None:
    """The storage-safe text rule; see the module docstring. Messages never echo the value."""
    if not isinstance(value, str):
        msg = f"{name} must be a str, got {type(value).__name__}"
        raise TypeError(msg)
    if not value.strip():
        msg = f"{name} must not be blank"
        raise ValueError(msg)
    if len(value) > limit:
        msg = f"{name} must be at most {limit} characters"
        raise ValueError(msg)
    if "\x00" in value:
        msg = f"{name} must not contain NUL"
        raise ValueError(msg)
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        msg = f"{name} must be valid Unicode text"
        raise ValueError(msg) from None


def _scale(amount: Decimal) -> int:
    exponent = amount.as_tuple().exponent
    if not isinstance(exponent, int):
        msg = "a non-finite amount has no scale"
        raise ValueError(msg)
    return max(0, -exponent)


def amount_identity(amount: Decimal) -> tuple[Decimal, int]:
    """An amount's replay identity: its value and its scale as PostgreSQL stores it.

    ``Decimal("1E+2")`` and ``Decimal("100")`` have the same identity;
    ``Decimal("1.5")`` and ``Decimal("1.50")`` do not.
    """
    return amount, _scale(amount)


def _check_amount(value: object) -> None:
    if not isinstance(value, Decimal):
        msg = f"reported_amount must be a Decimal, got {type(value).__name__}"
        raise TypeError(msg)
    if not value.is_finite():
        msg = "reported_amount must be finite"
        raise ValueError(msg)
    if value.is_signed():
        msg = "reported_amount must not be negative or negative zero"
        raise ValueError(msg)
    if _scale(value) > AMOUNT_MAX_SCALE:
        msg = f"reported_amount must have at most {AMOUNT_MAX_SCALE} decimal places"
        raise ValueError(msg)
    if value >= AMOUNT_LIMIT:
        msg = "reported_amount must be below 1E+20"
        raise ValueError(msg)


# --- what callers record ----------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class CallIntent:
    """A provider call about to be made: call ``call_seq`` of attempt ``attempt_id``.

    ``estimated_cost_units`` is the caller's route estimate (``RouteOption``), not
    accounting. ``field_kind`` and ``item_index`` say which amount the call is about.
    """

    attempt_id: uuid.UUID
    call_seq: int
    provider: str
    route_id: str
    capability: str
    prompt_version: str | None = None
    model: str | None = None
    field_kind: FieldKind | None = None
    item_index: int | None = None
    estimated_cost_units: int | None = None

    def __post_init__(self) -> None:
        _check_uuid("attempt_id", self.attempt_id)
        _check_int("call_seq", self.call_seq, INTEGER_MAX)
        _check_identifier("provider", self.provider)
        _check_identifier("route_id", self.route_id)
        _check_text("capability", self.capability, CAPABILITY_MAX_LENGTH)
        if self.prompt_version is not None:
            _check_identifier("prompt_version", self.prompt_version)
        if self.model is not None:
            _check_text("model", self.model, MODEL_MAX_LENGTH)
        if self.field_kind is not None:
            _check_enum("field_kind", self.field_kind, FieldKind)
        if self.item_index is not None:
            _check_int("item_index", self.item_index, INTEGER_MAX)
        if self.estimated_cost_units is not None:
            _check_int("estimated_cost_units", self.estimated_cost_units, BIGINT_MAX)


@dataclass(frozen=True, slots=True, kw_only=True)
class CallResult:
    """How call ``call_seq`` of attempt ``attempt_id`` ended, as the provider reported it.

    ``failure_code`` is set exactly when the call failed. ``reported_amount`` and
    ``reported_unit`` are the provider's own usage report, given together or not at
    all, and stored exactly; a failed call may report usage too.
    """

    attempt_id: uuid.UUID
    call_seq: int
    outcome: CallOutcome
    failure_code: CallFailure | None = None
    reported_amount: Decimal | None = None
    reported_unit: str | None = None
    model_served: str | None = None
    provider_generation_id: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    latency_ms: int | None = None

    def __post_init__(self) -> None:
        _check_uuid("attempt_id", self.attempt_id)
        _check_int("call_seq", self.call_seq, INTEGER_MAX)
        _check_enum("outcome", self.outcome, CallOutcome)
        if self.failure_code is not None:
            _check_enum("failure_code", self.failure_code, CallFailure)
        if (self.failure_code is not None) != (self.outcome is CallOutcome.FAILED):
            msg = "failure_code must be set exactly when the outcome is FAILED"
            raise ValueError(msg)
        if (self.reported_amount is None) != (self.reported_unit is None):
            msg = "reported_amount and reported_unit must be given together"
            raise ValueError(msg)
        if self.reported_amount is not None:
            _check_amount(self.reported_amount)
        if self.reported_unit is not None:
            _check_text("reported_unit", self.reported_unit, UNIT_MAX_LENGTH)
        if self.model_served is not None:
            _check_text("model_served", self.model_served, MODEL_MAX_LENGTH)
        if self.provider_generation_id is not None:
            _check_text(
                "provider_generation_id", self.provider_generation_id, GENERATION_ID_MAX_LENGTH
            )
        if self.input_tokens is not None:
            _check_int("input_tokens", self.input_tokens, BIGINT_MAX)
        if self.output_tokens is not None:
            _check_int("output_tokens", self.output_tokens, BIGINT_MAX)
        if self.latency_ms is not None:
            _check_int("latency_ms", self.latency_ms, INTEGER_MAX)


@dataclass(frozen=True, slots=True, kw_only=True)
class UsageClosure:
    """Whether attempt ``attempt_id``'s capture is complete.

    ``call_count`` is set exactly when the capture is complete, and then must equal
    the number of recorded intents, each with its result. Recovery cannot close a
    capture as complete at tier T1 or T2.
    """

    attempt_id: uuid.UUID
    capture: CaptureState
    closed_by: ClosedBy
    capture_tier: CaptureTier
    call_count: int | None = None

    def __post_init__(self) -> None:
        _check_uuid("attempt_id", self.attempt_id)
        _check_enum("capture", self.capture, CaptureState)
        _check_enum("closed_by", self.closed_by, ClosedBy)
        _check_enum("capture_tier", self.capture_tier, CaptureTier)
        complete = self.capture is CaptureState.COMPLETE
        if complete:
            if self.call_count is None:
                msg = "a complete capture must give call_count"
                raise ValueError(msg)
            _check_int("call_count", self.call_count, INTEGER_MAX)
        elif self.call_count is not None:
            msg = "only a complete capture gives call_count"
            raise ValueError(msg)
        if (
            complete
            and self.closed_by is ClosedBy.RECOVERY
            and self.capture_tier in (CaptureTier.T1, CaptureTier.T2)
        ):
            msg = "recovery cannot close a capture as complete below tier T3"
            raise ValueError(msg)


# --- what is stored ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StoredCall:
    """A recorded intent, with the attribution and attempt state copied when it was recorded."""

    intent: CallIntent
    job_id: uuid.UUID
    task_id: uuid.UUID
    attempt_state_at_record: AttemptState
    recorded_at: datetime

    @property
    def late(self) -> bool:
        """Recorded after the attempt stopped running. Advisory only."""
        return self.attempt_state_at_record is not AttemptState.RUNNING


@dataclass(frozen=True, slots=True)
class StoredResult:
    """A recorded result, with the attempt's state when it was recorded."""

    result: CallResult
    attempt_state_at_record: AttemptState
    recorded_at: datetime

    @property
    def late(self) -> bool:
        """Recorded after the attempt stopped running. Advisory only."""
        return self.attempt_state_at_record is not AttemptState.RUNNING


@dataclass(frozen=True, slots=True)
class StoredClosure:
    """A recorded closure."""

    closure: UsageClosure
    recorded_at: datetime


@dataclass(frozen=True)
class Recorded[R]:
    """A write's stored record; ``created`` is ``False`` when it repeated an earlier write."""

    record: R
    created: bool


@dataclass(frozen=True, slots=True)
class UsageCall:
    """One call of an attempt: its intent and, if one was recorded, its result."""

    call: StoredCall
    result: StoredResult | None


@dataclass(frozen=True, slots=True)
class AttemptUsage:
    """One attempt's ledger, read from one snapshot, calls ordered by ``call_seq``.

    ``job_id``, ``task_id`` and ``attempt_state`` are the attempt's current ones.
    ``observed_at`` is the ``statement_timestamp()`` of the snapshot's first read.
    """

    attempt_id: uuid.UUID
    job_id: uuid.UUID
    task_id: uuid.UUID
    attempt_state: AttemptState
    observed_at: datetime
    calls: tuple[UsageCall, ...]
    closure: StoredClosure | None


# --- errors -----------------------------------------------------------------------------


class UsageReplayConflictError(OrqestraError):
    """A different record already exists under this key; it was kept as it was."""

    def __init__(self, record: str, attempt_id: uuid.UUID, call_seq: int | None = None) -> None:
        where = f"attempt {attempt_id}"
        if call_seq is not None:
            where += f" call {call_seq}"
        super().__init__(f"a different {record} is already recorded for {where}; it was kept")
        self.record = record
        self.attempt_id = attempt_id
        self.call_seq = call_seq


class CallIntentNotFoundError(OrqestraError, LookupError):
    """A result names a call whose intent was never recorded."""

    def __init__(self, attempt_id: uuid.UUID, call_seq: int) -> None:
        super().__init__(f"no intent is recorded for attempt {attempt_id} call {call_seq}")
        self.attempt_id = attempt_id
        self.call_seq = call_seq


class CaptureNotCompleteError(OrqestraError):
    """A complete closure does not match the recorded intents and results.

    ``intents`` and ``unanswered`` are the counts the check saw, or ``None`` when
    the database's own check refused the closure.
    """

    def __init__(
        self,
        attempt_id: uuid.UUID,
        call_count: int | None,
        intents: int | None = None,
        unanswered: int | None = None,
    ) -> None:
        detail = (
            "the database found it incomplete"
            if intents is None
            else f"{intents} intents are recorded and {unanswered} have no result"
        )
        super().__init__(
            f"attempt {attempt_id}'s capture is not complete: the closure counts "
            f"{call_count} calls, but {detail}"
        )
        self.attempt_id = attempt_id
        self.call_count = call_count
        self.intents = intents
        self.unanswered = unanswered


# --- SQL --------------------------------------------------------------------------------

# Every parameter in a SELECT list is cast: psycopg sends str and None untyped, and
# PostgreSQL would resolve them to text there, whatever the target column.
# FOR KEY SHARE OF a is the lock the foreign key takes anyway, taken before the row is
# read: an intent that waits behind a lifecycle lock on its attempt re-reads a.state
# once that transaction commits (READ COMMITTED), so the late flag is the committed
# state. The task row is read without a lock.
_INSERT_INTENT = """
INSERT INTO provider_calls (
    attempt_id, call_seq, job_id, task_id, provider, route_id, prompt_version, capability,
    model, field_kind, item_index, estimated_cost_units, attempt_state_at_record
)
SELECT a.id, %s::integer, t.job_id, t.id, %s::text, %s::text, %s::text, %s::text,
    %s::text, %s::text, %s::integer, %s::bigint, a.state
FROM attempts a JOIN tasks t ON t.id = a.task_id
WHERE a.id = %s::uuid
FOR KEY SHARE OF a
ON CONFLICT (attempt_id, call_seq) DO NOTHING
RETURNING job_id, task_id, attempt_state_at_record, recorded_at
"""

_SELECT_INTENT = """
SELECT provider, route_id, prompt_version, capability, model, field_kind, item_index,
    estimated_cost_units, job_id, task_id, attempt_state_at_record, recorded_at
FROM provider_calls
WHERE attempt_id = %s AND call_seq = %s
"""

_INSERT_RESULT = """
INSERT INTO provider_call_results (
    attempt_id, call_seq, outcome, failure_code, reported_amount, reported_unit, model_served,
    provider_generation_id, input_tokens, output_tokens, latency_ms, attempt_state_at_record
)
SELECT c.attempt_id, c.call_seq, %s::text, %s::text, %s::numeric, %s::text, %s::text,
    %s::text, %s::bigint, %s::bigint, %s::integer, a.state
FROM provider_calls c JOIN attempts a ON a.id = c.attempt_id
WHERE c.attempt_id = %s::uuid AND c.call_seq = %s::integer
ON CONFLICT (attempt_id, call_seq) DO NOTHING
RETURNING attempt_state_at_record, recorded_at
"""

_SELECT_RESULT = """
SELECT outcome, failure_code, reported_amount, reported_unit, model_served,
    provider_generation_id, input_tokens, output_tokens, latency_ms,
    attempt_state_at_record, recorded_at
FROM provider_call_results
WHERE attempt_id = %s AND call_seq = %s
"""

_SELECT_CLOSURE = """
SELECT capture, call_count, closed_by, capture_tier, recorded_at
FROM attempt_usage_closures
WHERE attempt_id = %s
"""

_ATTEMPT_EXISTS = "SELECT EXISTS (SELECT FROM attempts WHERE id = %s)"

_CAPTURE_COUNTS = """
SELECT count(*), count(*) FILTER (WHERE r.attempt_id IS NULL)
FROM provider_calls c
LEFT JOIN provider_call_results r ON r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq
WHERE c.attempt_id = %s
"""

_INSERT_CLOSURE = """
INSERT INTO attempt_usage_closures (attempt_id, capture, call_count, closed_by, capture_tier)
VALUES (%s::uuid, %s::text, %s::integer, %s::text, %s::text)
ON CONFLICT (attempt_id) DO NOTHING
RETURNING recorded_at
"""

_SET_TRANSACTION = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"

_READ_ATTEMPT = """
SELECT t.job_id, t.id, a.state, statement_timestamp()
FROM attempts a JOIN tasks t ON t.id = a.task_id
WHERE a.id = %s
"""

_READ_CALLS = """
SELECT c.call_seq, c.provider, c.route_id, c.prompt_version, c.capability, c.model,
    c.field_kind, c.item_index, c.estimated_cost_units, c.job_id, c.task_id,
    c.attempt_state_at_record, c.recorded_at,
    r.call_seq IS NOT NULL, r.outcome, r.failure_code, r.reported_amount, r.reported_unit,
    r.model_served, r.provider_generation_id, r.input_tokens, r.output_tokens, r.latency_ms,
    r.attempt_state_at_record, r.recorded_at
FROM provider_calls c
LEFT JOIN provider_call_results r ON r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq
WHERE c.attempt_id = %s
ORDER BY c.call_seq
"""

_COMPLETE_CAPTURE = "attempt_usage_closures_complete_capture"
_ATTEMPT_EXISTS_CONSTRAINT = "attempt_usage_closures_attempt_exists"


# --- writes -----------------------------------------------------------------------------


def _intent_content(intent: CallIntent) -> tuple[object, ...]:
    """The fields of an intent the caller supplies, other than its key, as stored."""
    return (
        intent.provider,
        intent.route_id,
        intent.prompt_version,
        intent.capability,
        intent.model,
        None if intent.field_kind is None else intent.field_kind.value,
        intent.item_index,
        intent.estimated_cost_units,
    )


def _result_identity(content: tuple[object, ...]) -> tuple[object, ...]:
    """Result content with the amount (third field) replaced by its identity."""
    outcome, failure_code, amount, *rest = content
    identity = None if amount is None else amount_identity(_as_decimal(amount))
    return (outcome, failure_code, identity, *rest)


def _as_decimal(value: object) -> Decimal:
    if not isinstance(value, Decimal):  # pragma: no cover - numeric is read as Decimal
        msg = f"expected a Decimal amount, got {type(value).__name__}"
        raise TypeError(msg)
    return value


def _result_content(result: CallResult) -> tuple[object, ...]:
    """The fields of a result the caller supplies, other than its key, as stored."""
    return (
        result.outcome.value,
        None if result.failure_code is None else result.failure_code.value,
        result.reported_amount,
        result.reported_unit,
        result.model_served,
        result.provider_generation_id,
        result.input_tokens,
        result.output_tokens,
        result.latency_ms,
    )


def _closure_content(closure: UsageClosure) -> tuple[object, ...]:
    return (
        closure.capture.value,
        closure.call_count,
        closure.closed_by.value,
        closure.capture_tier.value,
    )


def _late_note(state: AttemptState) -> str:
    return "" if state is AttemptState.RUNNING else f" (late: attempt {state.value})"


def record_call_intent(conn: Conn, intent: CallIntent) -> Recorded[StoredCall]:
    """Record that call ``intent.call_seq`` of ``intent.attempt_id`` is about to be made.

    The attempt's job and task are copied into the row, and the attempt's state is
    recorded with it. A repeat with the same content returns the stored record with
    ``created=False``; different content raises ``UsageReplayConflictError``. Raises
    ``NotFoundError`` if the attempt does not exist, and ``RuntimeError`` if the
    caller already has a transaction open on ``conn``.
    """
    content = _intent_content(intent)
    with owned_transaction(conn, "record_call_intent") as cur:
        row = cur.execute(_INSERT_INTENT, (intent.call_seq, *content, intent.attempt_id)).fetchone()
        created = row is not None
        if row is None:
            row = cur.execute(_SELECT_INTENT, (intent.attempt_id, intent.call_seq)).fetchone()
            if row is None:
                raise NotFoundError("attempt", intent.attempt_id)
            *stored, job_id, task_id, state, recorded_at = row
            if tuple(stored) != content:
                raise UsageReplayConflictError("call intent", intent.attempt_id, intent.call_seq)
        else:
            job_id, task_id, state, recorded_at = row
    record = StoredCall(intent, job_id, task_id, AttemptState(state), recorded_at)
    if created:
        logger.info(
            "recorded provider call intent: attempt %s call %s%s",
            intent.attempt_id,
            intent.call_seq,
            _late_note(record.attempt_state_at_record),
        )
    return Recorded(record, created)


def record_call_result(conn: Conn, result: CallResult) -> Recorded[StoredResult]:
    """Record how call ``result.call_seq`` of ``result.attempt_id`` ended.

    Its intent must have been recorded: otherwise ``CallIntentNotFoundError``. The
    attempt's state is recorded with it. A repeat with the same content, the amount
    compared by ``amount_identity``, returns the stored record with
    ``created=False``; different content raises ``UsageReplayConflictError``. Raises
    ``RuntimeError`` if the caller already has a transaction open on ``conn``.
    """
    with owned_transaction(conn, "record_call_result") as cur:
        recorded = _write_result(cur, result)
    if recorded.created:
        logger.info(
            "recorded provider call result: attempt %s call %s%s",
            result.attempt_id,
            result.call_seq,
            _late_note(recorded.record.attempt_state_at_record),
        )
    return recorded


def _write_result(cur: Cursor, result: CallResult) -> Recorded[StoredResult]:
    """Insert the result or match it against the stored one, on the caller's cursor."""
    content = _result_content(result)
    row = cur.execute(_INSERT_RESULT, (*content, result.attempt_id, result.call_seq)).fetchone()
    created = row is not None
    if row is None:
        row = cur.execute(_SELECT_RESULT, (result.attempt_id, result.call_seq)).fetchone()
        if row is None:
            raise CallIntentNotFoundError(result.attempt_id, result.call_seq)
        *stored, state, recorded_at = row
        if _result_identity(tuple(stored)) != _result_identity(content):
            raise UsageReplayConflictError("call result", result.attempt_id, result.call_seq)
    else:
        state, recorded_at = row
    return Recorded(StoredResult(result, AttemptState(state), recorded_at), created)


@dataclass(frozen=True, slots=True)
class LateUsage:
    """A result recorded by ``record_late_usage``, and the attempt's closure as then read.

    ``closure`` is the closure committed when the result was written, or ``None``.
    It is read, never written or changed.
    """

    result: Recorded[StoredResult]
    closure: StoredClosure | None

    @property
    def after_closure(self) -> bool:
        """The attempt's capture was already closed: the closure may not count this call."""
        return self.closure is not None


def record_late_usage(conn: Conn, result: CallResult) -> LateUsage:
    """Record a call's result that may arrive after its attempt ended, and read its closure.

    One READ COMMITTED transaction: the same insert-or-match as
    ``record_call_result``, then a read of the attempt's closure. It never writes or
    changes a closure, and it does not refuse a running attempt: whether the attempt
    has ended is decided by the stored state, which is the authoritative late flag
    (``result.record.late``). A created late row is logged as a warning. Raises
    exactly what ``record_call_result`` raises.
    """
    with owned_transaction(conn, "record_late_usage") as cur:
        recorded = _write_result(cur, result)
        row = cur.execute(_SELECT_CLOSURE, (result.attempt_id,)).fetchone()
    late = LateUsage(recorded, None if row is None else _stored_closure(result.attempt_id, row))
    if recorded.created:
        log = logger.warning if recorded.record.late else logger.info
        log(
            "recorded provider call result: attempt %s call %s%s%s",
            result.attempt_id,
            result.call_seq,
            _late_note(recorded.record.attempt_state_at_record),
            " after its usage closure" if late.after_closure else "",
        )
    return late


def record_usage_closure(conn: Conn, closure: UsageClosure) -> Recorded[StoredClosure]:
    """Record whether ``closure.attempt_id``'s capture is complete.

    A complete closure must count every recorded intent, each with its result:
    otherwise ``CaptureNotCompleteError``. A repeat with the same content returns
    the stored record with ``created=False``, even if a call was recorded since;
    different content raises ``UsageReplayConflictError``. Raises ``NotFoundError``
    if the attempt does not exist, and ``RuntimeError`` if the caller already has a
    transaction open on ``conn``. It does not lock the attempt: a closure recorded here
    while the attempt runs can be followed by intents that are not late. Pass the
    closure to ``complete_attempt`` to close the capture atomically with the attempt.
    """
    with owned_transaction(conn, "record_usage_closure") as cur:
        recorded_at, created = _write_closure(cur, closure)
    if created:
        logger.info(
            "recorded usage closure: attempt %s %s by %s at %s",
            closure.attempt_id,
            closure.capture.value,
            closure.closed_by.value,
            closure.capture_tier.value,
        )
    return Recorded(StoredClosure(closure, recorded_at), created)


def _write_closure(cur: Cursor, closure: UsageClosure) -> tuple[datetime, bool]:
    """Insert the closure or match it against the stored one; ``(recorded_at, created)``.

    An existing closure is looked up first, so a replay is not re-checked against
    calls recorded since.
    """
    content = _closure_content(closure)
    stored = cur.execute(_SELECT_CLOSURE, (closure.attempt_id,)).fetchone()
    if stored is None:
        _check_closable(cur, closure)
        inserted = _insert_closure(cur, closure, content)
        if inserted is not None:
            return inserted[0], True
        # Another transaction committed a closure for this attempt meanwhile.
        stored = cur.execute(_SELECT_CLOSURE, (closure.attempt_id,)).fetchone()
        if stored is None:  # pragma: no cover - a conflict leaves a committed row
            msg = f"the closure of attempt {closure.attempt_id} could not be read"
            raise RuntimeError(msg)
    *existing, recorded_at = stored
    if tuple(existing) != content:
        raise UsageReplayConflictError("usage closure", closure.attempt_id)
    return recorded_at, False


def _check_closable(cur: Cursor, closure: UsageClosure) -> None:
    """Refuse a closure for a missing attempt, or a complete one that does not match."""
    exists = cur.execute(_ATTEMPT_EXISTS, (closure.attempt_id,)).fetchone()
    if exists is None or not exists[0]:
        raise NotFoundError("attempt", closure.attempt_id)
    if closure.capture is not CaptureState.COMPLETE:
        return
    counts = cur.execute(_CAPTURE_COUNTS, (closure.attempt_id,)).fetchone()
    intents, unanswered = (0, 0) if counts is None else counts
    if intents != closure.call_count or unanswered != 0:
        raise CaptureNotCompleteError(closure.attempt_id, closure.call_count, intents, unanswered)


def _insert_closure(
    cur: Cursor, closure: UsageClosure, content: tuple[object, ...]
) -> TupleRow | None:
    """Insert the closure; return its ``(recorded_at,)``, or ``None`` if one already existed."""
    try:
        return cur.execute(_INSERT_CLOSURE, (closure.attempt_id, *content)).fetchone()
    except errors.CheckViolation as exc:
        # The trigger re-checks what _check_closable saw, against rows committed since.
        constraint = exc.diag.constraint_name
        if constraint == _COMPLETE_CAPTURE:
            raise CaptureNotCompleteError(closure.attempt_id, closure.call_count) from exc
        if constraint == _ATTEMPT_EXISTS_CONSTRAINT:  # pragma: no cover - attempts stay
            raise NotFoundError("attempt", closure.attempt_id) from exc
        raise


# --- closures written by the lifecycle (DEC-057) -----------------------------------------
#
# Both run on the lifecycle's cursor, inside the transaction that ends the attempt and
# after it has locked the attempt row FOR UPDATE. No intent of the attempt can commit
# while that lock is held (an intent needs KEY SHARE on the row), and results are only
# added, so the counts read here stay true until the transaction commits.


def close_capture(cur: Cursor, closure: UsageClosure) -> Recorded[StoredClosure]:
    """Write the worker's closure in the caller's transaction, by ``record_usage_closure``'s rule.

    An identical stored closure is a replay (``created=False``); a different one
    raises ``UsageReplayConflictError``; a complete closure that does not count every
    recorded intent, each with its result, raises ``CaptureNotCompleteError``. Any
    error leaves the caller's transaction to roll back. For ``complete_attempt`` only.
    """
    recorded_at, created = _write_closure(cur, closure)
    return Recorded(StoredClosure(closure, recorded_at), created)


def close_capture_on_recovery(
    cur: Cursor, attempt_id: uuid.UUID, tier: CaptureTier
) -> Recorded[StoredClosure]:
    """Close a revoked attempt's capture in the caller's transaction; never a replay conflict.

    An existing closure, whatever it says, is kept and returned with
    ``created=False``. Otherwise, at tier T3 the capture is ``complete`` with
    ``call_count`` the number of intents when every intent has a result (zero
    intents is ``complete(0)``), and ``incomplete`` otherwise; at T1 and T2 it is
    ``incomplete``. It is closed by ``RECOVERY`` at ``tier``. If a concurrent
    closure commits first, that one is kept and returned with ``created=False``.
    For ``revoke_next_expired`` only.
    """
    stored = cur.execute(_SELECT_CLOSURE, (attempt_id,)).fetchone()
    if stored is not None:
        return Recorded(_stored_closure(attempt_id, stored), False)
    closure = _recovery_closure(cur, attempt_id, tier)
    inserted = _insert_closure(cur, closure, _closure_content(closure))
    if inserted is not None:
        return Recorded(StoredClosure(closure, inserted[0]), True)
    # Another transaction committed a closure for this attempt meanwhile: it stands.
    stored = cur.execute(_SELECT_CLOSURE, (attempt_id,)).fetchone()
    if stored is None:  # pragma: no cover - a conflict leaves a committed row
        msg = f"the closure of attempt {attempt_id} could not be read"
        raise RuntimeError(msg)
    return Recorded(_stored_closure(attempt_id, stored), False)


def _recovery_closure(cur: Cursor, attempt_id: uuid.UUID, tier: CaptureTier) -> UsageClosure:
    """The closure recovery can vouch for at ``tier``, from the intents and results now stored."""
    if tier is CaptureTier.T3:
        counts = cur.execute(_CAPTURE_COUNTS, (attempt_id,)).fetchone()
        intents, unanswered = (0, 0) if counts is None else counts
        if unanswered == 0:
            return UsageClosure(
                attempt_id=attempt_id,
                capture=CaptureState.COMPLETE,
                closed_by=ClosedBy.RECOVERY,
                capture_tier=tier,
                call_count=intents,
            )
    return UsageClosure(
        attempt_id=attempt_id,
        capture=CaptureState.INCOMPLETE,
        closed_by=ClosedBy.RECOVERY,
        capture_tier=tier,
    )


# --- read -------------------------------------------------------------------------------


def read_attempt_usage(conn: Conn, attempt_id: uuid.UUID) -> AttemptUsage | None:
    """Read one attempt's ledger from one snapshot; ``None`` if the attempt does not exist.

    One ``REPEATABLE READ, READ ONLY`` transaction: ``SET TRANSACTION`` first, then
    the attempt and its task, its calls with their results, and its closure. Every
    read sees the database as of the first, takes no row lock and computes no totals.
    An attempt with nothing recorded has no calls and no closure. Raises
    ``RuntimeError`` if the caller already has a transaction open on ``conn``.
    """
    status = conn.info.transaction_status
    if status in _CALLER_TRANSACTION:
        msg = (
            "read_attempt_usage runs its own transaction; call it outside any open "
            f"transaction (connection status: {status.name})"
        )
        raise RuntimeError(msg)
    with conn.transaction(), tuple_cursor(conn) as cur:
        cur.execute(_SET_TRANSACTION)
        head = cur.execute(_READ_ATTEMPT, (attempt_id,)).fetchone()
        if head is None:
            return None
        rows = cur.execute(_READ_CALLS, (attempt_id,)).fetchall()
        closure_row = cur.execute(_SELECT_CLOSURE, (attempt_id,)).fetchone()
    job_id, task_id, state, observed_at = head
    return AttemptUsage(
        attempt_id=attempt_id,
        job_id=job_id,
        task_id=task_id,
        attempt_state=AttemptState(state),
        observed_at=observed_at,
        calls=tuple(_usage_call(attempt_id, row) for row in rows),
        closure=None if closure_row is None else _stored_closure(attempt_id, closure_row),
    )


def _usage_call(attempt_id: uuid.UUID, row: TupleRow) -> UsageCall:
    (
        call_seq,
        provider,
        route_id,
        prompt_version,
        capability,
        model,
        field_kind,
        item_index,
        estimated_cost_units,
        job_id,
        task_id,
        intent_state,
        intent_recorded_at,
        has_result,
        outcome,
        failure_code,
        reported_amount,
        reported_unit,
        model_served,
        provider_generation_id,
        input_tokens,
        output_tokens,
        latency_ms,
        result_state,
        result_recorded_at,
    ) = row
    intent = CallIntent(
        attempt_id=attempt_id,
        call_seq=call_seq,
        provider=provider,
        route_id=route_id,
        capability=capability,
        prompt_version=prompt_version,
        model=model,
        field_kind=None if field_kind is None else FieldKind(field_kind),
        item_index=item_index,
        estimated_cost_units=estimated_cost_units,
    )
    call = StoredCall(intent, job_id, task_id, AttemptState(intent_state), intent_recorded_at)
    if not has_result:
        return UsageCall(call, None)
    result = CallResult(
        attempt_id=attempt_id,
        call_seq=call_seq,
        outcome=CallOutcome(outcome),
        failure_code=None if failure_code is None else CallFailure(failure_code),
        reported_amount=reported_amount,
        reported_unit=reported_unit,
        model_served=model_served,
        provider_generation_id=provider_generation_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        latency_ms=latency_ms,
    )
    return UsageCall(call, StoredResult(result, AttemptState(result_state), result_recorded_at))


def _stored_closure(attempt_id: uuid.UUID, row: TupleRow) -> StoredClosure:
    capture, call_count, closed_by, capture_tier, recorded_at = row
    closure = UsageClosure(
        attempt_id=attempt_id,
        capture=CaptureState(capture),
        closed_by=ClosedBy(closed_by),
        capture_tier=CaptureTier(capture_tier),
        call_count=call_count,
    )
    return StoredClosure(closure, recorded_at)


# --- classification ---------------------------------------------------------------------


class AttemptCapture(StrEnum):
    """What one attempt's ledger can be trusted to say. Values are stable identifiers."""

    OPEN = "open"  # the attempt is still running: calls may still be recorded
    CLOSED = "closed"  # a complete closure counts exactly the calls stored, each answered
    LOWER_BOUND = "lower_bound"  # calls are stored, but more may have been made
    NOT_CAPTURED = "not_captured"  # nothing was recorded: unknown, never zero


def capture_of(usage: AttemptUsage) -> AttemptCapture:
    """Classify an attempt's ledger as read by ``read_attempt_usage``. Pure; no totals.

    In order: a running attempt is ``OPEN``, even if a closure exists. A complete
    closure whose ``call_count`` equals the number of calls read, each with a
    result, is ``CLOSED``. Any other closure (incomplete, or contradicted by a call
    or a result recorded since), and a finished attempt with calls but no closure,
    is ``LOWER_BOUND``. A finished attempt with neither is ``NOT_CAPTURED``: its
    calls were not recorded, which is not the same as none being made.
    """
    if usage.attempt_state is AttemptState.RUNNING:
        return AttemptCapture.OPEN
    if usage.closure is not None:
        closure = usage.closure.closure
        if (
            closure.capture is CaptureState.COMPLETE
            and closure.call_count == len(usage.calls)
            and all(call.result is not None for call in usage.calls)
        ):
            return AttemptCapture.CLOSED
        return AttemptCapture.LOWER_BOUND
    if usage.calls:
        return AttemptCapture.LOWER_BOUND
    return AttemptCapture.NOT_CAPTURED
