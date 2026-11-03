"""Provider usage records: what callers may record, amount identity, and the write surface.

The database checks in migration 0005 are a backstop; these rules are the gate. The
PostgreSQL behaviour is covered by ``tests/integration/test_provider_usage_pg.py``.
"""

import re
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from orqestra.ai.provider import FieldKind, PermanentCode, TransientCode
from orqestra.ai.routing import RouteOption, choose_route
from orqestra.domain.states import AttemptState
from orqestra.persistence import provider_usage
from orqestra.persistence.provider_usage import (
    AMOUNT_LIMIT,
    BIGINT_MAX,
    INTEGER_MAX,
    CallFailure,
    CallIntent,
    CallOutcome,
    CallResult,
    CaptureState,
    CaptureTier,
    ClosedBy,
    StoredCall,
    StoredResult,
    UsageClosure,
    amount_identity,
)

pytestmark = pytest.mark.unit

ATTEMPT = uuid.UUID(int=1)
NOW = datetime(2026, 1, 1, tzinfo=UTC)
MICRO_CREDITS = "\N{MICRO SIGN}-credits"  # as a provider might declare its unit
WIDE_SPACE = "\N{IDEOGRAPHIC SPACE}"  # whitespace to str.strip, not to PostgreSQL's btrim


def intent(**changes: Any) -> CallIntent:
    fields: dict[str, Any] = {
        "attempt_id": ATTEMPT,
        "call_seq": 0,
        "provider": "mock",
        "route_id": "route-a",
        "capability": "amount.parse",
    }
    fields.update(changes)
    return CallIntent(**fields)


def result(**changes: Any) -> CallResult:
    fields: dict[str, Any] = {
        "attempt_id": ATTEMPT,
        "call_seq": 0,
        "outcome": CallOutcome.ANSWERED,
    }
    fields.update(changes)
    return CallResult(**fields)


def closure(**changes: Any) -> UsageClosure:
    fields: dict[str, Any] = {
        "attempt_id": ATTEMPT,
        "capture": CaptureState.COMPLETE,
        "closed_by": ClosedBy.WORKER,
        "capture_tier": CaptureTier.T1,
        "call_count": 0,
    }
    fields.update(changes)
    return UsageClosure(**fields)


def accepts(build: Any, **fields: Any) -> bool:
    try:
        build(**fields)
    except ValueError:
        return False
    return True


def routing_accepts(capability: str) -> bool:
    try:
        choose_route([], capability=capability)
    except ValueError:
        return False
    return True


# --- the text rule ----------------------------------------------------------------------


@pytest.mark.parametrize("capability", ["  x  ", "é", "a b", "x\ty", "x" * 256])
def test_capabilities_routing_accepts_are_recorded_exactly(capability: str) -> None:
    assert routing_accepts(capability)
    assert intent(capability=capability).capability == capability


@pytest.mark.parametrize("capability", ["", "\t", WIDE_SPACE, " \n "])
def test_capabilities_routing_refuses_are_refused(capability: str) -> None:
    assert not routing_accepts(capability)
    assert not accepts(intent, capability=capability)


@pytest.mark.parametrize(
    "capability", ["x" * 257, "a\x00b", "\ud800"], ids=["too-long", "nul", "lone-surrogate"]
)
def test_only_the_storage_bounds_refuse_what_routing_accepts(capability: str) -> None:
    assert routing_accepts(capability)
    assert not accepts(intent, capability=capability)


def test_a_capability_must_be_a_str() -> None:
    with pytest.raises(TypeError, match="capability must be a str"):
        intent(capability=b"x")


def amount_result(unit: str) -> CallResult:
    return result(reported_amount=Decimal("1"), reported_unit=unit)


TEXT_FIELDS: list[tuple[str, int, Any]] = [
    ("capability", 256, lambda value: intent(capability=value)),
    ("model", 128, lambda value: intent(model=value)),
    ("reported_unit", 64, amount_result),
    ("model_served", 128, lambda value: result(model_served=value)),
    ("provider_generation_id", 128, lambda value: result(provider_generation_id=value)),
]


@pytest.mark.parametrize(
    ("field", "limit", "build"), TEXT_FIELDS, ids=[name for name, _, _ in TEXT_FIELDS]
)
def test_every_free_text_field_follows_the_text_rule(field: str, limit: int, build: Any) -> None:
    for good in [MICRO_CREDITS, "  padded  ", "x" * limit]:
        assert getattr(build(good), field) == good
    for bad in ["", "   ", "\t\n", WIDE_SPACE, "x" * (limit + 1), "a\x00b", "\ud800"]:
        with pytest.raises(ValueError, match=field):
            build(bad)
    with pytest.raises(TypeError, match=field):
        build(42)


def test_text_errors_never_echo_the_value() -> None:
    secret = "s3cret-label\x00"
    with pytest.raises(ValueError, match="reported_unit") as info:
        amount_result(secret)
    assert "s3cret" not in str(info.value)


# --- identifiers and enums --------------------------------------------------------------


IDENTIFIERS = ["provider", "route_id", "prompt_version"]


@pytest.mark.parametrize("field", IDENTIFIERS)
@pytest.mark.parametrize("value", ["a", "a.b_c-1", "x" * 64, "Z9"])
def test_identifiers_accept_route_id_shapes(field: str, value: str) -> None:
    assert getattr(intent(**{field: value}), field) == value


@pytest.mark.parametrize("field", IDENTIFIERS)
@pytest.mark.parametrize(
    "value", ["", "a b", "x" * 65, "é", "a\n", "a/b", "\N{ARABIC-INDIC DIGIT ONE}"]
)
def test_identifiers_refuse_anything_else(field: str, value: str) -> None:
    with pytest.raises(ValueError, match=field):
        intent(**{field: value})


@pytest.mark.parametrize("value", ["", "a b", "x" * 65, "é", "a\n", "a.b_c-1", "x" * 64])
def test_identifiers_match_the_routing_rule(value: str) -> None:
    try:
        RouteOption(value, frozenset(), None)
    except ValueError:
        routing = False
    else:
        routing = True
    assert accepts(intent, route_id=value) == routing


def test_an_identifier_must_be_a_str() -> None:
    with pytest.raises(TypeError, match="provider must be a str"):
        intent(provider=1)


def test_call_failures_are_exactly_the_provider_error_codes() -> None:
    codes = {code.value for code in TransientCode} | {code.value for code in PermanentCode}
    assert {failure.value for failure in CallFailure} == codes
    assert len(CallFailure) == len(TransientCode) + len(PermanentCode)


@pytest.mark.parametrize(
    ("build", "field"),
    [
        (lambda: intent(field_kind="total"), "field_kind"),
        (lambda: result(outcome="answered"), "outcome"),
        (lambda: result(outcome=CallOutcome.FAILED, failure_code="timeout"), "failure_code"),
        (lambda: closure(capture="complete"), "capture"),
        (lambda: closure(closed_by="worker"), "closed_by"),
        (lambda: closure(capture_tier="t1"), "capture_tier"),
    ],
)
def test_enumerated_fields_require_their_enum(build: Any, field: str) -> None:
    with pytest.raises(TypeError, match=field):
        build()


def test_every_field_kind_is_accepted() -> None:
    assert [intent(field_kind=kind).field_kind for kind in FieldKind] == list(FieldKind)


@pytest.mark.parametrize("build", [intent, result, closure])
def test_attempt_id_must_be_a_uuid(build: Any) -> None:
    with pytest.raises(TypeError, match="attempt_id"):
        build(attempt_id=str(ATTEMPT))


# --- integer bounds ---------------------------------------------------------------------


INTEGERS: list[tuple[Any, str, int]] = [
    (intent, "call_seq", INTEGER_MAX),
    (intent, "item_index", INTEGER_MAX),
    (intent, "estimated_cost_units", BIGINT_MAX),
    (result, "call_seq", INTEGER_MAX),
    (result, "input_tokens", BIGINT_MAX),
    (result, "output_tokens", BIGINT_MAX),
    (result, "latency_ms", INTEGER_MAX),
    (closure, "call_count", INTEGER_MAX),
]


@pytest.mark.parametrize(
    ("build", "field", "maximum"), INTEGERS, ids=[f"{b.__name__}.{f}" for b, f, _ in INTEGERS]
)
def test_integers_are_bounded_by_their_column(build: Any, field: str, maximum: int) -> None:
    assert getattr(build(**{field: 0}), field) == 0
    assert getattr(build(**{field: maximum}), field) == maximum
    for bad in [-1, maximum + 1]:
        with pytest.raises(ValueError, match=field):
            build(**{field: bad})
    for wrong in [True, False, 1.0, "1", Decimal(1)]:
        with pytest.raises(TypeError, match=field):
            build(**{field: wrong})


def test_the_bounds_are_postgresql_integer_and_bigint() -> None:
    assert INTEGER_MAX == 2147483647
    assert BIGINT_MAX == 9223372036854775807


# --- results ----------------------------------------------------------------------------


def test_a_failure_code_is_given_exactly_when_the_call_failed() -> None:
    assert result(outcome=CallOutcome.FAILED, failure_code=CallFailure.TIMEOUT).failure_code
    with pytest.raises(ValueError, match="failure_code"):
        result(outcome=CallOutcome.FAILED)
    with pytest.raises(ValueError, match="failure_code"):
        result(outcome=CallOutcome.ANSWERED, failure_code=CallFailure.REJECTED)


def test_a_failed_call_may_report_usage() -> None:
    failed = result(
        outcome=CallOutcome.FAILED,
        failure_code=CallFailure.RATE_LIMITED,
        reported_amount=Decimal("0.25"),
        reported_unit="credits",
    )
    assert failed.reported_amount == Decimal("0.25")


def test_an_amount_and_its_unit_come_together() -> None:
    with pytest.raises(ValueError, match="together"):
        result(reported_amount=Decimal("1"))
    with pytest.raises(ValueError, match="together"):
        result(reported_unit="credits")


# --- amounts ----------------------------------------------------------------------------


def with_amount(amount: Any) -> CallResult:
    return result(reported_amount=amount, reported_unit="credits")


@pytest.mark.parametrize("amount", [1.5, 1, True, "1.5", 0.0])
def test_an_amount_must_be_a_decimal(amount: Any) -> None:
    with pytest.raises(TypeError, match="reported_amount must be a Decimal"):
        with_amount(amount)


@pytest.mark.parametrize(
    "amount",
    [
        Decimal("-0"),
        Decimal("-0.00"),
        Decimal("-1"),
        Decimal("NaN"),
        Decimal("sNaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("0.0000000000000000001"),  # 19 decimal places
        Decimal("1E-19"),
        Decimal("1E+20"),
        Decimal("100000000000000000000"),
        Decimal("100000000000000000000.0"),
    ],
    ids=str,
)
def test_an_amount_must_be_a_finite_unsigned_numeric_value(amount: Decimal) -> None:
    with pytest.raises(ValueError, match="reported_amount"):
        with_amount(amount)


@pytest.mark.parametrize(
    "amount",
    [
        Decimal("0"),
        Decimal("0.000000000000000000"),
        Decimal("1E-18"),
        Decimal("1E+2"),
        Decimal("1.50"),
        Decimal("99999999999999999999.999999999999999999"),
    ],
    ids=str,
)
def test_an_amount_is_kept_exactly(amount: Decimal) -> None:
    kept = with_amount(amount).reported_amount
    assert kept is amount
    assert AMOUNT_LIMIT == Decimal(10) ** 20


def test_an_amounts_identity_is_its_value_and_scale() -> None:
    assert amount_identity(Decimal("1E+2")) == amount_identity(Decimal("100"))
    assert amount_identity(Decimal("100")) == (Decimal("100"), 0)
    assert amount_identity(Decimal("1.5")) != amount_identity(Decimal("1.50"))
    assert amount_identity(Decimal("1.50")) == (Decimal("1.5"), 2)
    assert amount_identity(Decimal("1.5E-3")) == (Decimal("0.0015"), 4)
    assert amount_identity(Decimal("1.50E+1")) == (Decimal("15.0"), 1)
    with pytest.raises(ValueError, match="no scale"):
        amount_identity(Decimal("NaN"))


# --- closures ---------------------------------------------------------------------------


@pytest.mark.parametrize("tier", [CaptureTier.T1, CaptureTier.T2])
def test_recovery_cannot_close_complete_below_t3(tier: CaptureTier) -> None:
    with pytest.raises(ValueError, match="below tier T3"):
        closure(closed_by=ClosedBy.RECOVERY, capture_tier=tier)
    incomplete = closure(
        closed_by=ClosedBy.RECOVERY,
        capture_tier=tier,
        capture=CaptureState.INCOMPLETE,
        call_count=None,
    )
    assert incomplete.capture is CaptureState.INCOMPLETE


def test_recovery_may_close_complete_at_t3_and_a_worker_at_any_tier() -> None:
    assert closure(closed_by=ClosedBy.RECOVERY, capture_tier=CaptureTier.T3, call_count=2)
    for tier in CaptureTier:
        assert closure(capture_tier=tier).capture_tier is tier


def test_a_call_count_is_given_exactly_for_a_complete_capture() -> None:
    with pytest.raises(ValueError, match="must give call_count"):
        closure(call_count=None)
    with pytest.raises(ValueError, match="only a complete capture"):
        closure(capture=CaptureState.INCOMPLETE, call_count=0)


# --- stored records ---------------------------------------------------------------------


@pytest.mark.parametrize("state", list(AttemptState))
def test_a_record_is_late_exactly_when_its_attempt_was_not_running(state: AttemptState) -> None:
    call = StoredCall(intent(), uuid.uuid4(), uuid.uuid4(), state, NOW)
    stored = StoredResult(result(), state, NOW)
    assert call.late == stored.late == (state is not AttemptState.RUNNING)


def test_records_are_frozen() -> None:
    record = intent()
    with pytest.raises(AttributeError):
        record.capability = "other"  # type: ignore[misc]


# --- the write surface ------------------------------------------------------------------


def sql_statements() -> dict[str, str]:
    """The module's SQL: every module-level str constant named in upper case."""
    return {
        name: value
        for name, value in vars(provider_usage).items()
        if isinstance(value, str) and name.isupper()
    }


def test_the_module_sql_never_deletes_truncates_or_updates() -> None:
    statements = sql_statements()
    starts = [sql.lstrip() for sql in statements.values()]
    assert any(sql.startswith("INSERT INTO provider_calls") for sql in starts)
    assert any(sql.startswith("SELECT") for sql in starts)
    forbidden = re.compile(r"\b(DELETE|TRUNCATE|UPDATE)\b", re.IGNORECASE)
    assert {name: sql for name, sql in statements.items() if forbidden.search(sql)} == {}


def test_no_public_name_deletes_or_truncates() -> None:
    public = [name for name in dir(provider_usage) if not name.startswith("_")]
    assert "record_call_intent" in public
    assert [name for name in public if re.search("delete|truncate", name, re.IGNORECASE)] == []
