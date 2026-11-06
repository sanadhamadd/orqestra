"""The provider contract: what a provider is asked, what it may answer, how it fails.

A ``ProviderRequest`` is everything a provider ever sees. It carries one bounded
raw string and the kind of field it came from, never an invoice id, line id or
path. ``request_id`` is a pure function of the capability, the route, the field
kind and the raw string, so the same question always has the same id and a
provider may answer a repeat from a cache. Collisions are intended: the answer
is a function of exactly what the provider sees.

Failures carry a code from a closed set and nothing else, so no provider-supplied
text can reach a stored ``error_message`` (``AttemptFailed.from_exception`` stores
``str(exc)``). A provider error is a ``TransientError`` or a ``PermanentError``
of the domain's taxonomy, so the engine's retry rules apply unchanged (DEC-009).
"""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Protocol

from orqestra.domain.errors import PermanentError, TransientError


class FieldKind(StrEnum):
    """Which kind of amount a raw string came from. Values are stable identifiers."""

    QUANTITY = "quantity"
    UNIT_AMOUNT = "unit_amount"
    TOTAL = "total"


class TransientCode(StrEnum):
    """Why a provider call failed in a way a later attempt may not repeat."""

    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    UNAVAILABLE = "unavailable"


class PermanentCode(StrEnum):
    """Why a provider call failed in a way no retry can fix."""

    REJECTED = "rejected"
    UNSUPPORTED_REQUEST = "unsupported_request"


def _checked_code[C: StrEnum](code: object, expected: type[C]) -> C:
    if not isinstance(code, expected):
        msg = f"a provider error code must be a {expected.__name__}, got {type(code).__name__}"
        raise TypeError(msg)
    return code


class ProviderTransientError(TransientError):
    """The provider could not answer now; ``str(exc)`` is the code and nothing else."""

    def __init__(self, code: TransientCode) -> None:
        checked = _checked_code(code, TransientCode)
        super().__init__(checked.value)
        self.code = checked


class ProviderPermanentError(PermanentError):
    """The provider refused the request for good; ``str(exc)`` is the code and nothing else."""

    def __init__(self, code: PermanentCode) -> None:
        checked = _checked_code(code, PermanentCode)
        super().__init__(checked.value)
        self.code = checked


def request_id_for(capability: str, route_id: str, field_kind: FieldKind, raw: str) -> str:
    """The SHA-256 (hex) of the canonical JSON of the four values that define a request."""
    canonical = json.dumps(
        {
            "capability": capability,
            "field_kind": field_kind.value,
            "raw": raw,
            "route_id": route_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class ProviderRequest:
    """One question for a provider: interpret ``raw``, an amount of kind ``field_kind``.

    ``timeout`` is the bound the adapter must enforce, by raising
    ``ProviderTransientError(TransientCode.TIMEOUT)``.
    """

    request_id: str
    route_id: str
    capability: str
    field_kind: FieldKind
    raw: str
    timeout: timedelta


def make_request(
    *, capability: str, route_id: str, field_kind: FieldKind, raw: str, timeout: timedelta
) -> ProviderRequest:
    """A request whose ``request_id`` is derived from its content."""
    return ProviderRequest(
        request_id=request_id_for(capability, route_id, field_kind, raw),
        route_id=route_id,
        capability=capability,
        field_kind=field_kind,
        raw=raw,
        timeout=timeout,
    )


# JSON number syntax without a sign or an exponent: no redundant leading zeros,
# and a fraction, if present, has at least one digit on each side of the point.
_USAGE_AMOUNT = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", re.ASCII)
_USAGE_UNIT = re.compile(r"[A-Za-z0-9._-]{1,64}", re.ASCII)
_MAX_AMOUNT_LENGTH = 64


@dataclass(frozen=True, slots=True)
class ProviderUsage:
    """What a provider says one call used: an amount in the provider's own unit.

    ``amount`` is a non-negative decimal, as text exactly as the provider stated
    it: no sign, no exponent, no redundant leading zeros, at most 64 characters.
    ``unit`` is the provider-declared label, kept verbatim and opaque: 1 to 64
    ASCII letters, digits, ``.``, ``_`` or ``-``.

    Nothing is converted. Neither field implies a currency or an actual charge.
    A wrong type raises ``TypeError`` and a bad value ``ValueError``; neither
    message quotes the rejected value.
    """

    amount: str
    unit: str

    def __post_init__(self) -> None:
        _check_usage_amount(self.amount)
        _check_usage_unit(self.unit)


def _check_usage_amount(amount: object) -> None:
    if not isinstance(amount, str):
        msg = f"a usage amount must be a str, got {type(amount).__name__}"
        raise TypeError(msg)
    if len(amount) > _MAX_AMOUNT_LENGTH or _USAGE_AMOUNT.fullmatch(amount) is None:
        msg = (
            "a usage amount must be a non-negative decimal without sign or exponent, "
            f"at most {_MAX_AMOUNT_LENGTH} characters"
        )
        raise ValueError(msg)


def _check_usage_unit(unit: object) -> None:
    if not isinstance(unit, str):
        msg = f"a usage unit must be a str, got {type(unit).__name__}"
        raise TypeError(msg)
    if _USAGE_UNIT.fullmatch(unit) is None:
        msg = "a usage unit must be 1 to 64 ASCII letters, digits, '.', '_' or '-'"
        raise ValueError(msg)


def _check_usage(usage: object) -> None:
    if usage is not None and not isinstance(usage, ProviderUsage):
        msg = f"usage must be a ProviderUsage or None, got {type(usage).__name__}"
        raise TypeError(msg)


@dataclass(frozen=True, slots=True)
class ProviderResponse:
    """A provider's answer: ``value`` is its reading of ``raw`` as a digit string.

    ``reported_cost_units`` is whatever usage the provider says it incurred, in
    opaque units, or ``None``. It is never trusted to decide a status and never
    stored with a result.

    ``usage``, if given, is the provider's own statement of what the call used,
    as a ``ProviderUsage``. It is not read when verifying and never stored with
    a result. Only ``usage`` is checked here: ``value`` and
    ``reported_cost_units`` are judged by the caller, which treats a bad one as
    an invalid response.
    """

    value: str
    reported_cost_units: int | None = None
    usage: ProviderUsage | None = None

    def __post_init__(self) -> None:
        _check_usage(self.usage)


class Provider(Protocol):
    """Interprets one raw amount string. Implementations must be safe to call repeatedly."""

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        """Answer ``request`` or raise ``ProviderTransientError`` / ``ProviderPermanentError``."""
        ...
