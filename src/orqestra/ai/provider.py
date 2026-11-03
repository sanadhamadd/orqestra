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


@dataclass(frozen=True, slots=True)
class ProviderResponse:
    """A provider's answer: ``value`` is its reading of ``raw`` as a digit string.

    ``reported_cost_units`` is whatever usage the provider says it incurred, in
    opaque units, or ``None``. It is never trusted to decide a status and never
    stored with a result.
    """

    value: str
    reported_cost_units: int | None = None


class Provider(Protocol):
    """Interprets one raw amount string. Implementations must be safe to call repeatedly."""

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        """Answer ``request`` or raise ``ProviderTransientError`` / ``ProviderPermanentError``."""
        ...
