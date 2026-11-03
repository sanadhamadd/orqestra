"""The synthetic invoice schema and its strict normalization. Pure, no I/O.

``normalize_invoice`` turns one JSON value into an ``Invoice`` or a ``Malformed``
result. The schema is closed: an object with exactly ``invoice_id``, ``lines``
(a non-empty list of exactly ``line_id``, ``quantity`` and ``unit_amount``) and
``total``. There is no tax, currency, rounding, date or vendor rule.

- An identifier (``invoice_id``, ``line_id``) is 1 to 64 characters of
  ``[A-Za-z0-9._-]``; ``line_id`` values are unique within an invoice.
- An amount (``quantity``, ``unit_amount``, ``total``) is an opaque
  non-negative integer in unspecified minor units, below ``10**64``, given as
  one of:

  - an ``int`` (never a ``bool``);
  - a canonical string, ``[0-9]{1,64}`` in ASCII, parsed to an ``int``;
  - an *ambiguous raw string*: any other string of 1 to 64 printable
    characters that is not blank. It is kept as ``Raw`` and is the only thing a
    provider is ever asked to interpret.

- Everything else (a float, ``bool``, ``null``, a negative or out-of-range
  number, an empty, blank, over-long or control-character string, an unknown or
  missing key, a wrong type) makes the whole invoice ``Malformed``.

Issues carry a code and a positional path such as ``lines[2].unit_amount``;
never a value, an identifier or a key name from the input. The 64-character
bounds are structural limits of this grammar, not pricing or production limits.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from orqestra.ai.provider import FieldKind

AMOUNT_DIGITS = 64
MAX_AMOUNT = 10**AMOUNT_DIGITS - 1

_IDENTIFIER = re.compile(r"[A-Za-z0-9._-]{1,64}", re.ASCII)
_CANONICAL = re.compile(r"[0-9]{1,64}", re.ASCII)

_INVOICE_KEYS = frozenset({"invoice_id", "lines", "total"})
_LINE_KEYS = frozenset({"line_id", "quantity", "unit_amount"})


class IssueCode(StrEnum):
    """Why an invoice is malformed. Values are stable identifiers."""

    NOT_AN_OBJECT = "not_an_object"
    MISSING_FIELD = "missing_field"
    UNKNOWN_FIELD = "unknown_field"
    WRONG_TYPE = "wrong_type"
    INVALID_IDENTIFIER = "invalid_identifier"
    DUPLICATE_LINE_ID = "duplicate_line_id"
    INVALID_AMOUNT = "invalid_amount"
    EMPTY_LINES = "empty_lines"


@dataclass(frozen=True, slots=True)
class Issue:
    """One problem: a code and the positional path of the field, never a value."""

    code: IssueCode
    path: str


@dataclass(frozen=True, slots=True)
class Known:
    """An amount that is a definite non-negative integer."""

    value: int


@dataclass(frozen=True, slots=True)
class Raw:
    """An ambiguous amount: the original string, still to be interpreted."""

    text: str


type Amount = Known | Raw


@dataclass(frozen=True, slots=True)
class Line:
    line_id: str
    quantity: Amount
    unit_amount: Amount


@dataclass(frozen=True, slots=True)
class Invoice:
    invoice_id: str
    lines: tuple[Line, ...]
    total: Amount


@dataclass(frozen=True, slots=True)
class Malformed:
    """An invoice that does not fit the schema, with every issue found, in schema order."""

    issues: tuple[Issue, ...]


type Normalized = Invoice | Malformed


@dataclass(frozen=True, slots=True)
class AmbiguousField:
    """A ``Raw`` amount of an invoice: where it is, what kind it is, and its text."""

    path: str
    kind: FieldKind
    raw: str


def parse_canonical(text: object) -> int | None:
    """The integer a canonical digit string denotes, or ``None`` for anything else.

    This is the only place a string becomes an integer, and it never calls
    ``int`` on a string it has not matched in full against ``[0-9]{1,64}``.
    """
    if type(text) is not str or _CANONICAL.fullmatch(text) is None:
        return None
    return int(text)


def normalize_invoice(raw: object) -> Normalized:
    """The invoice ``raw`` denotes, or every issue that makes it malformed."""
    if not isinstance(raw, dict):
        return Malformed((Issue(IssueCode.NOT_AN_OBJECT, "$"),))
    issues: list[Issue] = []
    _check_keys(raw, _INVOICE_KEYS, "$", issues)
    invoice_id = _identifier(raw.get("invoice_id"), "invoice_id", "invoice_id" in raw, issues)
    lines = _lines(raw.get("lines"), "lines" in raw, issues)
    total = _amount(raw.get("total"), "total", "total" in raw, issues)
    if issues or invoice_id is None or lines is None or total is None:
        return Malformed(tuple(issues))
    return Invoice(invoice_id, lines, total)


def ambiguous_fields(invoice: Invoice) -> tuple[AmbiguousField, ...]:
    """The invoice's ``Raw`` amounts in document order.

    Each line's quantity then unit amount, line by line, and the total last.
    """
    found: list[AmbiguousField] = []
    for index, line in enumerate(invoice.lines):
        if isinstance(line.quantity, Raw):
            found.append(
                AmbiguousField(f"lines[{index}].quantity", FieldKind.QUANTITY, line.quantity.text)
            )
        if isinstance(line.unit_amount, Raw):
            found.append(
                AmbiguousField(
                    f"lines[{index}].unit_amount", FieldKind.UNIT_AMOUNT, line.unit_amount.text
                )
            )
    if isinstance(invoice.total, Raw):
        found.append(AmbiguousField("total", FieldKind.TOTAL, invoice.total.text))
    return tuple(found)


def substitute(invoice: Invoice, values: Mapping[str, int]) -> Invoice:
    """The invoice with each ``Raw`` amount whose path is in ``values`` replaced by ``Known``."""

    def replaced(amount: Amount, path: str) -> Amount:
        if isinstance(amount, Raw) and path in values:
            return Known(values[path])
        return amount

    lines = tuple(
        Line(
            line.line_id,
            replaced(line.quantity, f"lines[{index}].quantity"),
            replaced(line.unit_amount, f"lines[{index}].unit_amount"),
        )
        for index, line in enumerate(invoice.lines)
    )
    return Invoice(invoice.invoice_id, lines, replaced(invoice.total, "total"))


def _check_keys(
    obj: Mapping[object, object], allowed: frozenset[str], path: str, issues: list[Issue]
) -> None:
    """Report unknown keys at the container's path; a key's text is never reported."""
    if any(key not in allowed for key in obj):
        issues.append(Issue(IssueCode.UNKNOWN_FIELD, path))


def _identifier(value: object, path: str, present: bool, issues: list[Issue]) -> str | None:
    if not present:
        issues.append(Issue(IssueCode.MISSING_FIELD, path))
        return None
    if not isinstance(value, str):
        issues.append(Issue(IssueCode.WRONG_TYPE, path))
        return None
    if _IDENTIFIER.fullmatch(value) is None:
        issues.append(Issue(IssueCode.INVALID_IDENTIFIER, path))
        return None
    return value


def _lines(value: object, present: bool, issues: list[Issue]) -> tuple[Line, ...] | None:
    if not present:
        issues.append(Issue(IssueCode.MISSING_FIELD, "lines"))
        return None
    if not isinstance(value, list):
        issues.append(Issue(IssueCode.WRONG_TYPE, "lines"))
        return None
    if not value:
        issues.append(Issue(IssueCode.EMPTY_LINES, "lines"))
        return None
    lines: list[Line | None] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        line = _line(item, f"lines[{index}]", seen, issues)
        lines.append(line)
    if any(line is None for line in lines):
        return None
    return tuple(line for line in lines if line is not None)


def _line(item: object, path: str, seen: set[str], issues: list[Issue]) -> Line | None:
    if not isinstance(item, dict):
        issues.append(Issue(IssueCode.NOT_AN_OBJECT, path))
        return None
    _check_keys(item, _LINE_KEYS, path, issues)
    line_id = _identifier(item.get("line_id"), f"{path}.line_id", "line_id" in item, issues)
    if line_id is not None:
        if line_id in seen:
            issues.append(Issue(IssueCode.DUPLICATE_LINE_ID, f"{path}.line_id"))
            line_id = None
        else:
            seen.add(line_id)
    quantity = _amount(item.get("quantity"), f"{path}.quantity", "quantity" in item, issues)
    unit_amount = _amount(
        item.get("unit_amount"), f"{path}.unit_amount", "unit_amount" in item, issues
    )
    if line_id is None or quantity is None or unit_amount is None:
        return None
    return Line(line_id, quantity, unit_amount)


def _amount(value: object, path: str, present: bool, issues: list[Issue]) -> Amount | None:
    if not present:
        issues.append(Issue(IssueCode.MISSING_FIELD, path))
        return None
    if isinstance(value, bool) or not isinstance(value, int | str):
        issues.append(Issue(IssueCode.WRONG_TYPE, path))
        return None
    if isinstance(value, int):
        if 0 <= value <= MAX_AMOUNT:
            return Known(value)
        issues.append(Issue(IssueCode.INVALID_AMOUNT, path))
        return None
    parsed = parse_canonical(value)
    if parsed is not None:
        return Known(parsed)
    if 1 <= len(value) <= AMOUNT_DIGITS and value.isprintable() and value.strip():
        return Raw(value)
    issues.append(Issue(IssueCode.INVALID_AMOUNT, path))
    return None
