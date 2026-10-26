from typing import Any

import pytest

from orqestra.domain.kinds import check_job_kinds

pytestmark = pytest.mark.unit


def test_kinds_are_returned_distinct_and_sorted() -> None:
    assert check_job_kinds(["b", "a", "b"]) == ["a", "b"]
    assert check_job_kinds({"x"}) == ["x"]
    assert check_job_kinds({"k": 1}.keys()) == ["k"]  # any collection, e.g. a mapping's keys


@pytest.mark.parametrize(
    "kind",
    ["invoice", "Invoice", " x ", "\t", "\n", "\r\n", "\u00a0", "in%oice", "é"],
    ids=["plain", "case", "spaced", "tab", "newline", "crlf", "nbsp", "percent", "accent"],
)
def test_every_kind_the_database_accepts_is_accepted_unchanged(kind: str) -> None:
    # jobs_kind_not_blank is btrim(kind) <> '', and btrim trims spaces only.
    assert check_job_kinds([kind]) == [kind]


@pytest.mark.parametrize(
    ("kinds", "error", "message"),
    [
        ([], ValueError, "at least one"),
        ((), ValueError, "at least one"),
        (frozenset(), ValueError, "at least one"),
        ("invoice", TypeError, "collection of job kinds"),
        (b"invoice", TypeError, "collection of job kinds"),
        (None, TypeError, "collection of job kinds"),
        (42, TypeError, "collection of job kinds"),
        (iter(["invoice"]), TypeError, "collection of job kinds"),
        ([""], ValueError, "must not be blank"),
        ([" "], ValueError, "must not be blank"),
        (["   "], ValueError, "must not be blank"),
        (["ok", ""], ValueError, "must not be blank"),
        (["\x00"], ValueError, "NUL character"),
        (["in\x00voice"], ValueError, "NUL character"),
        ([None], TypeError, "must be a str"),
        ([b"invoice"], TypeError, "must be a str"),
        ([1], TypeError, "must be a str"),
    ],
    ids=[
        "empty-list",
        "empty-tuple",
        "empty-frozenset",
        "bare-str",
        "bytes",
        "none",
        "int",
        "iterator",
        "empty",
        "space",
        "spaces",
        "one-blank",
        "nul",
        "embedded-nul",
        "none-item",
        "bytes-item",
        "int-item",
    ],
)
def test_invalid_kinds_are_refused(kinds: Any, error: type[Exception], message: str) -> None:
    with pytest.raises(error, match=message):
        check_job_kinds(kinds)
