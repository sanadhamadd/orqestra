"""The benchmark provider double: its answers, its usage and its import boundary."""

import ast
from datetime import timedelta
from pathlib import Path

import pytest

import benchmarks.provider
from benchmarks.provider import USAGE_UNIT, SeparatorReader
from orqestra.calls.provider import FieldKind, ProviderUsage, make_request
from orqestra.workloads.invoice.model import parse_canonical

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

# The only imports the module may make: the provider contract, nothing else.
ALLOWED_IMPORTS = frozenset({"orqestra.calls.provider"})


def ask(raw: str) -> tuple[str, ProviderUsage | None]:
    request = make_request(
        capability="interpret_amount",
        route_id="route-a",
        field_kind=FieldKind.TOTAL,
        raw=raw,
        timeout=timedelta(seconds=1),
    )
    response = SeparatorReader().interpret(request)
    return response.value, response.usage


@pytest.mark.parametrize("raw", ["1,234", "1_234", "1 234", "12,345,678", "1 2_3,4"])
def test_grouping_separators_are_removed(raw: str) -> None:
    value, _ = ask(raw)
    assert value == raw.replace(",", "").replace("_", "").replace(" ", "")
    assert parse_canonical(value) is not None


@pytest.mark.parametrize("raw", ["1.234", "12a", "1,2-3"])
def test_other_characters_stay_and_make_the_reply_invalid(raw: str) -> None:
    value, _ = ask(raw)
    assert parse_canonical(value) is None


def test_each_answer_reports_the_raw_length_in_tokens() -> None:
    _, usage = ask("12,345,678")
    assert usage == ProviderUsage("10", USAGE_UNIT)
    assert USAGE_UNIT == "tokens"


def test_answers_are_deterministic() -> None:
    assert ask("9_999") == ask("9_999")


def test_the_module_imports_only_the_provider_contract() -> None:
    tree = ast.parse(Path(benchmarks.provider.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "relative imports are not allowed"
            imported.add(node.module or "")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"__import__", "eval", "exec", "compile"}
    assert imported == ALLOWED_IMPORTS
