"""The providers package's import boundary, that nothing uses it yet, and its mirrored limits.

An AST scan holds every module of ``orqestra.providers`` to an allow-list: the
standard library modules below, ``orqestra.ai.provider``, ``orqestra.domain`` and the
package itself. Only ``stdlib_transport`` may import the network stack. No other
``src`` module imports the package, so the adapter is unwired. The adapter copies
the usage ledger's limits instead of importing persistence; a test keeps them equal.
"""

import ast
import json
import subprocess
import sys
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import SecretStr

import orqestra
import orqestra.providers
from orqestra.ai import routing
from orqestra.ai.provider import FieldKind, make_request
from orqestra.persistence import provider_usage
from orqestra.persistence.provider_usage import CallIntent, CallOutcome, CallResult
from orqestra.providers import openrouter, stdlib_transport
from orqestra.providers.openrouter import OpenRouterConfig, OpenRouterProvider
from orqestra.providers.transport import Deadline

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

STDLIB = frozenset(
    {
        "collections",
        "contextlib",
        "dataclasses",
        "datetime",
        "decimal",
        "enum",
        "json",
        "re",
        "time",
        "types",
        "typing",
    }
)
NETWORK = frozenset({"http", "ssl", "socket"})
NETWORK_MODULE = "stdlib_transport.py"
ORQESTRA = ("orqestra.ai.provider", "orqestra.domain", "orqestra.providers")
FORBIDDEN_CALLS = frozenset({"__import__", "eval", "exec", "compile"})


def allowed_orqestra(module: str) -> bool:
    return any(module == root or module.startswith(f"{root}.") for root in ORQESTRA)


def violations(source: str, *, network: bool) -> list[str]:
    """What in ``source`` breaks the providers import rules."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = ["." * node.level + (node.module or "")]
        else:
            modules = []
            if isinstance(node, ast.Name) and node.id in FORBIDDEN_CALLS:
                found.append(f"use of {node.id}")
        for module in modules:
            top = module.split(".")[0]
            if top in NETWORK:
                if not network:
                    found.append(f"network import {module}")
            elif top == "orqestra":
                if not allowed_orqestra(module):
                    found.append(f"import {module} is not allowed")
            elif top not in STDLIB:
                found.append(f"import {module} is not allowed")
    return found


def provider_modules() -> list[Path]:
    root = Path(orqestra.providers.__file__ or "").parent
    return sorted(root.rglob("*.py"))


def test_the_scan_finds_every_module() -> None:
    names = {path.name for path in provider_modules()}
    assert names == {"__init__.py", "transport.py", "stdlib_transport.py", "openrouter.py"}


def test_every_module_keeps_to_the_allow_list() -> None:
    for path in provider_modules():
        source = path.read_text(encoding="utf-8")
        assert violations(source, network=path.name == NETWORK_MODULE) == [], path.name


@pytest.mark.parametrize(
    ("source", "network", "expected"),
    [
        ("import http.client", False, ["network import http.client"]),
        ("import ssl", False, ["network import ssl"]),
        ("from socket import create_connection", False, ["network import socket"]),
        ("import http.client\nimport ssl", True, []),
        ("import urllib.request", True, ["import urllib.request is not allowed"]),
        ("import pydantic", False, ["import pydantic is not allowed"]),
        ("from orqestra.config import Settings", False, ["import orqestra.config is not allowed"]),
        ("import orqestra.persistence", False, ["import orqestra.persistence is not allowed"]),
        ("from orqestra.worker import runtime", False, ["import orqestra.worker is not allowed"]),
        ("import orqestra.workloads", False, ["import orqestra.workloads is not allowed"]),
        ("import orqestra.aggregation", False, ["import orqestra.aggregation is not allowed"]),
        ("from orqestra import cli", False, ["import orqestra is not allowed"]),
        ("import orqestra.ai.routing", False, ["import orqestra.ai.routing is not allowed"]),
        ("from orqestra.ai.provider import x", False, []),
        ("from orqestra.domain.errors import x", False, []),
        ("from orqestra.providers.transport import x", False, []),
        ("import orqestra.providersx", False, ["import orqestra.providersx is not allowed"]),
        ("import importlib", False, ["import importlib is not allowed"]),
        ("import subprocess", False, ["import subprocess is not allowed"]),
        ("from . import sibling", False, ["import . is not allowed"]),
        ("x = __import__('os')", False, ["use of __import__"]),
        ("eval('1')", False, ["use of eval"]),
    ],
)
def test_the_scan_catches_what_it_is_meant_to_catch(
    source: str, network: bool, expected: list[str]
) -> None:
    assert violations(source, network=network) == expected


def imports_providers(source: str) -> bool:
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            names = [module, *(f"{module}.{alias.name}" for alias in node.names)]
        else:
            continue
        if any(name.split(".")[:2] == ["orqestra", "providers"] for name in names):
            return True
    return False


def test_no_module_outside_the_package_imports_it() -> None:
    src = Path(orqestra.__file__ or "").parent
    providers = Path(orqestra.providers.__file__ or "").parent
    users = [
        path.relative_to(src).as_posix()
        for path in sorted(src.rglob("*.py"))
        if providers not in path.parents and imports_providers(path.read_text(encoding="utf-8"))
    ]
    assert users == []


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import orqestra.providers.openrouter", True),
        ("from orqestra.providers import openrouter", True),
        ("from orqestra import providers", True),
        ("from orqestra.ai import provider", False),
        ("import orqestra.persistence", False),
    ],
)
def test_the_unwired_check_sees_every_form_of_import(source: str, expected: bool) -> None:
    assert imports_providers(source) is expected


def test_the_adapter_loads_no_network_module() -> None:
    # A fresh interpreter without site hooks, so only this import decides what loads.
    root = Path(orqestra.__file__ or "").parent.parent
    probe = (
        "import sys\n"
        f"sys.path.insert(0, {str(root)!r})\n"
        "import orqestra.providers.openrouter\n"
        "print(sorted(m for m in ('http.client', 'ssl') if m in sys.modules))\n"
    )
    command = [sys.executable, "-I", "-S", "-c", probe]
    result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=60)
    assert result.stdout.strip() == "[]"


# --- mirrored constants -----------------------------------------------------------------


def test_the_mirrored_limits_equal_the_ledgers() -> None:
    assert openrouter.AMOUNT_MAX_SCALE == provider_usage.AMOUNT_MAX_SCALE
    assert openrouter.AMOUNT_LIMIT == provider_usage.AMOUNT_LIMIT
    assert str(openrouter.AMOUNT_LIMIT) == str(provider_usage.AMOUNT_LIMIT)
    assert openrouter.TOKEN_MAX == provider_usage.BIGINT_MAX
    assert openrouter.ID_MAX_LENGTH == provider_usage.GENERATION_ID_MAX_LENGTH
    assert openrouter.MODEL_MAX_LENGTH == provider_usage.MODEL_MAX_LENGTH


def test_the_mirrored_rules_equal_their_sources() -> None:
    assert routing._ROUTE_ID.pattern == openrouter.ROUTE_ID_PATTERN
    assert openrouter.CHAT_COMPLETIONS_PATH == stdlib_transport.CHAT_COMPLETIONS_PATH
    assert provider_usage._IDENTIFIER.fullmatch(openrouter.PROMPT_VERSION) is not None


class FakeExchange:
    def __init__(self, body: bytes) -> None:
        self.status = 200
        self._body = [body]

    def read(self, max_bytes: int, deadline: Deadline) -> bytes:
        return self._body.pop(0) if self._body else b""

    def close(self) -> None:
        pass


class FakeTransport:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def post(self, *, path: str, headers: object, body: bytes, deadline: Deadline) -> FakeExchange:
        return FakeExchange(self.body)


def test_an_answer_at_every_limit_can_be_recorded_in_the_ledger() -> None:
    top = 2**63 - 1
    cost = "99999999999999999999.999999999999999999"
    document = {
        "id": "g" * 128,
        "model": "m" * 128,
        "choices": [{"finish_reason": "stop", "message": {"content": "12"}}],
        "usage": {"prompt_tokens": top, "completion_tokens": top, "cost": "__COST__"},
    }
    body = json.dumps(document).replace('"__COST__"', cost).encode()
    config = OpenRouterConfig(credential=SecretStr("sk-test"), route_models={"route-a": "m"})
    provider = OpenRouterProvider(config, FakeTransport(body))
    request = make_request(
        capability="interpret_amount",
        route_id="route-a",
        field_kind=FieldKind.TOTAL,
        raw="twelve",
        timeout=timedelta(seconds=10),
    )
    report = provider.call(request)
    usage = report.response.usage
    assert usage is not None
    attempt = uuid.UUID(int=7)
    CallIntent(
        attempt_id=attempt,
        call_seq=0,
        provider="openrouter",
        route_id=request.route_id,
        capability=request.capability,
        prompt_version=openrouter.PROMPT_VERSION,
        model="m",
        field_kind=request.field_kind,
    )
    result = CallResult(
        attempt_id=attempt,
        call_seq=0,
        outcome=CallOutcome.ANSWERED,
        reported_amount=Decimal(usage.amount),
        reported_unit=usage.unit,
        model_served=report.model_served,
        provider_generation_id=report.generation_id,
        input_tokens=report.input_tokens,
        output_tokens=report.output_tokens,
        latency_ms=report.latency_ms,
    )
    assert result.reported_amount == Decimal(cost)
    assert provider_usage.amount_identity(Decimal(usage.amount)) == (Decimal(cost), 18)
