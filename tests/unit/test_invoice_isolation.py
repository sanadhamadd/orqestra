"""The new modules open no connection and are not wired into the worker.

Two layers keep the code offline. An AST scan of every new source module allows
only the imports listed below, which names no transport and no provider SDK, and
rejects dynamic imports and ``eval``/``exec``/``compile``. The scan sees direct
imports only: a transitive import of a transport is covered by the runtime guard,
the ``no_network`` fixture, which makes any connection or name lookup fail the
test. A meta-test below shows that the guard fires.

A second scan enforces the layers: ``ai/`` and ``workloads/`` never import storage or
aggregation, and ``ai/`` never imports a workload or the worker.
"""

import ast
import socket
from pathlib import Path

import pytest

import orqestra.ai
import orqestra.workloads
from orqestra import cli
from orqestra.config import Settings
from orqestra.worker import builtin
from orqestra.workloads.invoice.handler import JOB_KIND
from tests.unit.test_worker_runtime import SETTINGS

pytestmark = pytest.mark.unit

ALLOWED_TOP_LEVEL = frozenset(
    {
        "collections",
        "dataclasses",
        "datetime",
        "enum",
        "hashlib",
        "json",
        "orqestra",
        "re",
        "types",
        "typing",
        "uuid",
    }
)
# Named so that a failure reads clearly; the allow-list above already excludes them.
TRANSPORTS = frozenset(
    {
        "socket",
        "ssl",
        "http",
        "urllib",
        "ftplib",
        "smtplib",
        "requests",
        "httpx",
        "aiohttp",
        "asyncio",
        "subprocess",
        "importlib",
    }
)
FORBIDDEN_CALLS = frozenset({"__import__", "eval", "exec", "compile"})


def violations(source: str) -> list[str]:
    """What in ``source`` breaks the isolation rules."""
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
            if top in TRANSPORTS:
                found.append(f"transport import {module}")
            elif top not in ALLOWED_TOP_LEVEL:
                found.append(f"import {module} is not allowed")
    return found


def new_modules() -> list[Path]:
    roots = [Path(package.__file__ or "").parent for package in (orqestra.ai, orqestra.workloads)]
    return sorted(path for root in roots for path in root.rglob("*.py"))


def test_the_scan_finds_every_new_module() -> None:
    names = {path.name for path in new_modules()}
    assert {
        "provider.py",
        "mock.py",
        "routing.py",
        "model.py",
        "checks.py",
        "verify.py",
        "handler.py",
        "recording.py",
    } <= names


def test_no_new_module_imports_a_transport_or_evaluates_code() -> None:
    for path in new_modules():
        assert violations(path.read_text(encoding="utf-8")) == [], path.name


# Which orqestra packages a layer may not import: ai/ knows no workload and no worker,
# and neither ai/ nor workloads/ reaches storage or aggregation (DEC-057).
LAYER_FORBIDDEN = {
    "ai": (
        "orqestra.persistence",
        "orqestra.aggregation",
        "orqestra.workloads",
        "orqestra.worker",
    ),
    "workloads": ("orqestra.persistence", "orqestra.aggregation"),
}


def layer_violations(layer: str, source: str) -> list[str]:
    """The imports in ``source`` that ``layer`` may not make."""
    forbidden = LAYER_FORBIDDEN[layer]
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            modules = [node.module] + [f"{node.module}.{alias.name}" for alias in node.names]
        else:
            continue
        for module in modules:
            if any(module == p or module.startswith(f"{p}.") for p in forbidden):
                found.append(f"{layer} imports {module}")
                break
    return found


def test_no_ai_or_workload_module_imports_a_layer_it_must_not_know() -> None:
    for package, layer in ((orqestra.ai, "ai"), (orqestra.workloads, "workloads")):
        root = Path(package.__file__ or "").parent
        for path in sorted(root.rglob("*.py")):
            assert layer_violations(layer, path.read_text(encoding="utf-8")) == [], path.name


@pytest.mark.parametrize(
    ("layer", "source", "expected"),
    [
        ("ai", "import orqestra.persistence", ["ai imports orqestra.persistence"]),
        (
            "ai",
            "from orqestra.persistence.provider_usage import CallOutcome",
            ["ai imports orqestra.persistence.provider_usage"],
        ),
        ("ai", "from orqestra import worker", ["ai imports orqestra.worker"]),
        (
            "ai",
            "from orqestra.workloads.invoice import verify",
            ["ai imports orqestra.workloads.invoice"],
        ),
        (
            "ai",
            "import orqestra.aggregation.invoice",
            ["ai imports orqestra.aggregation.invoice"],
        ),
        (
            "workloads",
            "from orqestra import persistence",
            ["workloads imports orqestra.persistence"],
        ),
        (
            "workloads",
            "from orqestra.aggregation import invoice",
            ["workloads imports orqestra.aggregation"],
        ),
        ("workloads", "from orqestra.worker.handlers import Handler", []),
        ("workloads", "from orqestra.ai.recording import ProviderCallRecorder", []),
        ("ai", "from orqestra.ai.provider import FieldKind", []),
        ("ai", "import orqestra.persistence_like", []),
    ],
)
def test_the_layer_scan_catches_what_it_is_meant_to_catch(
    layer: str, source: str, expected: list[str]
) -> None:
    assert layer_violations(layer, source) == expected


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import socket", ["transport import socket"]),
        ("import urllib.request", ["transport import urllib.request"]),
        ("from http import client", ["transport import http"]),
        ("import importlib", ["transport import importlib"]),
        ("from . import sibling", ["import . is not allowed"]),
        ("import somepackage", ["import somepackage is not allowed"]),
        ("x = __import__('os')", ["use of __import__"]),
        ("eval('1')", ["use of eval"]),
        ("exec('x = 1')", ["use of exec"]),
        ("compile('1', 'f', 'eval')", ["use of compile"]),
        ("import re\nre.compile('x')", []),
        ("import json\nfrom orqestra.ai import provider", []),
    ],
)
def test_the_scan_catches_what_it_is_meant_to_catch(source: str, expected: list[str]) -> None:
    assert violations(source) == expected


@pytest.mark.usefixtures("no_network")
def test_the_runtime_guard_fires_on_every_way_in() -> None:
    with pytest.raises(AssertionError, match="no_network"):
        socket.create_connection(("127.0.0.1", 9))
    with pytest.raises(AssertionError, match="no_network"):
        socket.getaddrinfo("localhost", 80)
    with socket.socket() as sock, pytest.raises(AssertionError, match="no_network"):
        sock.connect(("127.0.0.1", 9))


def test_the_guard_is_off_unless_a_test_asks_for_it() -> None:
    assert socket.create_connection.__module__ == "socket"
    assert socket.getaddrinfo.__module__ == "socket"


def test_the_builtin_kinds_are_unchanged_and_exclude_the_invoice_kind() -> None:
    assert builtin.KINDS == ("diagnostic.echo", "diagnostic.wait")
    assert JOB_KIND == "invoice_verification"
    assert JOB_KIND not in builtin.KINDS


def test_the_cli_still_refuses_the_invoice_kind() -> None:
    with pytest.raises(cli._Refused, match="unsupported job kinds"):
        cli._registry([JOB_KIND], Settings(), SETTINGS)
