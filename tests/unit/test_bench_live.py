"""The benchmark's OpenRouter opt-in (DEC-071): refusals, the cell's call maximum, the
canned transport, where a real transport may be made, the scope, and the key's path.

No test here makes a live call or constructs the real transport: an autouse fixture
replaces ``HttpsTransport`` with a class that fails the test when it is constructed,
except in the one test that checks the live path builds it (and then sends nothing).
Sockets are blocked as well. The key is a sentinel string, never a real one.
"""

import ast
import inspect
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator, Mapping
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, NoReturn

import pytest
from pydantic import SecretStr

import benchmarks
import benchmarks.drills
import benchmarks.ladder
import benchmarks.live
import benchmarks.worker
from benchmarks import result, run
from benchmarks.live import (
    API_KEY_ENV,
    BUDGET_EXHAUSTED,
    CANNED_COST,
    DOUBLE,
    DRY_RUN_LABEL,
    OPENROUTER_USAGE_UNIT,
    BudgetedRecorder,
    CallBudget,
    CannedTransport,
    ProviderCallBudgetExhausted,
    ProviderCallBudgetFileInvalid,
    ProviderChoice,
    ProviderName,
    build_openrouter_provider,
    canned_requests,
    canned_requests_line,
    create_budget_file,
    credential_from,
    expected_usage,
    provider_block,
    read_budget_file,
)
from benchmarks.result import ROOT
from benchmarks.run import (
    BENCH_WORKER_SETTINGS,
    Cell,
    Profile,
    dry_run_preview,
    process_environment,
    worker_command,
)
from orqestra.calls.provider import FieldKind, ProviderUsage, make_request
from orqestra.calls.recording import InMemoryCallRecorder
from orqestra.domain.errors import PermanentError
from orqestra.domain.policy import FailureKind, classify_failure
from orqestra.providers import openrouter, stdlib_transport
from orqestra.providers.openrouter import SYSTEM_PROMPT, OpenRouterConfig, OpenRouterProvider
from orqestra.workloads.invoice.verify import ProviderCalls

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
DRY = ProviderChoice(name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=10, dry_run=True)
LIVE = ProviderChoice(
    name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=10, allow_live_calls=True
)


class RealTransportConstructed:
    """Stands in for ``HttpsTransport``; fails the test unless a test allows one build."""

    allowed = 0
    built = 0

    def __init__(self) -> None:
        type(self).built += 1
        if type(self).built > type(self).allowed:
            pytest.fail("the real HttpsTransport was constructed")

    def post(self, **kwargs: object) -> NoReturn:
        pytest.fail("a request was sent through the real transport")


@pytest.fixture(autouse=True)
def no_real_transport(monkeypatch: pytest.MonkeyPatch) -> type[RealTransportConstructed]:
    RealTransportConstructed.allowed = 0
    RealTransportConstructed.built = 0
    monkeypatch.setattr(stdlib_transport, "HttpsTransport", RealTransportConstructed)
    return RealTransportConstructed


class Unreadable(Mapping[str, str]):
    """An environment that fails the test if anything reads it."""

    def __getitem__(self, key: str) -> str:
        pytest.fail(f"the environment was read for {key}")

    def __iter__(self) -> Iterator[str]:
        pytest.fail("the environment was iterated")

    def __len__(self) -> int:
        pytest.fail("the environment was sized")


def request(raw: str = "1,234") -> Any:
    return make_request(
        capability="interpret_amount",
        route_id="route-a",
        field_kind=FieldKind.TOTAL,
        raw=raw,
        timeout=timedelta(seconds=1),
    )


# --- refusals, in DEC-071's order ------------------------------------------------------------


def test_the_double_needs_nothing_and_reads_no_environment() -> None:
    assert DOUBLE.refusal(Unreadable()) is None
    assert not DOUBLE.openrouter
    assert not DOUBLE.live


def test_openrouter_without_an_opt_in_is_refused() -> None:
    choice = ProviderChoice(name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=10)
    refusal = choice.refusal({API_KEY_ENV: SENTINEL})
    assert refusal is not None
    assert "--allow-live-calls" in refusal
    assert "--dry-run" in refusal


@pytest.mark.parametrize(
    ("changes", "fragment"),
    [
        ({"model": None}, "needs --model"),
        ({"model": "bad model"}, "--model must be"),
        ({"model": "-leading-dash"}, "--model must be"),
        ({"max_provider_calls": None}, "--max-provider-calls"),
        ({"max_provider_calls": 0}, "--max-provider-calls"),
        ({"max_provider_calls": -3}, "--max-provider-calls"),
        ({"allow_live_calls": True}, "contradict"),
    ],
)
def test_each_missing_or_bad_flag_is_refused(changes: dict[str, Any], fragment: str) -> None:
    fields: dict[str, Any] = {"model": MODEL, "max_provider_calls": 10, "dry_run": True}
    fields |= changes
    choice = ProviderChoice(name=ProviderName.OPENROUTER, **fields)
    refusal = choice.refusal({})
    assert refusal is not None
    assert fragment in refusal


def test_a_bad_model_is_not_echoed() -> None:
    refusal = ProviderChoice(
        name=ProviderName.OPENROUTER, model="x y SECRET", max_provider_calls=1, dry_run=True
    ).refusal({})
    assert refusal is not None
    assert "SECRET" not in refusal


@pytest.mark.parametrize("value", [None, "", "   "])
def test_a_live_run_without_a_key_is_refused(value: str | None) -> None:
    environ = {} if value is None else {API_KEY_ENV: value}
    assert LIVE.refusal(environ) == f"{API_KEY_ENV} is not set"


def test_a_live_run_with_a_key_is_allowed_and_the_key_is_never_in_a_message() -> None:
    assert LIVE.live
    assert LIVE.refusal({API_KEY_ENV: SENTINEL}) is None
    assert credential_from({API_KEY_ENV: f"  {SENTINEL} "}) == SecretStr(SENTINEL)


def test_a_dry_run_never_reads_the_key() -> None:
    assert not DRY.live
    assert DRY.refusal(Unreadable()) is None


def test_double_flags_given_with_the_double_are_refused() -> None:
    assert ProviderChoice(model=MODEL).refusal({}) is not None
    assert ProviderChoice(dry_run=True).refusal({}) is not None


# --- the cell's call maximum ---------------------------------------------------------------


def test_the_budget_counts_to_its_maximum_then_refuses(tmp_path: Path) -> None:
    path = create_budget_file(tmp_path / "budget")
    budget = CallBudget(path, 3)
    for _ in range(3):
        budget.reserve()
    with pytest.raises(ProviderCallBudgetExhausted) as caught:
        budget.reserve()
    assert str(caught.value) == BUDGET_EXHAUSTED
    assert isinstance(caught.value, PermanentError)
    assert classify_failure(caught.value) is FailureKind.PERMANENT
    assert read_budget_file(path) == 3


def test_an_existing_budget_file_is_never_reset(tmp_path: Path) -> None:
    path = create_budget_file(tmp_path / "budget")
    CallBudget(path, 5).reserve()
    with pytest.raises(FileExistsError):
        create_budget_file(path)
    assert read_budget_file(path) == 1


@pytest.mark.parametrize("text", ["", "x\n", "-1\n", "1.5\n"])
def test_a_budget_file_without_a_count_refuses_the_call(tmp_path: Path, text: str) -> None:
    path = tmp_path / "budget"
    path.write_text(text, encoding="ascii")
    with pytest.raises(ProviderCallBudgetFileInvalid):
        CallBudget(path, 5).reserve()


@pytest.mark.parametrize("maximum", [0, -1, True])
def test_the_maximum_must_be_positive(tmp_path: Path, maximum: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        CallBudget(tmp_path / "budget", maximum)


RESERVER = """
import sys
from pathlib import Path
from benchmarks.live import CallBudget, ProviderCallBudgetExhausted
budget = CallBudget(Path(sys.argv[1]), int(sys.argv[2]))
granted = 0
for _ in range(int(sys.argv[3])):
    try:
        budget.reserve()
        granted += 1
    except ProviderCallBudgetExhausted:
        pass
print(granted)
"""


def test_processes_sharing_the_file_never_exceed_the_maximum(tmp_path: Path) -> None:
    path = create_budget_file(tmp_path / "budget")
    env = {**os.environ, "PYTHONPATH": str(ROOT.resolve())}
    processes = [
        subprocess.Popen(
            [sys.executable, "-c", RESERVER, str(path), "100", "60"],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    granted = [int(process.communicate(timeout=60)[0]) for process in processes]
    assert all(process.returncode == 0 for process in processes)
    assert sum(granted) == 100
    assert read_budget_file(path) == 100


def test_the_recorder_reserves_before_the_intent_and_refuses_once_spent(tmp_path: Path) -> None:
    inner = InMemoryCallRecorder()
    recorder = BudgetedRecorder(inner, CallBudget(create_budget_file(tmp_path / "b"), 2))
    transport = CannedTransport()
    provider = OpenRouterProvider(
        OpenRouterConfig(credential=SecretStr("x"), route_models={"route-a": MODEL}), transport
    )
    calls = ProviderCalls(attempt_id=uuid.uuid4(), cap=16, recorder=recorder)
    for raw in ("1,000", "2_000"):
        calls.call(provider, request(raw), item_index=0, estimated_cost_units=1)
    with pytest.raises(ProviderCallBudgetExhausted):
        calls.call(provider, request("3 000"), item_index=0, estimated_cost_units=1)
    assert transport.requests == 2  # the refused call was never sent
    assert len(inner.events) == 4  # two intents and two results; nothing for the refused one
    assert calls.made == 2


# --- the canned transport and where a real one is made -------------------------------------


def test_a_dry_run_provider_answers_through_the_canned_transport(
    no_real_transport: type[RealTransportConstructed],
) -> None:
    config = OpenRouterConfig(credential=SecretStr("x"), route_models={"route-a": MODEL})
    provider, canned = build_openrouter_provider(config, dry_run=True)
    assert isinstance(canned, CannedTransport)
    report = provider.call(request("12,345_6"))
    assert report.response.value == "123456"
    assert report.response.usage == ProviderUsage(format(CANNED_COST, "f"), OPENROUTER_USAGE_UNIT)
    assert report.model_served == MODEL
    assert report.input_tokens == len("12,345_6")
    assert canned.requests == 1
    assert no_real_transport.built == 0


def test_the_live_path_builds_the_real_transport_once_and_sends_nothing(
    no_real_transport: type[RealTransportConstructed],
) -> None:
    no_real_transport.allowed = 1
    config = OpenRouterConfig(credential=SecretStr(SENTINEL), route_models={"route-a": MODEL})
    provider, canned = build_openrouter_provider(config, dry_run=False)
    assert canned is None
    assert no_real_transport.built == 1
    assert SENTINEL not in repr(provider)


def test_a_dry_run_worker_logs_its_request_count(tmp_path: Path) -> None:
    transport = CannedTransport()
    transport.requests = 7
    line = canned_requests_line(transport)
    assert line is not None
    (tmp_path / "worker-1.log").write_text(f"noise\n{line}\n", encoding="utf-8")
    (tmp_path / "worker-2.log").write_text(f"{line}\n", encoding="utf-8")
    assert canned_requests(tmp_path) == 14
    assert canned_requests_line(object()) is None


def _references(path: Path) -> set[str]:
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Name | ast.alias):
            name = node.id if isinstance(node, ast.Name) else node.name
            if "HttpsTransport" in name or "stdlib_transport" in name:
                found.add(name)
        elif isinstance(node, ast.Attribute) and node.attr == "HttpsTransport":
            found.add(node.attr)
        elif isinstance(node, ast.ImportFrom) and "stdlib_transport" in (node.module or ""):
            found.add(node.module or "")
    return found


def test_only_live_py_refers_to_the_real_transport_outside_the_providers_package() -> None:
    harness = Path(benchmarks.__file__ or "").parent
    users = sorted(path.name for path in harness.rglob("*.py") if _references(path))
    assert users == ["live.py"]
    # ...and there only inside build_openrouter_provider, after the dry-run return.
    tree = ast.parse(Path(benchmarks.live.__file__).read_text(encoding="utf-8"))
    builders = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and any(
            isinstance(inner, ast.Attribute) and inner.attr == "HttpsTransport"
            for inner in ast.walk(node)
        )
    ]
    assert builders == ["build_openrouter_provider"]


def test_the_scan_sees_each_form_of_reference(tmp_path: Path) -> None:
    for source in (
        "from orqestra.providers.stdlib_transport import HttpsTransport\n",
        "from orqestra.providers import stdlib_transport\n",
        "import orqestra.providers.stdlib_transport\n",
        "x.HttpsTransport()\n",
    ):
        path = tmp_path / "probe.py"
        path.write_text(source, encoding="utf-8")
        assert _references(path), source


# --- scope: only benchmarks.run accepts the provider ------------------------------------------


def test_the_ladder_has_no_provider_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        benchmarks.ladder.main(["batch", "--provider", "openrouter"])
    assert caught.value.code == 2
    assert "unrecognized arguments: --provider" in capsys.readouterr().err


def test_the_drills_have_no_provider_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        benchmarks.drills.main(["d1", "--provider", "openrouter"])
    assert caught.value.code == 2
    assert "unrecognized arguments: --provider" in capsys.readouterr().err


def test_every_shared_entry_point_defaults_to_the_double() -> None:
    for function in (
        run.run_cell,
        run.run_in_bench_database,
        run.worker_command,
        run.process_environment,
    ):
        assert inspect.signature(function).parameters["provider"].default is DOUBLE
    assert run.Fleet.__dataclass_fields__["provider"].default is DOUBLE


def test_benchmarks_run_refuses_openrouter_outside_one_invoice_cell(
    capsys: pytest.CaptureFixture[str],
) -> None:
    dry = ["--provider", "openrouter", "--model", MODEL, "--dry-run"]
    cases = [
        (["--profile", "echo", "--size", "10", "--workers", "1", "--max-provider-calls", "5"],
         "only the invoice profile"),
        (["--profile", "invoice", "--size", "10", "--batch", "5", "--workers", "6",
          "--max-provider-calls", "5"], "--workers is above --max-provider-calls"),
        (["--profile", "invoice", "--size", "10", "--batch", "5", "--workers", "1",
          "--max-provider-calls", "5", "--call-timeout", "5s"], "--call-timeout"),
    ]  # fmt: skip
    for arguments, fragment in cases:
        assert run.main([*arguments, *dry]) == 1
        assert fragment in capsys.readouterr().err
    assert run.main(["--profile", "echo", "--size", "1", "--workers", "1",
                     "--call-timeout", "PT1S"]) == 1  # fmt: skip
    assert "--call-timeout applies only" in capsys.readouterr().err


def test_benchmarks_run_refuses_a_live_cell_without_the_key(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv(API_KEY_ENV, raising=False)
    arguments = ["--profile", "invoice", "--size", "10", "--batch", "5", "--workers", "1",
                 "--provider", "openrouter", "--model", MODEL, "--max-provider-calls", "5",
                 "--allow-live-calls"]  # fmt: skip
    assert run.main(arguments) == 1
    assert f"{API_KEY_ENV} is not set" in capsys.readouterr().err


# --- the key's path: environment only, and only for a live run --------------------------------


class FakeConnection:
    def execute(self, query: str, params: object = None) -> "FakeConnection":
        return self

    def fetchone(self) -> tuple[str]:
        return ("fake",)


def test_a_live_cell_passes_the_key_only_as_its_environment_entry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    budget_file = tmp_path / "provider-call-budget"
    argv = worker_command(Profile.INVOICE, LIVE, budget_file, "PT5S")
    env = process_environment("postgresql:///orqestra_bench", BENCH_WORKER_SETTINGS, LIVE)

    assert "--allow-live-calls" in argv
    assert not [part for part in argv if SENTINEL in part]
    assert [key for key, value in env.items() if SENTINEL in value] == [API_KEY_ENV]
    assert [key for key in env if SENTINEL in key] == []

    recorded = {
        "worker_settings": dict(BENCH_WORKER_SETTINGS),
        "environment": result.environment(FakeConnection(), note="live"),  # type: ignore[arg-type]
        "provider": provider_block(
            LIVE, call_timeout="PT5S", cap=16, route="route-a", calls_made=0
        ),
    }
    assert SENTINEL not in json.dumps(recorded)


@pytest.mark.parametrize("choice", [DOUBLE, DRY])
def test_a_dry_run_or_double_cell_never_passes_the_key(
    monkeypatch: pytest.MonkeyPatch, choice: ProviderChoice, tmp_path: Path
) -> None:
    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    argv = worker_command(Profile.INVOICE, choice, tmp_path / "b" if choice.openrouter else None)
    env = process_environment("postgresql:///orqestra_bench", BENCH_WORKER_SETTINGS, choice)
    assert API_KEY_ENV not in env
    assert SENTINEL not in json.dumps([argv, env])


def test_the_double_keeps_its_old_command() -> None:
    assert worker_command(Profile.INVOICE)[3:] == ["--cap", "16", "--call-timeout", "PT0.1S"]


# --- what a dry run prints and records -----------------------------------------------------------


def test_the_dry_run_preview_states_the_calls_and_bounds() -> None:
    cell = Cell(Profile.INVOICE, size=100, batch=25, workers=2)
    small = ProviderChoice(
        name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=1, dry_run=True
    )
    lines = dry_run_preview(cell, small, "PT0.1S")
    assert all(line.startswith(f"{DRY_RUN_LABEL}: ") for line in lines)
    expected = run.build_workload(cell).expected
    assert expected is not None
    assert f"expected provider calls (oracle): {expected.calls}" in lines[0]
    assert lines[2].endswith(f": {16 * 3 * 4}")
    assert lines[3].endswith(": fits")
    assert "would stop this run" in lines[-1]


def test_a_too_long_call_timeout_is_shown_refused() -> None:
    cell = Cell(Profile.INVOICE, size=100, batch=25, workers=2)
    lines = dry_run_preview(cell, DRY, "PT60S")
    assert ": refused: " in lines[3]


def test_the_provider_block_names_the_choice_and_counts_only() -> None:
    assert provider_block(DOUBLE, call_timeout="PT0.1S", cap=16, route="route-a", calls_made=3) == {
        "name": "bench-double",
        "call_timeout": "PT0.1S",
        "cap": 16,
        "route": "route-a",
    }
    assert provider_block(DRY, call_timeout="PT0.1S", cap=16, route="route-a", calls_made=3) == {
        "name": "openrouter",
        "model": MODEL,
        "max_provider_calls": 10,
        "dry_run": True,
        "call_timeout": "PT0.1S",
        "cap": 16,
        "route": "route-a",
        "calls_made": 3,
        "usage_unit": "openrouter_credits",
    }


def test_only_a_dry_runs_credits_are_known() -> None:
    assert expected_usage(DRY, 4) == (OPENROUTER_USAGE_UNIT, Decimal("0.000004"))
    assert expected_usage(LIVE, 4) == (OPENROUTER_USAGE_UNIT, None)
    assert OPENROUTER_USAGE_UNIT == openrouter.USAGE_UNIT


def test_the_canned_request_carries_the_system_prompt_only_in_memory() -> None:
    # The adapter's prompt reaches the canned transport, and nothing it returns echoes it.
    seen: list[bytes] = []

    class Spy(CannedTransport):
        def post(self, **kwargs: Any) -> Any:
            seen.append(kwargs["body"])
            return super().post(**kwargs)

    provider = OpenRouterProvider(
        OpenRouterConfig(credential=SecretStr("x"), route_models={"route-a": MODEL}), Spy()
    )
    report = provider.call(request())
    assert SYSTEM_PROMPT in json.loads(seen[0])["messages"][0]["content"]
    assert SYSTEM_PROMPT not in repr(report)
