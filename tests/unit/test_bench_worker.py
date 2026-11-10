"""The benchmark worker's provider flags (DEC-071): refused before anything is built,
and wired to the canned transport for a dry run. No test makes a live call."""

import json
from pathlib import Path
from typing import Any, NoReturn

import pytest

import benchmarks.worker
from benchmarks.live import (
    API_KEY_ENV,
    CannedTransport,
    OpenRouterWiring,
    ProviderChoice,
    ProviderName,
    create_budget_file,
)
from benchmarks.run import BENCH_WORKER_SETTINGS
from orqestra.providers import stdlib_transport
from orqestra.worker.settings import WorkerSettings

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"


@pytest.fixture(autouse=True)
def no_real_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail("the real HttpsTransport was constructed")

    monkeypatch.setattr(stdlib_transport, "HttpsTransport", refuse)


@pytest.fixture
def bench_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in BENCH_WORKER_SETTINGS.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("ORQESTRA_DATABASE_URL", raising=False)


def _error(capsys: pytest.CaptureFixture[str]) -> str:
    output: dict[str, Any] = json.loads(capsys.readouterr().out)
    assert output["status"] == "error"
    return str(output["error"])


def _openrouter(budget: Path | None, *extra: str) -> list[str]:
    arguments = ["--provider", "openrouter", "--model", MODEL, "--max-provider-calls", "5"]
    if budget is not None:
        arguments += ["--budget-file", str(budget)]
    return [*arguments, *extra]


@pytest.mark.usefixtures("bench_settings")
def test_openrouter_without_an_opt_in_is_refused(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    budget = create_budget_file(tmp_path / "budget")
    assert benchmarks.worker.main(_openrouter(budget)) == 1
    assert "--allow-live-calls" in _error(capsys)


@pytest.mark.usefixtures("bench_settings")
def test_a_missing_budget_file_is_refused(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    assert benchmarks.worker.main(_openrouter(tmp_path / "typo", "--dry-run")) == 1
    assert "must already exist" in _error(capsys)
    assert not (tmp_path / "typo").exists()  # never created, so never a fresh count
    assert benchmarks.worker.main(_openrouter(None, "--dry-run")) == 1
    assert "must already exist" in _error(capsys)


@pytest.mark.usefixtures("bench_settings")
def test_a_live_worker_without_the_key_is_refused(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv(API_KEY_ENV, raising=False)
    budget = create_budget_file(tmp_path / "budget")
    assert benchmarks.worker.main(_openrouter(budget, "--allow-live-calls")) == 1
    assert _error(capsys) == f"{API_KEY_ENV} is not set"


@pytest.mark.usefixtures("bench_settings")
def test_a_budget_file_with_the_double_is_refused(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    budget = create_budget_file(tmp_path / "budget")
    assert benchmarks.worker.main(["--budget-file", str(budget)]) == 1
    assert "only to --provider openrouter" in _error(capsys)


@pytest.mark.usefixtures("bench_settings")
def test_a_valid_dry_run_gets_as_far_as_the_database_url(
    capsys: pytest.CaptureFixture[str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    budget = create_budget_file(tmp_path / "budget")
    assert benchmarks.worker.main(_openrouter(budget, "--dry-run")) == 1
    error = _error(capsys)
    assert error == "ORQESTRA_DATABASE_URL is not set"


@pytest.mark.usefixtures("bench_settings")
def test_a_dry_run_worker_is_wired_to_the_canned_transport(tmp_path: Path) -> None:
    budget = create_budget_file(tmp_path / "budget")
    choice = ProviderChoice(
        name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=5, dry_run=True
    )
    wiring: list[OpenRouterWiring] = []
    benchmarks.worker.build_worker(
        "postgresql:///never-connected",
        WorkerSettings(_env_file=None),
        cap=16,
        call_timeout=benchmarks.worker.parse_duration("PT0.1S"),
        choice=choice,
        budget_file=budget,
        environ={},
        wiring=wiring,
    )
    (wired,) = wiring
    assert isinstance(wired.canned, CannedTransport)
    assert wired.provider_name == "openrouter"
    assert wired.model == MODEL
    assert wired.canned.requests == 0


@pytest.mark.usefixtures("bench_settings")
def test_the_double_is_still_the_default(tmp_path: Path) -> None:
    wiring: list[OpenRouterWiring] = []
    benchmarks.worker.build_worker(
        "postgresql:///never-connected",
        WorkerSettings(_env_file=None),
        cap=16,
        call_timeout=benchmarks.worker.parse_duration("PT0.1S"),
        wiring=wiring,
    )
    assert wiring == []
