"""The plain-script baseline (DEC-072): loop and thread pool, retries, the call counter,
the recorded figures and the command.

A stub stands in for the extraction workload; no adapter or transport is built and
sockets are blocked. The key is a sentinel string.
"""

import ast
import inspect
import json
import os
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from benchmarks import baseline
from benchmarks.baseline import (
    FAILED,
    RETRIES_EXHAUSTED,
    UNEXPECTED,
    Extraction,
    Mode,
    Prices,
    Reply,
    Run,
    Score,
    extraction_figures,
    retry_delay,
    run_documents,
)
from benchmarks.campaign import Tool, result_path, step_figures
from benchmarks.live import (
    API_KEY_ENV,
    BUDGET_EXHAUSTED,
    BUDGET_FILE_NAME,
    CallBudget,
    ProviderChoice,
    create_budget_file,
    read_budget_file,
)
from orqestra.calls.provider import (
    PermanentCode,
    ProviderPermanentError,
    ProviderTransientError,
    TransientCode,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
PRICES = Prices(Decimal("0.31"), Decimal("1.17"), canned=False)  # arbitrary test prices


@dataclass(frozen=True)
class Doc:
    text: str
    expected: int


class StubExtraction:
    """Answers each document from a script of replies and errors; then a good reply.

    Document ``i`` is ``doc-i``. The score marks ``total`` correct always and
    ``vendor_name`` correct for even ``i``; ``i`` divisible by 5 needs review.
    """

    def __init__(self, script: Mapping[str, Sequence[Reply | Exception]] | None = None) -> None:
        self.script = {key: list(value) for key, value in (script or {}).items()}
        self.calls: list[str] = []
        self.lock = threading.Lock()

    fields = ("total", "vendor_name")

    def documents(self, count: int, seed: int) -> list[Doc]:
        return [Doc(f"doc-{index}", index) for index in range(count)]

    def call(self, text: str) -> Reply:
        with self.lock:
            self.calls.append(text)
            pending = self.script.get(text)
            step = pending.pop(0) if pending else None
        if isinstance(step, Exception):
            raise step
        return step or Reply("reply", 1_000, 200, 120, Decimal("0.0001"))

    def score(self, expected: object, content: str | None) -> Score:
        assert isinstance(expected, int)
        vendor = (int(expected % 2 == 0 and expected % 5 != 0), 1)
        fields = {"total": (1, 1), "vendor_name": vendor}
        if expected % 5 == 0:
            return Score("needs_review", "field_missing", fields, exact=False, status_matched=True)
        return Score("verified", None, fields, exact=vendor == (1, 1), status_matched=True)


def budget(tmp_path: Path, maximum: int = 1_000) -> CallBudget:
    tmp_path.mkdir(parents=True, exist_ok=True)
    return CallBudget(create_budget_file(tmp_path / BUDGET_FILE_NAME), maximum)


def run(
    tmp_path: Path,
    stub: StubExtraction,
    *,
    count: int = 10,
    mode: Mode = Mode.LOOP,
    workers: int = 1,
    maximum: int = 1_000,
    sleeps: list[float] | None = None,
) -> Run:
    slept = sleeps if sleeps is not None else []
    return run_documents(
        stub.documents(count, 1),
        stub,
        mode=mode,
        workers=workers,
        budget=budget(tmp_path, maximum),
        sleep=slept.append,
    )


def test_the_stub_is_an_extraction() -> None:
    extraction: Extraction = StubExtraction()
    assert extraction.documents(1, 1)[0].text == "doc-0"


def test_loop_and_threads_judge_every_document_the_same(tmp_path: Path) -> None:
    looped = run(tmp_path / "a", StubExtraction(), count=40)
    threaded = run(tmp_path / "b", StubExtraction(), count=40, mode=Mode.THREADS, workers=8)
    assert [outcome.status for outcome in looped.outcomes] == [
        outcome.status for outcome in threaded.outcomes
    ]
    first = extraction_figures(looped, PRICES)
    second = extraction_figures(threaded, PRICES)
    for name in ("accuracy", "needs_review", "tokens", "statuses", "calls"):
        assert first[name] == second[name]
    assert first["documents"] == 40
    assert first["calls"] == {
        "total": 40,
        "answered": 40,
        "failed": 0,
        "unresolved": 0,
        "latency_ms": {"n": 40, "p50": 120, "p95": 120, "p99": 120, "max": 120},
    }
    assert first["accuracy"]["per_field"] == {
        "total": {"matched": 40, "expected": 40},
        "vendor_name": {"matched": 16, "expected": 40},  # even and not divisible by 5
    }
    accuracy = first["accuracy"]
    assert (accuracy["matched"], accuracy["expected"], accuracy["overall"]) == (56, 80, 0.7)
    assert (accuracy["exact_documents"], accuracy["status_matched"]) == (16, 40)
    assert first["needs_review"] == {"count": 8, "by_reason": {"field_missing": 8}}
    assert first["statuses"] == {
        **{"verified": 0, "rejected": 0, "needs_review": 0, "malformed": 0, "failed": 0},
        "needs_review": 8,
        "verified": 32,
    }


def test_the_thread_pool_runs_documents_at_once(tmp_path: Path) -> None:
    barrier = threading.Barrier(4, timeout=10)

    class Meeting(StubExtraction):
        def call(self, text: str) -> Reply:
            barrier.wait()  # only returns once four calls are in flight together
            return super().call(text)

    outcome = run(tmp_path, Meeting(), count=8, mode=Mode.THREADS, workers=4)
    assert {item.status for item in outcome.outcomes} <= {"verified", "needs_review"}


def test_a_transient_error_is_retried_after_the_workers_delays(tmp_path: Path) -> None:
    limited = ProviderTransientError(TransientCode.RATE_LIMITED)
    timeout = ProviderTransientError(TransientCode.TIMEOUT)
    stub = StubExtraction({"doc-1": [limited, timeout]})
    sleeps: list[float] = []
    outcome = run(tmp_path, stub, count=3, sleeps=sleeps)
    assert stub.calls == ["doc-0", "doc-1", "doc-1", "doc-1", "doc-2"]
    assert sleeps == [0.5, 1.0]
    figures = extraction_figures(outcome, PRICES)
    assert (figures["calls"]["total"], figures["calls"]["answered"]) == (5, 3)
    assert figures["calls"]["failed"] == 2
    assert figures["retries"] == 2
    assert figures["transient_errors"] == {"timeout": 1, "rate_limited": 1, "unavailable": 0}
    assert figures["permanent_errors"] == {}


def test_a_document_stops_after_three_transient_errors(tmp_path: Path) -> None:
    unavailable = ProviderTransientError(TransientCode.UNAVAILABLE)
    stub = StubExtraction({"doc-0": [unavailable] * 5})
    sleeps: list[float] = []
    outcome = run(tmp_path, stub, count=1, sleeps=sleeps)
    assert stub.calls == ["doc-0"] * 3
    assert sleeps == [0.5, 1.0]  # no wait after the last attempt
    assert outcome.outcomes[0].status == FAILED
    assert outcome.outcomes[0].permanent == RETRIES_EXHAUSTED


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (ProviderPermanentError(PermanentCode.REJECTED), "rejected"),
        (RuntimeError("an adapter defect"), UNEXPECTED),
    ],
)
def test_any_other_error_ends_the_document_without_a_retry(
    tmp_path: Path, error: Exception, code: str
) -> None:
    stub = StubExtraction({"doc-0": [error]})
    outcome = run(tmp_path, stub, count=2)
    assert stub.calls == ["doc-0", "doc-1"]
    figures = extraction_figures(outcome, PRICES, stub.fields)
    assert figures["permanent_errors"] == {code: 1}
    assert figures["statuses"] == {
        **{"verified": 0, "rejected": 0, "needs_review": 0, "malformed": 0, "failed": 0},
        FAILED: 1,
        "verified": 1,
    }
    # the unanswered document counts every field as expected and none as matched
    assert figures["accuracy"]["per_field"]["total"] == {"matched": 1, "expected": 2}
    assert "adapter defect" not in json.dumps(figures)


@pytest.mark.parametrize(("mode", "workers"), [(Mode.LOOP, 1), (Mode.THREADS, 4)])
def test_the_call_counter_bounds_what_is_sent(tmp_path: Path, mode: Mode, workers: int) -> None:
    stub = StubExtraction()
    outcome = run(tmp_path, stub, count=20, mode=mode, workers=workers, maximum=7)
    assert len(stub.calls) == 7
    assert read_budget_file(tmp_path / BUDGET_FILE_NAME) == 7
    figures = extraction_figures(outcome, PRICES)
    assert figures["calls"]["total"] == 7
    assert figures["permanent_errors"] == {BUDGET_EXHAUSTED: 13}


def test_costs_come_from_tokens_and_prices_and_from_credits(tmp_path: Path) -> None:
    outcome = run(tmp_path, StubExtraction(), count=10)
    figures = extraction_figures(outcome, PRICES)
    assert figures["tokens"] == {
        "input": 10_000,
        "output": 2_000,
        "answered_without_counts": 0,
        "max_per_call": {"input": 1_000, "output": 200},
    }
    # 10,000 x 0.31 / 1e6 + 2,000 x 1.17 / 1e6
    assert Decimal(figures["cost"]["total"]) == Decimal("0.00544")
    assert Decimal(figures["cost"]["reported"]["openrouter_credits"]) == Decimal("0.0010")
    assert Decimal(figures["cost"]["per_1000_invoices"]) == Decimal("0.544")
    assert figures["cost"]["currency"] == "USD"
    assert figures["cost"]["prices"] == {
        "in_per_mtok": "0.31",
        "out_per_mtok": "1.17",
        "canned": False,
    }
    assert figures["calls"]["latency_ms"]["p50"] == 120
    assert figures["time_split"]["provider_s"] == pytest.approx(1.2)


def test_a_call_without_token_counts_leaves_the_token_cost_unknown(tmp_path: Path) -> None:
    blind = Reply("reply", None, 200, 120, None)
    outcome = run(tmp_path, StubExtraction({"doc-3": [blind]}), count=5)
    figures = extraction_figures(outcome, PRICES)
    assert figures["tokens"] == {
        "input": 4_000,
        "output": 800,
        "answered_without_counts": 1,
        "max_per_call": {"input": None, "output": None},  # the uncounted call could be larger
    }
    record = {"status": "complete", "extraction": figures}
    assert step_figures(record).token_cost is None  # priced over four of five calls only
    assert step_figures(record).calls == 5


def test_the_retry_delay_doubles_up_to_the_workers_maximum() -> None:
    assert [retry_delay(retry) for retry in range(1, 7)] == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0]


# --- the command ---------------------------------------------------------------------------


@pytest.fixture
def command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    raw = tmp_path / "raw"
    monkeypatch.setattr(baseline, "RAW_DIR", raw)
    monkeypatch.setattr(baseline, "refuse_dirty", lambda allow_dirty: None)
    monkeypatch.setattr(
        baseline, "code_state", lambda: {"git_sha": "abc1234ff", "git_dirty": False}
    )
    monkeypatch.setattr(os, "environ", {API_KEY_ENV: SENTINEL})
    return tmp_path


def args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "--invoices",
        "12",
        "--model",
        MODEL,
        "--max-provider-calls",
        "50",
        "--out-dir",
        str(tmp_path / "out dir"),  # the owner's workspace path has spaces too
        *extra,
    ]


def stub_connector(stub: StubExtraction) -> Any:
    def connect(choice: ProviderChoice, call_timeout: str, environ: Mapping[str, str]) -> Any:
        assert choice.model == MODEL
        assert call_timeout == "PT30S"
        return stub

    return connect


def test_the_command_writes_a_record_the_campaign_reads(
    command: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    stub = StubExtraction()
    argv = args(command, "--mode", "threads", "--workers", "4", "--dry-run", "--note", "c1")
    canned = Prices(Decimal(1), Decimal(2), canned=True)
    assert baseline.main(argv, stub_connector(stub), canned) == 0
    out = capsys.readouterr().out
    path = result_path(out, Tool.BASELINE)  # the campaign's own reading of the line
    assert path is not None
    assert " " in str(path)
    assert out.partition(" ")[0] == "complete"
    assert path.name.endswith("-abc1234-baseline-threads-12-w4.json")
    record = json.loads(path.read_text())
    assert record["format"] == "baseline-1"
    assert record["cell"] == {"mode": "threads", "workers": 4, "invoices": 12, "seed": 20261107}
    assert record["provider"]["model"] == MODEL
    assert record["provider"]["dry_run"] is True
    assert record["extraction"]["cost"]["prices"]["canned"] is True
    assert record["environment"]["note"] == "c1"
    (counter,) = (command / "raw").glob(f"*/baseline/{BUDGET_FILE_NAME}")
    assert read_budget_file(counter) == 12
    assert counter.parent.parent.name == record["run_id"]
    figures = step_figures(record)
    assert figures.calls == 12
    assert figures.accuracy == Decimal(str(16 / 24))  # 12 totals; vendors 2, 4, 6, 8
    assert figures.status == "complete"
    assert figures.items_per_s is not None
    text = path.read_text()
    assert SENTINEL not in text
    assert "doc-" not in text
    assert '"reply"' not in text


@pytest.mark.parametrize(
    ("extra", "words"),
    [
        (("--mode", "loop", "--workers", "4", "--dry-run"), "--workers 1"),
        (("--mode", "loop", "--allow-live-calls"), "--price-in-per-mtok"),
        (("--mode", "loop", "--dry-run", "--price-in-per-mtok", "1"), "canned prices"),
        (("--mode", "loop", "--dry-run", "--call-timeout", "soon"), "--call-timeout"),
    ],
)
def test_the_command_refuses_before_calling(
    command: Path, capsys: pytest.CaptureFixture[str], extra: tuple[str, ...], words: str
) -> None:
    stub = StubExtraction()
    assert baseline.main(args(command, *extra), stub_connector(stub)) == 1
    error = capsys.readouterr().err
    assert error.startswith("refused: ")
    assert words in error
    assert stub.calls == []
    assert not (command / "raw").exists()


def test_a_live_run_without_the_key_is_refused(
    command: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(os, "environ", {})
    prices = ("--price-in-per-mtok", "1", "--price-out-per-mtok", "2")
    argv = args(command, "--mode", "loop", "--allow-live-calls", *prices)
    assert baseline.main(argv, stub_connector(StubExtraction())) == 1
    assert "is not set" in capsys.readouterr().err


@pytest.mark.parametrize("mode", [["--mode", "loop"], ["--mode", "threads", "--workers", "3"]])
def test_a_dry_run_reads_every_document_through_the_extraction_workload(
    command: Path, capsys: pytest.CaptureFixture[str], mode: list[str]
) -> None:
    # The real binding: T1's documents, the adapter's extract call on the canned
    # transport (which reads each document perfectly), the strict parse and the oracle.
    assert baseline.main(args(command, *mode, "--dry-run")) == 0
    out = capsys.readouterr().out
    assert out.startswith("complete ")
    path = result_path(out, Tool.BASELINE)
    assert path is not None
    record = json.loads(path.read_text())
    block = record["extraction"]
    assert block["documents"] == 12
    assert block["calls"]["total"] == block["calls"]["answered"] == 12
    assert block["accuracy"]["overall"] == 1.0
    assert block["accuracy"]["status_matched"] == 12
    assert block["tokens"]["max_per_call"]["input"] > 0
    assert block["cost"]["prices"]["canned"] is True
    assert "E" not in block["cost"]["total"]
    assert SENTINEL not in json.dumps(record)


def test_the_baseline_imports_no_network_module() -> None:
    tree = ast.parse(inspect.getsource(baseline))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)
    assert not imported & {"socket", "http", "http.client", "urllib", "urllib.request", "ssl"}
    assert not any("stdlib_transport" in name for name in imported)
