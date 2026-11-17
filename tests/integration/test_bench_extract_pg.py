"""The extraction workload end to end, as a dry run: real worker processes, real
PostgreSQL, the real adapter's ``extract`` call, and ``CannedTransport`` in place of
the network.

No live call is made: every worker gets ``--dry-run``. A sentinel key is set in this
process's environment, as for a live run, and must reach no worker, log or result.
The canned model reads every document perfectly, so every count is known exactly.
"""

import json
import subprocess
from collections import Counter
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks import extract
from benchmarks.documents import Kind, generate_documents
from benchmarks.live import (
    API_KEY_ENV,
    CANNED_COST,
    CANNED_GENERATION_ID,
    ProviderChoice,
    ProviderName,
    canned_requests,
)
from benchmarks.result import EXTRACTION_FIELDS, Status, validate_result, write_result
from benchmarks.run import Cell, Profile, cell_cap, run_cell
from orqestra.calls.provider import ProviderUsage
from orqestra.calls.recording import CallOutcome, ProviderCallResult
from orqestra.persistence.provider_usage import UsageReplayConflictError, record_call_result
from orqestra.providers.openrouter import EXTRACT_PROMPT_VERSION, EXTRACT_SYSTEM_PROMPT
from orqestra.worker.usage import ledger_result
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

STOP = 180.0
SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
CELL = Cell(Profile.EXTRACT, size=40, batch=10, workers=2)
DOCUMENTS = generate_documents(CELL.size, CELL.seed)
CHECKS = [
    "terminal",
    "one_succeeded_attempt",
    "no_double_running",
    "nothing_running",
    "failed_tasks",
    "worker_exits",
    "extraction_complete",
    "extraction_usage",
]


@pytest.fixture
def started(monkeypatch: pytest.MonkeyPatch) -> list[tuple[list[str], dict[str, str]]]:
    """Every process this test starts, with its argv and environment; the key is set."""
    monkeypatch.setenv(API_KEY_ENV, SENTINEL)
    seen: list[tuple[list[str], dict[str, str]]] = []
    real = subprocess.Popen

    class Recording(real):  # type: ignore[valid-type,misc]
        def __init__(self, args: Sequence[str], *rest: Any, **options: Any) -> None:
            seen.append(([str(part) for part in args], dict(options.get("env") or {})))
            super().__init__(args, *rest, **options)

    monkeypatch.setattr(subprocess, "Popen", Recording)
    return seen


def cost_text(block: dict[str, Any]) -> str:
    """Every decimal text of the cost, joined: none may be in exponent form."""
    cost = block["cost"]
    texts = [cost[key] for key in ("total", "succeeded_attempts", "other_attempts")]
    texts += [cost["per_1000_invoices"], cost["per_1000_invoices_succeeded"]]
    return " ".join([*texts, *cost["reported"].values()]).upper()


def _cell(schema_url: str, raw_dir: Path) -> dict[str, Any]:
    return run_cell(
        schema_url,
        CELL,
        worker_settings=ENVIRONMENT,
        raw_dir=raw_dir,
        stop_after=STOP,
        idle_window=0.5,
        provider=ProviderChoice(
            name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=200, dry_run=True
        ),
        call_timeout="PT1S",
        prices=extract.CANNED_PRICES,
    )


def test_a_dry_extract_cell_reads_every_document_and_records_every_figure(
    db: psycopg.Connection[TupleRow],
    schema_url: str,
    tmp_path: Path,
    started: list[tuple[list[str], dict[str, str]]],
) -> None:
    raw_dir = tmp_path / "raw"
    record = _cell(schema_url, raw_dir)

    assert [check["name"] for check in record["gate"]] == CHECKS
    assert [check for check in record["gate"] if not check["passed"]] == []
    assert record["status"] == Status.COMPLETE
    assert record["provider"]["calls_made"] == CELL.size
    assert record["provider"]["cap"] == cell_cap(CELL) == 15  # batch 10 + room for retries
    assert canned_requests(raw_dir) == CELL.size

    block = record["extraction"]
    assert set(block) == EXTRACTION_FIELDS | {"documents_processed"}
    assert block["documents"] == block["documents_processed"] == CELL.size
    calls = block["calls"]
    assert (calls["total"], calls["answered"], calls["failed"], calls["unresolved"]) == (
        CELL.size,
        CELL.size,
        0,
        0,
    )
    assert (calls["succeeded_attempts"], calls["other_attempts"]) == (CELL.size, 0)
    assert calls["latency_ms"]["n"] == CELL.size
    assert all(calls["latency_ms"][key] is not None for key in ("p50", "p95", "p99", "max"))
    assert set(block["time_split"]) == {"provider_s", "handler_s", "provider_share"}
    assert block["time_split"]["handler_s"] > 0

    accuracy = block["accuracy"]
    assert accuracy["overall"] == 1.0
    assert accuracy["matched"] == accuracy["expected"] > 0
    assert accuracy["exact_documents"] == accuracy["status_matched"] == CELL.size
    assert all(f["matched"] == f["expected"] for f in accuracy["per_field"].values())

    oracle = Counter(document.expected_status.value for document in DOCUMENTS)
    assert {k: v for k, v in block["statuses"].items() if v} == dict(oracle)
    missing = sum(1 for document in DOCUMENTS if document.kind is Kind.MISSING_FIELD)
    assert block["needs_review"]["by_reason"]["field_missing"] == missing
    assert block["needs_review"]["count"] == missing

    tokens = block["tokens"]
    replies = [extract.canonical_reply(document.expected) for document in DOCUMENTS]
    assert tokens["input"] == sum(len(document.text) // 4 for document in DOCUMENTS)
    assert tokens["output"] == sum(len(reply) // 4 for reply in replies)
    assert tokens["other_attempts"] == {"input": 0, "output": 0}
    assert tokens["answered_without_counts"] == 0
    assert tokens["max_per_call"] == {
        "input": max(len(document.text) // 4 for document in DOCUMENTS),
        "output": max(len(reply) // 4 for reply in replies),
    }
    assert "E" not in cost_text(block)
    assert record["resources"]["sysload"]["samples"] >= 1

    cost = block["cost"]
    expected_cost = (
        Decimal(tokens["input"]) * extract.CANNED_PRICE_IN_PER_MTOK
        + Decimal(tokens["output"]) * extract.CANNED_PRICE_OUT_PER_MTOK
    ) / Decimal(1_000_000)
    assert Decimal(cost["total"]) == expected_cost == Decimal(cost["succeeded_attempts"])
    assert Decimal(cost["other_attempts"]) == 0
    assert Decimal(cost["per_1000_invoices"]) == expected_cost * 1000 / CELL.size
    assert cost["prices"]["canned"] is True
    assert cost["currency"] == "USD"
    assert Decimal(cost["reported"]["openrouter_credits"]) == CANNED_COST * CELL.size
    assert block["retries"] == 0
    assert set(block["transient_errors"].values()) == {0}
    assert set(block["permanent_errors"].values()) == {0}

    # The ledger: every call is a whole-document call with its metadata.
    rows = db.execute(
        "SELECT c.capability, c.field_kind, c.prompt_version, c.model, r.outcome, "
        "r.model_served, r.provider_generation_id, r.input_tokens, r.output_tokens, "
        "r.latency_ms FROM provider_calls c JOIN provider_call_results r "
        "ON r.attempt_id = c.attempt_id AND r.call_seq = c.call_seq"
    ).fetchall()
    assert len(rows) == CELL.size
    for row in rows:
        capability, field_kind, prompt_version, model, outcome, served, generation = row[:7]
        assert (capability, field_kind, prompt_version, model) == (
            "extract_invoice",
            None,
            EXTRACT_PROMPT_VERSION,
            MODEL,
        )
        assert (outcome, served, generation) == ("answered", MODEL, CANNED_GENERATION_ID)
        assert None not in row[7:]

    # Nothing secret, no document and no reply in the result file or the worker logs.
    stored = write_result(record, tmp_path / "results", allow_dirty=True)
    validate_result(json.loads(stored.read_text(encoding="utf-8")))
    texts = {"result": stored.read_text(encoding="utf-8")}
    for path in sorted(raw_dir.glob("worker-*.log")):
        texts[path.name] = path.read_text(encoding="utf-8", errors="replace")
    forbidden = [SENTINEL, EXTRACT_SYSTEM_PROMPT, DOCUMENTS[0].text, replies[0]]
    forbidden += [document.expected.vendor_name for document in DOCUMENTS]
    leaks = [name for name, text in texts.items() if any(item in text for item in forbidden)]
    assert leaks == []
    workers = [(argv, env) for argv, env in started if "benchmarks.worker" in argv]
    assert len(workers) == CELL.workers
    for argv, env in workers:
        assert API_KEY_ENV not in env
        assert "--dry-run" in argv
        assert argv[argv.index("--workload") + 1] == "extract"


def test_a_replayed_result_with_the_same_tokens_is_accepted_and_different_ones_refused(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    record = _cell(schema_url, tmp_path / "raw")
    assert record["status"] == Status.COMPLETE
    row = db.execute(
        "SELECT attempt_id, call_seq, reported_amount, reported_unit, model_served, "
        "provider_generation_id, input_tokens, output_tokens, latency_ms "
        "FROM provider_call_results ORDER BY attempt_id, call_seq LIMIT 1"
    ).fetchone()
    assert row is not None
    attempt_id, call_seq, amount, unit, served, generation, tokens_in, tokens_out, latency = row

    def replay(input_tokens: int) -> bool:
        result = ProviderCallResult(
            attempt_id=attempt_id,
            call_seq=call_seq,
            outcome=CallOutcome.ANSWERED,
            usage=ProviderUsage(format(amount, "f"), unit),
            model_served=served,
            provider_generation_id=generation,
            input_tokens=input_tokens,
            output_tokens=tokens_out,
            latency_ms=latency,
        )
        converted = ledger_result(result)
        assert converted is not None
        return record_call_result(db, converted).created

    assert replay(tokens_in) is False
    with pytest.raises(UsageReplayConflictError):
        replay(tokens_in + 1)
