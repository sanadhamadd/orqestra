"""The extraction workload's live wiring, offline: the handler, the canned model, the cap,
the worker and runner flags, and the per-cell worker settings. No network, no live call."""

import json
import sys
import threading
import uuid
from collections.abc import Callable
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from benchmarks import extract, run, worker
from benchmarks.documents import DEFAULT_SEED, Kind, generate_documents
from benchmarks.extract import Reason, canonical_reply, extraction_handler
from benchmarks.live import (
    UNKNOWN_DOCUMENT_REPLY,
    BudgetedRecorder,
    CallBudget,
    CannedTransport,
    ProviderCallBudgetExhausted,
    ProviderChoice,
    ProviderName,
    create_budget_file,
    document_key,
    perfect_replies,
)
from orqestra.calls.provider import (
    PermanentCode,
    ProviderPermanentError,
    ProviderTransientError,
    ProviderUsage,
    TransientCode,
)
from orqestra.calls.recording import (
    CallOutcome,
    InMemoryCallRecorder,
    ProviderCallIntent,
    ProviderCallResult,
)
from orqestra.domain.errors import PermanentError
from orqestra.providers.openrouter import (
    ExtractionReport,
    ExtractionRequest,
    OpenRouterConfig,
    OpenRouterProvider,
)
from orqestra.worker.handlers import CancellationSignal, HandlerCancelled, TaskContext
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.usage import check_provider_call_budget

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

MODEL = "vendor/model-a"
ROUTE = "route-a"
ATTEMPT = uuid.UUID(int=7)
TIMEOUT = timedelta(seconds=30)
DOCUMENTS = generate_documents(60)
DRY = ProviderChoice(
    name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=500, dry_run=True
)


def context(cancel: threading.Event | None = None) -> TaskContext:
    return TaskContext(
        uuid.UUID(int=1),
        ATTEMPT,
        1,
        extract.JOB_KIND,
        CancellationSignal(cancel or threading.Event()),
    )


def batch(offset: int, count: int) -> dict[str, Any]:
    return {"offset": offset, "items": [d.text for d in DOCUMENTS[offset : offset + count]]}


def report(content: str | None, **overrides: Any) -> ExtractionReport:
    values: dict[str, Any] = {
        "content": content,
        "usage": ProviderUsage("0.0002", "openrouter_credits"),
        "model_served": MODEL,
        "generation_id": "gen-1",
        "input_tokens": 300,
        "output_tokens": 90,
        "latency_ms": 840,
    }
    values.update(overrides)
    return ExtractionReport(**values)


class Scripted:
    """An extractor that answers from a script, one step per request, and keeps nothing."""

    def __init__(self, *steps: object) -> None:
        self.steps = list(steps)
        self.requests: list[ExtractionRequest] = []

    def extract(self, request: ExtractionRequest) -> ExtractionReport:
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if callable(step):
            return step(request)  # type: ignore[no-any-return]
        return step  # type: ignore[return-value]


def perfect(request: ExtractionRequest) -> ExtractionReport:
    for document in DOCUMENTS:
        if document.text == request.document:
            return report(canonical_reply(document.expected))
    raise AssertionError


def handler(extractor: Any, recorder: Any = None, cap: int = 100) -> Callable[..., Any]:
    return extraction_handler(
        extractor,
        ROUTE,
        TIMEOUT,
        max_provider_calls_per_attempt=cap,
        recorder=recorder if recorder is not None else InMemoryCallRecorder(),
    )


def items(result: Any) -> list[dict[str, Any]]:
    return list(result["results"])


# --- the handler ----------------------------------------------------------------------------


def test_a_perfect_reader_gives_the_oracle_status_for_every_document() -> None:
    recorder = InMemoryCallRecorder()
    extractor = Scripted(*[perfect] * 50)
    result = handler(extractor, recorder)(context(), batch(0, 50))
    assert result["offset"] == 0
    for item, document in zip(items(result), DOCUMENTS[:50], strict=True):
        assert item["index"] == document.index
        assert item["status"] == document.expected_status.value
        if document.kind is Kind.MISSING_FIELD:
            assert item["reason"] == Reason.FIELD_MISSING.value
    assert [r.document for r in extractor.requests] == [d.text for d in DOCUMENTS[:50]]
    assert all(r.timeout == TIMEOUT and r.route_id == ROUTE for r in extractor.requests)


def test_every_call_is_recorded_in_order_with_its_metadata() -> None:
    recorder = InMemoryCallRecorder()
    handler(Scripted(perfect, perfect), recorder)(context(), batch(10, 2))
    events = recorder.events
    assert [type(e) for e in events] == [
        ProviderCallIntent,
        ProviderCallResult,
        ProviderCallIntent,
        ProviderCallResult,
    ]
    first = events[0]
    assert isinstance(first, ProviderCallIntent)
    assert (first.call_seq, first.item_index, first.field_kind) == (0, 10, None)
    assert (first.capability, first.route_id) == ("extract_invoice", ROUTE)
    answered = events[1]
    assert isinstance(answered, ProviderCallResult)
    assert answered.outcome is CallOutcome.ANSWERED
    assert (answered.input_tokens, answered.output_tokens, answered.latency_ms) == (300, 90, 840)
    assert (answered.model_served, answered.provider_generation_id) == (MODEL, "gen-1")
    assert answered.usage == ProviderUsage("0.0002", "openrouter_credits")


@pytest.mark.parametrize(
    ("content", "reason"),
    [
        (None, Reason.REPLY_INCOMPLETE),
        ("I cannot read this invoice.", Reason.REPLY_NOT_JSON),
        ('{"total": "1.00"}', Reason.REPLY_SCHEMA_INVALID),
        (json.dumps(dict.fromkeys(extract.KEYS)), Reason.FIELD_MISSING),
    ],
)
def test_a_bad_reply_needs_review_and_the_batch_goes_on(
    content: str | None, reason: Reason
) -> None:
    result = handler(Scripted(report(content), perfect))(context(), batch(0, 2))
    first, second = items(result)
    assert (first["status"], first["reason"]) == ("needs_review", reason.value)
    if reason is not Reason.FIELD_MISSING:
        assert first["fields"] is None
    assert second["status"] == DOCUMENTS[1].expected_status.value


@pytest.mark.parametrize(
    ("error", "kind", "code"),
    [
        (
            ProviderTransientError(TransientCode.RATE_LIMITED),
            ProviderTransientError,
            TransientCode.RATE_LIMITED,
        ),
        (
            ProviderTransientError(TransientCode.TIMEOUT),
            ProviderTransientError,
            TransientCode.TIMEOUT,
        ),
        (
            ProviderPermanentError(PermanentCode.REJECTED),
            ProviderPermanentError,
            PermanentCode.REJECTED,
        ),
    ],
)
def test_a_provider_error_is_recorded_then_ends_the_attempt_fresh(
    error: Exception, kind: type[Exception], code: TransientCode | PermanentCode
) -> None:
    recorder = InMemoryCallRecorder()
    with pytest.raises(kind) as raised:
        handler(Scripted(perfect, error), recorder)(context(), batch(0, 3))
    assert raised.value is not error
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert raised.value.code is code  # type: ignore[attr-defined]
    results = recorder.results
    assert [r.outcome for r in results] == [CallOutcome.ANSWERED, CallOutcome.FAILED]
    assert results[1].failure_code is code
    assert len(recorder.intents) == 2  # the third document was never asked


@pytest.mark.parametrize("step", [ValueError("SENTINEL remote text"), "not a report", None])
def test_anything_else_is_a_protocol_violation_and_leaves_the_call_unresolved(step: object) -> None:
    recorder = InMemoryCallRecorder()
    extractor = Scripted(step if isinstance(step, BaseException) else (lambda _r: step))
    with pytest.raises(PermanentError) as raised:
        handler(extractor, recorder)(context(), batch(0, 1))
    assert str(raised.value) == "provider_protocol_violation"
    assert "SENTINEL" not in repr(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert len(recorder.unresolved()) == 1


def test_documents_beyond_the_cap_need_review_without_a_call() -> None:
    recorder = InMemoryCallRecorder()
    result = handler(Scripted(perfect, perfect), recorder, cap=2)(context(), batch(0, 4))
    reasons = [item["reason"] for item in items(result)]
    assert reasons[2:] == [Reason.PROVIDER_CALL_CAP_REACHED.value] * 2
    assert len(recorder.intents) == 2


def test_a_cancellation_stops_before_the_next_document() -> None:
    cancel = threading.Event()

    def cancel_after(request: ExtractionRequest) -> ExtractionReport:
        cancel.set()
        return perfect(request)

    extractor = Scripted(cancel_after, perfect)
    with pytest.raises(HandlerCancelled):
        handler(extractor)(context(cancel), batch(0, 2))
    assert len(extractor.requests) == 1


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"offset": 0},
        {"offset": -1, "items": []},
        {"offset": True, "items": []},
        {"offset": 0, "items": "text"},
        {"offset": 0, "items": [{"invoice_id": "x"}]},
        {"offset": 0, "items": [], "extra": 1},
    ],
)
def test_a_payload_that_is_not_a_batch_of_texts_is_permanent(payload: object) -> None:
    with pytest.raises(PermanentError, match="batch_payload_invalid"):
        handler(Scripted())(context(), payload)


def test_the_handler_needs_a_recorder_and_a_valid_cap() -> None:
    with pytest.raises(ValueError, match="recorder"):
        extraction_handler(
            Scripted(),
            ROUTE,
            TIMEOUT,
            max_provider_calls_per_attempt=1,
            recorder=None,  # type: ignore[arg-type]
        )
    with pytest.raises((TypeError, ValueError)):
        handler(Scripted(), cap=-1)


def test_nothing_the_model_wrote_reaches_the_item_result_except_canonical_fields() -> None:
    sentinel = canonical_reply(DOCUMENTS[0].expected).replace(
        DOCUMENTS[0].expected.vendor_name, "SENTINEL Vendor"
    )
    result = handler(Scripted(report(f"```json\n{sentinel}\n```")))(context(), batch(0, 1))
    (item,) = items(result)
    assert item["fields"]["vendor_name"] == "SENTINEL Vendor"  # the reading, kept to score it
    assert "```" not in json.dumps(item)


# --- the cell's call maximum covers extract ----------------------------------------------


def test_a_spent_call_maximum_fails_closed_before_the_intent(tmp_path: Path) -> None:
    counter = create_budget_file(tmp_path / "budget")
    inner = InMemoryCallRecorder()
    recorder = BudgetedRecorder(inner, CallBudget(counter, 1))
    extractor = Scripted(perfect, perfect)
    with pytest.raises(ProviderCallBudgetExhausted):
        handler(extractor, recorder)(context(), batch(0, 2))
    assert len(extractor.requests) == 1
    assert len(inner.intents) == 1
    assert isinstance(ProviderCallBudgetExhausted(), PermanentError)


# --- the canned model --------------------------------------------------------------------


def dry_provider(replies: dict[str, str]) -> tuple[OpenRouterProvider, CannedTransport]:
    transport = CannedTransport(replies)
    config = OpenRouterConfig(
        credential=SecretStr("dry-run-placeholder"), route_models={ROUTE: MODEL}
    )
    return OpenRouterProvider(config, transport), transport


def test_the_canned_model_reads_every_cell_document_perfectly() -> None:
    replies = perfect_replies(60, DEFAULT_SEED)
    provider, transport = dry_provider(replies)
    for document in DOCUMENTS[:5]:
        answer = provider.extract(
            ExtractionRequest(route_id=ROUTE, document=document.text, timeout=TIMEOUT)
        )
        assert answer.content == canonical_reply(document.expected)
        assert answer.input_tokens == len(document.text) // 4
        assert answer.output_tokens == len(answer.content) // 4
        assert answer.usage is not None
        assert answer.model_served == MODEL
    assert transport.requests == 5


def test_an_unknown_document_gets_nulls_and_needs_review() -> None:
    provider, _ = dry_provider({})
    answer = provider.extract(
        ExtractionRequest(route_id=ROUTE, document="no such invoice", timeout=TIMEOUT)
    )
    assert answer.content == UNKNOWN_DOCUMENT_REPLY
    assert extract.judge(extract.parse_reply(answer.content)).reason is Reason.FIELD_MISSING


def test_the_canned_amount_replies_are_unchanged() -> None:
    from tests.unit.test_openrouter_adapter import request

    provider, _ = dry_provider({})
    assert provider.call(request(raw="1,234")).response.value == "1234"


def test_document_keys_are_content_hashes() -> None:
    assert document_key("a") == document_key("a")
    assert document_key("a") != document_key("b")
    assert len(document_key("a")) == 64


# --- worker flags --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("workload", "choice", "documents", "seed", "fragment"),
    [
        (worker.WorkloadName.EXTRACT, ProviderChoice(), None, None, "needs --provider openrouter"),
        (worker.WorkloadName.EXTRACT, DRY, None, 1, "needs --documents"),
        (worker.WorkloadName.EXTRACT, DRY, 0, 1, "needs --documents"),
        (worker.WorkloadName.MIX, ProviderChoice(), 10, 1, "apply only to --workload extract"),
        (
            worker.WorkloadName.EXTRACT,
            ProviderChoice(
                name=ProviderName.OPENROUTER,
                model=MODEL,
                max_provider_calls=5,
                allow_live_calls=True,
            ),
            10,
            1,
            "apply only to an extract dry run",
        ),
    ],
)
def test_the_worker_refuses_inconsistent_workload_flags(
    workload: worker.WorkloadName,
    choice: ProviderChoice,
    documents: int | None,
    seed: int | None,
    fragment: str,
) -> None:
    refusal = worker.workload_refusal(workload, choice, documents, seed)
    assert refusal is not None
    assert fragment in refusal


def test_the_worker_accepts_a_dry_extract_run_and_the_default_mix() -> None:
    assert worker.workload_refusal(worker.WorkloadName.EXTRACT, DRY, 10, 1) is None
    assert worker.workload_refusal(worker.WorkloadName.MIX, ProviderChoice(), None, None) is None


def test_the_extract_worker_command_carries_the_batch_cap_and_the_dry_run_documents() -> None:
    command = run.worker_command(
        run.Profile.EXTRACT,
        DRY,
        Path("/tmp/budget"),
        "PT30S",
        cap=25,
        extra=("--documents", "100", "--seed", "3"),
    )
    assert command[1:5] == ["-m", "benchmarks.worker", "--workload", "extract"]
    assert command[command.index("--cap") + 1] == "25"
    assert command[command.index("--call-timeout") + 1] == "PT30S"
    assert command[-4:] == ["--documents", "100", "--seed", "3"]
    assert "--dry-run" in command
    with pytest.raises(ValueError, match="needs --provider openrouter"):
        run.worker_command(run.Profile.EXTRACT, ProviderChoice())


def test_the_invoice_worker_command_is_unchanged() -> None:
    assert run.worker_command(run.Profile.INVOICE) == [
        sys.executable, "-m", "benchmarks.worker", "--cap", "16", "--call-timeout", "PT0.1S",
    ]  # fmt: skip


# --- per-cell worker settings ----------------------------------------------------------------


def validated(settings: dict[str, str]) -> WorkerSettings:
    return run.bench_settings(settings)


def test_the_double_keeps_the_bench_settings_exactly() -> None:
    cell = run.Cell(run.Profile.INVOICE, 100, 25, 2)
    assert run.cell_worker_settings(cell, ProviderChoice(), "PT0.1S") == dict(
        run.BENCH_WORKER_SETTINGS
    )


def test_a_dry_invoice_cell_at_the_default_timeout_keeps_two_minutes() -> None:
    cell = run.Cell(run.Profile.INVOICE, 100, 25, 2)
    assert run.cell_worker_settings(cell, DRY, "PT0.1S") == dict(run.BENCH_WORKER_SETTINGS)


@pytest.mark.parametrize(
    ("profile", "batch", "timeout", "expected"),
    [
        (run.Profile.INVOICE, 25, "PT30S", "PT578S"),  # 16 x (30 + 6) + 2
        (run.Profile.EXTRACT, 100, "PT30S", "PT3602S"),  # 100 x (30 + 6) + 2
        (run.Profile.EXTRACT, 5, "PT30S", "PT182S"),
        (run.Profile.EXTRACT, 1, "PT1S", "PT2M"),  # never below the bench's own
    ],
)
def test_an_openrouter_cell_gets_an_execution_timeout_that_fits_its_cap(
    profile: run.Profile, batch: int, timeout: str, expected: str
) -> None:
    cell = run.Cell(profile, 100, batch, 2)
    settings = run.cell_worker_settings(cell, DRY, timeout)
    assert settings["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] == expected
    changed = {k for k in settings if settings[k] != run.BENCH_WORKER_SETTINGS[k]}
    assert changed <= {"ORQESTRA_WORKER_EXECUTION_TIMEOUT"}
    cap = batch if profile is run.Profile.EXTRACT else worker.DEFAULT_CAP
    check_provider_call_budget(
        validated(settings),
        max_provider_calls_per_attempt=cap,
        provider_call_timeout=worker.parse_duration(timeout),
    )


# --- runner flags --------------------------------------------------------------------------

DRY_ARGS = ["--provider", "openrouter", "--model", MODEL, "--max-provider-calls", "50", "--dry-run"]
EXTRACT = ["--profile", "extract", "--size", "20", "--batch", "5", "--workers", "1"]


@pytest.mark.parametrize(
    ("arguments", "fragment"),
    [
        (EXTRACT, "needs --provider openrouter"),
        (
            [*EXTRACT, "--provider", "openrouter", "--model", MODEL, "--dry-run"],
            "--max-provider-calls",
        ),
        ([*EXTRACT, *DRY_ARGS, "--price-in-per-mtok", "2.5"], "synthetic prices"),
        (
            [
                *EXTRACT[:-2],
                "--workers",
                "1",
                "--provider",
                "openrouter",
                "--model",
                MODEL,
                "--max-provider-calls",
                "50",
                "--allow-live-calls",
            ],
            "price",
        ),
        (
            [
                "--profile",
                "invoice",
                "--size",
                "10",
                "--batch",
                "5",
                "--workers",
                "1",
                "--price-out-per-mtok",
                "0.5",
            ],
            "apply only to the extract profile",
        ),
        (["--profile", "extract", "--size", "20", "--workers", "1", *DRY_ARGS], "batch"),
        (
            ["--profile", "echo", "--size", "10", "--workers", "1", *DRY_ARGS],
            "only the invoice profile",
        ),
    ],
)
def test_benchmarks_run_refuses_a_wrong_extract_cell_before_any_database(
    arguments: list[str],
    fragment: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ORQESTRA_OPENROUTER_API_KEY", "sk-SENTINEL-key")
    monkeypatch.setattr(run, "refuse_dirty", lambda _allowed: None)
    monkeypatch.setattr(run, "run_in_bench_database", _never)
    assert run.main(arguments) == 1
    err = capsys.readouterr().err
    assert fragment in err
    assert "SENTINEL" not in err


def _never(*args: object, **kwargs: object) -> Any:
    raise AssertionError


def test_prices_are_required_for_a_live_extract_cell_and_canned_for_a_dry_one() -> None:
    cell = run.Cell(run.Profile.EXTRACT, 10, 5, 1)
    assert run.prices_of(cell, DRY, None, None) == extract.CANNED_PRICES
    live = ProviderChoice(
        name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=5, allow_live_calls=True
    )
    prices = run.prices_of(cell, live, Decimal("2.5"), Decimal("7.25"))
    assert prices == extract.Prices(Decimal("2.5"), Decimal("7.25"))
    assert prices is not None
    assert not prices.canned
    with pytest.raises(ValueError, match="non-negative"):
        extract.Prices(Decimal("-1"), Decimal("1"))


def test_the_extract_dry_run_preview_states_the_calls_and_the_timeout() -> None:
    cell = run.Cell(run.Profile.EXTRACT, 100, 25, 2)
    lines = run.dry_run_preview(cell, DRY, "PT30S")
    assert "expected provider calls (one per document): 100" in lines[0]
    assert lines[2].endswith(f": {25 * 3 * 4}")
    assert lines[3].endswith(": PT902S")  # 25 x 36 + 2
