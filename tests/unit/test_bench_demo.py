"""The demonstration's pure parts, on fakes only: no database, worker or subprocess."""

import json
import re
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

import benchmarks.demo
from benchmarks import report
from benchmarks.database import BENCH_DATABASE_URL_ENV
from benchmarks.demo import (
    DEMO_DIR,
    LABEL,
    TAGS,
    CliResult,
    DemoConfig,
    VictimWatch,
    bound_of,
    cli,
    figure_lines,
    main,
    status_check,
    submission_checks,
    summary_against_oracle,
    usage_against_oracle,
)
from benchmarks.drills import DrillFleet, KillDrill
from benchmarks.generate import build_tasks, expected_outcome, generate_invoices
from benchmarks.result import RAW_DIR, RESULTS_DIR, ROOT, write_result
from benchmarks.run import MAX_ATTEMPTS, ORQESTRA
from benchmarks.worker import DEFAULT_CAP
from tests.unit.test_bench_report import cell_record

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

DOC = ROOT / "docs" / "demo.md"
_LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
_SCHEDULE_LABEL = re.compile(r"\bday ?\d+[a-z]?\b", re.IGNORECASE)
_OLD_TERM = re.compile(r"\bai\b", re.IGNORECASE)


def default_oracle() -> Any:
    config = DemoConfig()
    invoices = generate_invoices(config.invoices, config.seed)
    return build_tasks(invoices, config.batch, MAX_ATTEMPTS), expected_outcome(
        invoices, config.batch, DEFAULT_CAP
    )


# --- configuration -------------------------------------------------------------------------


def test_the_defaults_are_the_documented_run() -> None:
    config = DemoConfig()
    assert (config.invoices, config.seed, config.batch, config.workers) == (5000, 20261107, 100, 4)
    assert config.idempotency_key == "demo-20261107"
    assert config.batches == 50
    with pytest.raises(SystemExit):
        main(["--help"])


def test_the_defaults_give_the_oracle_and_the_bound() -> None:
    tasks, expected = default_oracle()
    counts = (expected.verified, expected.rejected, expected.needs_review, expected.malformed)
    assert counts == (4825, 150, 0, 25)
    assert expected.calls == expected.provider_assisted == 250
    assert bound_of(tasks) == 16 * 3 * 50


@pytest.mark.parametrize(
    "overrides",
    [{"workers": 1}, {"invoices": 200, "batch": 100, "workers": 2}, {"stop_after": 0.0}],
    ids=["one-worker", "too-few-batches", "no-time"],
)
def test_a_demo_that_cannot_show_recovery_is_refused(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match=r"demo|stop_after"):
        DemoConfig(**overrides)


def test_the_record_goes_where_git_and_the_report_never_look() -> None:
    assert DEMO_DIR.is_relative_to(RAW_DIR)
    assert not DEMO_DIR.is_relative_to(RESULTS_DIR)


# --- the commands --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stdout", "document"),
    [('{"status": "ok", "applied": []}\n', {"status": "ok", "applied": []}), ("", {}), ("x", {})],
    ids=["json", "empty", "not-json"],
)
def test_cli_runs_the_installed_command_in_the_raw_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stdout: str, document: dict[str, Any]
) -> None:
    seen: dict[str, Any] = {}

    def run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        seen.update(command=command, **kwargs)
        return subprocess.CompletedProcess(command, 0, stdout, "warning\n")

    monkeypatch.setenv("ORQESTRA_ENVIRONMENT", "production")
    monkeypatch.setattr(subprocess, "run", run)
    result = cli("postgresql:///orqestra_bench", ["migrate"], tmp_path, "migrate")

    assert seen["command"] == [str(ORQESTRA), "migrate"]
    assert seen["cwd"] == tmp_path
    assert [k for k in seen["env"] if k.startswith("ORQESTRA_")] == ["ORQESTRA_DATABASE_URL"]
    assert result.document == document
    assert (tmp_path / "migrate.json").read_text() == stdout
    assert result.stderr_path.read_text() == "warning\n"


def _cli(code: int = 0, **document: Any) -> CliResult:
    return CliResult(code, {"status": "ok", **document}, Path("x.log"))


def _names(checks: list[dict[str, Any]]) -> dict[str, bool]:
    return {check["name"]: check["passed"] for check in checks}


def test_submission_checks() -> None:
    first = _cli(created=True, job_id="j1")
    assert _names(submission_checks(first, _cli(created=False, job_id="j1"))) == {
        "submit_created": True,
        "resubmit_same_job": True,
    }
    assert not _names(submission_checks(first, _cli(created=False, job_id="j2")))[
        "resubmit_same_job"
    ]
    assert not _names(submission_checks(first, _cli(created=True, job_id="j1")))[
        "resubmit_same_job"
    ]
    assert not _names(submission_checks(_cli(1, created=True, job_id="j1"), first))[
        "submit_created"
    ]


def _status(**counts: int) -> dict[str, Any]:
    base = {"queued": 0, "running": 0, "succeeded": 50, "failed": 0, "cancelled": 0}
    return {"job_status": "succeeded", "task_counts": base | counts}


def test_status_check() -> None:
    assert status_check(_status(), 50)["passed"]
    assert not status_check(_status(succeeded=49, queued=1), 50)["passed"]


def _summary(expected: Any, **changes: Any) -> dict[str, Any]:
    document = {
        "verified": expected.verified,
        "rejected": expected.rejected,
        "needs_review": expected.needs_review,
        "malformed": expected.malformed,
        "provider_assisted_items": expected.provider_assisted,
        "expected_items": expected.invoices,
        "processed_items": expected.invoices,
        "complete": True,
        "integrity_ok": True,
        "anomalies": {"index_duplicate": 0},
    }
    return document | changes


def test_the_summary_must_equal_the_oracle() -> None:
    _, expected = default_oracle()
    assert summary_against_oracle(_summary(expected), expected)["passed"]


@pytest.mark.parametrize(
    "change",
    [
        {"verified": 4824},
        {"rejected": 151},
        {"needs_review": 1},
        {"malformed": 24},
        {"provider_assisted_items": 249},
        {"processed_items": 4999},
        {"complete": False},
        {"integrity_ok": False},
        {"anomalies": {"index_duplicate": 1}},
    ],
)
def test_any_summary_difference_fails(change: dict[str, Any]) -> None:
    _, expected = default_oracle()
    assert not summary_against_oracle(_summary(expected, **change), expected)["passed"]


def _usage(expected: Any, **changes: Any) -> dict[str, Any]:
    values = {
        "finality": "closed",
        "verdict": "within",
        "bound": 2400,
        "calls": expected.calls,
        "tokens": str(expected.tokens),
    } | changes
    return {
        "finality": values["finality"],
        "budget": {"verdict": values["verdict"], "bound": values["bound"]},
        "by_attempt_state": {
            "succeeded": {
                "calls": values["calls"],
                "reported_totals": {"tokens": {"amount": values["tokens"], "calls": 1}},
            }
        },
    }


def test_the_usage_must_be_closed_within_and_equal_the_oracle() -> None:
    _, expected = default_oracle()
    assert usage_against_oracle(_usage(expected), expected, 2400)["passed"]


@pytest.mark.parametrize(
    "change",
    [
        {"finality": "lower_bound", "verdict": "unknown"},
        {"verdict": "exceeded"},
        {"bound": 2399},
        {"calls": 249},
        {"tokens": "not a number"},
    ],
)
def test_any_usage_difference_fails(change: dict[str, Any]) -> None:
    _, expected = default_oracle()
    assert not usage_against_oracle(_usage(expected, **change), expected, 2400)["passed"]


def test_tokens_off_by_one_fail() -> None:
    _, expected = default_oracle()
    document = _usage(expected, tokens=str(expected.tokens + 1))
    assert not usage_against_oracle(document, expected, 2400)["passed"]


# --- timings are printed, never checked ----------------------------------------------------


def test_every_timing_line_is_labelled_and_none_is_not_measured() -> None:
    figures = {"timing": {"makespan_s": None}, "throughput": {"makespan_items_per_s": None}}
    lines = figure_lines(figures, {"recovery_delay_s": {"max": None}})
    assert [tag for tag, _ in lines] == ["makespan", "throughput", "recovery"]
    for _, text in lines:
        assert LABEL in text
        assert "not measured" in text


# --- the victim's revocation and replacement -----------------------------------------------


class Rows:
    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self.row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row


class FakeConn:
    """Answers the victim and successor reads from ``victims`` and ``successors`` in turn."""

    def __init__(self, victims: list[tuple[Any, ...]], successors: list[Any]) -> None:
        self.victims, self.successors = victims, successors

    def execute(self, query: str, params: tuple[Any, ...]) -> Rows:
        source = self.victims if "JOIN tasks" in query else self.successors
        return Rows(source.pop(0) if len(source) > 1 else source[0])


class FakeFleet:
    def __init__(self) -> None:
        self.started = 0

    def start(self) -> Any:
        self.started += 1
        return type("Handle", (), {"process": type("P", (), {"pid": 9000 + self.started})})()


AT = datetime(2026, 11, 8, 12, tzinfo=UTC)
TASK = uuid.uuid4()


def _victim_row(state: str, reason: str | None) -> tuple[Any, ...]:
    return (state, reason, AT, AT if reason else None, TASK, 1, "batch-07")


def _killed(drill: KillDrill) -> KillDrill:
    drill.injected = True
    drill.injections.append({"pid": 4242, "attempt_id": str(uuid.uuid4()), "killed_at": "t"})
    return drill


def test_the_watch_prints_each_line_once_and_restarts_once() -> None:
    lines: list[tuple[str, str]] = []
    emit = lambda tag, text: lines.append((tag, text))  # noqa: E731
    watch = VictimWatch()
    drill = _killed(KillDrill(1, at_call_boundary=True))
    conn = FakeConn(
        [_victim_row("running", None), _victim_row("revoked", "lease_expired")],
        [None, (uuid.uuid4(), "host:5151:abc", AT)],
    )
    fleet = FakeFleet()

    watch.after_step(drill, 15, 50, emit)
    watch.after_step(drill, 16, 50, emit)
    for _ in range(4):
        watch.observe(conn, cast(DrillFleet, fleet), emit)

    assert [tag for tag, _ in lines] == ["kill", "revoked", "replaced", "restarted"]
    assert "pid 5151" in lines[2][1]
    assert fleet.started == 1
    assert all(check["passed"] for check in watch.checks())


def test_no_successor_fails_the_replacement_check() -> None:
    watch = VictimWatch()
    drill = _killed(KillDrill(1, at_call_boundary=True))
    conn = FakeConn([_victim_row("revoked", "lease_expired")], [None])
    watch.after_step(drill, 15, 50, lambda tag, text: None)
    watch.observe(conn, cast(DrillFleet, FakeFleet()), lambda tag, text: None)
    assert _names(watch.checks()) == {"revocation_seen": True, "replacement_seen": False}


# --- the command ---------------------------------------------------------------------------


def test_without_the_database_url_the_demo_is_refused(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv(BENCH_DATABASE_URL_ENV, raising=False)
    monkeypatch.setattr(benchmarks.demo, "refuse_dirty", lambda allow: None)
    assert main([]) == 1
    assert capsys.readouterr().err.startswith("refused:")


def test_one_worker_is_refused_before_anything_runs(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(BENCH_DATABASE_URL_ENV, "postgresql:///orqestra_bench")
    assert main(["--workers", "1"]) == 1
    assert capsys.readouterr().err.startswith("refused:")


# --- docs/demo.md --------------------------------------------------------------------------


def test_the_page_explains_every_printed_line() -> None:
    text = DOC.read_text(encoding="utf-8")
    for tag in TAGS:
        assert f"`{tag}`" in text, tag
    assert "DEC-062" in text
    assert "python -m benchmarks.worker" in text
    assert _OLD_TERM.search(text) is None
    assert _SCHEDULE_LABEL.search(text) is None


def test_every_relative_link_of_the_page_resolves() -> None:
    targets = _LINK.findall(DOC.read_text(encoding="utf-8"))
    assert targets
    for target in targets:
        path_part, _, anchor = target.partition("#")
        path = (DOC.parent / path_part).resolve()
        assert path.exists(), target
        if anchor:
            headings = re.findall(r"^#{1,6}\s+(.*?)\s*$", path.read_text(), re.MULTILINE)
            slugs = {re.sub(r"[^\w\- ]", "", h.strip().lower()).replace(" ", "-") for h in headings}
            assert anchor in slugs, target


# --- the report never reads a demo record --------------------------------------------------


def test_a_demo_record_in_a_results_subfolder_leaves_the_report_unchanged(tmp_path: Path) -> None:
    before = report.render(tmp_path)
    record = cell_record(ladder="demo", profile="invoice", size=200, batch=100, workers=2)
    record["drill"] = {"name": "demo"}
    write_result(record, tmp_path / "demo", allow_dirty=True)
    assert report.render(tmp_path) == before
    assert json.loads(next((tmp_path / "demo").glob("*.json")).read_text())["ladder"] == "demo"
