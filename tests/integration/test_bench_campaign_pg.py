"""The campaign's budget against a real usage ledger (DEC-072).

The campaign charges each cell from the result file its command wrote, so the
budget is only as good as that file's agreement with the ledger. Here one campaign
cell runs end to end on real worker processes, real PostgreSQL and the canned
transport: ``Campaign.run_cell`` builds the command, a bridge runs it as
``run_cell`` in a schema of the test database (``benchmarks.run`` itself only
accepts the benchmark database) with the call maximum from that command, and the
campaign reads the result back. Its charge and its billing row must match the
ledger and the cell's call counter.
"""

import contextlib
import io
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest import mock

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

from benchmarks import baseline, campaign
from benchmarks import drills as drills_module
from benchmarks import result as result_module
from benchmarks import run as run_module
from benchmarks.campaign import (
    LOG_NAME,
    UNBOUNDED_CALL_COST,
    Basis,
    Campaign,
    CampaignLog,
    Finished,
    PlannedCell,
    Settings,
    Step,
    Tool,
    Workload,
    billing_row,
    charge_for,
    step_figures,
)
from benchmarks.database import check_run_schema
from benchmarks.documents import generate_documents
from benchmarks.live import API_KEY_ENV, ProviderChoice, ProviderName
from benchmarks.result import ROOT, Status, code_state, validate_result, write_result
from benchmarks.run import Cell, Profile, run_cell
from orqestra.providers.openrouter import EXTRACT_SYSTEM_PROMPT
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
RUN_ID = "campaignpg"
STOP = 180.0
MIX = PlannedCell(
    step=Step.RULES_FIRST, tool=Tool.RUN, workload=Workload.MIX, size=100, batch=25, workers=2
)


class NoExtraction:
    """The mix cell needs no extraction limits; any use of them fails the test."""

    @property
    def prompt_bytes(self) -> int:
        raise AssertionError

    @property
    def max_output_tokens(self) -> int:
        raise AssertionError

    def largest_document_bytes(self, size: int) -> int:
        raise AssertionError


class Bridge:
    """Runs a campaign's ``benchmarks.run`` command as ``run_cell`` in a test schema."""

    def __init__(self, schema_url: str, raw: Path, results: Path) -> None:
        self.schema_url = schema_url
        self.raw = raw
        self.results = results
        self.environments: list[dict[str, str]] = []

    def __call__(self, argv: Sequence[str], env: Mapping[str, str]) -> Finished:
        self.environments.append(dict(env))

        def value(flag: str) -> str:
            return argv[list(argv).index(flag) + 1]

        assert "--dry-run" in argv
        assert value("--provider") == "openrouter"
        cell = Cell(
            Profile(value("--profile")),
            int(value("--size")),
            int(value("--batch")),
            int(value("--workers")),
        )
        choice = ProviderChoice(
            name=ProviderName.OPENROUTER,
            model=value("--model"),
            max_provider_calls=int(value("--max-provider-calls")),
            dry_run=True,
        )
        record = run_cell(
            self.schema_url,
            cell,
            worker_settings=ENVIRONMENT,
            raw_dir=self.raw / RUN_ID / cell.name,
            stop_after=STOP,
            run_id=RUN_ID,
            idle_window=0.5,
            provider=choice,
        )
        path = write_result(record, self.results, allow_dirty=True)
        return Finished(0, f"{record['status']} {path}\n", "")


def ledger(db: psycopg.Connection[TupleRow]) -> tuple[int, Decimal]:
    row = db.execute(
        "SELECT count(*), coalesce(sum(r.reported_amount), 0) FROM provider_calls c "
        "LEFT JOIN provider_call_results r USING (attempt_id, call_seq)"
    ).fetchone()
    assert row is not None
    return int(row[0]), Decimal(row[1])


def test_a_campaign_cells_charge_and_billing_row_match_its_ledger(
    db: psycopg.Connection[TupleRow],
    schema_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = tmp_path / "raw"
    monkeypatch.setattr(campaign, "RAW_DIR", raw)
    out_dir = tmp_path / "campaign"
    out_dir.mkdir()
    bridge = Bridge(schema_url, raw, tmp_path / "results")
    settings = Settings(
        campaign_id="c1",
        model=MODEL,
        live=False,
        price_in=None,
        price_out=None,
        out_dir=out_dir,
    )
    run = Campaign(
        settings,
        Decimal(45),
        campaign.APPROVED_PLAN,
        bridge,
        {API_KEY_ENV: SENTINEL},
        CampaignLog(out_dir / LOG_NAME),
        NoExtraction(),
        alive=list,
    )

    done = run.run_cell(MIX)

    assert done is not None
    assert [API_KEY_ENV in env for env in bridge.environments] == [False]
    assert done.figures is not None
    assert done.figures.status == Status.COMPLETE
    calls, credits = ledger(db)
    assert calls == MIX.expected_calls  # the oracle's calls, all within the maximum
    assert done.figures.calls == calls
    assert done.figures.credits == credits
    # The mix has no token figures, so every call is charged at the ceiling.
    assert done.charge.basis is Basis.CALLS
    assert done.charge.cost == calls * done.ceiling
    assert run.spent == done.charge.cost

    row = billing_row(done, live=False)
    assert row["calls"] == calls
    assert row["counted_calls"] == calls
    assert row["counted_minus_calls"] == 0
    assert row["transport_requests"] == calls
    assert Decimal(row["credits"]) == credits

    record: dict[str, Any] = json.loads(Path(row["result"]).read_text())
    assert step_figures(record) == done.figures
    again = charge_for(
        step_figures(record), ceiling=done.ceiling, call_maximum=MIX.call_maximum, reserved=None
    )
    assert again == done.charge
    assert done.ceiling == UNBOUNDED_CALL_COST
    started = [event for event in CampaignLog(out_dir / LOG_NAME).events()]
    assert [event["event"] for event in started] == ["cell_started", "cell_finished"]
    assert started[0]["call_maximum"] == MIX.call_maximum
    assert SENTINEL not in (out_dir / LOG_NAME).read_text()


# --- the whole campaign, as a dry run -------------------------------------------------

# Every step, every tool and both workloads, at sizes that keep the suite short. The
# approved sizes are pinned by the unit tests; the full-size dry run is one command
# against the benchmark database (``python -m benchmarks.campaign run --dry-run``).
SMALL_PLAN = campaign.CampaignPlan(
    pilot=40,
    pilot_batch=10,
    pilot_workers=2,
    scaling=60,
    scaling_batch=10,
    worker_counts=(1, 2),
    repetitions=1,
    task_size=40,
    task_size_batches=(5, 20),
    task_size_workers=2,
    mix_same_size=100,
    mix_same_size_batch=25,
    mix=200,
    mix_batch=50,
    mix_workers=2,
    failures=300,
    failure_batch=25,
    rate_limit_batch=5,
    failure_workers=2,
    baseline=30,
    large_max=120,
    large_batch=10,
)


class MainBridge:
    """Runs each command the campaign builds through that tool's real ``main(argv)``.

    Argv parsing, refusals, the dry-run preview and the ``<status> <path>`` line are the
    commands' own. Only the database seam is replaced (``database_seam``): the commands
    reach the integration database instead of the guarded benchmark one. The child's
    environment is the one the campaign built, set for the call. Workers are real
    processes.
    """

    MODULES: Mapping[str, Any] = {
        "benchmarks.run": run_module,
        "benchmarks.drills": drills_module,
        "benchmarks.baseline": baseline,
    }

    def __init__(self) -> None:
        self.environments: list[dict[str, str]] = []
        self.argvs: list[list[str]] = []

    def __call__(self, argv: Sequence[str], env: Mapping[str, str]) -> Finished:
        self.environments.append(dict(env))
        self.argvs.append(list(argv))
        assert argv[1] == "-m"
        module = self.MODULES[argv[2]]
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, env, clear=True),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = module.main(list(argv[3:]))
        return Finished(code, out.getvalue(), err.getvalue())


def database_seam(monkeypatch: pytest.MonkeyPatch, database_url: str, raw: Path) -> None:
    """Point ``benchmarks.run`` and ``benchmarks.drills`` at the integration database.

    The bench guard (``connect_verified``, and the target check inside
    ``create_run_schema`` and ``drop_run_schema``) is the only thing replaced: the
    schema name checks and the SQL stay the real ones. Raw files go to ``raw``; the tree
    counts as clean, as a submit's tree is.
    """

    def connect(url: str) -> psycopg.Connection[TupleRow]:
        return psycopg.connect(url, autocommit=True)

    def create(conn: psycopg.Connection[TupleRow], schema: str) -> None:
        name = check_run_schema(schema)
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    def drop(conn: psycopg.Connection[TupleRow], schema: str) -> None:
        name = check_run_schema(schema)
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))

    clean = {"git_sha": code_state()["git_sha"], "git_dirty": False}
    for module in (run_module, drills_module):
        monkeypatch.setattr(module, "require_bench_database_url", lambda environ: database_url)
        monkeypatch.setattr(module, "connect_verified", connect)
        monkeypatch.setattr(module, "create_run_schema", create)
        monkeypatch.setattr(module, "drop_run_schema", drop)
    for module in (run_module, drills_module, baseline, campaign):
        monkeypatch.setattr(module, "RAW_DIR", raw)
    for module in (run_module, drills_module, baseline):
        monkeypatch.setattr(module, "refuse_dirty", lambda allow_dirty: None)
    monkeypatch.setattr(result_module, "code_state", lambda: clean)
    monkeypatch.setattr(baseline, "code_state", lambda: clean)


def test_the_whole_campaign_runs_as_a_dry_run_and_writes_every_file(
    database_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = tmp_path / "raw"
    database_seam(monkeypatch, database_url, raw)
    out_dir = tmp_path / "campaign"
    out_dir.mkdir()
    bridge = MainBridge()
    settings = Settings(
        campaign_id="dry1", model=MODEL, live=False, price_in=None, price_out=None, out_dir=out_dir
    )
    log = CampaignLog(out_dir / LOG_NAME)
    run = Campaign(
        settings,
        Decimal(45),
        SMALL_PLAN,
        bridge,
        {**os.environ, API_KEY_ENV: SENTINEL},  # set, as for a live run; no child may see it
        log,
        campaign.connect_limits(),
        alive=list,
    )

    assert run.execute() == 0

    events = list(log.events())
    kinds = [event["event"] for event in events]
    assert kinds[0] == "campaign_started"
    assert kinds[-1] == "campaign_finished"
    assert "pilot_passed" in kinds
    assert "campaign_stopped" not in kinds
    assert not run.skipped
    planned = [cell.key for cell in SMALL_PLAN.cells()]
    large = [key for key in run.runs if key.startswith("s8-")]
    assert len(large) == 1
    assert sorted(run.runs) == sorted([*planned, *large])
    assert {argv[2] for argv in bridge.argvs} == {
        "benchmarks.run",
        "benchmarks.drills",
        "benchmarks.baseline",
    }
    assert all("--dry-run" in argv for argv in bridge.argvs)
    for key, cell_run in run.runs.items():
        assert cell_run.result is not None, key
        assert cell_run.figures is not None, key
        assert cell_run.figures.status == Status.COMPLETE, key
        record = json.loads(cell_run.result.read_text(encoding="utf-8"))
        if record["format"] == 1:
            validate_result(
                {**record, "environment": {**record["environment"], "dirty_allowed": True}}
            )
    gates = [event for event in events if event["event"] == "gate"]
    assert gates
    assert all(gate["passed"] for gate in gates)
    assert {gate["gate"] for gate in gates} >= {
        "field_accuracy",
        "cost_per_call",
        "no_permanent_failure",
        "input_tokens_bound",
        "output_tokens_bound",
    }
    assert run.spent == sum((cell_run.charge.cost for cell_run in run.runs.values()), Decimal(0))
    assert run.spent <= Decimal(45)

    billing = json.loads((out_dir / campaign.BILLING_NAME).read_text(encoding="utf-8"))
    assert billing["final"] is True
    assert len(billing["cells"]) == len(run.runs)
    summary = json.loads((out_dir / campaign.SUMMARY_NAME).read_text(encoding="utf-8"))
    assert summary["pilot_passed"] is True
    assert summary["stopped"] is None
    assert summary["prices_per_mtok"]["synthetic"] is True
    assert len(summary["cells"]) == len(run.runs)

    # No child saw the key, and no file holds it, a prompt or a document's text.
    assert bridge.environments
    assert all(API_KEY_ENV not in env for env in bridge.environments)
    forbidden = [SENTINEL, EXTRACT_SYSTEM_PROMPT, generate_documents(1)[0].text]
    files = [path for path in out_dir.rglob("*") if path.is_file()]
    leaks = [
        str(path)
        for path in files
        if any(item in path.read_text(encoding="utf-8", errors="replace") for item in forbidden)
    ]
    assert leaks == []
    texts = (out_dir / LOG_NAME).read_text(encoding="utf-8")
    assert "E-" not in texts  # every decimal is fixed-point text


@pytest.mark.parametrize("tool", [Tool.RUN, Tool.DRILL, Tool.BASELINE])
def test_each_tool_takes_a_campaign_command_as_a_real_process(
    tool: Tool, database_url: str, tmp_path: Path
) -> None:
    """The campaign's argv, run as the real command with a database that is not the
    benchmark one: it parses (never exit 2) and stops at the database guard or the
    dirty-tree check, never at a provider refusal. The baseline needs no database, so on
    a clean tree it runs its dry run to the end."""
    cell = next(c for c in campaign.APPROVED_PLAN.cells() if c.tool is tool)
    settings = Settings(
        campaign_id="sub1", model=MODEL, live=False, price_in=None, price_out=None, out_dir=tmp_path
    )
    argv = campaign.command_for(cell, settings, cell.call_maximum)
    env = {key: value for key, value in os.environ.items() if key != API_KEY_ENV}
    env["ORQESTRA_BENCH_DATABASE_URL"] = database_url  # the test database: refused
    done = subprocess.run(  # a fixed command
        argv, cwd=ROOT, env=env, capture_output=True, text=True, timeout=600, check=False
    )
    error = done.stderr
    assert done.returncode != 2, error  # argparse accepted every flag
    if tool is Tool.BASELINE and done.returncode == 0:
        assert done.stdout.splitlines()[-1].startswith("complete ")
        return
    assert done.returncode == 1, error
    assert error.startswith("refused: "), error
    assert "orqestra_bench" in error or "uncommitted changes" in error, error
