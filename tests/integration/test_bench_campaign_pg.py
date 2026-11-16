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

import json
from collections.abc import Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks import campaign
from benchmarks.campaign import (
    LOG_NAME,
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
from benchmarks.live import API_KEY_ENV, ProviderChoice, ProviderName
from benchmarks.result import Status, write_result
from benchmarks.run import Cell, Profile, run_cell
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
MODEL = "vendor/model-a"
RUN_ID = "campaignpg"
STOP = 180.0
MIX = PlannedCell(
    step=Step.RULES_FIRST, tool=Tool.RUN, workload=Workload.MIX, size=100, batch=25, workers=2
)


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
    assert charge_for(step_figures(record), MIX.call_maximum, done.ceiling) == done.charge
    started = [event for event in CampaignLog(out_dir / LOG_NAME).events()]
    assert [event["event"] for event in started] == ["cell_started", "cell_finished"]
    assert started[0]["call_maximum"] == MIX.call_maximum
    assert SENTINEL not in (out_dir / LOG_NAME).read_text()
