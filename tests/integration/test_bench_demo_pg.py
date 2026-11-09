"""The demonstration end to end, on a throwaway schema of the integration database.

``run_demo`` migrates the empty schema through the installed ``orqestra``,
submits twice, starts real benchmark workers, kills one at a call boundary and
checks everything it prints. Only the checks are asserted, never a timing:
``STOP`` bounds a hung run. The workers use the short test profile of the
process-level tests (lease ``PT4S``).
"""

from pathlib import Path

import pytest

from benchmarks.demo import TAGS, DemoConfig, run_demo
from benchmarks.result import validate_result
from tests.integration.test_bench_smoke_pg import INVOICE_CHECKS
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

STOP = 180.0
DEMO_CHECKS = (
    "migrated",
    "submit_created",
    "resubmit_same_job",
    "revocation_seen",
    "replacement_seen",
    "worker_restarted",
    "cli_status",
    "cli_summary",
    "cli_usage",
)
KILL_CHECKS = (
    "injected",
    "victims_revoked_lease_expired",
    "victims_closed_by_recovery_t3",
    "victims_capture",
    "victims_ledger_kept",
)


def test_the_demo_passes_every_check_and_prints_every_step(schema_url: str, tmp_path: Path) -> None:
    lines: list[tuple[str, str]] = []
    outcome = run_demo(
        schema_url,
        DemoConfig(invoices=2_000, batch=100, workers=2, stop_after=STOP),
        worker_settings=ENVIRONMENT,
        raw_dir=tmp_path / "raw",
        out_dir=tmp_path / "results",
        allow_dirty=True,
        emit=lambda tag, text: lines.append((tag, text)),
    )

    failed = [check["name"] for check in outcome.checks if not check["passed"]]
    assert failed == []
    assert outcome.passed
    names = [check["name"] for check in outcome.checks]
    assert set(INVOICE_CHECKS) | set(KILL_CHECKS) | set(DEMO_CHECKS) <= set(names)
    printed = [tag for tag, _ in lines]
    run_tags = [tag for tag in TAGS if tag not in ("database", "schema", "wall time", "verdict")]
    assert printed == run_tags
    resubmit = dict(lines)["resubmit"]
    assert '"created": false' in resubmit
    assert "same job id: yes" in resubmit
    validate_result(outcome.record)
    assert outcome.path.parent == tmp_path / "results"
    assert outcome.record["drill"]["measured"]["recovery_delay_s"]["n"] == 1
