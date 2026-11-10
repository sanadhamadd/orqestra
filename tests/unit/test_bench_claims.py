"""The claim-cost command's refusals and code state, without a database."""

import subprocess
from pathlib import Path
from typing import NoReturn

import psycopg
import pytest

from benchmarks import claims, result
from benchmarks.database import BENCH_DATABASE_URL_ENV

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SECRET = "s3cr"


def _refused_once(capsys: pytest.CaptureFixture[str]) -> str:
    captured = capsys.readouterr()
    shown = captured.out + captured.err
    assert "Traceback" not in shown
    assert SECRET not in shown
    lines = captured.err.splitlines()
    assert len(lines) == 1
    assert lines[0].startswith("refused: ")
    return lines[0]


def test_a_malformed_url_is_refused_in_one_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    url = f"postgresql://bench:{SECRET}%zzet@localhost:5433/orqestra_bench"
    monkeypatch.setenv(BENCH_DATABASE_URL_ENV, url)
    assert claims.main(["--out", str(tmp_path / "claims.json")]) == 1
    assert "not a valid PostgreSQL connection string" in _refused_once(capsys)


def test_an_unreachable_database_is_refused_in_one_line(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    def unreachable(url: str) -> NoReturn:
        msg = "connection refused\n\tIs the server running?"
        raise psycopg.OperationalError(msg)

    monkeypatch.setenv(BENCH_DATABASE_URL_ENV, "postgresql://bench@localhost:5433/orqestra_bench")
    monkeypatch.setattr(claims, "connect_verified", unreachable)
    assert claims.main(["--out", str(tmp_path / "claims.json")]) == 1
    line = _refused_once(capsys)
    assert line == "refused: could not reach the benchmark database: connection refused"
    assert not (tmp_path / "claims.json").exists()


def test_claims_uses_the_shared_code_state() -> None:
    """One rule for every benchmark command: git at the repository root, results excluded."""
    assert vars(claims)["code_state"] is result.code_state
    assert not hasattr(claims, "_git")


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args],
        cwd=root,
        check=True,
        capture_output=True,
    )


def test_earlier_results_do_not_make_a_claims_run_dirty(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    (tmp_path / "code.py").write_text("x = 1\n")
    _git(tmp_path, "add", "code.py")
    _git(tmp_path, "commit", "-q", "-m", "code")
    results = tmp_path / "benchmarks" / "results"
    (results / "claims").mkdir(parents=True)
    (results / "20261108T000000Z-0123456-echo-50-b0-w2-r1.json").write_text("{}\n")
    (results / "claims" / "20261108T000000Z-0123456-claims.json").write_text("{}\n")
    assert result.code_state(tmp_path)["git_dirty"] is False


def churn_record() -> dict[str, object]:
    run = {
        "condition": "index",
        "clients": 4,
        "repetition": 1,
        "depth": 10,
        "ready": 10,
        "claims": 10,
        "elapsed_s": 1.0,
        "latency_ms": claims.distribution([1.0] * 10),
        "vacuums": 0,
        "index": {"idx_scan": 10.0, "idx_tup_read": 12.0, "idx_tup_fetch": 10.0},
        "table": dict.fromkeys(claims.TABLE_FIELDS, 0.0),
        "derived": {},
    }
    return {
        "format": claims.FORMAT,
        "kind": claims.CHURN_KIND,
        "environment": {"git_sha": "x", "git_dirty": False, "postgres": {}, "autovacuum": {}},
        "runs": [run],
    }


def test_a_well_formed_churn_record_is_accepted() -> None:
    claims.validate_churn(churn_record())


@pytest.mark.parametrize(
    "change",
    [
        {"condition": "other"},
        {"claims": 11},  # more than ready
        {"clients": 0},
        {"elapsed_s": -1.0},
        {"index": None},  # the index condition must record the index's counters
        {"vacuums": 2},  # only index+vacuum vacuums
        {"table": {}},
    ],
    ids=["condition", "claims", "clients", "elapsed", "index", "vacuums", "table"],
)
def test_a_malformed_churn_record_is_refused(change: dict[str, object]) -> None:
    record = churn_record()
    runs = record["runs"]
    assert isinstance(runs, list)
    runs[0] = {**runs[0], **change}
    with pytest.raises(ValueError, match="must"):
        claims.validate_churn(record)


def test_a_churn_run_without_the_index_records_no_index_counters() -> None:
    record = churn_record()
    runs = record["runs"]
    assert isinstance(runs, list)
    runs[0] = {**runs[0], "condition": "none", "index": None}
    claims.validate_churn(record)
    runs[0] = {**runs[0], "index": {"idx_scan": 1.0, "idx_tup_read": 1.0, "idx_tup_fetch": 1.0}}
    with pytest.raises(ValueError, match="exactly when the index exists"):
        claims.validate_churn(record)
