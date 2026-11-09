"""The performance report generator, on synthetic result records only.

Every number here is made up to exercise the arithmetic and the layout; none
is a measurement.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from benchmarks import claims, report
from benchmarks.result import write_result
from benchmarks.run import BENCH_WORKER_SETTINGS

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SHA = "0123456789abcdef0123456789abcdef01234567"
OTHER_SHA = "fedcba9876543210fedcba9876543210fedcba98"
POSTGRES = {"version": "PostgreSQL 16.15 on synthetic-test-platform"}


def cell_record(
    *,
    ladder: str = "overhead",
    profile: str = "echo",
    size: int = 100,
    batch: int | None = None,
    workers: int = 1,
    rep: int = 1,
    makespan: float = 1.0,
    rate: float = 100.0,
    steady: float | None = 100.0,
    service_p50: float = 0.004,
    status: str = "complete",
    failed_checks: tuple[str, ...] = (),
    dirty: bool = False,
    sha: str = SHA,
    cpu: str = "Synthetic CPU",
    recorded_at: str = "20261108T000000Z",
    drill: dict[str, Any] | None = None,
    wait: tuple[str, float] | None = None,
) -> dict[str, Any]:
    """A synthetic format 1 cell result that ``benchmarks.result`` accepts."""
    gate = [{"name": "terminal", "passed": True, "detail": None}]
    gate += [{"name": name, "passed": False, "detail": None} for name in failed_checks]
    return {
        "format": 1,
        "run_id": "synthetic",
        "ladder": ladder,
        "status": status,
        "recorded_at": recorded_at,
        "cell": {
            "profile": profile,
            "size": size,
            "batch": batch,
            "workers": workers,
            "repetition": rep,
            "seed": 1,
        },
        "environment": {
            "git_sha": sha,
            "git_dirty": dirty,
            "uv_lock_sha256": "0" * 64,
            "python": "3.12.0",
            "platform": "synthetic",
            "cpu_model": cpu,
            "cores": 8,
            "memory_bytes": 16 * 1024**3,
            "postgres": POSTGRES,
            "docker": "not_measured",
            "compose_project": "not_measured",
            "note": "",
        },
        "worker_settings": dict(BENCH_WORKER_SETTINGS),
        "provider": None,
        "timing": {
            "makespan_s": makespan,
            "tasks": size,
            "items": size,
            "stop_after_s": 60.0,
            **({"wait_ended": wait[0], "waited_s": wait[1]} if wait else {}),
        },
        "throughput": (
            {
                "makespan_items_per_s": rate,
                "makespan_tasks_per_s": rate,
                "processing_items_per_s": rate,
                "steady": {"completions": size, "seconds": 1.0, "items_per_s": steady},
            }
            if status == "complete"
            else None
        ),
        "latency": {
            "queue_wait_s": {"n": size, "p50": 0.1, "p95": 0.2, "p99": 0.3, "max": 0.4},
            "service_s": {"n": size, "p50": service_p50, "p95": 0.01, "p99": 0.02, "max": 0.03},
            "end_to_end_s": {"n": size, "p50": 0.5, "p95": 0.9, "p99": 1.0, "max": 1.0},
        },
        "claim_gap": {"n": size, "p50": 0.05, "p95": 0.1, "p99": 0.1, "max": 0.2},
        "idle_claim_rate": {},
        "resources": {"workers": [{"cpu_utilization": 0.5}, {"cpu_utilization": 0.7}]},
        "database_stats": {"commits_per_task": 4.0},
        "contention": {},
        "failures": {"worker_exit_codes": [0] * workers},
        "usage": None,
        "gate": gate,
        "drill": drill,
    }


def write(root: Path, record: dict[str, Any]) -> Path:
    return write_result(record, root, allow_dirty=True)


def claims_record(*, dirty: bool = False) -> dict[str, Any]:
    def distribution(p50: float, p95: float, top: float, n: int) -> dict[str, Any]:
        return {"n": n, "p50": p50, "p95": p95, "max": top}

    measurements = [
        {
            "variant": variant,
            "depth": depth,
            "ready": depth,
            "heap_pages": depth // 50,
            "claims": 10,
            "latency_ms": distribution(1.0 * depth / 100, 2.0 * depth / 100, 3.0 * depth / 100, 10),
            "explain": {
                "runs": 2,
                "execution_ms": distribution(0.5, 0.6, 0.7, 2),
                "planning_ms": distribution(0.1, 0.1, 0.1, 2),
                "plan_nodes": ["LockRows", "Sort", "Seq Scan on tasks"],
                "consistent_shape": True,
                "last_shared_hit_blocks": 1,
                "last_shared_read_blocks": 0,
            },
        }
        for variant, depth in (("waiting", 100), ("ready", 1000), ("ready", 100))
    ]
    record = claims.build_record(
        measurements,
        code={"git_sha": SHA, "git_dirty": dirty},
        server=POSTGRES,
        settings={},
    )
    claims.validate_result(record)
    return record


def write_claims(root: Path, name: str, record: dict[str, Any]) -> None:
    (root / "claims").mkdir(parents=True, exist_ok=True)
    (root / "claims" / name).write_text(json.dumps(record), encoding="utf-8")


def rows(text: str, label: str) -> list[list[str]]:
    """The table rows whose first cell is ``label``, split into their cells."""
    found = []
    for line in text.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split(" | ")]
        if line.startswith("| ") and cells[0] == label:
            found.append(cells)
    return found


# --- figures -------------------------------------------------------------------------------


def test_summary_is_the_median_with_min_and_max() -> None:
    assert report.summary([12.0, 10.0, 11.0], 3) == "11.000 [10.000, 12.000]"
    assert report.summary([10.0, 12.0], 3) == "11.000 [10.000, 12.000]"  # the middle two's mean
    assert report.summary([7.25], 2) == "7.25 [7.25, 7.25]"
    assert report.summary([1.0, None, 3.0], 1) == "2.0 [1.0, 3.0] (n=2)"
    assert report.summary([None], 1) == report.DASH


def test_fixed_never_prints_negative_zero() -> None:
    assert report.fixed(-0.0001, 3) == "0.000"
    assert report.fixed(None, 3) == report.DASH
    assert report.fixed(2.5, 1) == "2.5"


def test_a_cell_row_summarizes_its_repetitions(tmp_path: Path) -> None:
    for rep, makespan in enumerate((12.0, 10.0, 11.0), 1):
        write(tmp_path, cell_record(rep=rep, makespan=makespan, steady=None if rep == 2 else 50.0))
    text = report.render(tmp_path)
    row = rows(text, "echo 100 w1")[0]  # the cell table comes before the derived one
    assert row[1] == "3"
    assert row[2] == "11.000 [10.000, 12.000]"  # makespan
    assert row[5] == "50.0 [50.0, 50.0] (n=2)"  # steady: one repetition has none
    assert row[10] == "0.60 [0.60, 0.60]"  # worker CPU: median over its workers
    assert row[-1].count("`") == 6  # all three files are cited


def test_derived_figures_come_from_medians(tmp_path: Path) -> None:
    for rep in (1, 2, 3):
        write(tmp_path, cell_record(workers=1, rep=rep, makespan=1.0, rate=100.0))
        write(tmp_path, cell_record(workers=2, rep=rep, makespan=0.5, rate=170.0 + 10 * rep))
    text = report.render(tmp_path)
    assert report.DERIVED_NOTE in text
    one, two = rows(text, "echo 100 w1")[1], rows(text, "echo 100 w2")[1]
    # S(1) = 1; overhead = 1.0 s x 1 / 100 tasks - 0.004 s
    assert one[1:4] == ["1.00", "1.00", "0.006"]
    # S(2) = median 190 / 100; efficiency 1.9 / 2; overhead = 0.5 s x 2 / 100 - 0.004 s
    assert two[1:4] == ["1.90", "0.95", "0.006"]
    assert two[4].count("`") == 12  # its three files and the three of the 1-worker cell


def test_derived_figures_need_the_one_worker_cell(tmp_path: Path) -> None:
    write(tmp_path, cell_record(workers=4))
    [_, derived] = rows(report.render(tmp_path), "echo 100 w4")
    assert derived[1:3] == [report.DASH, report.DASH]


# --- what enters the tables ----------------------------------------------------------------


def test_records_that_do_not_count_are_listed_with_their_reason(tmp_path: Path) -> None:
    write(tmp_path, cell_record(rep=1))
    write(tmp_path, cell_record(rep=2, status="incomplete"))
    write(tmp_path, cell_record(rep=3, status="failed_correctness", failed_checks=("summary",)))
    write(tmp_path, cell_record(rep=4, dirty=True))
    text = report.render(tmp_path)
    row = rows(text, "echo 100 w1")[0]
    assert row[1] == "1"  # only the complete, clean repetition
    excluded = text.split("## Not in the tables", 1)[1]
    assert "incomplete: stopped before every task ended (limit 60.0 s)" in excluded
    assert "failed_correctness: summary" in excluded
    assert "dirty tree (dirty_allowed)" in excluded
    assert excluded.count("| `") == 3


def test_a_reason_says_how_the_wait_ended(tmp_path: Path) -> None:
    lost = cell_record(
        rep=1,
        status="failed_correctness",
        failed_checks=("worker_exits",),
        wait=("no_workers", 5.0),
    )
    lost["timing"]["stop_after_s"] = 3600.0
    limit = cell_record(rep=2, status="incomplete", failed_checks=("terminal",))
    limit["timing"] |= {"wait_ended": "stop_after", "waited_s": 3600.2}
    assert (
        report.reason(lost) == "failed_correctness: every worker exited after 5.0 s; worker_exits"
    )
    assert "3600" not in report.reason(lost)
    assert report.reason(limit) == "incomplete: stopped at the limit after 3600.2 s"
    write(tmp_path, lost)
    write(tmp_path, limit)
    excluded = report.render(tmp_path).split("## Not in the tables", 1)[1]
    assert "every worker exited after 5.0 s" in excluded
    assert "stopped at the limit after 3600.2 s" in excluded


def test_an_incomplete_reason_names_the_failures_a_timeout_does_not_explain(
    tmp_path: Path,
) -> None:
    stopped = cell_record(
        status="incomplete",
        failed_checks=("terminal", "nothing_running", "one_succeeded_attempt"),
        wait=("stop_after", 60.2),
    )
    assert report.reason(stopped) == (
        "incomplete: stopped at the limit after 60.2 s; also failed: one_succeeded_attempt"
    )
    plain = cell_record(status="incomplete", failed_checks=("terminal",), wait=("stop_after", 1.0))
    assert "also failed" not in report.reason(plain)


def test_an_invoice_cell_stopped_at_its_limit_is_not_charged_with_its_job_totals() -> None:
    # Summary and usage compare job totals with the oracle: unfinished tasks fail them.
    stopped = cell_record(
        ladder="scale",
        profile="invoice",
        batch=100,
        status="incomplete",
        failed_checks=("terminal", "nothing_running", "summary", "usage"),
        wait=("stop_after", 0.0),
    )
    assert report.reason(stopped) == "incomplete: stopped at the limit after 0.0 s"
    stopped["gate"].append({"name": "no_double_running", "passed": False, "detail": None})
    assert report.reason(stopped) == (
        "incomplete: stopped at the limit after 0.0 s; also failed: no_double_running"
    )


def _with(record: dict[str, Any], **environment: Any) -> dict[str, Any]:
    record["environment"] |= environment
    return record


VM = {
    "container": "bench",
    "image": "postgres:16.15",
    "image_id": "sha256:0",
    "vm_cpus": 8,
    "vm_memory_bytes": 4 * 1024**3,
}


def test_cells_with_other_server_settings_are_never_pooled(tmp_path: Path) -> None:
    on = {**POSTGRES, "synchronous_commit": "on", "fsync": "on"}
    off = {**POSTGRES, "synchronous_commit": "off", "fsync": "on"}
    write(tmp_path, _with(cell_record(rep=1), postgres=on, docker=VM))
    write(tmp_path, _with(cell_record(rep=2), postgres=off, docker=VM))
    text = report.render(tmp_path)
    assert text.count("### Ladder `overhead`") == 2  # two groups, one repetition each
    assert "(fsync on, synchronous_commit off)" in text
    assert "(fsync on, synchronous_commit on)" in text


def test_cells_on_another_docker_vm_are_never_pooled(tmp_path: Path) -> None:
    write(tmp_path, _with(cell_record(rep=1), docker=VM))
    write(tmp_path, _with(cell_record(rep=2), docker={**VM, "vm_cpus": 4}))
    text = report.render(tmp_path)
    assert text.count("### Ladder `overhead`") == 2
    assert "Docker VM: 8 CPUs, 4.0 GiB" in text
    assert "Docker VM: 4 CPUs, 4.0 GiB" in text


def test_a_cell_without_a_container_says_its_vm_was_not_measured(tmp_path: Path) -> None:
    write(tmp_path, cell_record())
    assert "Docker VM: not measured" in report.render(tmp_path)


def test_the_warm_up_has_its_own_table(tmp_path: Path) -> None:
    write(tmp_path, cell_record(ladder="warm-up", profile="invoice", batch=100, size=1000))
    text = report.render(tmp_path)
    performance, rest = text.split("## Warm-up", 1)
    assert "invoice 1000 b100 w1" not in performance
    assert "invoice 1000 b100 w1" in rest.split("## Claim cost", 1)[0]


def test_one_table_per_commit_and_machine(tmp_path: Path) -> None:
    write(tmp_path, cell_record())
    write(tmp_path, cell_record(sha=OTHER_SHA, recorded_at="20261108T000001Z"))
    write(tmp_path, cell_record(cpu="Other CPU", recorded_at="20261108T000002Z"))
    text = report.render(tmp_path)
    assert text.count("### Ladder `overhead`") == 3
    assert f"Commit `{SHA[:12]}`" in text
    assert f"Commit `{OTHER_SHA[:12]}`" in text
    assert "machine: Other CPU, 8 cores, 16.0 GiB memory · server: PostgreSQL 16.15" in text


def test_ladders_follow_the_plan_order(tmp_path: Path) -> None:
    write(tmp_path, cell_record(ladder="overhead"))
    write(tmp_path, cell_record(ladder="scale", profile="invoice", batch=100))
    text = report.render(tmp_path)
    assert text.index("### Ladder `scale`") < text.index("### Ladder `overhead`")


# --- claim cost and drills -----------------------------------------------------------------


def test_claim_cost_has_its_own_table(tmp_path: Path) -> None:
    write_claims(tmp_path, "claims-1.json", claims_record())
    write_claims(tmp_path, "claims-2.json", claims_record(dirty=True))
    text = report.render(tmp_path)
    assert "### Claim cost: `claims/claims-1.json`" in text
    assert "claims-2.json" not in text.split("## Not in the tables", 1)[0]
    ready = rows(text, "ready")
    assert [row[1] for row in ready] == ["100", "1000"]  # sorted by variant, then depth
    assert ready[1][4:8] == ["10.000", "20.000", "30.000", "0.500"]
    assert ready[0][8] == "LockRows > Sort > Seq Scan on tasks"
    assert "| `claims/claims-2.json` | claim cost | — | — | dirty tree |" in text


def test_drills_have_their_own_table(tmp_path: Path) -> None:
    drills = tmp_path / "drills"
    write_result(cell_record(drill={"name": "D1"}, workers=4), drills)
    record = cell_record(
        drill={"name": "D2"},
        status="failed_correctness",
        failed_checks=("worker_exits",),
        recorded_at="20261108T000001Z",
    )
    write_result(record, drills)
    text = report.render(tmp_path)
    assert rows(text, "D1")[0][2:5] == ["complete", "all passed", "0, 0, 0, 0"]
    assert rows(text, "D2")[0][2:4] == ["failed_correctness", "failed: worker_exits"]
    assert "D1" not in text.split("## Failure drills", 1)[0]


def test_a_drill_result_outside_the_drill_folder_is_refused(tmp_path: Path) -> None:
    write(tmp_path, cell_record(drill={"name": "D1"}))
    with pytest.raises(report.ReportError, match="drills/"):
        report.render(tmp_path)


def test_an_invalid_file_is_named_and_stops_the_report(tmp_path: Path) -> None:
    (tmp_path / "broken.json").write_text('{"format": 2}', encoding="utf-8")
    with pytest.raises(report.ReportError, match=r"broken\.json"):
        report.render(tmp_path)


def test_no_results_say_so(tmp_path: Path) -> None:
    text = report.render(tmp_path / "missing")
    assert text.count(report.NO_RESULTS) == 4
    assert text.endswith("_None._\n")


# --- byte-identical output and the document ------------------------------------------------


def _fill(root: Path, order: list[int]) -> None:
    for rep in order:
        write(root, cell_record(rep=rep, makespan=float(rep)))
    write_claims(root, "claims.json", claims_record())


def test_the_output_is_byte_identical_whatever_the_write_order(tmp_path: Path) -> None:
    _fill(tmp_path / "a", [1, 2, 3])
    _fill(tmp_path / "b", [3, 1, 2])
    first = report.render(tmp_path / "a")
    assert first == report.render(tmp_path / "a")
    assert first.encode() == report.render(tmp_path / "b").encode()
    assert str(tmp_path) not in first


def _document(path: Path) -> Path:
    path.write_text(f"# Performance\n\nProse.\n\n{report.BEGIN}\nold\n{report.END}\n\nMore.\n")
    return path


def test_write_then_check(tmp_path: Path) -> None:
    _fill(tmp_path / "results", [1, 2])
    doc = _document(tmp_path / "performance.md")
    args = ["--results", str(tmp_path / "results"), "--doc", str(doc)]
    assert report.main([*args, "--check"]) == 1
    assert report.main([*args, "--write"]) == 0
    text = doc.read_text()
    assert text.startswith("# Performance\n\nProse.\n\n")
    assert text.endswith(f"{report.END}\n\nMore.\n")
    assert report.main([*args, "--check"]) == 0
    doc.write_text(text.replace("Commit `", "Commit  `", 1))
    assert report.main([*args, "--check"]) == 1


@pytest.mark.parametrize(
    "content",
    [
        None,
        "no markers\n",
        f"{report.BEGIN}\n{report.BEGIN}\n{report.END}\n",
        f"{report.END}\n{report.BEGIN}\n",
    ],
)
def test_check_and_write_refuse_a_document_without_one_marker_pair(
    tmp_path: Path, content: str | None, capsys: pytest.CaptureFixture[str]
) -> None:
    doc = tmp_path / "performance.md"
    if content is not None:
        doc.write_text(content)
    args = ["--results", str(tmp_path / "results"), "--doc", str(doc)]
    assert report.main([*args, "--check"]) == 1
    assert report.main([*args, "--write"]) == 1
    assert doc.exists() == (content is not None)  # never created
    assert "refused:" in capsys.readouterr().err


def test_print_writes_the_rendered_section(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _fill(tmp_path, [1])
    assert report.main(["--results", str(tmp_path)]) == 0
    assert capsys.readouterr().out == report.render(tmp_path)


# --- the README block ----------------------------------------------------------------------


def _scale(root: Path) -> None:
    for workers in (1, 2):
        for rep in (1, 2):
            write(
                root,
                cell_record(
                    ladder="scale",
                    profile="invoice",
                    size=1000,
                    batch=100,
                    workers=workers,
                    rep=rep,
                    makespan=float(rep + workers),
                ),
            )


def test_every_readme_cell_string_is_in_the_performance_document(tmp_path: Path) -> None:
    _scale(tmp_path)
    write(tmp_path, cell_record(ladder="overhead"))  # not a README ladder
    readme = report.render_readme(tmp_path)
    document = report.render(tmp_path)
    readme_rows = rows(readme, "invoice 1000 b100 w2")
    assert len(readme_rows) == 1
    for cell in readme_rows[0][2:]:
        assert cell in document
    assert f"Commit `{SHA[:12]}`" in readme
    assert "Synthetic CPU" in readme
    assert "`overhead`" not in readme


def test_measured_limits_without_results(tmp_path: Path) -> None:
    text = report.render_readme(tmp_path / "missing")
    assert report.NO_RESULTS in text
    assert "### Measured limits" in text
    assert "**Largest rung completed:** _None recorded._" in text
    assert "**50,000 invoices:** no result is recorded" in text
    assert "Claim cost by depth: _None recorded._" in text


def test_measured_limits_name_the_largest_rung_and_every_large_record(tmp_path: Path) -> None:
    _scale(tmp_path)
    write(
        tmp_path,
        cell_record(
            ladder="large",
            profile="invoice",
            size=50_000,
            batch=100,
            workers=4,
            status="incomplete",
            wait=("stop_after", 7200.0),
        ),
    )
    text = report.render_readme(tmp_path)
    assert "**Largest rung completed:** 1,000 invoices:" in text
    assert "invoice 1000 b100 w1 (`scale`)" in text
    assert "**50,000 invoices:** 1 record(s):" in text
    assert "incomplete; incomplete: stopped at the limit after 7200.0 s." in text


def test_measured_limits_show_the_claim_plan_by_depth(tmp_path: Path) -> None:
    write_claims(tmp_path, "claims.json", claims_record())
    text = report.render_readme(tmp_path)
    assert rows(text, "100")[0][1:] == [
        "1.000",
        "1.000",
        "—",
        "LockRows > Sort > Seq Scan on tasks",
    ]
    assert rows(text, "1000")[0][1:] == ["10.000", "—", "—", "LockRows > Sort > Seq Scan on tasks"]


def _readme(path: Path) -> Path:
    block = f"{report.README_BEGIN}\nold\n{report.README_END}"
    path.write_text(f"# Project\n\n## Performance\n\n{block}\n\n## Next\n")
    return path


def test_the_readme_block_is_written_and_checked_on_its_own(tmp_path: Path) -> None:
    _scale(tmp_path / "results")
    readme = _readme(tmp_path / "README.md")
    doc = tmp_path / "performance.md"  # never created: the README call does not need it
    args = ["--results", str(tmp_path / "results"), "--doc", str(doc), "--readme", str(readme)]
    assert report.main([*args, "--check"]) == 1
    assert report.main([*args, "--write"]) == 0
    text = readme.read_text()
    assert text.startswith("# Project\n\n## Performance\n\n")
    assert text.endswith(f"{report.README_END}\n\n## Next\n")
    assert report.main([*args, "--check"]) == 0
    assert not doc.exists()
    readme.write_text(text.replace("Commit `", "Commit  `", 1))
    assert report.main([*args, "--check"]) == 1


def test_the_performance_document_calls_never_touch_the_readme(tmp_path: Path) -> None:
    _fill(tmp_path / "results", [1])
    doc = _document(tmp_path / "performance.md")
    readme = _readme(tmp_path / "README.md")
    before = readme.read_text()
    assert report.main(["--results", str(tmp_path / "results"), "--doc", str(doc), "--write"]) == 0
    assert readme.read_text() == before


def test_neither_marker_pair_contains_the_other() -> None:
    for marker in (report.BEGIN, report.END):
        assert marker not in report.README_BEGIN
        assert marker not in report.README_END
    for marker in (report.README_BEGIN, report.README_END):
        assert marker not in report.BEGIN
        assert marker not in report.END


def test_the_readme_block_is_byte_identical_whatever_the_write_order(tmp_path: Path) -> None:
    _fill(tmp_path / "a", [1, 2, 3])
    _fill(tmp_path / "b", [3, 1, 2])
    _scale(tmp_path / "a")
    _scale(tmp_path / "b")
    first = report.render_readme(tmp_path / "a")
    assert first.encode() == report.render_readme(tmp_path / "b").encode()
    assert str(tmp_path) not in first


def test_print_with_readme_prints_the_readme_block(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _scale(tmp_path)
    assert report.main(["--results", str(tmp_path), "--readme", "README.md"]) == 0
    assert capsys.readouterr().out == report.render_readme(tmp_path)
