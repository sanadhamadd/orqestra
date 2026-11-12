"""The benchmark harness without a database: statistics, ladders, results, the gate's
profile rule, and the benchmark worker's call budget and import boundary."""

import ast
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, NoReturn, cast

import pytest

import benchmarks.worker
from benchmarks import metrics, run
from benchmarks.generate import expected_outcome, generate_invoices
from benchmarks.ladder import (
    CELL_LIMIT,
    LADDERS,
    LARGE_LIMIT,
    large_stop,
    plan_cells,
    stop_limit,
)
from benchmarks.result import (
    DirtyTreeError,
    Status,
    WaitEnded,
    code_state,
    validate_result,
    write_result,
)
from benchmarks.run import (
    BENCH_WORKER_SETTINGS,
    Cell,
    Fleet,
    Profile,
    WorkerHandle,
    build_workload,
    cell_status,
    summary_check,
    usage_check,
    worker_command,
)
from orqestra.worker.settings import WorkerSettings
from orqestra.worker.usage import check_provider_call_budget

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

# The short test profile of the process-level tests.
TEST_SETTINGS = {
    "ORQESTRA_WORKER_LEASE": "PT4S",
    "ORQESTRA_WORKER_EXECUTION_TIMEOUT": "PT5M",
    "ORQESTRA_WORKER_HEARTBEAT_INTERVAL": "PT1S",
    "ORQESTRA_WORKER_DB_OPERATION_TIMEOUT": "PT0.8S",
    "ORQESTRA_WORKER_LOCK_TIMEOUT": "PT0.3S",
    "ORQESTRA_WORKER_FENCE_MARGIN": "PT1S",
    "ORQESTRA_WORKER_POLL_INTERVAL": "PT0.1S",
    "ORQESTRA_WORKER_RECOVERY_INTERVAL": "PT1S",
    "ORQESTRA_WORKER_RECOVERY_BATCH_LIMIT": "5",
    "ORQESTRA_WORKER_SHUTDOWN_TIMEOUT": "PT1S",
    "ORQESTRA_WORKER_RETRY_BASE_DELAY": "PT0.1S",
    "ORQESTRA_WORKER_RETRY_MAX_DELAY": "PT0.5S",
}


# --- statistics ----------------------------------------------------------------------------


def test_percentiles_are_nearest_rank_values() -> None:
    values = [float(v) for v in range(100, 0, -1)]  # unsorted on purpose
    assert metrics.percentiles(values) == {"n": 100, "p50": 50, "p95": 95, "p99": 99, "max": 100}


def test_percentiles_of_few_values_are_observed_values() -> None:
    assert metrics.percentiles([3.0, 1.0, 2.0]) == {"n": 3, "p50": 2, "p95": 3, "p99": 3, "max": 3}
    assert metrics.percentiles([7.5]) == {"n": 1, "p50": 7.5, "p95": 7.5, "p99": 7.5, "max": 7.5}


def test_percentiles_of_nothing_are_none() -> None:
    assert metrics.percentiles([]) == {"n": 0, "p50": None, "p95": None, "p99": None, "max": None}


def test_delta_subtracts_every_counter_and_recurses() -> None:
    before = {"database": {"xact_commit": 10.0, "deadlocks": 0}, "tables": {"tasks": {"seq": 4}}}
    after = {"database": {"xact_commit": 25.0, "deadlocks": 0}, "tables": {"tasks": {"seq": 1}}}
    assert metrics.delta(before, after) == {
        "database": {"deadlocks": 0, "xact_commit": 15.0},
        "tables": {"tasks": {"seq": -3}},  # a counter that went down is reported, not clamped
    }


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ({"a": 1}, {"b": 1}),
        ({"a": 1}, {"a": 1, "b": 2}),
        ({"a": {"x": 1}}, {"a": {"y": 1}}),
        ({"a": True}, {"a": False}),
        ({"a": "1"}, {"a": "2"}),
    ],
)
def test_delta_refuses_snapshots_that_do_not_match(
    before: dict[str, Any], after: dict[str, Any]
) -> None:
    with pytest.raises(ValueError, match="counter"):
        metrics.delta(before, after)


def test_rate_needs_a_positive_duration() -> None:
    assert metrics.rate(10, 4.0) == 2.5
    assert metrics.rate(10, 0.0) is None
    assert metrics.rate(10, None) is None


def test_steady_throughput_uses_the_middle_eighty_percent() -> None:
    completions = [(float(t), 10) for t in range(10)]  # one completion a second
    steady = metrics.steady_throughput(completions)
    # the first and the last completion are dropped; 7 completions after t=1, by t=8
    assert steady == {"completions": 8, "seconds": 7.0, "items_per_s": 10.0}


def test_steady_throughput_needs_two_completions() -> None:
    assert metrics.steady_throughput([(1.0, 5)])["items_per_s"] is None


def test_claim_gaps_are_per_worker() -> None:
    attempts = [
        ("w1", 0.0, 1.0),
        ("w2", 0.5, 2.0),
        ("w1", 1.25, 2.0),
        ("w2", 2.5, None),  # still running: opens no gap
        ("w2", 9.0, 9.5),
    ]
    assert sorted(metrics.claim_gaps(attempts)) == [0.25, 0.5]


def test_maxrss_is_normalised_to_bytes() -> None:
    assert metrics.maxrss_bytes(2048, "darwin") == 2048
    assert metrics.maxrss_bytes(2048, "linux") == 2048 * 1024


def test_docker_sizes_are_read_in_bytes() -> None:
    assert metrics.parse_size("1.5MiB") == 1.5 * 1024**2
    assert metrics.parse_size("2GB") == 2e9
    assert metrics.parse_size("12B") == 12
    assert metrics.parse_size("lots") is None


def test_contention_keeps_the_largest_and_mean_count() -> None:
    summary = metrics.summarize_contention(
        [{"active/none": 2}, {"active/none": 4, "idle/Client": 1}]
    )
    assert summary == {
        "samples": 2,
        "window_s": None,
        "by_state": {
            "active/none": {"max": 4, "mean": 3.0},
            "idle/Client": {"max": 1, "mean": 0.5},
        },
    }


def test_no_contention_sample_gives_no_states() -> None:
    assert metrics.summarize_contention([], 0.5) == {
        "samples": 0,
        "window_s": 0.5,
        "by_state": {},
    }
    assert metrics.summarize_docker([], 0.5)["window_s"] == 0.5


class _Recorder:
    """A connection that records its statements and the transactions they ran in."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.rolled_back: list[bool] = []
        self.depth = 0

    def execute(self, query: str, params: object = None) -> list[tuple[str, str, int]]:
        self.calls.append((query, params))
        return [("active", "Lock", 1)]

    def transaction(self, *, force_rollback: bool = False) -> "_Recorder":
        self.rolled_back.append(force_rollback)
        return self

    def __enter__(self) -> "_Recorder":
        self.depth += 1
        return self

    def __exit__(self, *exc: object) -> None:
        self.depth -= 1


def test_the_contention_sample_leaves_out_its_own_and_the_harness_backend() -> None:
    conn = _Recorder()
    assert metrics.contention_sample(cast(Any, conn), exclude=[4242]) == {"active/Lock": 1}
    [(query, params)] = conn.calls
    assert "pid <> pg_backend_pid()" in query
    assert "pid <> ALL(%s)" in query
    assert params == ([4242],)


def test_the_harness_reads_in_a_transaction_that_rolls_back() -> None:
    conn = _Recorder()
    inside: list[int] = []

    def read(c: Any) -> str:
        inside.append(c.depth)
        return "read"

    assert metrics.read_only(cast(Any, conn), read) == "read"
    assert conn.rolled_back == [True]
    assert inside == [1]


def test_job_commits_subtract_the_idle_claims_outside_the_job() -> None:
    commits = metrics.job_commits(1_000.0, 40.0, 2.5, 2.0)
    assert commits["job_commits"] == 900.0  # 1,000 - 40/s x 2.5 s
    assert commits["estimate_commits"] == 900.0
    # Derived bound: one second of idle claims at each end, plus the rate's own error.
    assert commits["uncertainty_commits"] == pytest.approx(40.0 * 2 + 2.5 * (40.0 * 2 / 2.0))
    assert (commits["xact_commit_delta"], commits["idle_rate_per_s"]) == (1_000.0, 40.0)
    assert (commits["idle_seconds"], commits["idle_window_s"]) == (2.5, 2.0)
    assert commits["method"] == metrics.JOB_COMMITS_METHOD


def test_job_commits_are_not_measured_without_an_idle_rate() -> None:
    commits = metrics.job_commits(1_000.0, None, 2.5, None)
    assert commits["job_commits"] is None
    assert commits["method"].startswith("not measured")


def test_a_short_cells_commits_are_not_measured_within_their_uncertainty() -> None:
    # A real 4-task cell (invoice 100 b25, W1 r3 on cdf6fe7): the idle rate came out at
    # 30.2/s against 18.3/s in its two repetitions, and the estimate went negative.
    commits = metrics.job_commits(91.0, 30.2, 3.30, 2.0)
    assert commits["estimate_commits"] == pytest.approx(91.0 - 30.2 * 3.30)
    assert commits["estimate_commits"] < 0
    assert commits["job_commits"] is None
    assert commits["method"].startswith("not measured: the job's commits")
    # Its sibling, with a positive estimate, is inside the bound too.
    assert metrics.job_commits(92.0, 18.3, 3.24, 2.0)["job_commits"] is None


def test_the_implied_idle_rate_counts_polls_and_sweeps() -> None:
    settings = {
        "ORQESTRA_WORKER_POLL_INTERVAL": "PT0.05S",
        "ORQESTRA_WORKER_RECOVERY_INTERVAL": "PT2S",
    }
    # Each idle worker: 20 claims and half a sweep step a second.
    assert run.implied_idle_rate(2, settings) == pytest.approx(2 * (20 + 0.5))
    # The campaign's measured idle rate at one worker, 17.4 a second, sits below it:
    # an upper estimate, since each call takes time.
    assert run.implied_idle_rate(1, run.BENCH_WORKER_SETTINGS) > 17.4


def test_an_idle_window_that_counted_nothing_still_bounds_the_estimate() -> None:
    # Counters are published at most once a second, so a 2 s idle window can miss
    # every idle claim: a measured rate of 0 must not make the bound 0. Two workers
    # polling every 0.05 s imply 40 idle claims a second.
    commits = metrics.job_commits(150.0, 0.0, 3.2, 2.0, expected_idle_rate_per_s=40.0)
    assert commits["uncertainty_commits"] == pytest.approx(40.0 * 2 + 3.2 * (40.0 * 2 / 2.0))
    assert commits["expected_idle_rate_per_s"] == 40.0
    assert commits["job_commits"] is None
    assert commits["estimate_commits"] == 150.0


@pytest.mark.parametrize("rate", [5.0, 18.3, 50.9, 73.7, 150.0])
@pytest.mark.parametrize("seconds", [-1.0, -0.5, 0.0, 0.5, 1.0])
@pytest.mark.parametrize("job", [0.0, 10.0, 40.0, 400.0, 40_000.0])
def test_a_recorded_figure_is_never_negative_or_inside_its_bound(
    rate: float, seconds: float, job: float
) -> None:
    # The counter's error: up to a second of idle claims more or fewer at an end.
    idle_seconds = 3.3
    delta = job + rate * idle_seconds + rate * seconds
    commits = metrics.job_commits(delta, rate, idle_seconds, 2.0)
    measured = commits["job_commits"]
    if measured is not None:
        assert measured > 0
        assert measured >= 2 * commits["uncertainty_commits"]


def test_the_harness_usage_of_a_cell_is_a_delta() -> None:
    start = {"user_s": 10.0, "sys_s": 2.0, "maxrss_bytes": 100}
    end = {"user_s": 10.5, "sys_s": 2.25, "maxrss_bytes": 300}
    assert metrics.harness_delta(start, end) == {
        "scope": "cell",
        "user_s": 0.5,
        "sys_s": 0.25,
        "process_maxrss_bytes": 300,
    }


def test_a_failing_sample_is_counted_and_skipped() -> None:
    calls = iter([1, None, 3])  # None: that sample fails

    def sample() -> int:
        value = next(calls)
        if value is None:
            raise ValueError("lost")
        if value == 3:
            sampler._stop.set()  # the last sample
        return value

    sampler = metrics.Sampler(sample, interval=0)
    sampler._loop()  # on this thread, so the test needs no timing
    assert (sampler.samples, sampler.errors) == ([1, 3], 1)


def test_an_exited_process_is_reaped_once_with_its_accounting() -> None:
    process = subprocess.Popen([sys.executable, "-c", "raise SystemExit(3)"])
    exited = metrics.reap(process, timeout=30)
    assert (exited["exit_code"], exited["killed"], process.returncode) == (3, False, 3)
    assert exited["maxrss_bytes"] > 0


def test_a_process_that_outlives_the_timeout_is_killed() -> None:
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    assert metrics.poll_exit(process) is None
    exited = metrics.reap(process, timeout=0)
    assert (exited["exit_code"], exited["killed"]) == (-9, True)


def _exited_unreaped(code: int) -> subprocess.Popen[bytes]:
    """A child that has exited and is still unreaped (a zombie), so a poll would reap it."""
    process = subprocess.Popen([sys.executable, "-c", f"raise SystemExit({code})"])
    for _ in range(3000):  # a bound on a hung child, not an expectation
        state = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(process.pid)],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        if state.startswith("Z"):
            return process
        time.sleep(0.01)
    process.kill()
    pytest.fail("the child never exited")


def _hidden_once(monkeypatch: pytest.MonkeyPatch, process: subprocess.Popen[bytes]) -> None:
    """The next ``poll_exit`` of ``process`` misses its exit, as if it exited just after."""
    real = metrics.poll_exit
    hidden = [process.pid]

    def poll_exit(candidate: subprocess.Popen[Any]) -> dict[str, Any] | None:
        if candidate.pid in hidden:
            hidden.remove(candidate.pid)
            return None
        return real(candidate)

    monkeypatch.setattr(metrics, "poll_exit", poll_exit)


def test_a_process_that_exits_just_before_its_kill_keeps_its_accounting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _exited_unreaped(3)
    _hidden_once(monkeypatch, process)
    exited = metrics.reap(process, timeout=0)  # kills an exited process, then waits for it
    assert (exited["exit_code"], exited["killed"], process.returncode) == (3, True, 3)
    assert exited["maxrss_bytes"] > 0


def test_stopping_a_fleet_reaps_every_worker_when_one_exits_just_before_its_signal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    sleeper = [sys.executable, "-c", "import time; time.sleep(60)"]
    processes = [_exited_unreaped(3), subprocess.Popen(sleeper)]
    fleet = Fleet(Profile.ECHO, {}, tmp_path)
    for number, process in enumerate(processes, 1):
        path = tmp_path / f"worker-{number}.log"
        fleet.workers.append(WorkerHandle(process, path, path.open("wb")))
    _hidden_once(monkeypatch, processes[0])

    accounting = fleet.stop(timeout=30)

    assert [entry["exit_code"] for entry in accounting] == [3, -15]
    assert [entry["pid"] for entry in accounting] == [process.pid for process in processes]
    assert all(handle.log.closed for handle in fleet.workers)


# --- cells and ladders ---------------------------------------------------------------------


def test_only_the_invoice_profile_has_a_batch() -> None:
    with pytest.raises(ValueError, match="batch"):
        Cell(Profile.INVOICE, 100, None, 1)
    with pytest.raises(ValueError, match="batch"):
        Cell(Profile.ECHO, 100, 10, 1)
    with pytest.raises(ValueError, match="workers"):
        Cell(Profile.ECHO, 100, None, 0)
    assert Cell(Profile.INVOICE, 50_000, 1_000, 8, 3).name == "inv50000b1000w8r3"


def test_workloads_follow_their_profile() -> None:
    invoice = build_workload(Cell(Profile.INVOICE, 200, 20, 2))
    assert (len(invoice.tasks), invoice.items) == (10, 200)
    assert invoice.expected == expected_outcome(generate_invoices(200), 20, 16)
    echo = build_workload(Cell(Profile.ECHO, 3, None, 1))
    assert [task["payload"] for task in echo.tasks] == [{"n": 0}, {"n": 1}, {"n": 2}]
    assert {task["max_attempts"] for task in echo.tasks} == {3}
    wait = build_workload(Cell(Profile.WAIT, 2, None, 1))
    assert wait.tasks[0]["payload"] == {"seconds": 0.05}
    assert wait.expected is None


def test_the_invoice_profile_runs_the_benchmark_worker() -> None:
    assert worker_command(Profile.INVOICE)[1:3] == ["-m", "benchmarks.worker"]
    assert worker_command(Profile.ECHO)[-4:] == ["worker", "run", "--kind", "diagnostic.echo"]


def test_repetitions_are_interleaved_by_worker_count() -> None:
    cells = plan_cells(LADDERS["overhead"], cpu_count=8)
    assert [(c.workers, c.repetition) for c, _ in cells] == [
        (w, r) for r in (1, 2, 3) for w in (1, 2, 4, 8)
    ]
    assert not any(skipped for _, skipped in cells)


def test_sizes_run_in_order_and_batches_interleave() -> None:
    scale = [cell.size for cell, _ in plan_cells(LADDERS["scale"], cpu_count=8)]
    assert scale == [1_000] * 12 + [5_000] * 12 + [10_000] * 12
    batch = [(c.repetition, c.batch) for c, _ in plan_cells(LADDERS["batch"], cpu_count=8)]
    assert batch == [(r, b) for r in (1, 2, 3) for b in (10, 100, 1_000)]


def test_worker_counts_above_the_cores_are_skipped() -> None:
    cells = plan_cells(LADDERS["scale"], cpu_count=4)
    assert {cell.workers for cell, skipped in cells if skipped} == {8}
    assert len(cells) == 36


def test_stop_limit() -> None:
    assert stop_limit([], 5) == CELL_LIMIT
    assert stop_limit([10.0, 30.0, 20.0], 5) == 200.0  # 2 x median 20 s x ratio 5
    assert stop_limit([1_000.0], 10) == CELL_LIMIT


def _scale_record(workers: int, repetition: int, makespan: float, status: str) -> dict[str, Any]:
    return {
        "ladder": "scale",
        "status": status,
        "cell": {"size": 10_000, "workers": workers, "repetition": repetition},
        "timing": {"makespan_s": makespan},
    }


def _scale(makespan: float, status: str = "complete") -> list[dict[str, Any]]:
    return [_scale_record(w, r, makespan, status) for w in (1, 2, 4, 8) for r in (1, 2, 3)]


def test_the_large_rung_gate() -> None:
    assert large_stop(_scale(100.0), workers=4, cpu_count=8) == 1_000.0  # 2 x 5 x 100 s
    assert large_stop(_scale(720.0), workers=4, cpu_count=8) == LARGE_LIMIT  # 5 x 720 s = 60 min
    assert large_stop(_scale(720.5), workers=4, cpu_count=8) is None  # projection over 60 min
    assert large_stop(_scale(100.0, "incomplete"), workers=4, cpu_count=8) is None
    assert large_stop(_scale(100.0)[1:], workers=4, cpu_count=8) is None  # a cell is missing
    only_four_cores = [r for r in _scale(100.0) if r["cell"]["workers"] <= 4]
    assert large_stop(only_four_cores, workers=4, cpu_count=4) == 1_000.0
    assert large_stop(only_four_cores, workers=8, cpu_count=4) is None


def test_the_large_rung_gate_never_raises_for_a_missing_cell() -> None:
    # Records of a worker count this machine skips sit beside a missing wanted cell.
    four_cores = [r for r in _scale(100.0) if r["cell"]["workers"] <= 4][1:]
    eight = _scale_record(8, 1, 100.0, "complete")
    assert large_stop([*four_cores, eight], workers=4, cpu_count=4) is None
    assert large_stop(_scale(100.0)[1:], workers=4, cpu_count=4) is None


def test_the_large_rung_gate_needs_every_record_of_a_cell_complete() -> None:
    older = _scale_record(8, 2, 100.0, "incomplete")
    assert large_stop([older, *_scale(100.0)], workers=4, cpu_count=8) is None
    assert large_stop([*_scale(100.0), older], workers=4, cpu_count=8) is None


# --- results -------------------------------------------------------------------------------


def _record(status: str = "complete", passed: bool = True, dirty: bool = False) -> dict[str, Any]:
    environment = dict.fromkeys(
        (
            "uv_lock_sha256",
            "python",
            "platform",
            "cpu_model",
            "cores",
            "memory_bytes",
            "postgres",
            "docker",
            "compose_project",
            "note",
        ),
        "x",
    )
    return {
        "format": 1,
        "run_id": "abc123",
        "ladder": "single",
        "status": status,
        "recorded_at": "20261107T120000Z",
        "cell": {
            "profile": "echo",
            "size": 50,
            "batch": None,
            "workers": 2,
            "repetition": 1,
            "seed": 1,
        },
        "environment": {**environment, "git_sha": "0123456789abcdef", "git_dirty": dirty},
        "worker_settings": dict(BENCH_WORKER_SETTINGS),
        "provider": None,
        "timing": {},
        "throughput": {} if status == "complete" else None,
        "latency": {},
        "claim_gap": {},
        "idle_claim_rate": {},
        "resources": {},
        "database_stats": {},
        "contention": {},
        "failures": {},
        "usage": None,
        "gate": [{"name": "terminal", "passed": True}, {"name": "worker_exits", "passed": passed}],
        "drill": None,
    }


def test_a_well_formed_record_validates() -> None:
    validate_result(_record())
    validate_result(_record("incomplete", passed=False))
    validate_result(_record("failed_correctness", passed=False))
    for status, passed, ended in (
        ("complete", True, "finished"),
        ("failed_correctness", False, "finished"),
        ("incomplete", False, "stop_after"),
        ("failed_correctness", False, "no_workers"),
    ):
        record = _record(status, passed=passed)
        record["timing"] = {"wait_ended": ended, "waited_s": 5.0}
        validate_result(record)


def test_the_status_follows_how_the_wait_ended() -> None:
    passed = [{"name": "terminal", "passed": True}]
    failed = [{"name": "terminal", "passed": False}, {"name": "worker_exits", "passed": False}]
    assert cell_status(WaitEnded.FINISHED, passed) is Status.COMPLETE
    assert cell_status(WaitEnded.FINISHED, failed) is Status.FAILED_CORRECTNESS
    assert cell_status(WaitEnded.STOP_AFTER, failed) is Status.INCOMPLETE
    # A fleet that exited before its job ended failed a check; it was not stopped at the limit.
    assert cell_status(WaitEnded.NO_WORKERS, failed) is Status.FAILED_CORRECTNESS


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda r: r.pop("gate"), "missing"),
        (lambda r: r.update(format=2), "format 1"),
        (lambda r: r.update(status="fast"), "status"),
        (lambda r: r["cell"].update(workers=0), "workers"),
        (lambda r: r["cell"].pop("seed"), "cell"),
        (lambda r: r["environment"].pop("git_sha"), "environment"),
        (lambda r: r.update(gate=[]), "gate"),
        (lambda r: r["gate"].append({"name": "x", "passed": "yes"}), "boolean"),
        (lambda r: r["gate"][1].update(passed=False), "complete"),
        (lambda r: r.update(status="failed_correctness", throughput=None), "failed gate"),
        (lambda r: r.update(status="incomplete"), "throughput"),
        (lambda r: r["environment"].update(git_dirty=True), "dirty"),
        (lambda r: r.update(timing={"wait_ended": "slept", "waited_s": 1.0}), "wait_ended"),
        (lambda r: r.update(timing={"wait_ended": "finished", "waited_s": -1.0}), "waited_s"),
        (lambda r: r.update(timing={"wait_ended": "finished", "waited_s": True}), "waited_s"),
        (lambda r: r.update(timing={"wait_ended": "finished"}), "waited_s"),
        (lambda r: r.update(timing={"wait_ended": "stop_after", "waited_s": 1.0}), "stop_after"),
    ],
)
def test_malformed_records_are_refused(change: Any, message: str) -> None:
    record = _record()
    change(record)
    with pytest.raises(ValueError, match=message):
        validate_result(record)


def test_a_result_from_a_dirty_tree_is_refused(tmp_path: Path) -> None:
    with pytest.raises(DirtyTreeError):
        write_result(_record(dirty=True), tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_an_allowed_dirty_result_is_marked(tmp_path: Path) -> None:
    path = write_result(_record(dirty=True), tmp_path, allow_dirty=True)
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["environment"]["dirty_allowed"] is True
    assert path.name == "20261107T120000Z-0123456-echo-50-b0-w2-r1.json"


def test_a_result_file_is_never_overwritten(tmp_path: Path) -> None:
    write_result(_record(), tmp_path)
    with pytest.raises(FileExistsError):
        write_result(_record(), tmp_path)


def test_results_are_named_by_cell() -> None:
    record = _record()
    record["cell"].update(profile="invoice", size=10_000, batch=100, workers=4, repetition=3)
    path = write_result(record, Path.cwd())
    assert path.name == "20261107T120000Z-0123456-invoice-10000-b100-w4-r3.json"
    assert Status(json.loads(path.read_text())["status"]) is Status.COMPLETE


# --- the gate ------------------------------------------------------------------------------


class Untouchable:
    """A connection stand-in that fails the test if anything uses it."""

    def __getattr__(self, name: str) -> NoReturn:
        msg = f"the connection was used ({name})"
        raise AssertionError(msg)


@pytest.mark.parametrize("profile", [Profile.ECHO, Profile.WAIT])
def test_summary_and_usage_checks_refuse_other_profiles_before_reading(profile: Profile) -> None:
    expected = expected_outcome(generate_invoices(200), 20, 16)
    conn = cast(Any, Untouchable())
    with pytest.raises(ValueError, match="only to the invoice profile"):
        summary_check(conn, uuid.uuid4(), profile, expected)
    with pytest.raises(ValueError, match="only to the invoice profile"):
        usage_check(conn, uuid.uuid4(), profile, expected, clean=True)


# --- the benchmark worker ------------------------------------------------------------------


@pytest.fixture
def test_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in TEST_SETTINGS.items():
        monkeypatch.setenv(key, value)


def _output(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return dict(json.loads(capsys.readouterr().out))


@pytest.mark.usefixtures("test_settings")
def test_the_worker_refuses_a_cap_that_fails_the_call_budget(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # PT5M - PT1S leaves 299 s; each call takes 0.1 s + 2 x 2.4 s, so 62 calls do not fit.
    assert benchmarks.worker.main(["--cap", "62"]) == 1
    output = _output(capsys)
    assert output["status"] == "error"
    assert output["error"].startswith("refused: max_provider_calls_per_attempt")


@pytest.mark.usefixtures("test_settings")
def test_the_worker_checks_the_budget_before_it_needs_a_database(
    capsys: pytest.CaptureFixture[str],
) -> None:
    # 61 calls fit (298.9 s): the refusal is then only the missing URL.
    assert benchmarks.worker.main(["--cap", "61"]) == 1
    assert _output(capsys)["error"] == "ORQESTRA_DATABASE_URL is not set"


@pytest.mark.usefixtures("test_settings")
def test_the_worker_refuses_a_cap_out_of_range(capsys: pytest.CaptureFixture[str]) -> None:
    assert benchmarks.worker.main(["--cap", "-1"]) == 1
    assert _output(capsys)["error"].startswith("refused:")


def test_the_worker_refuses_missing_settings(capsys: pytest.CaptureFixture[str]) -> None:
    assert benchmarks.worker.main([]) == 1
    assert _output(capsys)["error"].startswith("invalid ORQESTRA_WORKER_* settings: ")


def test_the_default_cap_fits_the_measurement_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in BENCH_WORKER_SETTINGS.items():
        monkeypatch.setenv(key, value)
    settings = WorkerSettings(_env_file=None)
    check_provider_call_budget(
        settings,
        max_provider_calls_per_attempt=benchmarks.worker.DEFAULT_CAP,
        provider_call_timeout=benchmarks.worker.parse_duration(
            benchmarks.worker.DEFAULT_CALL_TIMEOUT
        ),
    )


NETWORK_ROOTS = frozenset({"http", "ssl", "socket", "urllib", "requests", "httpx", "aiohttp"})


def _imports(path: Path) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "relative imports are not allowed"
            imported.add(node.module or "")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id not in {"__import__", "eval", "exec", "compile"}
    return imported


def test_the_worker_imports_no_network_module_and_no_real_provider() -> None:
    # OpenRouter reaches the worker only through benchmarks.live, behind its opt-in (DEC-071).
    imported = _imports(Path(benchmarks.worker.__file__))
    assert not {name for name in imported if name.split(".")[0] in NETWORK_ROOTS}
    assert not {name for name in imported if name.startswith("orqestra.providers")}
    assert "benchmarks.provider" in imported
    assert "benchmarks.live" in imported


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=bench",
            "-c",
            "user.email=bench@example.invalid",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        cwd=root,
        check=True,
        capture_output=True,
    )


def test_a_result_file_alone_does_not_make_the_tree_dirty(tmp_path: Path) -> None:
    _git(tmp_path, "init", "-q")
    (tmp_path / "code.py").write_text("x = 1\n")
    _git(tmp_path, "add", "code.py")
    _git(tmp_path, "commit", "-q", "-m", "code")
    assert code_state(tmp_path)["git_dirty"] is False

    results = tmp_path / "benchmarks" / "results"
    results.mkdir(parents=True)
    (results / "20261108T000000Z-0123456-echo-50-b0-w2-r1.json").write_text("{}\n")
    assert code_state(tmp_path)["git_dirty"] is False

    (tmp_path / "benchmarks" / "other.py").write_text("y = 2\n")
    assert code_state(tmp_path)["git_dirty"] is True
    (tmp_path / "benchmarks" / "other.py").unlink()
    (tmp_path / "code.py").write_text("x = 2\n")
    assert code_state(tmp_path)["git_dirty"] is True
