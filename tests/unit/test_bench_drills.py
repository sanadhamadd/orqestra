"""The drills' D3b checks and command-line refusals, without a database or Docker."""

import signal
import subprocess
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn, cast

import psycopg
import pytest

import benchmarks.drills
from benchmarks.database import BENCH_DATABASE_URL_ENV, UnsafeBenchDatabaseError
from benchmarks.drills import (
    IDENTITY_COMMAND,
    RESTART_COMMAND,
    Drill,
    DrillFleet,
    FreezeDrill,
    Freezes,
    Harness,
    KillDrill,
    Poll,
    RestartDrill,
    RestartRefusedError,
    Victim,
    _process_state,
    check_restart_target,
    freeze_victims,
    freeze_window,
    wait_over,
)
from benchmarks.generate import generate_invoices
from benchmarks.live import BUDGET_FILE_NAME, DOUBLE, ProviderChoice, ProviderName
from benchmarks.run import BENCH_WORKER_SETTINGS, Cell, Profile, bench_settings
from benchmarks.worker import parse_duration
from orqestra.domain.retry import RetryPolicy
from orqestra.domain.states import TaskState
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

BOOT = datetime(2026, 11, 8, 12, 0, tzinfo=UTC)
NO_OP = ("true",)


class FakeHarness:
    """A harness whose server reports ``starts`` in turn as its start time."""

    def __init__(self, *starts: datetime) -> None:
        self.starts = list(starts)
        self.reconnects = 0

    def clock(self) -> datetime:
        return BOOT + timedelta(hours=1)

    def server_started(self) -> datetime:
        return self.starts.pop(0)

    def reconnect(self) -> float:
        self.reconnects += 1
        return 0.0


def _poll(harness: FakeHarness) -> Poll:
    """A poll past the injection point: every task has succeeded."""
    return Poll(
        cast(Harness, harness), uuid.uuid4(), cast(DrillFleet, None), 10, 10, quiet_bound=1.0
    )


@pytest.fixture
def commands(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Each command ``subprocess.run`` is given; none is run."""
    ran: list[tuple[str, ...]] = []

    def run(command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        ran.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    return ran


# --- D3b counts only a restart the server shows -------------------------------------------


def test_a_restart_that_did_nothing_is_not_injected(commands: list[tuple[str, ...]]) -> None:
    drill = RestartDrill(command=NO_OP)
    harness = FakeHarness(BOOT, BOOT)

    drill.step(_poll(harness))
    drill.step(_poll(harness))  # attempted once only

    assert commands == [NO_OP]
    assert harness.reconnects == 1
    assert drill.injected is False
    [injection] = drill.injections
    assert injection["restarted"] is False
    assert injection["server_started_before"] == injection["server_started_after"]
    [check] = drill.checks(cast(Any, None), uuid.uuid4(), cast(DrillFleet, None))
    assert check["name"] == "injected"
    assert check["passed"] is False


def test_a_restart_the_server_shows_is_injected(commands: list[tuple[str, ...]]) -> None:
    drill = RestartDrill(command=NO_OP)

    drill.step(_poll(FakeHarness(BOOT, BOOT + timedelta(seconds=3))))

    assert commands == [NO_OP]
    assert drill.injected is True
    assert drill.injections[0]["restarted"] is True


def test_the_default_command_restarts_the_bench_project() -> None:
    assert RestartDrill().command == RESTART_COMMAND
    assert RESTART_COMMAND[:4] == ("docker", "compose", "-p", "orqestra-bench")


# --- D3b restarts only the server the URL reaches -----------------------------------------

SYSTEM = 7705849931930558508


class FakeConn:
    def __init__(self, identifier: int) -> None:
        self.identifier = identifier

    def execute(self, query: str) -> "FakeConn":
        assert "pg_control_system()" in query
        return self

    def fetchone(self) -> tuple[int]:
        return (self.identifier,)


def _controldata(monkeypatch: pytest.MonkeyPatch, stdout: str) -> list[tuple[str, ...]]:
    ran: list[tuple[str, ...]] = []

    def run(command: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        ran.append(tuple(command))
        return subprocess.CompletedProcess(command, 0, stdout, "")

    monkeypatch.setattr(subprocess, "run", run)
    return ran


def _shown(identifier: int) -> str:
    return (
        "pg_control version number:            1300\n"
        f"Database system identifier:           {identifier}\n"
        "Database cluster state:               in production\n"
    )


def test_the_container_that_is_the_reached_server_is_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ran = _controldata(monkeypatch, _shown(SYSTEM))

    check_restart_target(cast(Any, FakeConn(SYSTEM)))

    assert ran == [IDENTITY_COMMAND]


@pytest.mark.parametrize("stdout", [_shown(SYSTEM + 1), "", "pg_controldata: no data\n"])
def test_a_container_that_is_another_server_is_refused(
    monkeypatch: pytest.MonkeyPatch, stdout: str
) -> None:
    _controldata(monkeypatch, stdout)

    with pytest.raises(RestartRefusedError, match="not the server"):
        check_restart_target(cast(Any, FakeConn(SYSTEM)))


@pytest.mark.parametrize(
    "error",
    [
        subprocess.CalledProcessError(1, IDENTITY_COMMAND),
        subprocess.TimeoutExpired(IDENTITY_COMMAND, 60),
        FileNotFoundError("docker"),
    ],
)
def test_a_container_whose_identity_cannot_be_read_is_refused(
    monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    def run(command: Sequence[str], **kwargs: Any) -> NoReturn:
        raise error

    monkeypatch.setattr(subprocess, "run", run)

    with pytest.raises(RestartRefusedError, match="could not read the identity") as raised:
        check_restart_target(cast(Any, FakeConn(SYSTEM)))
    assert raised.value.__cause__ is None


# --- the command line ---------------------------------------------------------------------


@pytest.fixture
def named(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Each drill ``main`` would run; nothing is run, and the tree counts as clean."""
    ran: list[str] = []

    def run_named(name: str, args: object, url: str) -> list[object]:
        ran.append(name)
        return []

    monkeypatch.setattr(benchmarks.drills, "run_named", run_named)
    monkeypatch.setattr(benchmarks.drills, "refuse_dirty", lambda allow: None)
    monkeypatch.setenv(BENCH_DATABASE_URL_ENV, "postgresql://bench@localhost:5433/orqestra_bench")
    return ran


@pytest.mark.parametrize("drill", ["d2", "d3a", "d4"])
def test_one_worker_is_enough_for_a_drill_without_victims(named: list[str], drill: str) -> None:
    assert benchmarks.drills.main([drill, "--workers", "1"]) == 0
    assert named == [drill]


def test_d1_still_needs_a_worker_for_each_victim(
    named: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    assert benchmarks.drills.main(["d1", "--workers", "1"]) == 1
    assert capsys.readouterr().err == "refused: --victims cannot exceed --workers\n"
    assert named == []


UNREACHABLE = (
    'connection to server at "127.0.0.1", port 5433 failed: Connection refused\n'
    "\tIs the server running on that host and accepting TCP/IP connections?\n"
)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            UnsafeBenchDatabaseError("refusing to use database 'other'"),
            "refused: refusing to use database 'other'",
        ),
        (
            psycopg.OperationalError(UNREACHABLE),
            "refused: could not reach the benchmark database: "
            'connection to server at "127.0.0.1", port 5433 failed: Connection refused',
        ),
    ],
)
def test_a_database_error_is_one_refused_line(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    named: list[str],
    error: Exception,
    expected: str,
) -> None:
    def run_named(name: str, args: object, url: str) -> NoReturn:
        raise error

    monkeypatch.setattr(benchmarks.drills, "run_named", run_named)

    assert benchmarks.drills.main(["d2"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    [line] = captured.err.splitlines()
    assert line == expected
    assert "Traceback" not in captured.err


def test_d3b_on_another_server_is_refused_before_any_schema_or_restart(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def no_schema(*args: object, **kwargs: object) -> NoReturn:
        pytest.fail("d3b created a run schema although it was refused")

    ran = _controldata(monkeypatch, _shown(SYSTEM + 1))
    monkeypatch.setattr(benchmarks.drills, "connect_verified", lambda url: FakeAdmin(SYSTEM))
    monkeypatch.setattr(benchmarks.drills, "create_run_schema", no_schema)
    monkeypatch.setattr(benchmarks.drills, "refuse_dirty", lambda allow: None)
    monkeypatch.setenv(BENCH_DATABASE_URL_ENV, "postgresql://bench@localhost:5433/orqestra_bench")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "orqestra-bench")

    assert benchmarks.drills.main(["d3b", "--allow-db-restart"]) == 1
    assert capsys.readouterr().err.startswith("refused: d3b restarts only the orqestra-bench")
    assert ran == [IDENTITY_COMMAND]  # the identity was read; nothing was restarted


class FakeAdmin(FakeConn):
    """A verified admin connection, used as a context manager."""

    def __enter__(self) -> "FakeAdmin":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


# --- D2: the freeze ends in the drill's own step, and says how long it lasted ----------------


@pytest.fixture
def signals(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int]]:
    """Each signal the drills would send; none is sent."""
    sent: list[tuple[int, int]] = []
    monkeypatch.setattr("benchmarks.drills.os.kill", lambda pid, sig: sent.append((pid, sig)))
    return sent


def _frozen(seconds: float, frozen_for: float) -> FreezeDrill:
    """A D2 drill whose victim was frozen ``frozen_for`` seconds ago."""
    drill = FreezeDrill(seconds)
    drill.victim = Victim(4242, uuid.uuid4())
    drill.frozen_at = time.monotonic() - frozen_for
    drill.thaw_at = drill.frozen_at + seconds
    drill.injections.append({"pid": 4242, "frozen_at": "t0", "seconds": seconds})
    return drill


def test_a_freeze_is_pending_until_the_drill_thaws_it(signals: list[tuple[int, int]]) -> None:
    drill = _frozen(seconds=12.0, frozen_for=12.5)
    assert drill.pending()

    drill.step(_poll(FakeHarness()))

    assert not drill.pending()
    assert drill.injected is True
    [injection] = drill.injections
    assert injection["thawed_by"] == "drill"
    assert injection["frozen_s"] >= 12.5
    assert injection["thawed_at"] == (BOOT + timedelta(hours=1)).isoformat()
    assert signals == [(4242, signal.SIGCONT)]


def test_a_thaw_by_cleanup_is_not_an_injection(signals: list[tuple[int, int]]) -> None:
    drill = _frozen(seconds=12.0, frozen_for=9.0)  # the job ended first

    drill.release()

    assert drill.injected is False
    [injection] = drill.injections
    assert injection["thawed_by"] == "cleanup"
    assert 9.0 <= injection["frozen_s"] < 12.0  # the real time, not the planned 12 s
    assert injection["thawed_at"] is None
    assert signals == [(4242, signal.SIGCONT)]
    [check] = Drill.checks(drill, cast(Any, None), uuid.uuid4(), cast(DrillFleet, None))
    assert (check["name"], check["passed"]) == ("injected", False)


FINISHED = dict.fromkeys(TaskState, 0) | {TaskState.SUCCEEDED: 20}
RUNNING = FINISHED | {TaskState.RUNNING: 1}


def test_the_wait_outlasts_a_finished_job_while_a_freeze_is_pending(
    signals: list[tuple[int, int]],
) -> None:
    assert wait_over(FINISHED, Drill())
    assert not wait_over(RUNNING, Drill())
    frozen = _frozen(seconds=12.0, frozen_for=1.0)
    assert not wait_over(FINISHED, frozen)
    frozen.release()
    assert wait_over(FINISHED, frozen)


# --- D1 at a call boundary: the demo never kills between a call's intent and result ---------


class ConnOnly:
    """A harness with a connection that is never used: ``_one`` is replaced."""

    conn = None


def _freezing(victim: Victim, window: float) -> Any:
    """A ``freeze_victims`` that returns ``victim``, its freeze ending ``window`` s from now."""

    def freeze(poll: Poll, count: int, taken: set[int], *, freezes: Freezes) -> list[Victim]:
        freezes.started = time.monotonic()
        freezes.deadline = freezes.started + window
        return [victim]

    return freeze


@pytest.fixture
def one_victim(monkeypatch: pytest.MonkeyPatch) -> Victim:
    victim = Victim(4242, uuid.uuid4())
    monkeypatch.setattr(benchmarks.drills, "freeze_victims", _freezing(victim, 60.0))
    return victim


def test_a_victim_with_a_call_in_flight_is_continued_not_killed(
    signals: list[tuple[int, int]], one_victim: Victim, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(benchmarks.drills, "_one", lambda conn, query, *params: (1,))
    drill = KillDrill(1, at_call_boundary=True)

    drill.step(_poll(cast(FakeHarness, ConnOnly())))

    assert signals == [(4242, signal.SIGCONT)]
    assert drill.injected is False
    assert drill.killed == set()
    assert drill.injections == []


def test_a_failed_in_flight_read_continues_the_victim_and_raises(
    signals: list[tuple[int, int]], one_victim: Victim, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(conn: object, query: str, *params: object) -> NoReturn:
        raise psycopg.OperationalError("the connection was lost")

    monkeypatch.setattr(benchmarks.drills, "_one", broken)
    drill = KillDrill(1, at_call_boundary=True)

    with pytest.raises(psycopg.OperationalError):
        drill.step(_poll(cast(FakeHarness, ConnOnly())))

    assert signals == [(4242, signal.SIGCONT)]
    assert drill.killed == set()


def test_a_victim_past_the_freeze_window_is_continued_without_a_read(
    signals: list[tuple[int, int]], monkeypatch: pytest.MonkeyPatch
) -> None:
    def unread(conn: object, query: str, *params: object) -> NoReturn:
        raise AssertionError("read after the freeze window")

    monkeypatch.setattr(
        benchmarks.drills, "freeze_victims", _freezing(Victim(4242, uuid.uuid4()), -1.0)
    )
    monkeypatch.setattr(benchmarks.drills, "_one", unread)
    drill = KillDrill(1, at_call_boundary=True)

    drill.step(_poll(cast(FakeHarness, ConnOnly())))

    assert signals == [(4242, signal.SIGCONT)]
    assert drill.killed == set()
    # The workers run at least as long as the victim was stopped before the next try.
    assert drill.freezes.not_before > drill.freezes.started


# --- the freeze window (DEC-076): real stopped processes, a fake database ----------------

WINDOW = 0.3  # seconds; a fake database answers at once
SLACK = 0.5  # the stop and quiet checks poll; a bound on scheduling, not an expectation


class _Rows(list[tuple[Any, ...]]):
    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self)


class ActivityConn:
    """Reports ``waits`` as active worker statements and ``owners`` as running attempts."""

    def __init__(self, waits: Sequence[str] = (), owners: Sequence[tuple[Any, ...]] = ()) -> None:
        self.waits = list(waits)
        self.owners = list(owners)
        self.owner_read = 0.0  # seconds the owner read takes
        self.queries = 0

    def execute(self, query: str, params: Sequence[object]) -> _Rows:
        self.queries += 1
        if "pg_stat_activity" in query:
            return _Rows((wait,) for wait in self.waits)
        time.sleep(self.owner_read)
        return _Rows(self.owners)


class _Handle:
    def __init__(self) -> None:
        self.process = subprocess.Popen(["sleep", "30"])

    def running(self) -> bool:
        return self.process.poll() is None


class _Fleet:
    def __init__(self, workers: int) -> None:
        self.workers = [_Handle() for _ in range(workers)]


class _Harness:
    def __init__(self, conn: ActivityConn) -> None:
        self.conn = conn


@pytest.fixture
def fleet() -> Any:
    workers = _Fleet(2)
    yield workers
    for handle in workers.workers:
        handle.process.kill()
        handle.process.wait()


def _freeze_poll(conn: ActivityConn, workers: _Fleet) -> Poll:
    return Poll(
        cast(Harness, _Harness(conn)),
        uuid.uuid4(),
        cast(DrillFleet, workers),
        10,
        10,
        quiet_bound=WINDOW,
    )


def _stopped(workers: _Fleet) -> list[bool]:
    return [_process_state(h.process.pid).startswith("T") for h in workers.workers]


def test_the_freeze_window_is_half_the_operation_bound() -> None:
    assert freeze_window(timedelta(seconds=3)) == 1.5
    assert freeze_window(timedelta(milliseconds=800)) == 0.4


def test_a_database_that_is_never_quiet_ends_the_freeze_within_the_window(fleet: Any) -> None:
    conn = ActivityConn(waits=["Timeout:PgSleep"])
    freezes = Freezes()

    began = time.monotonic()
    victims = freeze_victims(_freeze_poll(conn, fleet), 1, set(), freezes=freezes)
    took = time.monotonic() - began

    assert victims == []
    assert took < WINDOW + SLACK
    assert _stopped(fleet) == [False, False]  # every worker continued
    [entry] = freezes.log
    assert entry["quiet"] is False
    assert entry["waits"] == ["Timeout:PgSleep"]
    assert entry["stopped_s"] < WINDOW + SLACK
    assert freezes.not_quiet == 1
    # The workers then run at least as long as they were stopped before the next freeze.
    assert freezes.not_before >= began + 2 * entry["stopped_s"]

    queried = conn.queries
    assert freeze_victims(_freeze_poll(conn, fleet), 1, set(), freezes=freezes) == []
    assert conn.queries == queried  # inside the cooldown: nobody stopped, nothing read
    assert len(freezes.log) == 1


def test_statements_only_sending_to_stopped_workers_end_the_freeze_at_once(fleet: Any) -> None:
    # A result a stopped worker never reads keeps its statement active: waiting gains nothing.
    conn = ActivityConn(waits=["Client:ClientWrite", "Client:ClientWrite"])
    freezes = Freezes()

    began = time.monotonic()
    assert freeze_victims(_freeze_poll(conn, fleet), 1, set(), freezes=freezes) == []

    assert time.monotonic() - began < WINDOW
    assert _stopped(fleet) == [False, False]
    [entry] = freezes.log
    assert entry["quiet"] is False
    assert entry["waits"] == ["Client:ClientWrite", "Client:ClientWrite"]
    assert freezes.not_before - began < WINDOW + SLACK  # retried soon, after a short rest


def test_a_quiet_database_gives_the_owner_and_continues_the_others(fleet: Any) -> None:
    owner = fleet.workers[0].process.pid
    attempt = uuid.uuid4()
    conn = ActivityConn(owners=[(attempt, f"host:{owner}:abc")])
    freezes = Freezes()

    victims = freeze_victims(_freeze_poll(conn, fleet), 1, set(), freezes=freezes)

    try:
        assert victims == [Victim(owner, attempt)]
        assert _stopped(fleet) == [True, False]  # the victim stays stopped for the drill
    finally:
        fleet.workers[0].process.send_signal(signal.SIGCONT)
    [entry] = freezes.log
    assert entry["quiet"] is True
    assert entry["stopped_s"] < WINDOW + SLACK
    assert freezes.not_quiet == 0


def test_an_owner_read_that_ends_past_the_window_continues_everyone(fleet: Any) -> None:
    owner = fleet.workers[0].process.pid
    conn = ActivityConn(owners=[(uuid.uuid4(), f"host:{owner}:abc")])
    conn.owner_read = WINDOW + 0.1
    freezes = Freezes()

    assert freeze_victims(_freeze_poll(conn, fleet), 1, set(), freezes=freezes) == []

    assert _stopped(fleet) == [False, False]
    assert freezes.log[-1]["quiet"] is False


# --- the crash drill with a live provider (DEC-071, T2) ------------------------------------

MODEL = "vendor/model-a"
SENTINEL = "sk-or-v1-SENTINEL-not-a-real-key-0123456789abcdef"
DRY = ["--provider", "openrouter", "--model", MODEL, "--max-provider-calls", "50", "--dry-run"]


def test_d1_accepts_the_provider_flags(named: list[str]) -> None:
    assert benchmarks.drills.main(["d1", *DRY]) == 0
    assert named == ["d1"]


def test_the_double_stays_the_default(monkeypatch: pytest.MonkeyPatch, named: list[str]) -> None:
    chosen: list[ProviderChoice] = []

    def run_named(name: str, args: Any, url: str) -> list[object]:
        chosen.append(benchmarks.drills.choice_of(args))
        return []

    monkeypatch.setattr(benchmarks.drills, "run_named", run_named)
    assert benchmarks.drills.main(["d1"]) == 0
    assert chosen == [DOUBLE]


@pytest.mark.parametrize("drill", ["d2", "d3a", "d3b", "d4"])
def test_every_other_drill_refuses_the_provider(
    named: list[str], capsys: pytest.CaptureFixture[str], drill: str
) -> None:
    assert benchmarks.drills.main([drill, *DRY]) == 1
    assert capsys.readouterr().err == "refused: --provider openrouter runs only with d1\n"
    assert named == []


@pytest.mark.parametrize(
    ("arguments", "fragment"),
    [
        (["--provider", "openrouter", "--model", MODEL, "--max-provider-calls", "5"], "--dry-run"),
        (["--provider", "openrouter", "--max-provider-calls", "5", "--dry-run"], "needs --model"),
        (["--provider", "openrouter", "--model", MODEL, "--dry-run"], "--max-provider-calls"),
        ([*DRY[:5], "2", "--dry-run", "--workers", "4", "--victims", "1"], "--workers is above"),
        (["--call-timeout", "PT1S"], "--call-timeout applies only"),
        ([*DRY, "--allow-live-calls"], "contradict"),
    ],
)
def test_the_l1_refusals_reach_the_drill_before_any_database(
    named: list[str],
    capsys: pytest.CaptureFixture[str],
    arguments: list[str],
    fragment: str,
) -> None:
    assert benchmarks.drills.main(["d1", *arguments]) == 1
    assert fragment in capsys.readouterr().err
    assert named == []


def test_a_live_drill_without_the_key_is_refused(
    named: list[str], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ORQESTRA_OPENROUTER_API_KEY", raising=False)
    live = [*DRY[:-1], "--allow-live-calls", "--call-timeout", "PT30S"]
    assert benchmarks.drills.main(["d1", *live]) == 1
    assert "ORQESTRA_OPENROUTER_API_KEY is not set" in capsys.readouterr().err
    assert named == []


def test_a_live_timeout_raises_only_the_execution_timeout() -> None:
    settings = benchmarks.run.provider_worker_settings(BENCH_WORKER_SETTINGS, 16, "PT30S")
    # 16 x (30 s + 2 x 3 s hand-off) + 2 s fence margin = 578 s.
    assert settings == {**BENCH_WORKER_SETTINGS, "ORQESTRA_WORKER_EXECUTION_TIMEOUT": "PT578S"}
    unchanged = benchmarks.run.provider_worker_settings(BENCH_WORKER_SETTINGS, 16, "PT0.1S")
    assert unchanged == dict(BENCH_WORKER_SETTINGS)


def test_expected_calls_per_item_match_the_oracle() -> None:
    from benchmarks.generate import expected_outcome

    for count, batch, cap in [(2_000, 100, 16), (1_000, 25, 16), (997, 40, 1), (300, 300, 3)]:
        by_item = benchmarks.drills.expected_calls_by_item(count, batch, cap)
        expected = expected_outcome(generate_invoices(count), batch, cap)
        assert sum(by_item.values()) == expected.calls, (count, batch, cap)
        assert set(by_item.values()) <= {1}


def test_duplicate_calls_count_only_calls_beyond_the_oracle() -> None:
    expected = {7: 1, 27: 1}
    assert benchmarks.drills.duplicate_calls([(7, 1), (27, 1)], expected) == 0
    assert benchmarks.drills.duplicate_calls([(7, 2), (27, 1)], expected) == 1
    assert benchmarks.drills.duplicate_calls([(7, 1), (8, 3)], expected) == 3  # 8 expects none
    assert benchmarks.drills.duplicate_calls([], expected) == 0


class _Stop(Exception):  # noqa: N818 - ends run_drill once its workers' setup is known
    pass


def test_a_live_drill_passes_the_key_only_in_the_workers_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setenv("ORQESTRA_OPENROUTER_API_KEY", SENTINEL)
    seen: dict[str, Any] = {}

    def fleet(profile: object, env: dict[str, str], raw_dir: object, *, command: list[str]) -> None:
        seen["env"], seen["command"] = env, command

    def harness(url: str) -> NoReturn:
        raise _Stop

    monkeypatch.setattr(benchmarks.drills, "DrillFleet", fleet)
    monkeypatch.setattr(benchmarks.drills, "Harness", harness)
    choice = ProviderChoice(
        name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=50, allow_live_calls=True
    )
    with pytest.raises(_Stop):
        benchmarks.drills.run_drill(
            "postgresql:///never-connected",
            Cell(Profile.INVOICE, 200, 100, 2),
            KillDrill(1),
            benchmarks.drills.standard_workload(generate_invoices(200), 100),
            raw_dir=tmp_path,
            stop_after=1.0,
            provider=choice,
            call_timeout="PT30S",
        )
    command, env = seen["command"], seen["env"]
    assert "--allow-live-calls" in command
    assert command[command.index("--budget-file") + 1] == str(tmp_path / BUDGET_FILE_NAME)
    assert (tmp_path / BUDGET_FILE_NAME).read_text(encoding="ascii") == "0\n"
    assert not [part for part in command if SENTINEL in part]
    assert [key for key, value in env.items() if SENTINEL in value] == [
        "ORQESTRA_OPENROUTER_API_KEY"
    ]
    assert env["ORQESTRA_WORKER_EXECUTION_TIMEOUT"] == "PT578S"


def test_the_provider_is_refused_with_a_drill_other_than_d1() -> None:
    with pytest.raises(ValueError, match="only with d1"):
        benchmarks.drills.run_drill(
            "postgresql:///never-connected",
            Cell(Profile.INVOICE, 200, 100, 2),
            benchmarks.drills.FreezeDrill(1.0),
            benchmarks.drills.standard_workload(generate_invoices(200), 100),
            raw_dir=Path("/nonexistent"),
            stop_after=1.0,
            provider=ProviderChoice(
                name=ProviderName.OPENROUTER, model=MODEL, max_provider_calls=5, dry_run=True
            ),
        )


# --- d1 on the extraction workload (T4) ------------------------------------------------------

EXTRACT_DRY = [
    "--workload",
    "extract",
    "--provider",
    "openrouter",
    "--model",
    "vendor/model-a",
    "--max-provider-calls",
    "500",
    "--dry-run",
    "--batch",
    "25",
]


def test_d1_accepts_the_extract_workload_on_a_dry_run(named: list[str]) -> None:
    assert benchmarks.drills.main(["d1", *EXTRACT_DRY]) == 0
    assert named == ["d1"]


@pytest.mark.parametrize(
    ("arguments", "fragment"),
    [
        (["d2", *EXTRACT_DRY], "only with d1"),
        (["d1", "--workload", "extract", "--batch", "25"], "needs --provider openrouter"),
        (["d1", *EXTRACT_DRY, "--price-in-per-mtok", "2.5"], "synthetic prices"),
        (["d1", "--price-out-per-mtok", "7"], "apply only to the extract profile"),
    ],
)
def test_the_extract_drill_refuses_inconsistent_flags(
    arguments: list[str],
    fragment: str,
    named: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert benchmarks.drills.main(arguments) == 1
    assert fragment in capsys.readouterr().err
    assert named == []


def test_a_live_extract_drill_needs_both_prices(
    named: list[str], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ORQESTRA_OPENROUTER_API_KEY", "sk-SENTINEL")
    live = [*EXTRACT_DRY[:-3], "--allow-live-calls", "--batch", "25"]
    assert benchmarks.drills.main(["d1", *live]) == 1
    error = capsys.readouterr().err
    assert "price" in error
    assert "SENTINEL" not in error
    assert named == []


def test_the_extract_drill_cell_and_its_call_timeout() -> None:
    import argparse

    args = argparse.Namespace(
        workload="extract", invoices=40, batch=10, workers=2, call_timeout=None
    )
    assert benchmarks.drills.drill_cell(args) == Cell(Profile.EXTRACT, 40, 10, 2)
    assert benchmarks.drills.drill_call_timeout(args) == "PT30S"
    mix = argparse.Namespace(workload="mix", invoices=40, batch=10, workers=2, call_timeout=None)
    assert benchmarks.drills.drill_cell(mix) == Cell(Profile.INVOICE, 40, 10, 2)
    assert benchmarks.drills.drill_call_timeout(mix) == "PT0.1S"
    workload = benchmarks.drills.extract_workload(Cell(Profile.EXTRACT, 40, 10, 2))
    assert workload.items == 40
    assert len(workload.tasks) == 4
    assert workload.expected is None
    with pytest.raises(ValueError, match="no invoice oracle"):
        _ = workload.oracle


# --- settings read once, through bench_settings ---------------------------------------------


@pytest.mark.parametrize(
    "settings", [BENCH_WORKER_SETTINGS, ENVIRONMENT], ids=["bench", "test-profile"]
)
def test_the_drills_retry_policy_is_the_validated_settings_own(settings: dict[str, str]) -> None:
    policy = benchmarks.drills.retry_policy(settings)
    assert policy == RetryPolicy(
        parse_duration(settings["ORQESTRA_WORKER_RETRY_BASE_DELAY"]),
        parse_duration(settings["ORQESTRA_WORKER_RETRY_MAX_DELAY"]),
    )
    assert policy == bench_settings(settings).retry_policy


def test_the_freeze_drill_holds_a_worker_past_the_benchs_own_lease() -> None:
    lease = bench_settings(BENCH_WORKER_SETTINGS).lease
    assert (
        lease
        == parse_duration(BENCH_WORKER_SETTINGS["ORQESTRA_WORKER_LEASE"])
        == timedelta(seconds=10)
    )
    assert lease.total_seconds() + benchmarks.drills.FREEZE_EXTRA == 12.0
