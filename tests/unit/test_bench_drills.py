"""The drills' D3b checks and command-line refusals, without a database or Docker."""

import signal
import subprocess
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
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
    Harness,
    Poll,
    RestartDrill,
    RestartRefusedError,
    Victim,
    check_restart_target,
    wait_over,
)
from orqestra.domain.states import TaskState

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
