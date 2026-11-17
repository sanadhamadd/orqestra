"""A database pause in the middle of a cell: stalled workers are replaced after a delay,
the cell still completes, and the checkpoints the server ran are recorded.

The cell runs on the double, offline, and reaches PostgreSQL through the test ``PgProxy``
(harness and workers alike). The proxy holds all traffic from the moment a worker sends
its completion: that worker owns a running attempt, so it stops itself for the stalled
call (exit 4) whether or not the engine lets an idle worker ride out a pause. Nothing
here depends on what idle workers do.
"""

import threading
import time
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from benchmarks.result import Status, WaitEnded
from benchmarks.run import STALL_REPLACEMENT_DELAY, Cell, Profile, Running, run_cell
from orqestra.worker.fence import ExitCode
from tests.integration.pg_proxy import _COMPLETION as COMPLETION
from tests.integration.pg_proxy import PgProxy
from tests.integration.test_worker_process import ENVIRONMENT

pytestmark = pytest.mark.integration

STOP = 180.0
PAUSE = 3.0  # past the test settings' 0.8 s operation bound and a 1 s heartbeat
CELL = Cell(Profile.INVOICE, size=2_000, batch=100, workers=2)


def request_checkpoint(url: str) -> bool:
    """Ask the server for a checkpoint on a direct connection; False if the role may not."""
    with psycopg.connect(url, autocommit=True) as conn:
        try:
            row = conn.execute(
                "SELECT rolsuper OR pg_has_role(current_user, 'pg_checkpoint', 'MEMBER') "
                "FROM pg_roles WHERE rolname = current_user"
            ).fetchone()
        except psycopg.Error:  # a server without the pg_checkpoint role
            return False
        if not row or not row[0]:
            return False
        conn.execute("CHECKPOINT")
        return True


def test_a_pause_while_owning_is_replaced_after_the_delay_and_the_cell_completes(
    db: psycopg.Connection[TupleRow], schema_url: str, tmp_path: Path
) -> None:
    checkpointed: list[bool] = []
    armed: list[bool] = []

    with PgProxy(schema_url) as proxy:

        def arm_on_the_first_poll(running: Running) -> None:
            if not armed:
                proxy.hold_on(COMPLETION)  # the next completion stalls everything
                armed.append(True)

        def release() -> None:
            if not proxy.held.wait(timeout=STOP):
                return
            checkpointed.append(request_checkpoint(schema_url))
            time.sleep(PAUSE)
            proxy.resume()

        releaser = threading.Thread(target=release, daemon=True)
        releaser.start()
        try:
            record: dict[str, Any] = run_cell(
                proxy.url(schema_url),
                CELL,
                worker_settings=ENVIRONMENT,
                raw_dir=tmp_path / "raw",
                stop_after=STOP,
                idle_window=0.5,
                hook=arm_on_the_first_poll,
            )
        finally:
            proxy.resume()
            releaser.join(timeout=STOP)

    assert proxy.held.is_set()
    exits = record["failures"]["exits_while_running"]
    # At least the owner that sent the held completion stopped itself; every exit was a stall.
    assert exits
    assert {died["exit_code"] for died in exits} == {ExitCode.STALLED}
    for died in exits:
        if died["replaced"]:
            assert died["replacement_at_s"] - died["at_s"] >= STALL_REPLACEMENT_DELAY
    assert record["failures"]["replacements"] >= 1
    # The work the stalled workers held came back through lease expiry and recovery.
    assert record["timing"]["wait_ended"] == WaitEnded.FINISHED
    assert [check for check in record["gate"] if not check["passed"]] == []
    assert record["status"] == Status.COMPLETE

    block = record["database_stats"]["checkpoints"]
    assert isinstance(block, dict)
    assert block["source"] in {"pg_stat_checkpointer", "pg_stat_bgwriter"}
    assert block["samples"] >= 2
    if checkpointed == [True]:
        assert block["requested"] >= 1
        assert block["events"]
        last = block["events"][-1]["ended_by_s"]
        assert 0 <= last <= record["timing"]["waited_s"] + 10
