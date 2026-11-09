"""Worker runtime settings (DEC-044).

Every timing value is required: there are no defaults, because operational
values are undecided and need the owner's approval (DEC-034). Values come
from the environment with the prefix ``ORQESTRA_WORKER_`` (or a local
``.env``). Durations there are ISO 8601 durations such as ``PT30S`` (or
``HH:MM:SS``); a bare number has no unit and is refused.

The worker is single-slot: one task at a time per process. The existing
``Settings.worker_concurrency`` is not read by the worker.

Checked here are the relationships a healthy worker needs: the lease rules of
``LeasePolicy``; a database call that ends before the next heartbeat is due;
``heartbeat_interval + db_operation_timeout + fence_margin < lease``, so that a
heartbeat sent on time and taking its full database time is confirmed while
more than ``fence_margin`` of the lease it renews really remains; room for a
usage record or sweep step between heartbeats that take their full database
time: ``heartbeat_interval >= 2 * db_operation_timeout`` or
``lease >= 3 * db_operation_timeout + fence_margin`` (DEC-070); a lock wait
that PostgreSQL ends before the database call's own bound, compared in the
whole milliseconds PostgreSQL uses (DEC-050); a retry policy whose base delay
does not exceed its maximum (DEC-053); and an ``execution_timeout`` and a
``retry_max_delay`` that leave every deadline and retry time representable for
at least a century from now. Nothing here bounds how quickly expired attempts
are recovered.

The worker's local fence comes earlier than the lease by the attempt's claim
latency: the time from the local clock read just before the claim was sent to
the claim's database instant, which the operation guard keeps to about
``db_operation_timeout``. Every ownership deadline is mapped from the claim
(``local_deadline``). The first heartbeat is scheduled from the same clock
read, so the latency cancels out; each later one is scheduled from when the
previous one was sent, so it does not, and the checks above leave it out.
Settings they accept can therefore fence a healthy worker (exit 3) whose claim
took longer than ``lease - (heartbeat_interval + db_operation_timeout +
fence_margin)``. ``heartbeat_interval + 2 * db_operation_timeout +
fence_margin < lease`` leaves room for any claim the operation guard lets
through; it is not checked (DEC-044 note). A fence that comes early never lets
a handler outlive its lease. The database connection itself is the existing
``ORQESTRA_DATABASE_URL`` (``Settings.database_url``).
"""

from datetime import UTC, datetime, timedelta
from typing import Self

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from orqestra.domain.leases import LeasePolicy, check_duration
from orqestra.domain.retry import RetryPolicy

# PostgreSQL's statement_timeout and lock_timeout count whole milliseconds, and 0
# disables them.
_SHORTEST_DATABASE_TIMEOUT = timedelta(milliseconds=1)
_MILLISECOND = timedelta(milliseconds=1)
# How long a deadline or retry time computed from a setting must stay representable.
_REPRESENTABLE_FOR = timedelta(days=36525)  # a century


def _representable_for_a_century(name: str, value: timedelta, what: str) -> None:
    """Raise unless ``now + value`` stays a representable timestamp for a century."""
    try:
        datetime.now(UTC) + value + _REPRESENTABLE_FOR
    except OverflowError:
        msg = (
            f"{name} {value} is too long: it must leave every {what} representable for at "
            "least a century from now"
        )
        raise ValueError(msg) from None


def server_milliseconds(value: timedelta) -> int:
    """``value`` in the whole milliseconds a PostgreSQL timeout setting takes, rounded up.

    Exact: ``timedelta`` arithmetic, not floating point, which rounds some
    whole-millisecond values (2007 ms, for one) up a millisecond too far.
    """
    return -(-value // _MILLISECOND)


class WorkerSettings(BaseSettings):
    """Validated, required timing settings of one single-slot worker process."""

    model_config = SettingsConfigDict(
        env_prefix="ORQESTRA_WORKER_", env_file=".env", extra="ignore", frozen=True
    )

    # Each claim and heartbeat grants this much lease (LeasePolicy.lease).
    lease: timedelta
    # An attempt's non-renewable deadline after its claim (LeasePolicy.execution_timeout).
    execution_timeout: timedelta
    # How often the worker renews its lease while a handler runs.
    heartbeat_interval: timedelta
    # The longest a single database call may take before it is abandoned.
    db_operation_timeout: timedelta
    # The longest a supervisor statement waits for a row or table lock (DEC-050).
    lock_timeout: timedelta
    # How long before its lease may end the worker stops a handler it cannot vouch for.
    fence_margin: timedelta
    # How long an idle worker waits before trying to claim again.
    poll_interval: timedelta
    # How often the worker sweeps for expired attempts to revoke.
    recovery_interval: timedelta
    # The most attempts one sweep revokes.
    recovery_batch_limit: int = Field(ge=1)
    # How long a stopping worker gives the task it holds to settle (its handler, the
    # completion and any reconciliation), from the latch; 0 means not at all (DEC-049).
    shutdown_timeout: timedelta
    # How long a requeued task waits after its first ended attempt; doubled after each
    # later one, up to retry_max_delay, with jitter (RetryPolicy, DEC-053). Every worker
    # of a deployment must use the same values: nothing enforces that.
    retry_base_delay: timedelta
    retry_max_delay: timedelta

    @field_validator(
        "lease",
        "execution_timeout",
        "heartbeat_interval",
        "db_operation_timeout",
        "lock_timeout",
        "fence_margin",
        "poll_interval",
        "recovery_interval",
        "retry_base_delay",
        "retry_max_delay",
    )
    @classmethod
    def _positive(cls, value: timedelta, info: ValidationInfo) -> timedelta:
        check_duration(info.field_name or "duration", value)
        return value

    @field_validator("shutdown_timeout")
    @classmethod
    def _not_negative(cls, value: timedelta) -> timedelta:
        if value < timedelta(0):
            msg = f"shutdown_timeout must not be negative, got {value}"
            raise ValueError(msg)
        if value:
            check_duration("shutdown_timeout", value)
        return value

    @field_validator("db_operation_timeout", "lock_timeout")
    @classmethod
    def _database_resolution(cls, value: timedelta, info: ValidationInfo) -> timedelta:
        if value < _SHORTEST_DATABASE_TIMEOUT:
            msg = f"{info.field_name} must be at least 1 ms, got {value}"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _relationships(self) -> Self:
        self.lease_policy  # noqa: B018 - LeasePolicy checks lease <= execution_timeout
        self.retry_policy  # noqa: B018 - RetryPolicy checks base <= max
        if self.db_operation_timeout >= self.heartbeat_interval:
            msg = (
                f"db_operation_timeout {self.db_operation_timeout} must be shorter than "
                f"heartbeat_interval {self.heartbeat_interval}: a database call must end "
                "before the next heartbeat is due"
            )
            raise ValueError(msg)
        needed = self.heartbeat_interval + self.db_operation_timeout + self.fence_margin
        if needed >= self.lease:
            msg = (
                f"heartbeat_interval + db_operation_timeout + fence_margin ({needed}) must be "
                f"shorter than lease {self.lease}: a heartbeat sent on time must be confirmed "
                "while more than fence_margin of the lease it renews remains"
            )
            raise ValueError(msg)
        bound = self.db_operation_timeout
        if self.heartbeat_interval < 2 * bound and self.lease < 3 * bound + self.fence_margin:
            msg = (
                f"heartbeat_interval {self.heartbeat_interval} is shorter than "
                f"2 x db_operation_timeout and lease {self.lease} is shorter than "
                f"3 x db_operation_timeout + fence_margin ({3 * bound + self.fence_margin}): "
                "no usage record or sweep step could start between heartbeats that take "
                "their full database time (DEC-070)"
            )
            raise ValueError(msg)
        _representable_for_a_century("execution_timeout", self.execution_timeout, "deadline")
        _representable_for_a_century("retry_max_delay", self.retry_max_delay, "retry time")
        lock, statement = (
            server_milliseconds(self.lock_timeout),
            server_milliseconds(self.db_operation_timeout),
        )
        if lock >= statement:
            msg = (
                f"lock_timeout {self.lock_timeout} ({lock} ms in PostgreSQL) must be shorter than "
                f"db_operation_timeout {self.db_operation_timeout} ({statement} ms): a lock wait "
                "must end, and be retried, before the call's own bound ends the process"
            )
            raise ValueError(msg)
        return self

    @property
    def lease_policy(self) -> LeasePolicy:
        """The lease policy every claim and heartbeat of this worker uses."""
        return LeasePolicy(lease=self.lease, execution_timeout=self.execution_timeout)

    @property
    def retry_policy(self) -> RetryPolicy:
        """The retry policy every requeue this worker records uses (completion and recovery)."""
        return RetryPolicy(base_delay=self.retry_base_delay, max_delay=self.retry_max_delay)
