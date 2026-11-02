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
a heartbeat that, sent on time and taking its full database time, is
confirmed before the local fencing margin is reached; and a lock wait that
PostgreSQL ends before the database call's own bound, compared in the whole
milliseconds PostgreSQL uses (DEC-050); and a retry policy whose base delay
does not exceed its maximum (DEC-053). Nothing here bounds how
quickly expired attempts are recovered. The database connection itself is the
existing ``ORQESTRA_DATABASE_URL`` (``Settings.database_url``).
"""

from datetime import timedelta
from typing import Self

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from orqestra.domain.leases import LeasePolicy, check_duration
from orqestra.domain.retry import RetryPolicy

# PostgreSQL's statement_timeout and lock_timeout count whole milliseconds, and 0
# disables them.
_SHORTEST_DATABASE_TIMEOUT = timedelta(milliseconds=1)
_MILLISECOND = timedelta(milliseconds=1)


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
                "before the worker's fencing margin is reached"
            )
            raise ValueError(msg)
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
