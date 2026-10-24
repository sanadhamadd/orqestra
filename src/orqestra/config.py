"""Runtime configuration.

Settings come from the process environment (prefix ``ORQESTRA_``) and, in
development, from a local ``.env`` file. Every value is validated once at
startup so bad configuration fails fast instead of surfacing mid-job.
Secrets are ``SecretStr`` so they never leak through ``repr`` or logs.
"""

from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["development", "test", "production"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class Settings(BaseSettings):
    """Validated process configuration."""

    model_config = SettingsConfigDict(env_prefix="ORQESTRA_", env_file=".env", extra="ignore")

    environment: Environment = "development"
    log_level: LogLevel = "INFO"
    # PostgreSQL DSN. Optional on Day 1; persistence (Day 2) requires it.
    database_url: SecretStr | None = None
    # Concurrent task slots held by one worker process (not OS threads or hosts).
    worker_concurrency: int = Field(default=4, ge=1, le=256)

    def redacted(self) -> dict[str, object]:
        """Return settings safe to print: secrets replaced, not revealed."""
        return {
            "environment": self.environment,
            "log_level": self.log_level,
            "database_url": "***" if self.database_url is not None else None,
            "worker_concurrency": self.worker_concurrency,
        }
