"""Runtime configuration.

Settings come from the process environment (prefix ``ORQESTRA_``) and, in
development, from a local ``.env`` file. Every value is validated once at
startup so bad configuration fails fast instead of surfacing mid-job.
Secrets are ``SecretStr`` so they never leak through ``repr`` or logs.
"""

from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["development", "test", "production"]
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]


class Settings(BaseSettings):
    """Validated process configuration."""

    model_config = SettingsConfigDict(env_prefix="ORQESTRA_", env_file=".env", extra="ignore")

    environment: Environment = "development"
    log_level: LogLevel = "INFO"
    # PostgreSQL DSN. Optional for `version` and `health`; every command that uses the
    # database requires it. A blank value counts as not set.
    database_url: SecretStr | None = None
    # Concurrent task slots per worker process (not OS threads or hosts). Not read by
    # anything yet: the worker is single-slot by approved direction (DEC-044).
    worker_concurrency: int = Field(default=4, ge=1, le=256)
    # OpenRouter API key. Optional, and not read by anything yet: the OpenRouter adapter
    # (orqestra.providers.openrouter) is not wired into a worker or the CLI.
    openrouter_api_key: SecretStr | None = None

    @field_validator("database_url", "openrouter_api_key", mode="before")
    @classmethod
    def _blank_is_unset(cls, value: object) -> object:
        """An empty or whitespace-only secret is not set: libpq would read an empty DSN as
        "connect to the default server", so it must not count as a URL."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    def redacted(self) -> dict[str, object]:
        """Return settings safe to print: secrets replaced, not revealed."""
        return {
            "environment": self.environment,
            "log_level": self.log_level,
            "database_url": "***" if self.database_url is not None else None,
            "worker_concurrency": self.worker_concurrency,
            "openrouter_api_key": "***" if self.openrouter_api_key is not None else None,
        }
