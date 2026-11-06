import pytest
from pydantic import ValidationError

from orqestra.config import Settings

pytestmark = pytest.mark.unit


def test_defaults() -> None:
    settings = Settings()
    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.database_url is None
    assert settings.worker_concurrency == 4
    assert settings.openrouter_api_key is None
    assert settings.redacted()["openrouter_api_key"] is None


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORQESTRA_ENVIRONMENT", "test")
    monkeypatch.setenv("ORQESTRA_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("ORQESTRA_WORKER_CONCURRENCY", "16")
    settings = Settings()
    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"
    assert settings.worker_concurrency == 16


@pytest.mark.parametrize("value", ["0", "-1", "257", "many"])
def test_invalid_concurrency_rejected(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("ORQESTRA_WORKER_CONCURRENCY", value)
    with pytest.raises(ValidationError):
        Settings()


def test_invalid_log_level_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORQESTRA_LOG_LEVEL", "LOUD")
    with pytest.raises(ValidationError):
        Settings()


def test_database_url_is_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "postgresql://user:hunter2@localhost:5432/orqestra"
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", secret)
    settings = Settings()
    assert settings.database_url is not None
    assert settings.database_url.get_secret_value() == secret
    assert "hunter2" not in repr(settings)
    assert "hunter2" not in str(settings.redacted())
    assert settings.redacted()["database_url"] == "***"


def test_openrouter_api_key_is_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "sk-or-v1-synthetic-not-a-key-0000"
    monkeypatch.setenv("ORQESTRA_OPENROUTER_API_KEY", secret)
    settings = Settings()
    assert settings.openrouter_api_key is not None
    assert settings.openrouter_api_key.get_secret_value() == secret
    assert secret not in repr(settings)
    assert secret not in str(settings)
    assert secret not in str(settings.redacted())
    assert settings.redacted()["openrouter_api_key"] == "***"
