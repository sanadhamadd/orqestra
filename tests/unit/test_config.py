from pathlib import Path

import pytest
from pydantic import ValidationError

from orqestra.config import Settings
from orqestra.worker.settings import WorkerSettings

pytestmark = pytest.mark.unit


def test_defaults() -> None:
    settings = Settings()
    assert settings.environment == "development"
    assert settings.log_level == "INFO"
    assert settings.database_url is None
    assert settings.openrouter_api_key is None
    assert settings.redacted()["openrouter_api_key"] is None


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORQESTRA_ENVIRONMENT", "test")
    monkeypatch.setenv("ORQESTRA_LOG_LEVEL", "DEBUG")
    settings = Settings()
    assert settings.environment == "test"
    assert settings.log_level == "DEBUG"


@pytest.mark.parametrize("value", ["staging", "PRODUCTION", "prod", "dev"])
def test_an_unknown_environment_is_rejected(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("ORQESTRA_ENVIRONMENT", value)
    with pytest.raises(ValidationError):
        Settings()


BLANK = ["", "   ", "\t\n"]


@pytest.mark.parametrize("blank", BLANK)
@pytest.mark.parametrize("name", ["ORQESTRA_DATABASE_URL", "ORQESTRA_OPENROUTER_API_KEY"])
def test_a_blank_secret_is_not_set(monkeypatch: pytest.MonkeyPatch, name: str, blank: str) -> None:
    monkeypatch.setenv(name, blank)
    settings = Settings()
    assert settings.database_url is None
    assert settings.openrouter_api_key is None
    assert settings.redacted()["database_url"] is None
    assert settings.redacted()["openrouter_api_key"] is None


def test_a_blank_secret_given_directly_is_not_set() -> None:
    assert Settings(database_url="  ", openrouter_api_key="").database_url is None


def test_a_set_secret_is_kept_as_given(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORQESTRA_DATABASE_URL", " postgresql://u@h/db ")
    url = Settings().database_url
    assert url is not None
    assert url.get_secret_value() == " postgresql://u@h/db "


@pytest.mark.parametrize("blank", BLANK)
def test_a_blank_setting_that_is_not_a_secret_is_still_refused(
    monkeypatch: pytest.MonkeyPatch, blank: str
) -> None:
    monkeypatch.setenv("ORQESTRA_LOG_LEVEL", blank)
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


ENV_EXAMPLE = Path(__file__).resolve().parents[2] / ".env.example"
PROFILE_HEADER = "# Example values, not recommendations"


def example_profile() -> dict[str, str]:
    """The commented ``ORQESTRA_WORKER_*`` lines of ``.env.example``'s example profile."""
    lines = ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith(PROFILE_HEADER))
    profile: dict[str, str] = {}
    for line in lines[start + 1 :]:
        if not line.strip():
            break
        name, sep, value = line.removeprefix("# ").partition("=")
        if sep and name.startswith("ORQESTRA_WORKER_"):
            profile[name] = value
    return profile


def test_the_example_worker_profile_is_complete_and_valid(monkeypatch: pytest.MonkeyPatch) -> None:
    profile = example_profile()
    required = {name for name, field in WorkerSettings.model_fields.items() if field.is_required()}
    named = {name.removeprefix("ORQESTRA_WORKER_").lower() for name in profile}
    assert required <= named
    for name, value in profile.items():
        monkeypatch.setenv(name, value)
    settings = WorkerSettings()  # validates every relationship, DEC-070's included
    assert settings.lease.total_seconds() == 4.0
