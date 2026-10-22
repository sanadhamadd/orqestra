from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolated_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Strip ORQESTRA_* variables and hide any local .env so tests are hermetic."""
    import os

    for key in list(os.environ):
        if key.startswith("ORQESTRA_"):
            monkeypatch.delenv(key)
    monkeypatch.chdir(tmp_path)
