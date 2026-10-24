from importlib.metadata import version

import pytest

import orqestra

pytestmark = pytest.mark.unit


def test_version_matches_installed_metadata() -> None:
    assert orqestra.__version__ == version("orqestra")
