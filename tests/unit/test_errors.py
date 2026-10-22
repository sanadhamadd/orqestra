import pytest

from orqestra.domain.errors import (
    InvalidTransitionError,
    OrqestraError,
    PermanentError,
    TransientError,
    is_retryable,
)

pytestmark = pytest.mark.unit


def test_transient_errors_are_retryable() -> None:
    assert is_retryable(TransientError("rate limited"))


def test_permanent_errors_are_not_retryable() -> None:
    assert not is_retryable(PermanentError("invalid invoice"))


def test_unknown_exceptions_are_not_retryable() -> None:
    assert not is_retryable(RuntimeError("a bug"))
    assert not is_retryable(KeyError("missing"))


def test_hierarchy() -> None:
    assert issubclass(TransientError, OrqestraError)
    assert issubclass(PermanentError, OrqestraError)
    assert issubclass(InvalidTransitionError, PermanentError)
    assert issubclass(InvalidTransitionError, ValueError)
