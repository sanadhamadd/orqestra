"""Error taxonomy.

Retry policy depends on classifying failures, so the classification is a
first-class domain concept rather than an afterthought in the worker.

- ``TransientError``: a later attempt may succeed (rate limit, timeout,
  temporary provider outage). Eligible for bounded retry.
- ``PermanentError``: retrying cannot help (invalid input, business rule
  violation, exhausted budget). Terminal for the task.
- Anything else (an unexpected exception, i.e. a bug) is treated as permanent:
  retrying a defect wastes time and money and hides the defect.
"""


class OrqestraError(Exception):
    """Base class for all Orqestra errors."""

    retryable: bool = False


class TransientError(OrqestraError):
    """A failure that a later attempt may not repeat."""

    retryable = True


class PermanentError(OrqestraError):
    """A failure that no retry can fix."""

    retryable = False


class InvalidTransitionError(PermanentError, ValueError):
    """Raised when a lifecycle state change is not allowed."""


def is_retryable(exc: BaseException) -> bool:
    """Return whether ``exc`` should be retried.

    Only explicit ``TransientError`` instances are retryable. Unknown
    exceptions are deliberately *not* retried.
    """
    return isinstance(exc, OrqestraError) and exc.retryable
