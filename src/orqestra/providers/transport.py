"""The HTTP transport contract an adapter is given, and the deadline it must respect.

An adapter never opens a connection itself. It is handed an ``HttpTransport``,
which sends exactly one POST and returns an ``HttpExchange`` to read the response
from. Tests hand it a fake; ``orqestra.providers.stdlib_transport`` holds the one
implementation that touches the network. This module imports nothing network-related.

A transport fails in one of two ways, each with fixed text and nothing else:
``TransportTimeout`` when the deadline ran out or a socket operation timed out, and
``TransportFailure`` for anything else that went wrong on the wire. Neither carries
an exception chain, so no remote text, header value or credential travels with it.
"""

from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Protocol

TRANSPORT_TIMEOUT = "transport_timeout"
TRANSPORT_FAILURE = "transport_failure"


class TransportTimeout(Exception):  # noqa: N818 - a timeout is an outcome, named as one
    """The deadline ran out, or a blocking operation timed out. The text is fixed."""

    def __init__(self) -> None:
        super().__init__(TRANSPORT_TIMEOUT)


class TransportFailure(Exception):  # noqa: N818 - a failure is an outcome, named as one
    """A connection, TLS, protocol or framing failure. The text is fixed."""

    def __init__(self) -> None:
        super().__init__(TRANSPORT_FAILURE)


class Deadline:
    """A wall-clock budget measured on an injected monotonic clock.

    ``remaining()`` is the number of seconds left. When none are left (zero
    included) it raises ``TransportTimeout`` instead of returning, so a caller can
    never pass a zero or negative timeout to a socket, where zero would mean
    non-blocking rather than "no time left".
    """

    __slots__ = ("_clock", "_expires")

    def __init__(self, clock: Callable[[], float], budget: timedelta) -> None:
        given: object = budget  # checked: a caller may pass anything
        if not isinstance(given, timedelta):
            msg = f"a deadline budget must be a timedelta, got {type(given).__name__}"
            raise TypeError(msg)
        self._clock = clock
        self._expires = clock() + given.total_seconds()

    def remaining(self) -> float:
        """Seconds left, always > 0; ``TransportTimeout`` when the budget is spent."""
        left = self._expires - self._clock()
        if left <= 0:
            raise TransportTimeout
        return left


class HttpExchange(Protocol):
    """One response being read. ``close`` must be called on every path."""

    @property
    def status(self) -> int:
        """The HTTP status code."""
        ...

    def read(self, max_bytes: int, deadline: Deadline) -> bytes:
        """Up to ``max_bytes`` of the body, ``b""`` at its end.

        Raises ``TransportTimeout`` or ``TransportFailure``.
        """
        ...

    def close(self) -> None:
        """Release the connection. Never raises."""
        ...


class HttpTransport(Protocol):
    """Sends one POST to a fixed endpoint and returns the response to read."""

    def post(
        self, *, path: str, headers: Mapping[str, str], body: bytes, deadline: Deadline
    ) -> HttpExchange:
        """Send ``body`` with exactly ``headers`` (plus framing) to ``path``.

        Raises ``TransportTimeout`` or ``TransportFailure``; never retries and
        never follows a redirect.
        """
        ...
