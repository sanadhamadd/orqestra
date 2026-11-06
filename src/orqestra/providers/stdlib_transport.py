"""The one module that opens a network connection: a single HTTPS POST to OpenRouter.

``HttpsTransport`` is built on ``http.client`` and ``ssl`` alone:

- **One endpoint.** The host (``openrouter.ai``), port (443) and path
  (``/api/v1/chat/completions``) are constants. Redirects are not followed (a 3xx is
  just a status), no proxy setting is read from the environment, and the HTTP debug
  level stays 0, so nothing is printed.
- **Deadline before every blocking step.** The connection is made with the time left
  as its timeout, then the socket's timeout is set to the time left before the request
  is sent, before the response head is read, and before each body read. With no time
  left, ``TransportTimeout`` is raised before the step starts.
- **Closed failures.** A ``TimeoutError`` becomes ``TransportTimeout``; an ``OSError``
  (``ssl.SSLError`` included), an ``http.client.HTTPException``, a ``ValueError`` or an
  ``OverflowError`` becomes ``TransportFailure``. Only the kind is kept: the new error is
  raised after the ``try`` statement, so it has no ``__cause__`` or ``__context__``. This
  matters because standard-library messages can quote what was sent, for example an
  illegal header value, credential included. An ``OverflowError`` comes from a huge
  budget: it overflows the socket timeout after the name lookup but before any
  connection is made or request bytes are sent, and it is reported as ``unavailable``.
  Checking the cap, timeout and fence together remains the wiring change's job.
- **Always closed.** A failed request closes its connection before raising; a
  response is closed by its exchange's ``close``.

Limits (see the README): the standard library has no timeout for name lookup
(``getaddrinfo``). Every other blocking step (connecting, including the TLS handshake;
sending; reading the response head; each body read) is given the time left when that
step starts, and the time is checked again only before the next step. Reading the
response head (the status line, up to 100 header lines, and any ``100 Continue``
responses) and reading one body chunk can each wait on the socket several times, and
sending writes the head and the body separately. So a slow server can make one call
overrun its budget by more than one wait.
"""

import contextlib
import http.client
import ssl
from collections.abc import Mapping
from enum import Enum

from orqestra.providers.transport import (
    Deadline,
    HttpExchange,
    TransportFailure,
    TransportTimeout,
)

HOST = "openrouter.ai"
PORT = 443
CHAT_COMPLETIONS_PATH = "/api/v1/chat/completions"

# ``ssl.SSLError`` is an ``OSError``; it is named so the mapping reads completely.
# ``OverflowError``: a huge budget overflows the socket timeout after the name lookup but
# before any connection is made or request bytes are sent.
_FAILURES = (ssl.SSLError, OSError, http.client.HTTPException, ValueError, OverflowError)


class _Kind(Enum):
    TIMEOUT = "timeout"
    FAILURE = "failure"


def _failure(kind: _Kind) -> Exception:
    return TransportTimeout() if kind is _Kind.TIMEOUT else TransportFailure()


def _close_quietly(resource: http.client.HTTPSConnection | http.client.HTTPResponse) -> None:
    with contextlib.suppress(OSError):
        resource.close()


class _HttpsExchange:
    """A response from ``HttpsTransport``; it owns the connection until ``close``."""

    def __init__(
        self,
        connection: http.client.HTTPSConnection,
        sock: object,
        response: http.client.HTTPResponse,
    ) -> None:
        self._connection = connection
        # Held because ``getresponse`` drops the connection's own reference when the
        # server will close, while the response still reads from this socket.
        self._sock = sock
        self._response = response

    @property
    def status(self) -> int:
        return self._response.status

    def read(self, max_bytes: int, deadline: Deadline) -> bytes:
        kind: _Kind
        try:
            _set_timeout(self._sock, deadline)
            data = self._response.read1(max_bytes)
        except TransportTimeout:
            kind = _Kind.TIMEOUT
        except TimeoutError:
            kind = _Kind.TIMEOUT
        except TransportFailure:
            kind = _Kind.FAILURE
        except _FAILURES:
            kind = _Kind.FAILURE
        else:
            return data
        raise _failure(kind)  # outside the handlers: nothing is chained

    def close(self) -> None:
        _close_quietly(self._response)
        _close_quietly(self._connection)


def _set_timeout(sock: object, deadline: Deadline) -> None:
    remaining = deadline.remaining()  # raises TransportTimeout with no time left
    settimeout = getattr(sock, "settimeout", None)
    if settimeout is None:
        raise TransportFailure  # no socket: the connection was not established
    settimeout(remaining)


class HttpsTransport:
    """An ``HttpTransport`` that sends one POST to OpenRouter over TLS."""

    def __init__(self, context: ssl.SSLContext | None = None) -> None:
        self._context = ssl.create_default_context() if context is None else context

    def post(
        self, *, path: str, headers: Mapping[str, str], body: bytes, deadline: Deadline
    ) -> HttpExchange:
        if path != CHAT_COMPLETIONS_PATH:
            msg = "this transport posts only to the chat completions path"
            raise ValueError(msg)
        connection: http.client.HTTPSConnection | None = None
        kind: _Kind
        try:
            opened = http.client.HTTPSConnection(
                HOST, PORT, timeout=deadline.remaining(), context=self._context
            )
            connection = opened
            opened.connect()
            sock = opened.sock
            _set_timeout(sock, deadline)
            # Host and Content-Length are the only headers added, for framing.
            opened.putrequest("POST", path, skip_accept_encoding=True)
            for name, value in headers.items():
                opened.putheader(name, value)
            opened.putheader("Content-Length", str(len(body)))
            opened.endheaders(body)
            _set_timeout(sock, deadline)
            response = opened.getresponse()
        except TransportTimeout:
            kind = _Kind.TIMEOUT
        except TimeoutError:
            kind = _Kind.TIMEOUT
        except TransportFailure:
            kind = _Kind.FAILURE
        except _FAILURES:
            kind = _Kind.FAILURE
        else:
            return _HttpsExchange(opened, sock, response)
        if connection is not None:
            _close_quietly(connection)
        raise _failure(kind)  # outside the handlers: nothing is chained
