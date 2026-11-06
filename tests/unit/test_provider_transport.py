"""The stdlib HTTPS transport over a stand-in for ``http.client.HTTPSConnection``.

No socket is opened: ``no_network`` is on, and the connection class the transport
looks up is replaced by either a scripted fake set as ``http.client.HTTPSConnection``
(to check the order of steps, the timeouts and the failure mapping) or a subclass of
the real class whose socket is a stand-in, reached through a copy of ``http.client``
set as the transport module's ``http`` name (so the real header checks and request
framing run).
"""

import http.client
import ssl
import traceback
from collections.abc import Callable, Mapping
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from pydantic import SecretStr

from orqestra.ai.provider import FieldKind, ProviderTransientError, TransientCode, make_request
from orqestra.providers import openrouter, stdlib_transport
from orqestra.providers.openrouter import OpenRouterConfig, OpenRouterProvider
from orqestra.providers.stdlib_transport import (
    CHAT_COMPLETIONS_PATH,
    HOST,
    PORT,
    HttpsTransport,
)
from orqestra.providers.transport import (
    Deadline,
    HttpExchange,
    TransportFailure,
    TransportTimeout,
)

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_network")]

SENTINEL = "SENTINEL-transport-secret"
CREDENTIAL = "sk-or-v1-SENTINELcredential0123456789"
HEADERS = {
    "Authorization": f"Bearer {CREDENTIAL}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
BODY = b'{"model":"vendor/model","stream":false}'
CONTEXT = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


class FakeClock:
    def __init__(self) -> None:
        self.now = 500.0

    def __call__(self) -> float:
        return self.now


class Script:
    """What the fake connection does, and a log of every call it received, in order."""

    def __init__(self, step_seconds: float = 0.5) -> None:
        self.clock = FakeClock()
        self.step_seconds = step_seconds
        self.log: list[tuple[str, object]] = []
        self.failures: dict[str, Exception] = {}
        self.connections: list[FakeConnection] = []
        self.status = 200
        self.chunks = [b'{"ok":', b"true}"]
        self.socket = True

    def note(self, name: str, detail: object = None) -> None:
        self.log.append((name, detail))
        failure = self.failures.get(name)
        if failure is not None:
            raise failure

    def block(self, name: str, detail: object = None) -> None:
        """A blocking step: it takes ``step_seconds`` of the clock."""
        self.clock.now += self.step_seconds
        self.note(name, detail)

    def names(self) -> list[str]:
        return [name for name, _ in self.log]

    def details(self, name: str) -> list[object]:
        return [detail for logged, detail in self.log if logged == name]


class FakeSocket:
    def __init__(self, script: Script) -> None:
        self.script = script

    def settimeout(self, value: float) -> None:
        self.script.note("settimeout", value)

    def close(self) -> None:
        self.script.note("socket.close")


class FakeResponse:
    def __init__(self, script: Script) -> None:
        self.script = script
        self.status = script.status

    def read1(self, size: int) -> bytes:
        self.script.block("read1", size)
        return self.script.chunks.pop(0) if self.script.chunks else b""

    def close(self) -> None:
        self.script.note("response.close")


class FakeConnection:
    def __init__(
        self, script: Script, host: str, port: int, *, timeout: float, context: Any
    ) -> None:
        self.script = script
        self.context = context
        self.sock: FakeSocket | None = None
        script.note("init", (host, port, timeout))

    def set_debuglevel(self, level: int) -> None:
        self.script.note("set_debuglevel", level)

    def set_tunnel(self, *args: object, **kwargs: object) -> None:
        self.script.note("set_tunnel")

    def connect(self) -> None:
        self.script.block("connect")
        if self.script.socket:
            self.sock = FakeSocket(self.script)

    def putrequest(self, method: str, url: str, **kwargs: bool) -> None:
        self.script.note("putrequest", (method, url, kwargs))

    def putheader(self, header: str, *values: str) -> None:
        self.script.note("putheader", (header, *values))

    def endheaders(self, message_body: bytes | None = None) -> None:
        self.script.block("endheaders", message_body)

    def getresponse(self) -> FakeResponse:
        self.script.block("getresponse")
        return FakeResponse(self.script)

    def close(self) -> None:
        self.script.note("close")


@pytest.fixture
def script(monkeypatch: pytest.MonkeyPatch) -> Script:
    made = Script()

    def connection(host: str, port: int, *, timeout: float, context: Any) -> FakeConnection:
        conn = FakeConnection(made, host, port, timeout=timeout, context=context)
        made.connections.append(conn)
        return conn

    monkeypatch.setattr(http.client, "HTTPSConnection", connection)
    return made


def post(
    script: Script, budget: float = 10.0, headers: Mapping[str, str] = HEADERS
) -> tuple[HttpExchange, Deadline]:
    deadline = Deadline(script.clock, timedelta(seconds=budget))
    transport = HttpsTransport(context=CONTEXT)
    exchange = transport.post(
        path=CHAT_COMPLETIONS_PATH, headers=headers, body=BODY, deadline=deadline
    )
    return exchange, deadline


def raised(call: Callable[[], object]) -> Exception:
    """The exception ``call`` raised, checked to carry no chain and no sentinel."""
    failure: Exception | None = None
    try:
        call()
    except Exception as exc:
        failure = exc
    assert failure is not None, "nothing was raised"
    assert failure.__cause__ is None
    assert failure.__context__ is None
    rendered = [str(failure), repr(failure), "".join(traceback.format_exception(failure))]
    for text in rendered:
        assert SENTINEL not in text
        assert CREDENTIAL not in text
    return failure


# --- the happy path ---------------------------------------------------------------------


def test_every_blocking_step_is_bounded_by_the_time_left(script: Script) -> None:
    exchange, deadline = post(script)
    assert exchange.status == 200
    assert exchange.read(100, deadline) == b'{"ok":'
    assert exchange.read(100, deadline) == b"true}"
    assert exchange.read(100, deadline) == b""
    exchange.close()
    assert script.names() == [
        "init",
        "connect",
        "settimeout",
        "putrequest",
        "putheader",
        "putheader",
        "putheader",
        "putheader",
        "endheaders",
        "settimeout",
        "getresponse",
        "settimeout",
        "read1",
        "settimeout",
        "read1",
        "settimeout",
        "read1",
        "response.close",
        "close",
    ]
    # Each blocking step takes 0.5 s, so the time left shrinks before each one.
    assert script.details("init") == [(HOST, PORT, 10.0)]
    assert script.details("settimeout") == [9.5, 9.0, 8.5, 8.0, 7.5]
    assert script.details("read1") == [100, 100, 100]


def test_the_request_goes_to_the_fixed_endpoint_with_only_the_given_headers(
    script: Script,
) -> None:
    post(script)
    assert HOST == "openrouter.ai"
    assert PORT == 443
    assert CHAT_COMPLETIONS_PATH == "/api/v1/chat/completions"
    assert CHAT_COMPLETIONS_PATH == openrouter.CHAT_COMPLETIONS_PATH
    expected = ("POST", CHAT_COMPLETIONS_PATH, {"skip_accept_encoding": True})
    assert script.details("putrequest") == [expected]
    sent = dict(cast(tuple[str, str], header) for header in script.details("putheader"))
    assert sent == {**HEADERS, "Content-Length": str(len(BODY))}
    assert script.details("endheaders") == [BODY]
    assert script.connections[0].context is CONTEXT


def test_no_debug_output_no_tunnel_and_no_proxy_from_the_environment(
    script: Script, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name in ("HTTPS_PROXY", "https_proxy", "ALL_PROXY"):
        monkeypatch.setenv(name, "http://proxy.invalid:3128")
    exchange, _ = post(script)
    exchange.close()
    assert "set_debuglevel" not in script.names()
    assert "set_tunnel" not in script.names()
    assert script.details("init") == [(HOST, PORT, 10.0)]
    assert capsys.readouterr().out == ""
    source = Path(stdlib_transport.__file__ or "").read_text(encoding="utf-8")
    assert "set_debuglevel" not in source
    assert "set_tunnel" not in source


def test_a_redirect_is_only_a_status(script: Script) -> None:
    script.status = 301
    exchange, _ = post(script)
    assert exchange.status == 301
    exchange.close()
    assert len(script.connections) == 1


def test_the_default_context_verifies_certificates_and_host_names(script: Script) -> None:
    deadline = Deadline(script.clock, timedelta(seconds=10))
    transport = HttpsTransport()
    transport.post(path=CHAT_COMPLETIONS_PATH, headers=HEADERS, body=BODY, deadline=deadline)
    context = script.connections[0].context
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_another_path_is_refused_before_any_connection(script: Script) -> None:
    deadline = Deadline(script.clock, timedelta(seconds=10))
    transport = HttpsTransport(context=CONTEXT)
    with pytest.raises(ValueError, match="chat completions"):
        transport.post(path="/api/v1/other", headers=HEADERS, body=BODY, deadline=deadline)
    assert script.connections == []


# --- the deadline -----------------------------------------------------------------------


def test_no_time_left_means_no_connection(script: Script) -> None:
    exc = raised(lambda: post(script, budget=0))
    assert type(exc) is TransportTimeout
    assert script.connections == []


def test_a_deadline_spent_midway_stops_before_the_next_step_and_closes(script: Script) -> None:
    script.step_seconds = 6.0  # connecting takes 6 s and sending 6 more: past 10 s
    exc = raised(lambda: post(script))
    assert type(exc) is TransportTimeout
    assert script.details("settimeout") == [4.0]
    assert "getresponse" not in script.names()
    assert script.names()[-1] == "close"


def test_a_read_after_the_deadline_never_reaches_the_socket(script: Script) -> None:
    exchange, deadline = post(script, budget=2.0)  # 1.5 s used: 0.5 s left
    assert exchange.read(100, deadline) == b'{"ok":'  # ends at 2.0 s: no time left
    exc = raised(lambda: exchange.read(100, deadline))
    assert type(exc) is TransportTimeout
    assert script.details("read1") == [100]
    exchange.close()


# --- failures ---------------------------------------------------------------------------

FAILURES: list[tuple[Callable[[], Exception], type[Exception]]] = [
    (lambda: TimeoutError(SENTINEL), TransportTimeout),
    (lambda: ssl.SSLError(1, SENTINEL), TransportFailure),
    (lambda: ssl.SSLCertVerificationError(1, SENTINEL), TransportFailure),
    (lambda: ConnectionRefusedError(SENTINEL), TransportFailure),
    (lambda: ConnectionResetError(SENTINEL), TransportFailure),
    (lambda: OSError(SENTINEL), TransportFailure),
    (lambda: http.client.BadStatusLine(SENTINEL), TransportFailure),
    (lambda: http.client.RemoteDisconnected(SENTINEL), TransportFailure),
    (lambda: http.client.LineTooLong(SENTINEL), TransportFailure),
    (lambda: http.client.IncompleteRead(SENTINEL.encode()), TransportFailure),
    (lambda: ValueError(SENTINEL), TransportFailure),
    (lambda: UnicodeEncodeError("latin-1", SENTINEL, 0, 1, SENTINEL), TransportFailure),
    (lambda: OverflowError(SENTINEL), TransportFailure),
]


@pytest.mark.parametrize("step", ["connect", "putheader", "endheaders", "getresponse"])
@pytest.mark.parametrize(("make", "expected"), FAILURES)
def test_a_failed_post_has_a_closed_kind_no_chain_and_a_closed_connection(
    script: Script, step: str, make: Callable[[], Exception], expected: type[Exception]
) -> None:
    script.failures[step] = make()
    exc = raised(lambda: post(script))
    assert type(exc) is expected
    assert str(exc) in {"transport_timeout", "transport_failure"}
    assert script.names()[-1] == "close"


@pytest.mark.parametrize(("make", "expected"), FAILURES)
def test_a_failed_read_has_a_closed_kind_and_no_chain(
    script: Script, make: Callable[[], Exception], expected: type[Exception]
) -> None:
    exchange, deadline = post(script)
    script.failures["read1"] = make()
    exc = raised(lambda: exchange.read(100, deadline))
    assert type(exc) is expected
    exchange.close()
    assert script.names()[-2:] == ["response.close", "close"]


def test_a_connection_without_a_socket_is_a_failure(script: Script) -> None:
    script.socket = False
    exc = raised(lambda: post(script))
    assert type(exc) is TransportFailure
    assert script.names()[-1] == "close"


def test_closing_never_raises(script: Script) -> None:
    exchange, _ = post(script)
    script.failures["response.close"] = OSError(SENTINEL)
    script.failures["close"] = OSError(SENTINEL)
    exchange.close()
    assert script.names()[-2:] == ["response.close", "close"]


# --- the real standard-library framing and header checks --------------------------------


class SocketlessConnection(http.client.HTTPSConnection):
    """The real ``HTTPSConnection`` with a stand-in socket: nothing leaves the process."""

    made: ClassVar[list["SocketlessConnection"]] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.sent: list[bytes] = []
        self.made.append(self)

    def connect(self) -> None:
        self.sock = cast(Any, FakeSocket(Script()))

    def send(self, data: Any) -> None:
        self.sent.append(bytes(data))

    def getresponse(self) -> http.client.HTTPResponse:
        raise http.client.BadStatusLine(SENTINEL)


@pytest.fixture
def socketless(monkeypatch: pytest.MonkeyPatch) -> list[SocketlessConnection]:
    monkeypatch.setattr(SocketlessConnection, "made", [])
    # The stdlib global stays real: ``HTTPSConnection.__init__`` names it in ``super``.
    client = SimpleNamespace(**{**vars(http.client), "HTTPSConnection": SocketlessConnection})
    monkeypatch.setattr(stdlib_transport, "http", SimpleNamespace(client=client))
    return SocketlessConnection.made


def test_the_standard_library_quotes_an_illegal_header_value() -> None:
    # Why a failure keeps only its kind: this message would carry the credential.
    connection = http.client.HTTPConnection("example.invalid")
    connection.putrequest("POST", "/")
    with pytest.raises(ValueError, match=SENTINEL):
        connection.putheader("Authorization", f"Bearer {SENTINEL}\r\nX-Injected: 1")


def test_an_illegal_header_value_is_a_failure_without_the_value(
    socketless: list[SocketlessConnection],
) -> None:
    headers = {**HEADERS, "Authorization": f"Bearer {SENTINEL}\r\nX-Injected: 1"}
    exc = raised(lambda: post(Script(), headers=headers))
    assert type(exc) is TransportFailure
    (connection,) = socketless
    assert connection.sent == []
    assert connection.sock is None  # closed


def test_the_real_framing_sends_only_host_the_given_headers_and_a_length(
    socketless: list[SocketlessConnection], capsys: pytest.CaptureFixture[str]
) -> None:
    exc = raised(lambda: post(Script()))
    assert type(exc) is TransportFailure  # the status line carried the sentinel
    (connection,) = socketless
    head, _, body = b"".join(connection.sent).partition(b"\r\n\r\n")
    assert body == BODY
    request_line, *header_lines = head.decode("ascii").split("\r\n")
    assert request_line == "POST /api/v1/chat/completions HTTP/1.1"
    assert [line.split(": ", 1) for line in header_lines] == [
        ["Host", "openrouter.ai"],
        ["Authorization", f"Bearer {CREDENTIAL}"],
        ["Content-Type", "application/json"],
        ["Accept", "application/json"],
        ["Content-Length", str(len(BODY))],
    ]
    assert connection.debuglevel == 0
    assert capsys.readouterr().out == ""
    assert connection.sock is None  # closed


# --- through the adapter: a credential-quoting header error and remote status-line text ---


def adapter_call(clock: Callable[[], float]) -> Exception:
    config = OpenRouterConfig(credential=SecretStr(CREDENTIAL), route_models={"r": "vendor/m"})
    provider = OpenRouterProvider(config, HttpsTransport(context=CONTEXT), clock=clock)
    request = make_request(
        capability="interpret_amount",
        route_id="r",
        field_kind=FieldKind.TOTAL,
        raw=SENTINEL,
        timeout=timedelta(seconds=10),
    )
    return raised(lambda: provider.call(request))


def test_a_header_error_quoting_the_credential_is_only_unavailable(script: Script) -> None:
    script.failures["putheader"] = ValueError(f"Invalid header value {CREDENTIAL!r}")
    exc = adapter_call(script.clock)
    assert type(exc) is ProviderTransientError
    assert exc.code is TransientCode.UNAVAILABLE
    assert script.names()[-1] == "close"


def test_a_bad_status_line_with_remote_text_is_only_unavailable(
    socketless: list[SocketlessConnection],
) -> None:
    exc = adapter_call(FakeClock())
    assert type(exc) is ProviderTransientError
    assert exc.code is TransientCode.UNAVAILABLE
    (connection,) = socketless
    assert connection.sock is None  # closed
