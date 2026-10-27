"""A controllable TCP proxy in front of the test database, for worker tests.

It forwards bytes between a client and the verified test database endpoint,
and a test can make it:

- ``stall()``: hold every byte, keeping both sockets open. The client cannot
  tell this from a frozen server: TCP stays up, so neither keepalives nor
  ``statement_timeout`` end its call.
- ``refuse()``: close every connection and every new one at once.
- ``drop_completion(committed=...)``: on the next completion (the transaction
  whose ``UPDATE`` sets ``failure_kind``), cut the connection either after the
  server has answered its ``COMMIT`` (the commit happened, the client never
  hears of it) or before forwarding the ``COMMIT`` (the server rolls back).
"""

import contextlib
import socket
import threading
from collections.abc import Callable

from psycopg.conninfo import conninfo_to_dict, make_conninfo

_COMPLETION = b"failure_kind = $"  # only complete_attempt's UPDATE sets failure_kind
_COMMIT = b"COMMIT"


def upstream_for(database_url: str) -> Callable[[], socket.socket]:
    """A connector to the endpoint ``database_url`` is pinned to (TCP or Unix socket)."""
    params = conninfo_to_dict(database_url)
    host, port = str(params.get("host") or "localhost"), int(params.get("port") or 5432)
    address = str(params.get("hostaddr") or host)
    if host.startswith("/"):
        path = f"{host}/.s.PGSQL.{port}"

        def unix() -> socket.socket:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(path)
            return sock

        return unix
    return lambda: socket.create_connection((address, port))


class _Pair:
    def __init__(self, client: socket.socket, server: socket.socket) -> None:
        self.client, self.server = client, server
        self.seen = b""  # the tail of what the client sent, to find markers across reads
        self.completion_seen = False
        self.drop_reply = False

    def close(self) -> None:
        for sock in (self.client, self.server):
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)
            sock.close()


class PgProxy:
    """See the module docstring. Use as a context manager."""

    def __init__(self, database_url: str) -> None:
        self._upstream = upstream_for(database_url)
        self._listener = socket.create_server(("127.0.0.1", 0))
        self.port = self._listener.getsockname()[1]
        self._lock = threading.Lock()
        self._pairs: list[_Pair] = []
        self._forwarding = threading.Event()
        self._forwarding.set()
        self._refusing = False
        self._drop: bool | None = None  # None: no drop armed; else whether to let the commit happen
        self.dropped = threading.Event()
        self._closed = False
        threading.Thread(target=self._accept, daemon=True).start()

    def url(self, schema_url: str) -> str:
        """``schema_url`` (with its search_path) routed through this proxy."""
        return make_conninfo(
            schema_url, host="127.0.0.1", hostaddr="127.0.0.1", port=str(self.port)
        )

    def stall(self) -> None:
        self._forwarding.clear()

    def resume(self) -> None:
        self._forwarding.set()

    def refuse(self) -> None:
        with self._lock:
            self._refusing = True
            pairs, self._pairs = self._pairs, []
        for pair in pairs:
            pair.close()

    def drop_completion(self, *, committed: bool) -> None:
        with self._lock:
            self._drop = committed

    def close(self) -> None:
        self._closed = True
        self._forwarding.set()
        self.refuse()
        self._listener.close()

    def __enter__(self) -> "PgProxy":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _accept(self) -> None:
        while not self._closed:
            try:
                client, _ = self._listener.accept()
            except OSError:
                return
            with self._lock:
                refusing = self._refusing
            if refusing:
                client.close()
                continue
            pair = _Pair(client, self._upstream())
            with self._lock:
                self._pairs.append(pair)
            threading.Thread(target=self._pipe, args=(pair, True), daemon=True).start()
            threading.Thread(target=self._pipe, args=(pair, False), daemon=True).start()

    def _pipe(self, pair: _Pair, from_client: bool) -> None:
        source, target = (pair.client, pair.server) if from_client else (pair.server, pair.client)
        try:
            while data := source.recv(65536):
                self._forwarding.wait()
                if not from_client and pair.drop_reply:
                    pair.close()  # the server answered the COMMIT; the client never hears it
                    self.dropped.set()
                    return
                if from_client and self._intercept(pair, data):
                    return
                target.sendall(data)
        except OSError:
            pass
        finally:
            pair.close()

    def _intercept(self, pair: _Pair, data: bytes) -> bool:
        """Apply an armed drop to the client's ``data``; True if the pair was cut."""
        with self._lock:
            committed = self._drop
        if committed is None:
            return False
        window = pair.seen + data
        pair.seen = window[-64:]
        if _COMPLETION in window:
            pair.completion_seen = True
        if not (pair.completion_seen and _COMMIT in data):
            return False
        with self._lock:
            self._drop = None
        if committed:
            pair.drop_reply = True  # forward the COMMIT; cut when the answer comes back
            return False
        pair.close()  # never forward the COMMIT: the server rolls back
        self.dropped.set()
        return True
