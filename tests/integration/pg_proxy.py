"""A controllable TCP proxy in front of the test database, for worker tests.

It forwards bytes between a client and the verified test database endpoint,
and a test can make it:

- ``stall()``: hold every byte, keeping both sockets open. The client cannot
  tell this from a frozen server: TCP stays up, so neither keepalives nor
  ``statement_timeout`` end its call. ``resume()`` lets traffic flow again.
- ``refuse()``: close every connection and every new one at once, until
  ``accept()``.
- ``drop_completion(committed=...)``: on the next completion (the transaction
  whose ``UPDATE`` sets ``failure_kind``), cut the connection either after the
  server has answered its ``COMMIT`` (the commit happened, the client never
  hears of it) or before forwarding the ``COMMIT`` (the server rolls back).
  Drops queue up: each completion takes the next one. With ``then_refuse``,
  new connections are refused from the drop on, until ``accept()``.
- ``drop_commit(marker, committed=...)``: the same for the next transaction in
  which the client sends ``marker`` (``drop_completion`` is this with the
  completion's marker). Each marker has its own queue of drops.
- ``cut_on(marker)``: close the next connection whose client sends ``marker``,
  without forwarding it.
- ``hold_on(marker)``: stall all traffic as soon as a client sends ``marker``,
  before forwarding it, until ``resume()``.
"""

import contextlib
import socket
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from psycopg.conninfo import conninfo_to_dict, make_conninfo

_COMPLETION = b"failure_kind = $"  # only complete_attempt's UPDATE sets failure_kind
_COMMIT = b"COMMIT"
_ROLLBACK = b"ROLLBACK"


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


@dataclass
class _Drop:
    committed: bool
    then_refuse: bool


class _Pair:
    def __init__(self, client: socket.socket, server: socket.socket) -> None:
        self.client, self.server = client, server
        self.seen = b""  # the tail of what the client sent, to find markers across reads
        self.marked: bytes | None = None  # the drop marker this transaction has sent
        self.drop_reply: _Drop | None = None

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
        self._drops: dict[bytes, deque[_Drop]] = {_COMPLETION: deque()}
        self._cut: bytes | None = None
        self._hold: bytes | None = None
        self.dropped = threading.Event()  # set on each drop
        self.drop_count = 0
        self.refused = threading.Event()  # set on each refused connection
        self.cut = threading.Event()
        self.held = threading.Event()
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

    def accept(self) -> None:
        with self._lock:
            self._refusing = False

    def drop_completion(self, *, committed: bool, then_refuse: bool = False) -> None:
        self.drop_commit(_COMPLETION, committed=committed, then_refuse=then_refuse)

    def drop_commit(self, marker: bytes, *, committed: bool, then_refuse: bool = False) -> None:
        with self._lock:
            self._drops.setdefault(marker, deque()).append(_Drop(committed, then_refuse))

    def cut_on(self, marker: bytes) -> None:
        with self._lock:
            self._cut = marker

    def hold_on(self, marker: bytes) -> None:
        with self._lock:
            self._hold = marker

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
                self.refused.set()
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
                if from_client and self._intercept(pair, data):
                    return
                self._forwarding.wait()
                if not from_client and pair.drop_reply is not None:
                    self._dropped(pair, pair.drop_reply)  # the COMMIT was answered: cut
                    return
                target.sendall(data)
        except OSError:
            pass
        finally:
            pair.close()

    def _dropped(self, pair: _Pair, drop: _Drop) -> None:
        with self._lock:
            if drop.then_refuse:
                self._refusing = True
            self.drop_count += 1
        pair.close()
        self.dropped.set()

    def _intercept(self, pair: _Pair, data: bytes) -> bool:
        """Apply armed controls to the client's ``data``; True if the pair was cut."""
        window = pair.seen + data
        pair.seen = window[-256:]
        with self._lock:
            hold, cut = self._hold, self._cut
            if hold is not None and hold in window:
                self._hold = None
                self._forwarding.clear()  # stall before this is forwarded
                self.held.set()
            if cut is not None and cut in window:
                self._cut = None
            markers = list(self._drops)
        if cut is not None and cut in window:
            pair.close()
            self.cut.set()
            return True
        if pair.marked is not None and _ROLLBACK in data:
            pair.marked, pair.seen = None, b""  # that transaction ended without a COMMIT
            return False
        if pair.marked is None:
            pair.marked = next((marker for marker in markers if marker in window), None)
        if not (pair.marked is not None and _COMMIT in data):
            return False
        marker, pair.marked, pair.seen = pair.marked, None, b""  # this COMMIT ends it
        with self._lock:
            queue = self._drops[marker]
            drop = queue.popleft() if queue else None
        if drop is None:
            return False
        if drop.committed:
            pair.drop_reply = drop  # forward the COMMIT; cut when the answer comes back
            return False
        self._dropped(pair, drop)  # never forward the COMMIT: the server rolls back
        return True
