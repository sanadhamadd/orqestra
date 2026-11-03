import socket
from typing import NoReturn

import pytest


@pytest.fixture
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make any attempt to open a connection or resolve a name fail the test.

    Not autouse: a unit module enables it with ``pytest.mark.usefixtures("no_network")``.
    The integration lane keeps its real sockets. The patches are undone after each test.
    """

    def refuse(*args: object, **kwargs: object) -> NoReturn:
        msg = "network access is blocked by the no_network fixture"
        raise AssertionError(msg)

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)
    monkeypatch.setattr(socket, "getaddrinfo", refuse)
