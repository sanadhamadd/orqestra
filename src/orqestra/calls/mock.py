"""A scripted, deterministic provider for tests and local runs.

``MockProvider`` imports nothing network-related. It is given a script, one step
per distinct request, and answers by step:

- a ``str`` is returned as ``ProviderResponse(value=that string)``;
- a ``ProviderResponse`` is returned as it is;
- an ``Exception`` instance is raised (provider errors, or any other exception, to
  exercise the handler's classification);
- any other object is returned unchanged, so a test can simulate a provider that
  breaks the protocol with a wrong return type.

A request whose ``request_id`` has already been answered with a value gets the
identical answer again without using a step, like a real provider that dedupes
on the id. A raised exception is not remembered. Every request received is
recorded, repeats included.
"""

from collections.abc import Iterable
from typing import cast

from orqestra.calls.provider import ProviderRequest, ProviderResponse


class ScriptExhaustedError(RuntimeError):
    """The mock was asked for a new answer after its script ran out."""


class MockProvider:
    """A ``Provider`` that follows its script and records what it was asked."""

    def __init__(self, script: Iterable[object] = ()) -> None:
        self._script = list(script)
        self._position = 0
        self._answers: dict[str, object] = {}
        self._requests: list[ProviderRequest] = []

    @property
    def requests(self) -> tuple[ProviderRequest, ...]:
        """Every request received, in order, repeats included."""
        return tuple(self._requests)

    @property
    def steps_used(self) -> int:
        """How many script steps have been consumed."""
        return self._position

    def interpret(self, request: ProviderRequest) -> ProviderResponse:
        self._requests.append(request)
        if request.request_id in self._answers:
            return cast(ProviderResponse, self._answers[request.request_id])
        if self._position >= len(self._script):
            msg = "the mock provider's script is exhausted"
            raise ScriptExhaustedError(msg)
        step = self._script[self._position]
        self._position += 1
        if isinstance(step, Exception):
            raise step
        answer: object = ProviderResponse(step) if isinstance(step, str) else step
        self._answers[request.request_id] = answer
        # Deliberately not checked: a test may script a non-ProviderResponse reply.
        return cast(ProviderResponse, answer)
