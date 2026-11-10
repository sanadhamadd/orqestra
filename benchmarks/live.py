"""The benchmark's provider choice: the offline double, or OpenRouter behind an opt-in (DEC-071).

Only ``python -m benchmarks.run`` (one cell) and the benchmark worker it starts
accept ``--provider openrouter``; ``benchmarks.ladder`` and ``benchmarks.drills``
have no provider flag and always use the double. ``orqestra worker run`` is not
touched: it runs only the diagnostics (DEC-051, DEC-062).

This module holds everything provider-specific:

- ``ProviderChoice``: the flags and their refusals, checked before any worker,
  transport or database connection exists;
- ``CallBudget``: the cell's call maximum, a counter in a locked file shared by every
  worker process of the cell. A reservation is never returned, so the count can only
  overstate what was sent;
- ``BudgetedRecorder``: wraps the usage sink's recorder and reserves one call before
  each intent. With the budget spent it raises ``ProviderCallBudgetExhausted``, a
  ``PermanentError``, so no intent is written, no request is sent and the attempt fails
  without a retry (fail closed);
- ``CannedTransport``: a well-formed OpenRouter response for every request, made
  in memory. It is what a dry run and every test use. It never opens a socket;
- ``build_openrouter_provider``: the only place a real ``HttpsTransport`` is made, and
  only for a live run. The network module is imported there, so a dry run never loads it.

No credential, prompt or response is ever logged or written: refusals name flags,
never values, and the result records only the choice and the call counts.
"""

import fcntl
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import SecretStr

from orqestra.calls.recording import ProviderCallIntent, ProviderCallRecorder, ProviderCallResult
from orqestra.domain.errors import PermanentError
from orqestra.providers import openrouter
from orqestra.providers.openrouter import OpenRouterConfig, OpenRouterProvider
from orqestra.providers.transport import Deadline, HttpExchange

API_KEY_ENV = "ORQESTRA_OPENROUTER_API_KEY"
BUDGET_FILE_NAME = "provider-call-budget"
BUDGET_EXHAUSTED = "provider_call_budget_exhausted"
BUDGET_FILE_INVALID = "provider_call_budget_file_invalid"
OPENROUTER = "openrouter"
DRY_RUN_LABEL = "dry run: canned responses, no network, no cost"
CANNED_REQUESTS_EVENT = "canned_transport_requests"
# A dry run's adapter needs a credential to build its headers; this placeholder only ever
# reaches CannedTransport. A dry run never reads the real key.
DRY_RUN_CREDENTIAL = "dry-run-placeholder"
# The synthetic usage CannedTransport reports for every call, in OpenRouter credits. It
# says nothing about any real model's price.
CANNED_COST = Decimal("0.000001")
CANNED_GENERATION_ID = "gen-canned"


class ProviderName(StrEnum):
    DOUBLE = "double"
    OPENROUTER = "openrouter"


class ProviderCallBudgetExhausted(PermanentError):  # noqa: N818 - the stored error_type
    """The cell's call maximum is spent; no call was made. The text is fixed."""

    def __init__(self) -> None:
        super().__init__(BUDGET_EXHAUSTED)


class ProviderCallBudgetFileInvalid(PermanentError):  # noqa: N818 - the stored error_type
    """The budget file does not hold a count; no call was made. The text is fixed."""

    def __init__(self) -> None:
        super().__init__(BUDGET_FILE_INVALID)


# --- the choice and its refusals ---------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class ProviderChoice:
    """What answers the invoice worker's calls. The default is the offline double."""

    name: ProviderName = ProviderName.DOUBLE
    model: str | None = None
    max_provider_calls: int | None = None
    allow_live_calls: bool = False
    dry_run: bool = False

    @property
    def openrouter(self) -> bool:
        return self.name is ProviderName.OPENROUTER

    @property
    def live(self) -> bool:
        """Whether real, billable calls would be made."""
        return self.openrouter and self.allow_live_calls and not self.dry_run

    def refusal(self, environ: Mapping[str, str]) -> str | None:
        """Why this choice may not run, or ``None``. Checked in the order DEC-071 states.

        ``environ`` is read only for a live run, and only for whether the key is set.
        """
        if not self.openrouter:
            if self.model or self.max_provider_calls is not None:
                return "--model and --max-provider-calls apply only to --provider openrouter"
            if self.allow_live_calls or self.dry_run:
                return "--allow-live-calls and --dry-run apply only to --provider openrouter"
            return None
        if not self.allow_live_calls and not self.dry_run:
            return (
                "openrouter makes live, billable calls: pass --allow-live-calls (the owner's "
                "approval is required, DEC-071), or --dry-run"
            )
        if not self.model:
            return "--provider openrouter needs --model"
        try:
            OpenRouterConfig(
                credential=SecretStr(DRY_RUN_CREDENTIAL), route_models={"route-a": self.model}
            )
        except (TypeError, ValueError):
            return (
                "--model must be 1 to 128 characters of letters, digits, '.', '_', ':', '/' "
                "or '-', starting with a letter or digit"
            )
        maximum: object = self.max_provider_calls
        if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum <= 0:
            return "--provider openrouter needs --max-provider-calls, a positive whole number"
        if self.allow_live_calls and self.dry_run:
            return "--allow-live-calls and --dry-run contradict each other: pass one"
        if self.live and credential_from(environ) is None:
            return f"{API_KEY_ENV} is not set"
        return None

    def worker_arguments(self, budget_file: Path | None) -> list[str]:
        """The flags the benchmark worker needs for this choice; none for the double."""
        if not self.openrouter:
            return []
        assert self.model is not None
        assert self.max_provider_calls is not None
        assert budget_file is not None
        arguments = [
            "--provider",
            OPENROUTER,
            "--model",
            self.model,
            "--max-provider-calls",
            str(self.max_provider_calls),
            "--budget-file",
            str(budget_file),
        ]
        arguments.append("--dry-run" if self.dry_run else "--allow-live-calls")
        return arguments


DOUBLE = ProviderChoice()


def credential_from(environ: Mapping[str, str]) -> SecretStr | None:
    """The key from ``environ`` (never from ``.env``); blank counts as unset (config.py)."""
    value = environ.get(API_KEY_ENV, "")
    if not value.strip():
        return None
    return SecretStr(value.strip())


# --- the cell's call maximum ---------------------------------------------------------------


def create_budget_file(path: Path) -> Path:
    """Create the cell's counter holding 0; refuse if it already exists (never reset a count)."""
    with path.open("x", encoding="ascii") as file:
        file.write("0\n")
        file.flush()
        os.fsync(file.fileno())
    return path


def read_budget_file(path: Path) -> int:
    """The calls reserved so far."""
    return _count_of(path.read_text(encoding="ascii"))


def _count_of(text: str) -> int:
    stripped = text.strip()
    if not stripped.isdigit() or not stripped.isascii():
        raise ProviderCallBudgetFileInvalid
    return int(stripped)


class CallBudget:
    """At most ``maximum`` reservations across every process that shares ``path``."""

    __slots__ = ("maximum", "path")

    def __init__(self, path: Path, maximum: int) -> None:
        if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum <= 0:
            msg = "the call maximum must be a positive int"
            raise ValueError(msg)
        self.path = path
        self.maximum = maximum

    def reserve(self) -> None:
        """Count one call, or raise ``ProviderCallBudgetExhausted`` if none is left."""
        with self.path.open("r+", encoding="ascii") as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)  # released when the file closes
            count = _count_of(file.read())
            if count >= self.maximum:
                raise ProviderCallBudgetExhausted
            file.seek(0)
            file.truncate()
            file.write(f"{count + 1}\n")
            file.flush()
            os.fsync(file.fileno())


class BudgetedRecorder:
    """A ``ProviderCallRecorder`` that reserves a call from ``budget`` before each intent."""

    __slots__ = ("_budget", "_inner")

    def __init__(self, inner: ProviderCallRecorder, budget: CallBudget) -> None:
        self._inner = inner
        self._budget = budget

    def record_intent(self, intent: ProviderCallIntent) -> None:
        self._budget.reserve()  # raises before the intent: no record, no call
        self._inner.record_intent(intent)

    def record_result(self, result: ProviderCallResult) -> None:
        self._inner.record_result(result)


# --- the canned transport ------------------------------------------------------------------


class CannedExchange:
    """One canned 200 response, read like a real one."""

    def __init__(self, body: bytes) -> None:
        self._body = body
        self._offset = 0

    @property
    def status(self) -> int:
        return 200

    def read(self, max_bytes: int, deadline: Deadline) -> bytes:
        chunk = self._body[self._offset : self._offset + max_bytes]
        self._offset += len(chunk)
        return chunk

    def close(self) -> None:
        return None


def canned_reply(raw: str) -> str:
    """What the canned model answers: ``raw`` without separators, as the double reads it."""
    return "".join(char for char in raw if char not in {",", "_", " "})


class CannedTransport:
    """An ``HttpTransport`` that answers every request in memory, the way OpenRouter would.

    It reads the adapter's request body, answers 200 with a chat-completions body whose
    content is ``canned_reply(raw)``, its ``model`` the requested one, a fixed ``id``,
    token counts from ``len(raw)`` and the synthetic ``CANNED_COST``. ``requests``
    counts what it answered. Nothing touches a network.
    """

    def __init__(self) -> None:
        self.requests = 0

    def post(
        self, *, path: str, headers: Mapping[str, str], body: bytes, deadline: Deadline
    ) -> HttpExchange:
        deadline.remaining()
        request = json.loads(body)
        question = json.loads(request["messages"][-1]["content"])
        raw = str(question["raw"])
        self.requests += 1
        answer = {
            "id": CANNED_GENERATION_ID,
            "model": request["model"],
            "choices": [
                {
                    "message": {"role": "assistant", "content": canned_reply(raw)},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": len(raw),
                "completion_tokens": len(canned_reply(raw)),
                "total_tokens": len(raw) + len(canned_reply(raw)),
                "cost": float(CANNED_COST),
            },
        }
        return CannedExchange(json.dumps(answer).encode("utf-8"))


def build_openrouter_provider(
    config: OpenRouterConfig, *, dry_run: bool
) -> tuple[OpenRouterProvider, CannedTransport | None]:
    """The adapter on ``CannedTransport`` for a dry run (returned too, for its count); on
    the real transport otherwise. The only place a real transport is made."""
    if dry_run:
        canned = CannedTransport()
        return OpenRouterProvider(config, canned), canned
    from orqestra.providers import stdlib_transport  # a live run only: a dry run never loads it

    return OpenRouterProvider(config, stdlib_transport.HttpsTransport()), None


@dataclass(frozen=True, slots=True)
class OpenRouterWiring:
    """What the benchmark worker needs to run the invoice handler on OpenRouter."""

    provider: OpenRouterProvider
    provider_name: str
    model: str
    prompt_version: str
    canned: CannedTransport | None


def openrouter_wiring(
    choice: ProviderChoice, route: str, environ: Mapping[str, str]
) -> OpenRouterWiring:
    """The adapter for ``choice`` on ``route``. A dry run never reads ``environ``'s key."""
    assert choice.openrouter
    assert choice.model is not None
    if choice.dry_run:
        credential = SecretStr(DRY_RUN_CREDENTIAL)
    else:
        found = credential_from(environ)
        if found is None:
            msg = f"{API_KEY_ENV} is not set"
            raise ValueError(msg)
        credential = found
    config = OpenRouterConfig(credential=credential, route_models={route: choice.model})
    provider, canned = build_openrouter_provider(config, dry_run=choice.dry_run)
    return OpenRouterWiring(provider, OPENROUTER, choice.model, openrouter.PROMPT_VERSION, canned)


def canned_requests_line(transport: object) -> str | None:
    """The line a dry-run worker prints on exit: how many requests its transport answered."""
    if not isinstance(transport, CannedTransport):
        return None
    return json.dumps({"event": CANNED_REQUESTS_EVENT, "requests": transport.requests})


def canned_requests(log_dir: Path) -> int:
    """The canned requests every worker of a dry-run cell reported in its log."""
    total = 0
    for path in sorted(log_dir.glob("worker-*.log")):
        for line in path.read_bytes().splitlines():
            if not line.startswith(b"{"):
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("event") == CANNED_REQUESTS_EVENT:
                total += int(event["requests"])
    return total


# --- what the result records -----------------------------------------------------------------

OPENROUTER_USAGE_UNIT = openrouter.USAGE_UNIT


def expected_usage(choice: ProviderChoice, calls: int) -> tuple[str, Decimal | None]:
    """The usage unit the ledger holds for ``choice``'s calls, and their total if known.

    For OpenRouter only a dry run's total is known: ``calls`` canned answers at
    ``CANNED_COST`` each. A live run's credits are whatever the provider reports.
    """
    if choice.dry_run:
        return OPENROUTER_USAGE_UNIT, CANNED_COST * calls
    return OPENROUTER_USAGE_UNIT, None


def provider_block(
    choice: ProviderChoice,
    *,
    call_timeout: str,
    cap: int,
    route: str,
    calls_made: int | None,
) -> dict[str, Any]:
    """The result's ``provider`` block: the choice and the counts, never a key or a prompt."""
    if not choice.openrouter:
        return {"name": "bench-double", "call_timeout": call_timeout, "cap": cap, "route": route}
    return {
        "name": OPENROUTER,
        "model": choice.model,
        "max_provider_calls": choice.max_provider_calls,
        "dry_run": choice.dry_run,
        "call_timeout": call_timeout,
        "cap": cap,
        "route": route,
        "calls_made": calls_made,
        "usage_unit": openrouter.USAGE_UNIT,
    }
