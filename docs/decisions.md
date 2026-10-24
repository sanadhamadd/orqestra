# Orqestra — engineering decision ledger

Material engineering decisions in WHAT / WHY / RUNNER-UP form. A proposal is not a
decision: every entry stays **Proposed** until the owner marks it **Accepted** in a
reviewed change. Trivial edits are not logged. Cross-cutting choices also get an ADR.

Deliberately **undecided** (do not infer a choice from the code): task queue / broker,
API framework, worker hosting, AI provider and models, DB driver and migration tool,
asyncio vs threads for worker slots.

## Day 1 — repository foundation

### DEC-001 — uv for dependency management and lockfile
- **Status:** Proposed · **ADR:** 0002
- **WHAT:** `uv` manages the virtualenv, resolves and locks dependencies (`uv.lock`), and runs tools.
- **WHY:** One fast tool for lock, sync, run and Python installation; `uv sync --locked` in CI fails when `uv.lock` is stale; `--frozen` installs the lock as-is.
- **RUNNER-UP:** Poetry — mature, but a slower resolver and a second configuration dialect.

### DEC-002 — Python 3.12
- **Status:** Proposed
- **WHAT:** `requires-python >= 3.12`, pinned in `.python-version`.
- **WHY:** `slots=True` dataclasses, `StrEnum`, better tracebacks; every candidate DB and HTTP library ships 3.12 wheels.
- **RUNNER-UP:** 3.13 — newer, but fewer verified wheels for the drivers still under evaluation.

### DEC-003 — src layout, one package, no empty subpackages
- **Status:** Proposed · **ADR:** 0002
- **WHAT:** `src/orqestra/`; subpackages appear only when they ship tested code.
- **WHY:** Tests import the installed package, not the working tree; the tree never implies unimplemented behaviour.
- **RUNNER-UP:** Scaffold every planned package now — looks complete, misleads about status.

### DEC-004 — Ruff for formatting and linting
- **Status:** Proposed
- **WHAT:** `ruff format` + `ruff check` with pycodestyle, pyflakes, isort, pyupgrade, bugbear, simplify, pytest-style and annotation rules.
- **WHY:** One fast tool replaces Black, isort and flake8; annotation rules keep the public surface typed.
- **RUNNER-UP:** Black + isort + flake8 — familiar, three tools, slower.

### DEC-005 — mypy in strict mode over src and tests
- **Status:** Proposed
- **WHAT:** `mypy --strict` with the pydantic plugin; tests are type-checked too.
- **WHY:** Strict from day one costs nothing now and is painful to retrofit; pip-installable, no Node toolchain.
- **RUNNER-UP:** pyright — faster, sharper inference, but adds a Node dependency to dev and CI.

### DEC-006 — pytest lanes by marker
- **Status:** Proposed
- **WHAT:** Markers `unit`, `integration`, `e2e`, `performance`; the PR lane runs everything except the last three; coverage threshold 80% as a heuristic.
- **WHY:** Maps directly to the charter's test gates and keeps PR CI fast and deterministic.
- **RUNNER-UP:** Separate test directories only — works, but cannot express "slow" per test.

### DEC-007 — pydantic-settings for configuration
- **Status:** Proposed
- **WHAT:** `Settings(BaseSettings)` with `ORQESTRA_` prefix, `.env` in development, `SecretStr` for the DSN, validated ranges.
- **WHY:** Fails fast on bad values; secrets cannot leak through `repr`; the same library will validate job manifests later.
- **RUNNER-UP:** Hand-rolled `os.environ` parsing — zero dependencies, no validation, silent bad values.

### DEC-008 — Explicit immutable transition table for task states
- **Status:** Proposed
- **WHAT:** `TASK_TRANSITIONS` as a `MappingProxyType` of frozensets; `transition()` raises `InvalidTransitionError`.
- **WHY:** The lifecycle is documented, exhaustively testable, and the single source persistence must enforce with conditional writes.
- **RUNNER-UP:** A state-machine library — more machinery than five states justify.

### DEC-009 — Unknown exceptions are not retried
- **Status:** Proposed
- **WHAT:** Only `TransientError` is retryable; `PermanentError` and any unclassified exception are terminal.
- **WHY:** Retrying a defect spends time and AI budget and hides the bug; transient classes must be opted into explicitly.
- **RUNNER-UP:** Retry everything except known-permanent errors — more forgiving, more expensive, masks defects.

### DEC-010 — Stdlib logging with a JSON formatter
- **Status:** Proposed
- **WHAT:** `JsonFormatter` emitting one object per line with optional `job_id`/`task_id`/`attempt_id`/`worker_id`.
- **WHY:** Zero dependencies and structured correlation IDs from the first commit.
- **RUNNER-UP:** structlog — better context binding; revisit on Day 4 when worker context grows.

### DEC-011 — argparse CLI
- **Status:** Proposed
- **WHAT:** `orqestra version|health` via argparse.
- **WHY:** Zero dependencies; keeps the API-framework decision genuinely open.
- **RUNNER-UP:** Typer — nicer UX, adds a dependency before the HTTP choice is made.

### DEC-012 — GitHub Actions pinned by commit SHA with least privilege
- **Status:** Proposed
- **WHAT:** `permissions: contents: read`; actions pinned to full SHAs with the tag in a comment; `uv sync --locked`.
- **WHY:** Immutable action references and read-only tokens are the charter's supply-chain baseline.
- **RUNNER-UP:** Tag pins (`@v7`) — readable, mutable.

### DEC-013 — No assistant-specific files tracked; Claude-specific files git-ignored
- **Status:** Proposed
- **WHAT:** No assistant-specific instruction file is tracked; general contributor guidance is in `docs/development.md`; `CLAUDE.md`, `CLAUDE.local.md` and `.claude/` are ignored.
- **WHY:** Owner instruction: tool-specific instructions and session records stay local. Deviates from charter §7, which placed `CLAUDE.md` in the repo.
- **RUNNER-UP:** Tracked `CLAUDE.md` — visible policy, but mixes tool-specific material into the public record.

### DEC-014 — Deferred on Day 1 (explicitly not chosen)
- **Status:** Proposed
- **WHAT:** Docker Compose, Dockerfile, pre-commit, security scanners, Dependabot, issue templates, architecture doc, `LICENSE`, `CHANGELOG`.
- **WHY:** None supports an immediate engineering objective; Docker could not be verified locally today (daemon not running) and unverified config is not committed.
- **RUNNER-UP:** Ship them all on Day 1 — fuller skeleton, more unverified surface.
