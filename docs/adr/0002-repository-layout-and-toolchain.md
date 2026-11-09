# ADR-0002: Repository layout and Python toolchain

- **Status:** Accepted (owner, 2026-11-08; was Proposed)
- **Date:** 2026-10-22
- **Ledger entries:** DEC-001 … DEC-007, DEC-012, DEC-013

## Context

The repository foundation must deliver a working, reproducible Python package with quality gates that every
later milestone (persistence, queue, workers, provider adapters) builds on. The task queue,
API framework, worker hosting, model providers, DB driver and async model are intentionally
undecided, so the toolchain must not pre-empt them.

## Decision

- **Layout:** `src/orqestra/`, where a subpackage is created only when it ships tested
  code (DEC-003): today `domain/`, `persistence/`, `worker/`, `calls/`, `providers/`,
  `workloads/invoice/` and `aggregation/`, beside the `cli`, `config` and `logging`
  modules. The planned `queue/` and `cost/` do not exist: PostgreSQL is the queue
  (DEC-033) and nothing computes a cost (DEC-062). Tests live in `tests/unit` and
  `tests/integration`; the `unit`, `integration`, `e2e` and `performance` markers select
  the lanes, and no test is marked `e2e` yet. The benchmark harness is `benchmarks/`,
  outside the package and its wheel.
- **Toolchain:** uv (lockfile; CI installs with `--locked`), Python 3.12, Ruff (format + lint),
  mypy strict, pytest + pytest-cov with an 80% heuristic threshold, `Makefile` targets
  mirroring CI (`make check`, `make verify`).
- **Configuration:** pydantic-settings with `ORQESTRA_` prefix; secrets as `SecretStr`.
- **CI:** two GitHub Actions jobs on pushes to `main` and on pull requests: the fast lane
  (format, lint, type check, unit tests) and `verify` (PostgreSQL integration tests, the
  combined coverage threshold and a package check, through `make verify`; DEC-023);
  actions pinned by SHA; read-only token.
- **Local tooling files:** none tracked; machine-local tool configuration and session
  files are git-ignored (see `.gitignore`). General contributor guidance lives in
  `docs/development.md`.

## Consequences

- Adding a dependency means editing `pyproject.toml`, running `uv lock`, and committing
  `uv.lock`. CI runs `uv sync --locked`, which fails if `uv.lock` is out of date with
  `pyproject.toml`. The README quickstart runs `uv sync --locked --managed-python`, so a
  fresh clone gets the same check. `--frozen` (used by `make sync`) installs from the lock
  without that check; `uv lock --check` verifies freshness explicitly.
- Code without type annotations fails CI; `Any` must be explicit.
- The domain package stays free of I/O and third-party imports so it remains cheap to
  test and reuse across the worker, the library and the CLI.
- Tooling is added with the milestone that needs it, not speculatively: the PostgreSQL
  integration lane came with persistence (DEC-023) and Docker Compose with local
  PostgreSQL (PR #33); pre-commit and security scanners are still deferred (DEC-014).

**Runner-up:** a flat package with Poetry, Black, flake8 and pyright — equally workable,
more tools and configuration dialects to keep aligned.
