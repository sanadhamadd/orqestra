# ADR-0002: Repository layout and Python toolchain

- **Status:** Proposed
- **Date:** 2026-10-22
- **Ledger entries:** DEC-001 … DEC-007, DEC-012, DEC-013

## Context

Day 1 must deliver a working, reproducible Python package with quality gates that every
later milestone (persistence, queue, workers, provider adapters) builds on. The task queue,
API framework, worker hosting, model providers, DB driver and async model are intentionally
undecided, so the toolchain must not pre-empt them.

## Decision

- **Layout:** `src/orqestra/` with `domain/` as the only subpackage today. Planned
  subpackages (`persistence/`, `queue/`, `worker/`, `workloads/invoice/`, `calls/`,
  `cost/`) are created only when they ship tested code. Tests live in `tests/unit`,
  `tests/integration`, `tests/e2e`, `tests/performance`, selected by pytest markers.
- **Toolchain:** uv (lockfile; CI installs with `--locked`), Python 3.12, Ruff (format + lint),
  mypy strict, pytest + pytest-cov with an 80% heuristic threshold, `Makefile` targets
  mirroring CI (`make check`).
- **Configuration:** pydantic-settings with `ORQESTRA_` prefix; secrets as `SecretStr`.
- **CI:** one GitHub Actions job (format, lint, type check, unit tests) on pushes to
  `main` and on pull requests; actions pinned by SHA; read-only token.
- **Local tooling files:** none tracked; machine-local tool configuration and session
  files are git-ignored (see `.gitignore`). General contributor guidance lives in
  `docs/development.md`.

## Consequences

- Adding a dependency means editing `pyproject.toml`, running `uv lock`, and committing
  `uv.lock`. CI runs `uv sync --locked`, which fails if `uv.lock` is out of date with
  `pyproject.toml`. `--frozen` (used by `make sync` and the README quickstart) installs
  from the lock without that check; `uv lock --check` verifies freshness explicitly.
- Code without type annotations fails CI; `Any` must be explicit.
- The domain package stays free of I/O and third-party imports so it remains cheap to
  test and reuse across worker, API and CLI.
- Docker, pre-commit, security scanners and the integration lane are added with the
  milestones that need them (Day 2 onward), not speculatively.

**Runner-up:** a flat package with Poetry, Black, flake8 and pyright — equally workable,
more tools and configuration dialects to keep aligned.
