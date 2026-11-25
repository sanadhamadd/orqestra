# Contributing

Set up a clone as [docs/development.md](docs/development.md) describes: `make sync` (the locked dependencies), and a
local PostgreSQL for the integration tests.

- **`make check`**, the fast lane: formatting, lint, strict typing and unit tests, with no services.
- **`make verify`**, the full lane: it adds the PostgreSQL integration tests, the combined coverage threshold and a package
  check. It needs `ORQESTRA_TEST_DATABASE_URL` pointing at a local, disposable `orqestra_test` database, and it is
  required for any persistence, migration or schema change.

A material design choice is recorded in the [decision log](docs/decisions.md) as WHAT, WHY and the runner-up, and stays
**Proposed** until it is accepted in a reviewed change. A cross-cutting choice also gets an [ADR](docs/adr/).

Open a pull request with the [template](.github/PULL_REQUEST_TEMPLATE.md) filled in: the exact checks run and their
results, the risks, and the decisions. Tests never make network or paid calls.
