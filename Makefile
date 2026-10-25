# Repeatable project commands. CI runs both lanes:
#   make check   fast lane: format, lint, strict typing, unit tests (no services)
#   make verify  full lane: adds PostgreSQL integration tests, the combined
#                coverage threshold and a package check. Required for
#                persistence changes; needs ORQESTRA_TEST_DATABASE_URL.
.PHONY: sync fmt lint type test check require-test-db test-all package-check verify

sync:
	uv sync --frozen

fmt:
	uv run ruff format .

lint:
	uv run ruff format --check .
	uv run ruff check .

type:
	uv run mypy

# Coverage is reported here but not enforced: persistence code is exercised by
# the integration tests, so the threshold applies to the combined run below.
test:
	uv run pytest -m "not integration and not e2e and not performance" --cov

check: lint type test

require-test-db:
	@test -n "$$ORQESTRA_TEST_DATABASE_URL" || { \
		echo "ORQESTRA_TEST_DATABASE_URL is not set: make verify needs the local" \
			"orqestra_test PostgreSQL database (see docs/development.md)" >&2; \
		exit 1; }

test-all:
	uv run pytest -m "not e2e and not performance" --cov --cov-fail-under=80

# Build the wheel and confirm every migration file is packaged.
package-check:
	rm -rf build/package-check
	uv build --wheel --out-dir build/package-check
	@for f in src/orqestra/persistence/migrations/*.sql; do \
		uv run python -m zipfile -l build/package-check/*.whl \
			| grep -q "orqestra/persistence/migrations/$$(basename $$f)" \
			|| { echo "wheel is missing $$f" >&2; exit 1; }; \
	done
	@echo "package-check: wheel contains every migration file"

verify: require-test-db lint type test-all package-check
