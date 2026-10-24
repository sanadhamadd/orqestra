# Repeatable project commands. `make check` is what CI runs.
.PHONY: sync fmt lint type test check

sync:
	uv sync --frozen

fmt:
	uv run ruff format .

lint:
	uv run ruff format --check .
	uv run ruff check .

type:
	uv run mypy

test:
	uv run pytest -m "not integration and not e2e and not performance" --cov

check: lint type test
