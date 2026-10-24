"""Command-line entry point.

Intentionally tiny: ``version`` and ``health`` prove the package installs,
configuration loads and logging works. Job submission arrives with later
milestones once the queue and persistence contracts exist.
"""

import argparse
import json
import sys
from collections.abc import Sequence

from pydantic import ValidationError

from orqestra import __version__
from orqestra.config import Settings
from orqestra.logging import configure_logging


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orqestra", description="Orqestra control CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version", help="print the installed version")
    sub.add_parser("health", help="load configuration and report process health as JSON")
    return parser


def _health() -> int:
    try:
        settings = Settings()
    except ValidationError as exc:
        print(json.dumps({"status": "error", "errors": exc.errors()}, default=str))
        return 1
    configure_logging(settings.log_level)
    print(json.dumps({"status": "ok", "version": __version__, "settings": settings.redacted()}))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI; returns the process exit code."""
    args = _build_parser().parse_args(argv)
    if args.command == "version":
        print(__version__)
        return 0
    if args.command == "health":
        return _health()
    return 2  # pragma: no cover - argparse rejects unknown commands first


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
