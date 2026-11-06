"""Orqestra: a distributed workload execution platform."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("orqestra")
except PackageNotFoundError:  # pragma: no cover - only when not installed
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
