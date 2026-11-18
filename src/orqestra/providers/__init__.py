"""Outbound provider adapters.

This package lives outside the provider-contract and workload packages because those
packages are scanned to stay offline. Only ``stdlib_transport`` may open a network
connection. No other ``src`` module imports it: no adapter is wired into a packaged
worker, a handler or the command line. It is used only by the unpackaged benchmarks
(``benchmarks/live.py``, ``benchmarks/extract.py`` and ``benchmarks/baseline.py``), each
opt-in and behind a hard call maximum (DEC-071, DEC-072, DEC-073).
"""
