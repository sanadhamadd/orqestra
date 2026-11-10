"""Outbound provider adapters.

This package lives outside the provider-contract and workload packages because those
packages are scanned to stay offline. Only ``stdlib_transport`` may open a network
connection. No other ``src`` module imports it: no adapter is wired into a packaged
worker, a handler or the command line. Its only user is the unpackaged benchmark worker
(``benchmarks/live.py``), behind an opt-in with a hard call maximum (DEC-071).
"""
