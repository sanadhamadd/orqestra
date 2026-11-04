"""Outbound provider adapters.

This package lives outside ``orqestra.ai`` and ``orqestra.workloads`` because those
packages are scanned to stay offline. Only ``stdlib_transport`` may open a network
connection. Nothing outside this package imports it yet: no adapter is wired into a
worker, a handler or the command line.
"""
