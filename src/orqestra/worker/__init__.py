"""Worker contracts: task handlers, the handler registry and runtime settings.

``handlers`` defines what a task handler is, which job kinds a worker
supports, and how a handler's return value or exception becomes an attempt
outcome; ``settings`` validates the worker's runtime configuration. Both are
pure: no I/O, no threads, no database access. There is no worker process yet:
nothing claims tasks, runs handlers, renews leases or sweeps for expired
attempts on its own (Day 3B, later slices).
"""
