"""The worker: task handlers, runtime settings, the runtime and its local fence.

``handlers`` defines what a task handler is, which job kinds a worker
supports, and how a handler's return value or exception becomes an attempt
outcome; ``settings`` validates the worker's runtime configuration (both pure).
``runtime`` is the first single-slot worker: a supervisor that claims, runs one
handler thread, renews leases, sweeps for expired attempts and records
outcomes; ``fence`` is the watchdog that ends the process when the worker
cannot establish that it still owns a running task, or a stopping worker's
drain runs out; ``shutdown`` turns SIGTERM and SIGINT into a graceful stop.
There is no command or service that runs a worker yet.
"""
