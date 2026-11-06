"""Provider-neutral contracts for provider calls: a request/response protocol,
a deterministic scripted provider for tests, and pure cost-aware route selection.

Nothing here opens a connection, reads credentials or names a vendor. A real
adapter would implement ``Provider`` elsewhere; this package imports only the
standard library and the domain's error taxonomy.
"""
