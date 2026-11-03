"""In-process summaries over the results that tasks have stored.

This layer joins a persistence read (``orqestra.persistence.snapshots``) to a pure
workload rule (``orqestra.workloads``), which neither may import from the other:
persistence knows no workload, and workloads open no connection. It adds no
table, migration, command or service.
"""
