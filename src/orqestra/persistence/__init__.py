"""PostgreSQL persistence: the authoritative store for jobs, tasks and attempts.

``migrate`` owns the schema; ``jobs`` submits jobs transactionally and reads
jobs, tasks and derived job progress; ``lifecycle`` discovers and claims
tasks, starts, completes and cancels attempts. Leases and recovery are not
implemented.
"""
