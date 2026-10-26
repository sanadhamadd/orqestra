"""PostgreSQL persistence: the authoritative store for jobs, tasks and attempts.

``migrate`` owns the schema; ``jobs`` submits jobs transactionally and reads
jobs, tasks and derived job progress; ``lifecycle`` discovers and claims
tasks with a lease, starts, renews, completes and cancels attempts, and
revokes attempts whose lease expired. Nothing runs these operations on a
schedule yet: there is no worker loop.
"""
