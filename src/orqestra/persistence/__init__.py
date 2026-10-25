"""PostgreSQL persistence: the authoritative store for jobs, tasks and attempts.

``migrate`` owns the schema; ``jobs`` submits jobs transactionally and reads
jobs, tasks and derived job progress; ``lifecycle`` starts, completes and
cancels attempts. Discovering claimable tasks, leases and recovery are not
implemented.
"""
