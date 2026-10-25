"""PostgreSQL persistence: the authoritative store for jobs, tasks and attempts.

``migrate`` owns the schema; ``jobs`` submits jobs transactionally and reads
jobs and tasks. Attempt, claiming and cancellation writes are not implemented.
"""
