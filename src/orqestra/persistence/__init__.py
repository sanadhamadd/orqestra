"""PostgreSQL persistence: the authoritative store for jobs, tasks and attempts.

This package currently provides the schema and its migration runner.
Transactional task operations build on this schema and live alongside it.
"""
