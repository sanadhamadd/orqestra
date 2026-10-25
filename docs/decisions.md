# Orqestra — engineering decision ledger

Material engineering decisions in WHAT / WHY / RUNNER-UP form. A proposal is not a
decision: every entry stays **Proposed** until the owner marks it **Accepted** in a
reviewed change. Trivial edits are not logged. Cross-cutting choices also get an ADR.

Deliberately **undecided** (do not infer a choice from the code): task queue / broker,
API framework, worker hosting, AI provider and models, asyncio vs threads for worker
slots. The DB driver and migration tool are no longer open: see DEC-016 and DEC-017.

## Day 1 — repository foundation

### DEC-001 — uv for dependency management and lockfile
- **Status:** Proposed · **ADR:** 0002
- **WHAT:** `uv` manages the virtualenv, resolves and locks dependencies (`uv.lock`), and runs tools.
- **WHY:** One fast tool for lock, sync, run and Python installation; `uv sync --locked` in CI fails when `uv.lock` is stale; `--frozen` installs the lock as-is.
- **RUNNER-UP:** Poetry — mature, but a slower resolver and a second configuration dialect.

### DEC-002 — Python 3.12
- **Status:** Proposed
- **WHAT:** `requires-python >= 3.12`, pinned in `.python-version`.
- **WHY:** `slots=True` dataclasses, `StrEnum`, better tracebacks; every candidate DB and HTTP library ships 3.12 wheels.
- **RUNNER-UP:** 3.13 — newer, but fewer verified wheels for the drivers still under evaluation.

### DEC-003 — src layout, one package, no empty subpackages
- **Status:** Proposed · **ADR:** 0002
- **WHAT:** `src/orqestra/`; subpackages appear only when they ship tested code.
- **WHY:** Tests import the installed package, not the working tree; the tree never implies unimplemented behaviour.
- **RUNNER-UP:** Scaffold every planned package now — looks complete, misleads about status.

### DEC-004 — Ruff for formatting and linting
- **Status:** Proposed
- **WHAT:** `ruff format` + `ruff check` with pycodestyle, pyflakes, isort, pyupgrade, bugbear, simplify, pytest-style and annotation rules.
- **WHY:** One fast tool replaces Black, isort and flake8; annotation rules keep the public surface typed.
- **RUNNER-UP:** Black + isort + flake8 — familiar, three tools, slower.

### DEC-005 — mypy in strict mode over src and tests
- **Status:** Proposed
- **WHAT:** `mypy --strict` with the pydantic plugin; tests are type-checked too.
- **WHY:** Strict from day one costs nothing now and is painful to retrofit; pip-installable, no Node toolchain.
- **RUNNER-UP:** pyright — faster, sharper inference, but adds a Node dependency to dev and CI.

### DEC-006 — pytest lanes by marker
- **Status:** Proposed
- **WHAT:** Markers `unit`, `integration`, `e2e`, `performance`; the PR lane runs everything except the last three; coverage threshold 80% as a heuristic.
- **WHY:** Maps directly to the charter's test gates and keeps PR CI fast and deterministic.
- **RUNNER-UP:** Separate test directories only — works, but cannot express "slow" per test.
- **Day 2 note:** where the coverage threshold is enforced is revisited by DEC-023 (proposed).

### DEC-007 — pydantic-settings for configuration
- **Status:** Proposed
- **WHAT:** `Settings(BaseSettings)` with `ORQESTRA_` prefix, `.env` in development, `SecretStr` for the DSN, validated ranges.
- **WHY:** Fails fast on bad values; secrets cannot leak through `repr`; the same library will validate job manifests later.
- **RUNNER-UP:** Hand-rolled `os.environ` parsing — zero dependencies, no validation, silent bad values.

### DEC-008 — Explicit immutable transition table for task states
- **Status:** Proposed
- **WHAT:** `TASK_TRANSITIONS` as a `MappingProxyType` of frozensets; `transition()` raises `InvalidTransitionError`.
- **WHY:** The lifecycle is documented, exhaustively testable, and the single source persistence must enforce with conditional writes.
- **RUNNER-UP:** A state-machine library — more machinery than five states justify.
- **Day 2 clarification (owner-approved direction, 2026-10-24):** the table stays the single source of *legal transitions* and is unchanged. Persistence must additionally check the task's ownership generation on every authoritative write, because a state-only conditional write cannot tell a current worker from a stale one (DEC-019, ADR-0003).

### DEC-009 — Unknown exceptions are not retried
- **Status:** Proposed
- **WHAT:** Only `TransientError` is retryable; `PermanentError` and any unclassified exception are terminal.
- **WHY:** Retrying a defect spends time and AI budget and hides the bug; transient classes must be opted into explicitly.
- **RUNNER-UP:** Retry everything except known-permanent errors — more forgiving, more expensive, masks defects.
- **Day 2 clarification (owner-approved direction, 2026-10-24):** the rule applies to exceptions raised by task handlers, classified by `classify_failure` as transient, permanent or unclassified (unclassified is recorded explicitly and not retried). Worker crashes, lost ownership and lease expiry are recovery events, not handler failures. Database or infrastructure errors must not be recorded as permanent task failures. Cancellation and shutdown signals are control-plane events and are not classified. A rejected stale write is an expected concurrency outcome, not an `InvalidTransitionError` or a task failure. `is_retryable` and the error hierarchy are unchanged.

### DEC-010 — Stdlib logging with a JSON formatter
- **Status:** Proposed
- **WHAT:** `JsonFormatter` emitting one object per line with optional `job_id`/`task_id`/`attempt_id`/`worker_id`.
- **WHY:** Zero dependencies and structured correlation IDs from the first commit.
- **RUNNER-UP:** structlog — better context binding; revisit on Day 4 when worker context grows.

### DEC-011 — argparse CLI
- **Status:** Proposed
- **WHAT:** `orqestra version|health` via argparse.
- **WHY:** Zero dependencies; keeps the API-framework decision genuinely open.
- **RUNNER-UP:** Typer — nicer UX, adds a dependency before the HTTP choice is made.

### DEC-012 — GitHub Actions pinned by commit SHA with least privilege
- **Status:** Proposed
- **WHAT:** `permissions: contents: read`; actions pinned to full SHAs with the tag in a comment; `uv sync --locked`.
- **WHY:** Immutable action references and read-only tokens are the charter's supply-chain baseline.
- **RUNNER-UP:** Tag pins (`@v7`) — readable, mutable.

### DEC-013 — Private tooling configuration is not tracked
- **Status:** Proposed
- **WHAT:** Machine-local tool configuration and session files are git-ignored (patterns in `.gitignore`); general contributor guidance is tracked in `docs/development.md`.
- **WHY:** Owner decision: local tool configuration and session records are specific to one machine, may hold private notes, and are not part of the product; tracked documentation should describe the software. Deviates from the initial planning layout, which proposed a tracked tool-specific instruction file.
- **RUNNER-UP:** Track a tool-specific instruction file — visible to contributors, but mixes machine-local material into the public record.

### DEC-014 — Deferred on Day 1 (explicitly not chosen)
- **Status:** Proposed
- **WHAT:** Docker Compose, Dockerfile, pre-commit, security scanners, Dependabot, issue templates, architecture doc, `LICENSE`, `CHANGELOG`.
- **WHY:** None supports an immediate engineering objective; Docker could not be verified locally today (daemon not running) and unverified config is not committed.
- **RUNNER-UP:** Ship them all on Day 1 — fuller skeleton, more unverified surface.

## Day 2 — PostgreSQL persistence foundation

Entries DEC-015 to DEC-023 record directions the owner approved on 2026-10-24 after an
architecture audit and an independent review. They stay **Proposed** until the owner
accepts them in a reviewed change. "Implemented" means verified by the tests named under
VERIFICATION; "direction" means not yet implemented.

### DEC-015 — PostgreSQL is the authoritative store for Job → Task → Attempt
- **Status:** Proposed · owner-approved direction · **ADR:** 0003 · implemented (schema)
- **WHAT:** Jobs, tasks and attempts live in PostgreSQL 16; every authoritative state change is a PostgreSQL transaction. SQLite is not a target.
- **WHY:** Durable, transactional row locks and constraints give claiming, fencing and recovery a single source of truth independent of any future queue.
- **RUNNER-UP:** A broker (Redis, RabbitMQ) as the system of record — fast delivery, but no relational invariants or transactional history.
- **TRADE-OFF:** One database is a single point of failure and a throughput ceiling; accepted at this project's scale.
- **VERIFICATION:** `tests/integration/test_migrations.py`, `tests/integration/test_schema_constraints.py`.

### DEC-016 — Psycopg 3, synchronous connections, explicit parameterized SQL
- **Status:** Proposed · owner-approved direction · implemented (migration runner)
- **WHAT:** `psycopg[binary]>=3.2,<4` (locked at 3.3.6, bundled libpq 18). No ORM, no async database layer.
- **WHY:** Transaction boundaries, conditional updates, row counts and locking stay visible in the code and testable.
- **RUNNER-UP:** SQLAlchemy Core — composable SQL, but an extra abstraction and dependency over the statements that matter most.
- **TRADE-OFF:** Hand-written SQL and manual row mapping. The binary wheel bundles its own libpq; revisit (`psycopg[c]` or system libpq) when building production images.
- **VERIFICATION:** strict mypy over the persistence code; integration tests run through psycopg against PostgreSQL 16.

### DEC-017 — Forward-only SQL migrations with an in-repository runner
- **Status:** Proposed · owner-approved direction · implemented
- **WHAT:** Versioned `NNNN_name.sql` files shipped in the package; `orqestra.persistence.migrate` applies each in its own transaction under `pg_advisory_xact_lock`, records version, name and SHA-256 checksum in `schema_migrations`, and refuses edited, renamed or unknown history. Migration SQL runs through PL/pgSQL `EXECUTE`, where PostgreSQL itself rejects transaction control and non-transactional statements, so no SQL is parsed on the client.
- **WHY:** Locking, checksums and history are built in, with no dependency beyond the approved driver.
- **RUNNER-UP:** Alembic with hand-written SQL — mature and widely known, but adds SQLAlchemy and Mako and has no built-in lock or checksum.
- **TRADE-OFF:** We own the runner. No downgrades; no non-transactional DDL. Revisit if downgrades, branching or frequent non-transactional DDL are needed.
- **VERIFICATION:** integration tests for empty and repeated runs, edited, renamed and unknown history, rollback and recovery, nine transaction-control variants, `CREATE INDEX CONCURRENTLY` and `VACUUM`, and concurrent runners (observed waiting on the lock). As a one-off negative control outside the suite, the same race without the lock failed 10 of 10 rounds.

### DEC-018 — Separate attempt states; states stored as constrained text; application UUIDs
- **Status:** Proposed · owner-approved direction · implemented
- **WHAT:** `AttemptState` (running → succeeded | failed | cancelled) beside the unchanged five-state `TaskState`; `FailureKind` for failed attempts. Stored as `text` with named `CHECK` constraints. UUID keys generated by the application.
- **WHY:** Attempt history and failure provenance without overloading task states; text constraints can be changed with a migration, unlike enum values; IDs are known before insert.
- **RUNNER-UP:** Native PostgreSQL enums; `bigint` identity keys.
- **TRADE-OFF:** Slightly larger rows. State values exist in Python and SQL, guarded by a drift test.
- **VERIFICATION:** `tests/unit/test_states.py`, `tests/unit/test_policy.py`; `test_stored_state_values_match_the_domain`.

### DEC-019 — `tasks.attempt_count` is the ownership generation, guarded in the database
- **Status:** Proposed · owner-approved direction · **ADR:** 0003 · guard implemented; generation-checked writes are Step 2
- **WHAT:** Attempt *N* holds generation *N*. The count starts at 0, changes only on a `queued → running` claim, by exactly +1, never decreases and never exceeds `max_attempts`. A narrow trigger (`tasks_attempt_count_fencing`) rejects any other change; it does not restate the transition table.
- **WHY:** A state-only conditional write accepts a stale worker's result after the task was requeued and reclaimed. The generation guard is the one invariant whose violation would be silent, so the database enforces it.
- **RUNNER-UP:** A separate `generation` column, or relying on write-path discipline and tests alone.
- **TRADE-OFF:** Every started attempt consumes retry budget, including attempts lost to crashes; returning budget later needs a separate counter.
- **VERIFICATION:** trigger tests for legal claims, decreases, resets, skipped generations, unrelated increments, frozen terminal tasks and retry exhaustion.

### DEC-020 — Cooperative cancellation
- **Status:** Proposed · owner-approved direction · policy and schema implemented; writes are Step 2, grace period later
- **WHAT:** A request cancels a queued task immediately. A running task records `cancel_requested_at` and keeps running. A legitimate success may still win; an outcome that would requeue cancels instead; other terminal outcomes are recorded truthfully. A future recovery step must revoke ownership after a bounded grace period.
- **WHY:** Attempt and usage records stay truthful while work is in flight, and no retries start after a request.
- **RUNNER-UP:** Immediate cancellation of running tasks, fencing off late results — simpler state, but the database would show no live attempt while a worker still spends.
- **TRADE-OFF:** Running tasks are not terminal the moment cancellation is requested. Revoking database ownership is not proof that an external AI call stopped or was not billed.
- **VERIFICATION:** `next_task_state` and `state_after_cancel_request` decision tables; `tasks_cancel_request_not_queued`, `tasks_cancelled_was_requested`. Race tests are Step 2; the grace period needs its own design and tests.

### DEC-021 — READ COMMITTED, short transactions, task-before-attempt locking (direction)
- **Status:** Proposed · owner-approved direction · partly implemented: job submission (DEC-024); task and attempt locking are Step 2
- **WHAT:** Authoritative writes are single conditional statements or lock-then-decide transactions (`SELECT … FOR UPDATE` on the task, then its attempt) under READ COMMITTED. No transaction is held open during external or AI work.
- **WHY:** Row locks plus re-checked conditions give compare-and-set without serialization-failure retry loops; one lock order prevents deadlocks.
- **RUNNER-UP:** SERIALIZABLE isolation with retries.
- **TRADE-OFF:** Cross-table invariants rely on the write paths and their tests.
- **VERIFICATION:** Step 2 concurrency tests (competing starts, stale generations, cancellation races).

### DEC-022 — Job status is derived from task states (direction)
- **Status:** Proposed · owner-approved direction · not yet implemented
- **WHAT:** No mutable job-state column; status is computed from task-state counts. Revisit only with a correctness need or reproducible measurements.
- **WHY:** No redundant state to drift and no hot job row updated by every task.
- **RUNNER-UP:** Stored job state or counters.
- **TRADE-OFF:** An aggregate query per status read; an index may be added when measured.
- **VERIFICATION:** Step 2 tests of the derivation.

### DEC-023 — Fast and full verification lanes
- **Status:** Proposed · owner-approved direction · implemented; both lanes passed on GitHub for PR #2 and for `main` after its merge
- **WHAT:** `make check` = format, lint, mypy, unit tests (coverage reported). `make verify` = lint, mypy, unit + PostgreSQL integration tests with `--cov-fail-under=80`, and a wheel check that every migration is packaged. It fails if `ORQESTRA_TEST_DATABASE_URL` is unset. Integration tests refuse any database other than a local `orqestra_test`. CI runs both lanes, the second against `postgres:16.15` pinned by digest.
- **WHY:** "Verified" must mean the persistence tests ran; the threshold applies where persistence code is exercised.
- **RUNNER-UP:** Keep the 80% threshold on the unit-only run — it fails once database code is covered only by integration tests.
- **TRADE-OFF:** `make check` no longer proves coverage on its own; contributors need local PostgreSQL for `make verify`.
- **VERIFICATION:** `make verify` without the URL exits non-zero; with it, the combined coverage is reported against the threshold; `tests/integration/test_fixture_safety.py`.

## Day 2 — Step 2A: transactional job submission

Entries DEC-024 to DEC-027 record implementation choices within the approved Day 2
direction. They stay **Proposed** until the owner accepts them in a reviewed change. No
schema change was needed.

### DEC-024 — Submission owns one short READ COMMITTED transaction
- **Status:** Proposed · implemented (`orqestra.persistence.jobs.submit_job`)
- **WHAT:** `submit_job(conn, submission)` takes a caller's connection. It refuses one whose caller holds an open, failed or busy transaction (a `RuntimeError` naming the status), and leaves a closed or broken connection to psycopg's own connection error. It sets `READ COMMITTED` for its own transaction, inserts the job row and then every task in a single `INSERT … SELECT FROM unnest(…)`, and commits. Reads (`get_job`, `get_task`, `list_tasks`) are single statements; on a non-autocommit connection psycopg opens an implicit transaction for them that the caller ends. Every function reads through its own tuple-row cursor, so the connection's row factory does not matter.
- **WHY:** Atomicity and idempotent replay depend on the submission committing on its own. A function that joined a caller's transaction could be held open or rolled back later, keeping other submitters with the same key waiting. Setting the isolation level makes behaviour independent of session, server and psycopg connection settings (DEC-021). Mapping rows by position from a cursor the function controls means a connection configured with, say, `dict_row` cannot turn rows into wrong records: before this was fixed, such a connection reported a conflicting submission as a successful replay of a garbage job. One statement for all tasks keeps the transaction to three round trips whatever the task count.
- **RUNNER-UP:** Caller-managed transactions, so submission could be combined with other writes — more composable, but the commit point and its duration would no longer be the function's. Inserting tasks with `executemany` — equally atomic, but more statements per submission.
- **TRADE-OFF:** A submission cannot share a transaction with other writes. A very large job is one transaction that holds its idempotency key until commit; large sizes have not been measured.
- **VERIFICATION:** `tests/integration/test_job_submission.py`: rejected submissions, failing either at the job insert or at a task insert, leave no rows behind; a test-only trigger that fails the third task reports that the job and two tasks existed before rollback; the function refuses implicit, explicit and failed caller transactions and leaves them as they were; a closed or server-terminated connection raises `psycopg.OperationalError`; a connection lost mid-submission stores nothing and frees the key; it commits on a non-autocommit connection, and one connection stays usable after success, constraint failure, conflict and replay, in both modes; it works on a `dict_row` connection; a waiting duplicate still replays under a `serializable` session default and under psycopg's `isolation_level = SERIALIZABLE`.

### DEC-025 — Idempotency: unique key, `ON CONFLICT DO NOTHING`, stored-content comparison
- **Status:** Proposed · implemented
- **WHAT:** `jobs_idempotency_key_unique` arbitrates. The job insert uses `ON CONFLICT ON CONSTRAINT jobs_idempotency_key_unique DO NOTHING`, so a concurrent submission with the same key waits for the first to commit or roll back. When the key is taken, one query loads the existing job and compares the request with its stored `kind` and tasks (`task_key`, `payload` by `jsonb` equality, `max_attempts`). An identical request returns the existing job with `created=False`; any difference raises `IdempotencyConflictError`.
- **WHY:** The stored rows already hold the entire logical request, so no new column is needed. PostgreSQL's unique index gives one winner under any concurrency; comparing in SQL uses `jsonb` semantics rather than Python equality, where `True == 1`.
- **RUNNER-UP:** A request-fingerprint column (hash of canonical JSON) — constant-cost comparison, but it needs a schema change and a canonical form every client must reproduce. An advisory lock on the key's hash, or SERIALIZABLE isolation with retries — extra mechanisms beside a unique index that already serialises the conflict.
- **TRADE-OFF:** Replay costs a comparison proportional to the task count. Equivalence follows `jsonb`: object key order is ignored and numbers compare by value (`1` equals `1.0`, `10**30` equals `1e30`); strings compare exactly, without Unicode normalisation. Keys match exactly, including case and whitespace. Correctness relies on submission columns never being updated, which no code does but the schema does not forbid. Keys are global and never expire. The conflict error carries the existing job ID but leaves the key out of its message.
- **VERIFICATION:** identical replays, including reordered tasks and reformatted JSON, store nothing; seven kinds of content change and eight JSON differences (type, order, float rounding, Unicode normalisation, case) are rejected; a replay the database cannot evaluate raises the database's error, not a conflict; a lock timeout while waiting on the key raises `LockNotAvailable` and leaves the connection reusable; 10 rounds × 8 concurrent identical submitters create exactly one job each; 10 rounds × 8 concurrent submitters split across two contents store one job and reject the losers. Deterministic gated tests observe the second submitter blocked on the first's transaction lock (`pg_locks`), then show it replaying after a commit, rejected after a conflicting commit, and taking the key after a rollback. Locally, removing `ON CONFLICT` failed 31 tests and removing the comparison failed 22.

### DEC-026 — Job and task records are frozen, slotted dataclasses; value rules stay in the schema
- **Status:** Proposed · implemented (`orqestra.domain.jobs`)
- **WHAT:** `TaskSpec` and `JobSubmission` describe a request; `Job` and `Task` mirror stored rows, with `Task.state` as a `TaskState`. `JobSubmission` rejects an empty task list and repeated task keys. Non-blank text and the retry-limit range are left to the named database constraints and surface as `psycopg` integrity errors after rollback.
- **WHY:** Standard library only, immutable and cheap, consistent with the approved direction. Checks that span the whole request belong to the request. Restating column rules in Python would create a second copy that could drift.
- **RUNNER-UP:** Pydantic models — already a dependency and good at validation, but heavier for internal records and a second validation layer over the constraints.
- **TRADE-OFF:** Value errors arrive as database errors with constraint names rather than as Python validation errors. Frozen dataclasses do not deep-freeze payload dicts.
- **VERIFICATION:** `tests/unit/test_jobs.py`; the rejected-submission integration tests assert the violated constraint names.

### DEC-027 — Tasks are listed by `task_key` in code-point order
- **Status:** Proposed · implemented
- **WHAT:** `list_tasks` orders by `task_key COLLATE "C"`. Task keys are unique within a job, so the order is total. Submission order is not stored.
- **WHY:** Deterministic without a new column, and independent of the database locale: it matches Python's `sorted`, whereas the default `en_US.UTF-8` collation sorts `a` before `B`.
- **RUNNER-UP:** A `position` column preserving submission order — a schema change with no demonstrated requirement yet.
- **TRADE-OFF:** Clients that need submission order must encode it in their keys. The sort cannot use the `(job_id, task_key)` index, which follows the default collation; not measured.
- **VERIFICATION:** `test_tasks_are_listed_in_code_point_order_whatever_the_locale`, which fails without the `COLLATE` clause against the `en_US.UTF-8` test database.
