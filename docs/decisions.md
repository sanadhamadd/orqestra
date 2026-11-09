# Orqestra — engineering decision ledger

Material engineering decisions in WHAT / WHY / RUNNER-UP form. A proposal is not a
decision: every entry stays **Proposed** until the owner marks it **Accepted** in a
reviewed change. Trivial edits are not logged. Cross-cutting choices also get an ADR.

No inbound API or framework is planned (DEC-062). Deliberately **undecided** (do not
infer a choice from the code): which
models to use (the provider direction is OpenRouter, through an adapter that nothing
wires in and that has no default model, DEC-060), operational lease and
execution-timeout values and any per-job-kind policy (DEC-034). The worker model and hosting boundary are
recorded in DEC-054. The DB driver, migration tool and task queue are no longer open:
see DEC-016, DEC-017 and DEC-033. Operational cancellation grace periods are
undecided too (DEC-040), and so are operational retry delays (DEC-053).

## Repository foundation

### DEC-001 — uv for dependency management and lockfile
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · **ADR:** 0002
- **WHAT:** `uv` manages the virtualenv, resolves and locks dependencies (`uv.lock`), and runs tools.
- **WHY:** One fast tool for lock, sync, run and Python installation; `uv sync --locked` in CI fails when `uv.lock` is stale; `--frozen` installs the lock as-is.
- **RUNNER-UP:** Poetry — mature, but a slower resolver and a second configuration dialect.

### DEC-002 — Python 3.12
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `requires-python >= 3.12`, pinned in `.python-version`.
- **WHY:** `slots=True` dataclasses, `StrEnum`, better tracebacks; every candidate DB and HTTP library ships 3.12 wheels.
- **RUNNER-UP:** 3.13 — newer, but fewer verified wheels for the drivers still under evaluation.

### DEC-003 — src layout, one package, no empty subpackages
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · **ADR:** 0002
- **WHAT:** `src/orqestra/`; subpackages appear only when they ship tested code.
- **WHY:** Tests import the installed package, not the working tree; the tree never implies unimplemented behaviour.
- **RUNNER-UP:** Scaffold every planned package now — looks complete, misleads about status.

### DEC-004 — Ruff for formatting and linting
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `ruff format` + `ruff check` with pycodestyle, pyflakes, isort, pyupgrade, bugbear, simplify, pytest-style and annotation rules.
- **WHY:** One fast tool replaces Black, isort and flake8; annotation rules keep the public surface typed.
- **RUNNER-UP:** Black + isort + flake8 — familiar, three tools, slower.

### DEC-005 — mypy in strict mode over src and tests
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `mypy --strict` with the pydantic plugin; tests are type-checked too.
- **WHY:** Strict from day one costs nothing now and is painful to retrofit; pip-installable, no Node toolchain.
- **RUNNER-UP:** pyright — faster, sharper inference, but adds a Node dependency to dev and CI.

### DEC-006 — pytest lanes by marker
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** Markers `unit`, `integration`, `e2e`, `performance`; the fast lane runs everything except the last three, and PR CI also runs the `integration` lane (DEC-023); coverage threshold 80% as a heuristic.
- **WHY:** Maps directly to the charter's test gates and keeps PR CI fast and deterministic.
- **RUNNER-UP:** Separate test directories only — works, but cannot express "slow" per test.
- **Persistence foundation note:** where the coverage threshold is enforced is revisited by DEC-023.

### DEC-007 — pydantic-settings for configuration
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `Settings(BaseSettings)` with `ORQESTRA_` prefix, `.env` in development, `SecretStr` for the DSN, validated ranges.
- **WHY:** Fails fast on bad values; secrets cannot leak through `repr`. It is used for settings only: job and task records are frozen standard-library dataclasses (DEC-026).
- **RUNNER-UP:** Hand-rolled `os.environ` parsing — zero dependencies, no validation, silent bad values.

### DEC-008 — Explicit immutable transition table for task states
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `TASK_TRANSITIONS` as a `MappingProxyType` of frozensets; `transition()` raises `InvalidTransitionError`.
- **WHY:** The lifecycle is documented, exhaustively testable, and the single source persistence must enforce with conditional writes.
- **RUNNER-UP:** A state-machine library — more machinery than five states justify.
- **Persistence clarification (owner-approved direction, 2026-10-24):** the table stays the single source of *legal transitions* and is unchanged. Persistence must additionally check the task's ownership generation on every authoritative write, because a state-only conditional write cannot tell a current worker from a stale one (DEC-019, ADR-0003).

### DEC-009 — Unknown exceptions are not retried
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** Only `TransientError` is retryable; `PermanentError` and any unclassified exception are terminal.
- **WHY:** Retrying a defect spends time and provider budget and hides the bug; transient classes must be opted into explicitly.
- **RUNNER-UP:** Retry everything except known-permanent errors — more forgiving, more expensive, masks defects.
- **Persistence clarification (owner-approved direction, 2026-10-24):** the rule applies to exceptions raised by task handlers, classified by `classify_failure` as transient, permanent or unclassified (unclassified is recorded explicitly and not retried). Worker crashes, lost ownership and lease expiry are recovery events, not handler failures. Database or infrastructure errors must not be recorded as permanent task failures. Cancellation and shutdown signals are control-plane events and are not classified. A rejected stale write is an expected concurrency outcome, not an `InvalidTransitionError` or a task failure. `is_retryable` and the error hierarchy are unchanged.
- **Lease and recovery clarification:** an attempt whose lease or deadline expired is recorded as `revoked` with a revocation reason, never as a failed attempt with a failure kind or error details (DEC-038). A task that uses up its budget through revocations ends `failed` because its retries are exhausted, not because a handler failed permanently.

### DEC-010 — Stdlib logging with a JSON formatter
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `JsonFormatter` emitting one object per line with optional `job_id`/`task_id`/`attempt_id`/`worker_id`.
- **WHY:** Zero dependencies and structured correlation IDs from the first commit.
- **RUNNER-UP:** structlog — better context binding; revisit with the worker runtime, when worker context grows.

### DEC-011 — argparse CLI
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `orqestra version|health|migrate`, `orqestra worker run` and `orqestra job submit|status|summary|usage` via argparse.
- **WHY:** Zero dependencies; no inbound API or web framework is planned (DEC-062).
- **RUNNER-UP:** Typer — nicer UX, adds a dependency before the HTTP choice is made.

### DEC-012 — GitHub Actions pinned by commit SHA with least privilege
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** `permissions: contents: read`; actions pinned to full SHAs with the tag in a comment; `uv sync --locked`.
- **WHY:** Immutable action references and read-only tokens are the charter's supply-chain baseline.
- **RUNNER-UP:** Tag pins (`@v7`) — readable, mutable.

### DEC-013 — Private tooling configuration is not tracked
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:** Machine-local tool configuration and session files are git-ignored (patterns in `.gitignore`); general contributor guidance is tracked in `docs/development.md`.
- **WHY:** Owner decision: local tool configuration and session records are specific to one machine, may hold private notes, and are not part of the product; tracked documentation should describe the software. Deviates from the initial planning layout, which proposed a tracked tool-specific instruction file.
- **RUNNER-UP:** Track a tool-specific instruction file — visible to contributors, but mixes machine-local material into the public record.

### DEC-014 — Deferred at the start (explicitly not chosen)
- **Status:** Proposed, superseded in part: Docker Compose (`compose.yaml`, PR #33) and the architecture document (`docs/architecture.md`, PR #38) now exist; the other seven deferrals stand.
- **WHAT:** Docker Compose, Dockerfile, pre-commit, security scanners, Dependabot, issue templates, architecture doc, `LICENSE`, `CHANGELOG`.
- **WHY:** None supports an immediate engineering objective; Docker could not be verified locally today (daemon not running) and unverified config is not committed.
- **RUNNER-UP:** Ship them all at the start — fuller skeleton, more unverified surface.

## PostgreSQL persistence foundation

Entries DEC-015 to DEC-023 record directions the owner approved on 2026-10-24 after an
architecture audit and an independent review. The owner accepted them on
2026-11-07. "Implemented" means verified by the tests named under
VERIFICATION; "direction" means not yet implemented.

### DEC-015 — PostgreSQL is the authoritative store for Job → Task → Attempt
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · **ADR:** 0003 · implemented (schema)
- **WHAT:** Jobs, tasks and attempts live in PostgreSQL 16; every authoritative state change is a PostgreSQL transaction. SQLite is not a target.
- **WHY:** Durable, transactional row locks and constraints give claiming, fencing and recovery a single source of truth independent of any future queue.
- **RUNNER-UP:** A broker (Redis, RabbitMQ) as the system of record — fast delivery, but no relational invariants or transactional history.
- **TRADE-OFF:** One database is a single point of failure and a throughput ceiling; accepted at this project's scale.
- **VERIFICATION:** `tests/integration/test_migrations.py`, `tests/integration/test_schema_constraints.py`.

### DEC-016 — Psycopg 3, synchronous connections, explicit parameterized SQL
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented (migration runner)
- **WHAT:** `psycopg[binary]>=3.2,<4` (locked at 3.3.6, bundled libpq 18). No ORM, no async database layer.
- **WHY:** Transaction boundaries, conditional updates, row counts and locking stay visible in the code and testable.
- **RUNNER-UP:** SQLAlchemy Core — composable SQL, but an extra abstraction and dependency over the statements that matter most.
- **TRADE-OFF:** Hand-written SQL and manual row mapping. The binary wheel bundles its own libpq; revisit (`psycopg[c]` or system libpq) when building production images.
- **VERIFICATION:** strict mypy over the persistence code; integration tests run through psycopg against PostgreSQL 16.

### DEC-017 — Forward-only SQL migrations with an in-repository runner
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented
- **WHAT:** Versioned `NNNN_name.sql` files shipped in the package; `orqestra.persistence.migrate` applies each in its own transaction under `pg_advisory_xact_lock`, records version, name and SHA-256 checksum in `schema_migrations`, and refuses edited, renamed or unknown history. Migration SQL runs through PL/pgSQL `EXECUTE`, where PostgreSQL itself rejects transaction control and non-transactional statements, so no SQL is parsed on the client.
- **WHY:** Locking, checksums and history are built in, with no dependency beyond the approved driver.
- **RUNNER-UP:** Alembic with hand-written SQL — mature and widely known, but adds SQLAlchemy and Mako and has no built-in lock or checksum.
- **TRADE-OFF:** We own the runner. No downgrades; no non-transactional DDL. Revisit if downgrades, branching or frequent non-transactional DDL are needed.
- **VERIFICATION:** integration tests for empty and repeated runs, edited, renamed and unknown history, rollback and recovery, nine transaction-control variants, `CREATE INDEX CONCURRENTLY` and `VACUUM`, and concurrent runners (observed waiting on the lock). As a one-off negative control outside the suite, the same race without the lock failed 10 of 10 rounds.
- **Note (2026-11-09):** each migration transaction now starts with `LOCK TABLE schema_migrations IN SHARE ROW EXCLUSIVE MODE`, which takes no snapshot, then takes `pg_advisory_xact_lock`, and only then reads the history; a first run creates the history table under the advisory lock in its first migration's transaction, and a runner that loses that race rolls back and takes the table's lock instead. Runners therefore serialise whatever the session's default isolation (a REPEATABLE READ snapshot taken at the advisory SELECT had read the history from before the previous runner committed). A runner of the previous code racing a new one takes the locks in the other order and can be aborted as a deadlock (`40P01`, a `MigrationError`), never applied twice. `tests/integration/test_migrate.py`.

### DEC-018 — Separate attempt states; states stored as constrained text; application UUIDs
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented
- **WHAT:** `AttemptState` (running → succeeded | failed | cancelled | revoked; `revoked` added by DEC-038 and migration 0002) beside the unchanged five-state `TaskState`; `FailureKind` for failed attempts. Stored as `text` with named `CHECK` constraints. UUID keys generated by the application.
- **WHY:** Attempt history and failure provenance without overloading task states; text constraints can be changed with a migration, unlike enum values; IDs are known before insert.
- **RUNNER-UP:** Native PostgreSQL enums; `bigint` identity keys.
- **TRADE-OFF:** Slightly larger rows. State values exist in Python and SQL, guarded by a drift test.
- **VERIFICATION:** `tests/unit/test_states.py`, `tests/unit/test_policy.py`; `test_stored_state_values_match_the_domain`.

### DEC-019 — `tasks.attempt_count` is the ownership generation, guarded in the database
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · **ADR:** 0003 · guard implemented; generation-checked completion implemented with the transactional attempt lifecycle (DEC-029)
- **WHAT:** Attempt *N* holds generation *N*. The count starts at 0, changes only on a `queued → running` claim, by exactly +1, never decreases and never exceeds `max_attempts`. A narrow trigger (`tasks_attempt_count_fencing`) rejects any other change; it does not restate the transition table.
- **WHY:** A state-only conditional write accepts a stale worker's result after the task was requeued and reclaimed. The generation guard is the one invariant whose violation would be silent, so the database enforces it.
- **RUNNER-UP:** A separate `generation` column, or relying on write-path discipline and tests alone.
- **TRADE-OFF:** Every started attempt consumes retry budget, including attempts lost to crashes; returning budget later needs a separate counter.
- **Lease and recovery note:** revoked attempts use budget like any other, and recovery fails a task whose budget is used up (DEC-038); no separate counter was added.
- **VERIFICATION:** trigger tests for legal claims, decreases, resets, skipped generations, unrelated increments, frozen terminal tasks and retry exhaustion.

### DEC-020 — Cooperative cancellation
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · policy, schema and writes implemented (transactional attempt lifecycle, DEC-032); grace period later
- **WHAT:** A request cancels a queued task immediately. A running task records `cancel_requested_at` and keeps running. A legitimate success may still win; an outcome that would requeue cancels instead; other terminal outcomes are recorded truthfully. A future recovery step must revoke ownership after a bounded grace period.
- **WHY:** Attempt and usage records stay truthful while work is in flight, and no retries start after a request.
- **RUNNER-UP:** Immediate cancellation of running tasks, fencing off late results — simpler state, but the database would show no live attempt while a worker still spends.
- **TRADE-OFF:** Running tasks are not terminal the moment cancellation is requested. Revoking database ownership is not proof that an external provider call stopped or was not billed.
- **VERIFICATION:** `next_task_state` and `state_after_cancel_request` decision tables; `tasks_cancel_request_not_queued`, `tasks_cancelled_was_requested`. Race tests belong to the transactional lifecycle work; the grace period needs its own design and tests.
- **Lease and recovery note:** a revocation counts as an outcome that would requeue while attempts remain, so a pending request cancels the task instead; an exhausted revocation fails it, as an exhausted transient failure does (DEC-038). Until the grace period exists, the execution deadline bounds how long a running attempt can ignore a request. A revocation caused by the request itself (the cancellation grace period) will need its own rule.
- **Cancellation grace note:** the bounded grace period now exists (DEC-040). A request carries a grace duration and sets the running attempt's lease to end that long after it; heartbeats stop renewing it, and recovery then cancels the task whatever budget is left. The cooperative rules above are unchanged within the grace period.

### DEC-021 — READ COMMITTED, short transactions, task-before-attempt locking (direction)
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented: job submission (DEC-024) and the attempt lifecycle (DEC-028)
- **WHAT:** Authoritative writes are single conditional statements or lock-then-decide transactions (`SELECT … FOR UPDATE` on the task, then its attempt) under READ COMMITTED. No transaction is held open during external or provider work.
- **WHY:** Row locks plus re-checked conditions give compare-and-set without serialization-failure retry loops; one lock order prevents deadlocks.
- **RUNNER-UP:** SERIALIZABLE isolation with retries.
- **TRADE-OFF:** Cross-table invariants rely on the write paths and their tests.
- **VERIFICATION:** Transactional lifecycle concurrency tests (competing starts, stale generations, cancellation races).
- **Lease and recovery note:** the heartbeat (DEC-036) and the provider-usage intent insert, which reads its attempt `FOR KEY SHARE` (DEC-058), are the writes that lock only the attempt row.

### DEC-022 — Job status is derived from task states (direction)
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented; the rules are DEC-031
- **WHAT:** No mutable job-state column; status is computed from task-state counts. Revisit only with a correctness need or reproducible measurements.
- **WHY:** No redundant state to drift and no hot job row updated by every task.
- **RUNNER-UP:** Stored job state or counters.
- **TRADE-OFF:** An aggregate query per status read; an index may be added when measured.
- **VERIFICATION:** Transactional lifecycle tests of the derivation.

### DEC-023 — Fast and full verification lanes
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented; both lanes passed on GitHub for PR #2 and for `main` after its merge
- **WHAT:** `make check` = format, lint, mypy, unit tests (coverage reported). `make verify` = lint, mypy, unit + PostgreSQL integration tests with `--cov-fail-under=80`, and a wheel check that every migration is packaged. It fails if `ORQESTRA_TEST_DATABASE_URL` is unset. Integration tests refuse any database other than a local `orqestra_test`. CI runs both lanes, the second against `postgres:16.15` pinned by digest.
- **WHY:** "Verified" must mean the persistence tests ran; the threshold applies where persistence code is exercised.
- **RUNNER-UP:** Keep the 80% threshold on the unit-only run — it fails once database code is covered only by integration tests.
- **TRADE-OFF:** `make check` no longer proves coverage on its own; contributors need local PostgreSQL for `make verify`.
- **VERIFICATION:** `make verify` without the URL exits non-zero; with it, the combined coverage is reported against the threshold; `tests/integration/test_fixture_safety.py`.

## Transactional job submission

Entries DEC-024 to DEC-027 record implementation choices within the approved persistence
direction. The owner accepted them on 2026-11-07. No
schema change was needed.

### DEC-024 — Submission owns one short READ COMMITTED transaction
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented (`orqestra.persistence.jobs.submit_job`)
- **WHAT:** `submit_job(conn, submission)` takes a caller's connection. It refuses one whose caller holds an open, failed or busy transaction (a `RuntimeError` naming the status), and leaves a closed or broken connection to psycopg's own connection error. It sets `READ COMMITTED` for its own transaction, inserts the job row and then every task in a single `INSERT … SELECT FROM unnest(…)`, and commits. Reads (`get_job`, `get_task`, `list_tasks`) are single statements; on a non-autocommit connection psycopg opens an implicit transaction for them that the caller ends. Every function reads through its own tuple-row cursor, so the connection's row factory does not matter.
- **WHY:** Atomicity and idempotent replay depend on the submission committing on its own. A function that joined a caller's transaction could be held open or rolled back later, keeping other submitters with the same key waiting. Setting the isolation level makes behaviour independent of session, server and psycopg connection settings (DEC-021). Mapping rows by position from a cursor the function controls means a connection configured with, say, `dict_row` cannot turn rows into wrong records: before this was fixed, such a connection reported a conflicting submission as a successful replay of a garbage job. One statement for all tasks keeps the transaction to three round trips whatever the task count.
- **RUNNER-UP:** Caller-managed transactions, so submission could be combined with other writes — more composable, but the commit point and its duration would no longer be the function's. Inserting tasks with `executemany` — equally atomic, but more statements per submission.
- **TRADE-OFF:** A submission cannot share a transaction with other writes. A very large job is one transaction that holds its idempotency key until commit; large sizes have not been measured.
- **VERIFICATION:** `tests/integration/test_job_submission.py`: rejected submissions, failing either at the job insert or at a task insert, leave no rows behind; a test-only trigger that fails the third task reports that the job and two tasks existed before rollback; the function refuses implicit, explicit and failed caller transactions and leaves them as they were; a closed or server-terminated connection raises `psycopg.OperationalError`; a connection lost mid-submission stores nothing and frees the key; it commits on a non-autocommit connection, and one connection stays usable after success, constraint failure, conflict and replay, in both modes; it works on a `dict_row` connection; a waiting duplicate still replays under a `serializable` session default and under psycopg's `isolation_level = SERIALIZABLE`.

### DEC-025 — Idempotency: unique key, `ON CONFLICT DO NOTHING`, stored-content comparison
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented
- **WHAT:** `jobs_idempotency_key_unique` arbitrates. The job insert uses `ON CONFLICT ON CONSTRAINT jobs_idempotency_key_unique DO NOTHING`, so a concurrent submission with the same key waits for the first to commit or roll back. When the key is taken, one query loads the existing job and compares the request with its stored `kind` and tasks (`task_key`, `payload` by `jsonb` equality, `max_attempts`). An identical request returns the existing job with `created=False`; any difference raises `IdempotencyConflictError`.
- **WHY:** The stored rows already hold the entire logical request, so no new column is needed. PostgreSQL's unique index gives one winner under any concurrency; comparing in SQL uses `jsonb` semantics rather than Python equality, where `True == 1`.
- **RUNNER-UP:** A request-fingerprint column (hash of canonical JSON) — constant-cost comparison, but it needs a schema change and a canonical form every client must reproduce. An advisory lock on the key's hash, or SERIALIZABLE isolation with retries — extra mechanisms beside a unique index that already serialises the conflict.
- **TRADE-OFF:** Replay costs a comparison proportional to the task count. Equivalence follows `jsonb`: object key order is ignored and numbers compare by value (`1` equals `1.0`, `10**30` equals `1e30`); strings compare exactly, without Unicode normalisation. Keys match exactly, including case and whitespace. Correctness relies on submission columns never being updated, which no code does but the schema does not forbid. Keys are global and never expire. The conflict error carries the existing job ID but leaves the key out of its message.
- **VERIFICATION:** identical replays, including reordered tasks and reformatted JSON, store nothing; seven kinds of content change and eight JSON differences (type, order, float rounding, Unicode normalisation, case) are rejected; a replay the database cannot evaluate raises the database's error, not a conflict; a lock timeout while waiting on the key raises `LockNotAvailable` and leaves the connection reusable; 10 rounds × 8 concurrent identical submitters create exactly one job each; 10 rounds × 8 concurrent submitters split across two contents store one job and reject the losers. Deterministic gated tests observe the second submitter blocked on the first's transaction lock (`pg_locks`), then show it replaying after a commit, rejected after a conflicting commit, and taking the key after a rollback. Locally, removing `ON CONFLICT` failed 31 tests and removing the comparison failed 22.

### DEC-026 — Job and task records are frozen, slotted dataclasses; value rules stay in the schema
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented (`orqestra.domain.jobs`)
- **WHAT:** `TaskSpec` and `JobSubmission` describe a request; `Job` and `Task` mirror stored rows, with `Task.state` as a `TaskState`. `JobSubmission` rejects an empty task list and repeated task keys. Non-blank text and the retry-limit range are left to the named database constraints and surface as `psycopg` integrity errors after rollback.
- **WHY:** Standard library only, immutable and cheap, consistent with the approved direction. Checks that span the whole request belong to the request. Restating column rules in Python would create a second copy that could drift.
- **RUNNER-UP:** Pydantic models — already a dependency and good at validation, but heavier for internal records and a second validation layer over the constraints.
- **TRADE-OFF:** Value errors arrive as database errors with constraint names rather than as Python validation errors. Frozen dataclasses do not deep-freeze payload dicts.
- **VERIFICATION:** `tests/unit/test_jobs.py`; the rejected-submission integration tests assert the violated constraint names.

### DEC-027 — Tasks are listed by `task_key` in code-point order
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented
- **WHAT:** `list_tasks` orders by `task_key COLLATE "C"`. Task keys are unique within a job, so the order is total. Submission order is not stored.
- **WHY:** Deterministic without a new column, and independent of the database locale: it matches Python's `sorted`, whereas the default `en_US.UTF-8` collation sorts `a` before `B`.
- **RUNNER-UP:** A `position` column preserving submission order — a schema change with no demonstrated requirement yet.
- **TRADE-OFF:** Clients that need submission order must encode it in their keys. The sort cannot use the `(job_id, task_key)` index, which follows the default collation; not measured.
- **VERIFICATION:** `test_tasks_are_listed_in_code_point_order_whatever_the_locale`, which fails without the `COLLATE` clause against the `en_US.UTF-8` test database.

## Transactional attempt lifecycle

Entries DEC-028 to DEC-032 record implementation choices within the approved persistence
direction (ADR-0003, DEC-019 to DEC-022). The owner accepted
them on 2026-11-07. No schema change, migration or dependency was needed, and the
lifecycle policy functions and transition tables are unchanged.

### DEC-028 — Lifecycle writes lock the task, then the attempt, decide with the policy, and commit
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented (`orqestra.persistence.lifecycle`)
- **WHAT:** `start_attempt`, `complete_attempt` and `request_cancellation` each own one short READ COMMITTED transaction (the guard shared with `submit_job`). Each one locks the task row (`SELECT … FOR UPDATE`) before anything else; completion then locks its attempt row. The domain functions decide (`can_transition`, `next_task_state`, `state_after_cancel_request`, `transition`, `transition_attempt`), the writes follow, and the transaction commits. Starting a task claims it (`queued → running`, `attempt_count + 1`) and inserts attempt *N = attempt_count* in the same transaction. No transaction spans task execution: a start commits before the work and a completion is a separate transaction.
- **WHY:** One lock order (task, then attempt) on every path means no deadlocks. Every writer of a task serialises on its row, so each decision sees the latest committed state. Deciding in Python keeps the retry and cancellation rules in one place (DEC-008, DEC-020), and the lock gives a precise reason for every rejection.
- **RUNNER-UP:** Single conditional `UPDATE … WHERE state = … AND attempt_count = …` statements — fewer round trips, but the policy would be restated in SQL and a rejection would need a second query to explain itself. SERIALIZABLE with retries — no explicit locks, but retry loops on every write path.
- **TRADE-OFF:** Three to five statements per operation (a completion adds the retry-time read of DEC-053 and the usage closure of DEC-058), and a hot task row serialises its writers (by design). A Python check that a queued task has attempts left was removed as unreachable: `tasks_queued_has_attempts_left` already guarantees it (DEC-026).
- **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py`: 20 rounds × 8 concurrent starters yield exactly one attempt each; a gated test observes the losing starter blocked on the winner's row lock (`pg_locks`), then refused; a failing attempt insert (constraint, or a test-only trigger reporting the task already `running` at generation 1) rolls the claim back; 5 rounds × 6 competing workers start exactly `max_attempts` attempts, numbered 1..5. A whole-schema invariant check runs at the end of every test and after every race round. Locally, removing the task lock on start failed 8 tests; removing it on completion failed 5; removing the start or completion transaction failed 10 and 13; disabling the transition-table check on start failed 19. Removing only the attempt lock failed none: the task lock alone serialises every writer, so the attempt lock is defence in depth.

### DEC-029 — Completion is accepted only from the current generation; rejections are typed
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented
- **WHAT:** `complete_attempt(conn, attempt_id, *, generation=N, outcome=…, retry_policy, usage_closure=None)` takes effect only when the attempt's number is *N*, the task's `attempt_count` is *N*, and the attempt is still running. Otherwise it changes nothing and raises:
  - `StaleAttemptError` — the generation does not match, or a newer one exists.
  - `AttemptFinishedError` — the current attempt already ended (a duplicate completion).
  - `NotFoundError` — an unknown task or attempt.

  Starting a non-queued task raises `TaskNotStartableError`. A running attempt whose task is not running is reported as an invariant violation (`RuntimeError`) instead of being completed. Outcomes are typed records — `AttemptSucceeded(result)`, `AttemptFailed(kind, error_type, error_message)` and `AttemptCancelled()` — and `AttemptFailed.from_exception` classifies handler exceptions with `classify_failure`.
- **WHY:** The attempt ID alone cannot tell a current worker from one whose task was requeued and restarted. The generation can, and requiring both catches mismatched pairs. Typed errors let a worker distinguish "my result was recorded earlier" from "someone else owns this task now", and neither is a task failure (DEC-009). Completion never classifies exceptions itself, and database errors during completion roll back and propagate, so infrastructure failures are never recorded as permanent handler failures.
- **RUNNER-UP:** Accepting an identical duplicate completion as an idempotent success — friendlier to retried calls, but it needs a result comparison and hides which call took effect. Result enums instead of exceptions — they would force a check at every call site of an operation that normally succeeds.
- **TRADE-OFF:** A worker that loses a completion's response (for example, its connection drops after the commit) learns the recorded outcome only through `AttemptFinishedError` or by reading the attempt. Nothing in this step revokes ownership of a running attempt; that is the later lease recovery.
- **Lease and recovery note:** a completion of a revoked attempt raises `AttemptRevokedError`, a subclass of `AttemptFinishedError`, so a worker can tell "my outcome was recorded" from "I lost ownership and nothing I report counts" (DEC-037).
- **VERIFICATION:** a generation older than the current one, its attempt already failed and requeued, cannot record success, permanent or transient failure, and the newer attempt's result stands. A test stand-in for future lease recovery revokes a running attempt; the slow worker then cannot overwrite the new result. Mismatched generations (0, older, future) are rejected. Five kinds of duplicate completion change nothing, including after a requeue. 20 rounds × 4 concurrent completions of one attempt record exactly one result. A completion that fails at the attempt update or at the task update leaves both untouched and can be retried. A connection terminated mid-completion records nothing. Locally, removing the generation check failed 5 tests, removing the finished check failed 10, and ignoring a pending cancellation request failed 4.

### DEC-030 — Result contract: SQL NULL is "no result"; JSON null is a valid result
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented; no schema change
- **WHAT:** A succeeded attempt's result is stored as `jsonb` on its task, always as a JSON value: a result of `None` is stored as the jsonb value `'null'`, never SQL `NULL`. `Task.result` is meaningful only when `Task.state` is `SUCCEEDED`; every other task has SQL `NULL`, enforced by `tasks_result_iff_succeeded`.
- **WHY:** The schema's existing constraint already makes the task's state the unambiguous signal of whether a result exists, so a result that is JSON null needs no new column and is not silently confused with "no result".
- **RUNNER-UP:** Forbid JSON null results — simpler to read, but it rejects a legitimate JSON value. Wrap results (`{"value": …}`) or add a `has_result` column — unambiguous on their own, but either changes the stored format or needs a migration.
- **TRADE-OFF:** Readers must check `state` before interpreting `result`; `None` alone does not mean "no result".
- **VERIFICATION:** `test_a_success_stores_its_result_and_json_null_is_a_result` checks `result IS NULL` and `jsonb_typeof` for six result kinds, including `null`, `false` and `0`; storing JSON null as SQL NULL fails it.

### DEC-031 — Job status rules
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented (`derive_job_status`, `get_job_progress`)
- **WHAT:** From the number of tasks in each state:
  - `queued` — every task is queued, including tasks requeued for a retry.
  - `running` — some task is queued or running, but not every task is queued.
  - `succeeded` — every task succeeded.
  - `failed` — every task finished and at least one failed.
  - `cancelled` — every task finished, none failed and at least one was cancelled.

  `get_job_progress` returns the status and the per-state counts from one statement.
- **WHY:** No stored job state to drift (DEC-022). A failure needs attention more than a requested cancellation does, so it dominates. A job is finished exactly when its status is a terminal one.
- **RUNNER-UP:** Partial statuses such as "completed with failures" — more nuanced, but the counts already carry that detail.
- **TRADE-OFF:** A job whose only started tasks were all requeued reads as `queued`. A single failed task makes a large job `failed`; callers that need proportions use the counts. One aggregate query per read, not yet measured on large jobs.
- **VERIFICATION:** `tests/unit/test_jobs.py` checks every combination of 0–2 tasks in each of the five states (242 cases) against an independent restatement of the rules. An integration test walks a four-task job through queued, running, requeued, mixed and failed states.

### DEC-032 — Cancellation requests report what they did and never touch finished tasks
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · implemented (`request_cancellation`)
- **WHAT:** `request_cancellation` returns one of four outcomes:
  - `cancelled` — the task was queued; it now has `cancel_requested_at = finished_at`.
  - `requested` — the task is running and keeps running (DEC-020).
  - `already_requested` — the earlier request's timestamp is kept.
  - `already_finished` — the task is terminal and nothing is written.
- **WHY:** All four are normal outcomes, not errors. Recording a request on a finished task would falsely suggest it was pending while the task ran.
- **RUNNER-UP:** Exceptions for repeated or late requests — they would turn a safe, idempotent call into an error path.
- **TRADE-OFF:** There is no job-level cancellation yet. The grace period after which a running attempt that ignores the request loses ownership now exists (DEC-040): `request_cancellation` takes a required `grace`.
- **VERIFICATION:** Cancelling a queued task makes it unstartable and keeps any earlier attempt history. A pending request turns a transient failure into cancellation, while a success, a permanent failure or an exhausted retry stands. Repeated requests keep the first timestamp, and finished tasks are unchanged. Deterministic gated tests force both commit orders for success and for transient failure, and 30-round races check the invariants.

## PostgreSQL task discovery

### DEC-033 — PostgreSQL-native queue discovery using `SKIP LOCKED`
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-25) · implemented (`claim_next_attempt`)
- **WHAT:** PostgreSQL is the task queue; there is no separate broker. `claim_next_attempt(conn, worker_id)` locks the first claimable task with `SELECT … WHERE state = ANY(<sources of running>) ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`, then claims it in the same transaction through the private `_claim` step it shares with `start_attempt`: transition check, generation advance and attempt insert. It returns the new attempt, or `None` when nothing is claimable. The claimable states come from the transition table (`allowed_sources`), not a literal.
- **WHY:** Tasks already live in PostgreSQL (DEC-015), so claiming a row and starting its attempt is one transaction with no second system to keep consistent. `SKIP LOCKED` lets concurrent claimants take distinct tasks without waiting for each other or for a cancellation in progress. Sharing `_claim` keeps one authoritative implementation of the claim, guarded by the existing fencing trigger.
- **RUNNER-UP:** An external broker (Redis, RabbitMQ) delivering task IDs — it pushes work and scales independently, but it is a second system that must agree with the authoritative rows (DEC-015's runner-up). Within PostgreSQL, a single `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED) RETURNING` saves a round trip, but it would restate the claim SQL and bypass the domain transition check.
- **TRADE-OFF:**
  - The order by `created_at`, then `id`, is a preference, not strict FIFO: locked tasks are skipped, and the tasks of one job share `created_at`.
  - A requeued task keeps its `created_at`, so it is claimed again straight away. There is no backoff, and a transient failure can use up its retries back to back once a worker loop exists.
  - Each claim scans and sorts the queued tasks until a partial index is added (planned with the lease migration).
  - There is no lease, so a claimed task whose worker dies stays `running` (addressed by attempt leases).
  - `None` means "nothing claimable right now", not "the queue is empty".
- **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py`, section 16 (20 tests):
  - Claims and empty queues; running and finished tasks are never claimed; one claim per generation; older tasks first.
  - 20 rounds × 8 concurrent claimants over 8 tasks yield 8 distinct attempts; 20 rounds × 8 claimants over 3 tasks yield exactly 3.
  - Gated tests hold a claimant mid-claim and show another claimant taking the next task, or returning `None`, under `lock_timeout = 200ms`, so no wait occurred.
  - A task being cancelled is skipped and never claimed; a claim before a cancellation becomes a recorded request; 30-round claim-versus-cancel races leave no invalid claim.
  - A failing attempt insert rolls the claim back.
  - 6 competing claimants never exceed `max_attempts` across 10 tasks.

  Locally, replacing `SKIP LOCKED` with a waiting `FOR UPDATE` failed 3 tests, removing the row lock failed 7 (the fencing trigger rejected the colliding claims), claiming from any state failed 11, removing the ordering failed 2, and removing the owned transaction failed 11.
- **Lease and recovery note:** the lease migration (0002, DEC-039) does not add the claim-ordering index: it is a performance change and stays deferred until measured. Claims now take a lease policy (DEC-034), and a revoked task is requeued with its original `created_at`, so it too is claimed again without backoff.
- **Job-kind filter note:** Accepted with DEC-033 (owner, 2026-11-07; was Proposed) · owner-approved scope (2026-10-26) · implemented; merged in PR #8 (`46acdd9`), with both CI jobs passing for the pull request (run 40397331066) and for `main` (run 40397973812). The worker contracts moved the kind check, unchanged, to `orqestra.domain.kinds.check_job_kinds`, shared with the handler registry (DEC-043).
  - **WHAT:** `claim_next_attempt(conn, worker_id, *, kinds, policy)` takes a required, non-empty collection of supported job kinds. Discovery adds `AND EXISTS (SELECT FROM jobs WHERE jobs.id = tasks.job_id AND jobs.kind = ANY(<kinds>))` to the same `FOR UPDATE SKIP LOCKED` query, so the filter is part of the atomic claim; ordering, the task-before-attempt lock order, `_claim`, fencing and the owned transaction are unchanged. Kinds match exactly. A bare string or bytes, a non-collection, an empty collection, a non-string kind, a blank kind or a kind with a NUL character raises `TypeError` or `ValueError` before any statement is sent; duplicates are accepted and collapsed. "Blank" is the database's rule for job kinds, `jobs_kind_not_blank` (`btrim(kind) <> ''`, where `btrim` trims spaces only), checked in Python as `kind.strip(" ")`, so every kind a job can have can be claimed, including one containing a tab or a newline. There is no unfiltered claim.
  - **WHY:** A worker can only run the kinds it has handlers for. Without the filter it would claim a task it cannot run, and could only fail or release it: that uses an attempt of the task's budget, records an outcome that says nothing about the task, and, because a requeued task keeps its place, can hold up the work behind it. Filtering inside the locking query means a claimant never locks, counts or reorders a task it cannot run.
  - **RUNNER-UP:** Denormalising the kind onto `tasks` (a `tasks.kind` column, possibly indexed with the claim order) avoids the per-candidate join, but needs a migration and a copy of an immutable job fact; deferred until claim cost is measured. Also rejected: one catch-all handler per worker, or filtering after the claim (which uses budget), or an optional `kinds` defaulting to "any" (a silent, unsafe fallback).
  - **TRADE-OFF:**
    - Breaking API change: every caller must name its kinds.
    - The SQL guarantees only which task is locked: the first queued task of a supported kind, by `created_at` then `id`, not locked elsewhere. How the planner evaluates the `EXISTS` (for example per candidate or as a semi-join), and what a backlog of unsupported tasks costs, has not been measured; it should be judged from reproducible query plans and a benchmark when claim cost matters, before any index or denormalisation.
    - The job row is read without a lock and is not re-checked once the task is locked. That is correct only while a job's kind never changes. No application code path updates `jobs.kind`, but the database does not enforce it: there is no trigger or permission against it (adding one needs a migration, out of scope here).
    - A task whose kind no worker supports waits forever; detecting that is left to monitoring, which does not exist yet.
    - Within a claimant's kinds the order is still `created_at`, then `id`; there is no per-kind fairness or priority.
  - **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py`, section 22 (13 tests, 47 cases): only supported kinds are claimed; a failed filtered claim rolls back completely; a kind is refused as blank exactly when PostgreSQL's own `btrim` rule refuses it (8 samples: only spaces are refused; tabs, newlines, CRLF and a no-break space are kinds); tabs and newlines match exactly and are never trimmed or confused with spaces; NUL characters are refused before any statement; a claim takes no lock on the job row, in either direction, under `lock_timeout` and `NOWAIT`; an eligible task is claimed past earlier candidates held mid-claim by two other claimants and an unsupported task; an older unsupported task with one attempt left never holds up newer supported work and keeps its state, generation and budget, then is claimed at generation 1 by a claimant supporting it; several kinds keep the age order; exact matching (case, whitespace, wildcard, prefix, literal `%`); 18 invalid inputs refused before any database work, also inside a caller's transaction, leaving every row unchanged; repeated kinds accepted; a gated claim is skipped without waiting by claimants of the same and another kind; 20 rounds of concurrent claimants of disjoint kinds each take only their own kind, and 20 rounds of overlapping kinds never claim a task twice and, as holds for every schedule, claim every task. The existing section 16 and task-discovery, lease and grace tests pass with `kinds` added.

    Locally, each of these mutations failed at least one section-22 test: blank checked with `strip()` (8 tests), no NUL check (3), the `EXISTS` taking `FOR SHARE` on `jobs` (1), a `FOR KEY SHARE` lock on the job after discovery (1), a waiting `FOR UPDATE` (3), and no kind filter (28).
- **Retry scheduling note (retry eligibility, DEC-053):** discovery and the claim's `UPDATE` now also require `available_at <= statement_timestamp()`, so a requeued task is not claimed again straight away. The ordering (`created_at`, then `id`), `SKIP LOCKED`, the kind filter and the lock order are unchanged, and there is still no claim index: delayed tasks are scanned with the rest.

## Attempt leases, execution deadlines and recovery

Entries DEC-034 to DEC-039 record the lease and recovery design the owner approved on 2026-10-26 after
an architecture audit, an independent review and a PostgreSQL locking investigation. The
owner accepted them on 2026-11-07. "Implemented" means
verified by the tests named, locally and in GitHub CI: they were merged in PR #6
(`a031666`), with both CI jobs passing for `main` (run 40260902718). These are
building blocks: there is no worker loop, nothing schedules recovery, and a revoked worker
is not stopped (left to the worker runtime).

### DEC-034 — Attempt leases with a non-renewable execution deadline, under an explicit policy
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented (`orqestra.domain.leases`, migration 0002)
- **WHAT:** Each attempt stores `lease_expires_at` and `deadline_at`, set at claim from one instant: the deadline is the claim instant plus `execution_timeout`, the lease the claim instant plus `lease`, capped at the deadline. A heartbeat sets the lease to `policy.lease` from now, capped at the deadline and never lower than before. The deadline never changes, enforced by the trigger `attempts_deadline_immutable`. An attempt may be revoked once the database time reaches `lease_expires_at`. The reason follows from the stored facts: a lease that reached its deadline cap means `deadline_exceeded`, any other expired lease `lease_expired`. `LeasePolicy(lease, execution_timeout)` is a required keyword argument of `start_attempt` and `claim_next_attempt`. Both values must be positive `timedelta`s within the representable range, `lease` cannot exceed `execution_timeout`, and a deadline beyond `datetime`'s range rolls the claim back with `ValueError`. There are no defaults.
- **WHY:** A heartbeat proves that a worker is alive, not that its work progresses: a worker whose handler hangs can renew its lease forever. A deadline that no heartbeat can extend bounds that case (R-EXEC-04). Capping the lease at the deadline leaves recovery with one predicate. Keeping leases on the attempt keeps, with each attempt's history, the facts its revocation was decided on. The policy is required because an optional "no lease" would bring back work that nobody recovers, and numeric defaults are an operational decision not yet made.
- **RUNNER-UP:** The lease on the task row: one place to look, but it loses per-attempt history. An independent deadline checked beside the lease: two predicates, and a precedence rule when both have expired. Defaults in `Settings`: convenient, but they invent operational values.
- **TRADE-OFF:**
  - A slow but healthy attempt is revoked at its deadline even while it heartbeats; choosing the timeout for each kind of work is open.
  - A worker that dies in its last lease window before the deadline is recorded as `deadline_exceeded`.
  - Every caller must pass a policy; the existing test call sites were updated.
  - Operational bounds (maximum lease and timeout, heartbeat interval, per-job-kind policies) are undecided and need owner approval before the worker runtime.
- **VERIFICATION:** `tests/unit/test_leases.py` (validation, expiry at the exact instant and one microsecond either side, the cap, never shortening, representability); `tests/integration/test_attempt_lifecycle.py` sections 17 and 18, including a lease kept alive by heartbeats until its deadline and then revoked as `deadline_exceeded`.
- **Cancellation grace note:** the revocation reason has a third case. In order: a lease at its deadline is `deadline_exceeded`; otherwise a lease that ended after a cancellation request made while it held is `cancellation_grace_expired`; otherwise `lease_expired`. A cancellation request is the one write besides a heartbeat that sets the lease, and it may shorten it (DEC-040).

### DEC-035 — One claim instant, and a database clock read after the locks
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented
- **WHAT:** The claim's `UPDATE tasks … SET updated_at = statement_timestamp() … RETURNING updated_at` is the claim instant. It becomes the attempt's `started_at` and the base of its lease and deadline. `renew_lease` and `revoke_next_expired` read `statement_timestamp()` in a separate statement after taking the row locks their decision depends on, and decide expiry in Python against the freshly locked rows. Other timestamps still use `now()`, except a requeued task's `available_at`, which DEC-053 takes from the clock read after the locks.
- **WHY:** `now()` is fixed when the transaction starts, before any lock wait. A claim that waited for a task row would get a shorter lease, possibly one that had already expired by its commit. The claim `UPDATE` runs after the task lock is held on both claim paths, so its statement timestamp leaves the wait out. One value keeps `started_at`, the lease, the deadline and the task's `updated_at` consistent. Reading the clock after the locks means a decision is never older than the rows it compares.
- **RUNNER-UP:** `clock_timestamp()` captured once after the lock: equally fresh, but evaluated on every call, so a later edit could give the columns different instants. `statement_timestamp()` of the locking statement: taken before that statement's own wait.
- **TRADE-OFF:** A claimant that stalls between its claim `UPDATE` and its commit still loses that time from its lease, which is right: it was not running the task. The claim's `updated_at` moves from transaction start to the claim statement. Expiry depends on one database clock: a step forward can expire leases early (fencing still holds), and a step back is absorbed because a lease never shrinks.
- **VERIFICATION:** `test_a_claim_that_waited_for_the_task_lock_still_gets_its_full_lease`, for `start_attempt` and `claim_next_attempt`, with and without server-side prepared statements. A holder keeps the claimant waiting (observed in `pg_locks`) and reads `clock_timestamp()` before releasing; the attempt must start after that. Locally, deriving the instant from `now()` fails it.
- **Cancellation grace note:** a cancellation request's instant (`cancel_requested_at`, and for a queued task `finished_at` and `updated_at`) is now `statement_timestamp()` of its task update, taken after its locks, because the revocation reason compares it with the lease (DEC-040).
- **Session time zone note:** the instants it reads are decoded in UTC inside the library's own transaction, so expiry is decided on instants, not wall times, whatever the session TimeZone (DEC-067).

### DEC-036 — The heartbeat renews under the attempt lock alone and refuses expired leases
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented (`renew_lease`)
- **WHAT:** `renew_lease(conn, attempt_id, *, generation, worker_id, policy)` locks only the attempt row. It checks the generation, the worker, that the attempt is running and that its lease has not expired, then writes the renewed lease. It rejects with `NotFoundError`, `StaleAttemptError`, `WorkerMismatchError`, `AttemptRevokedError`, `AttemptFinishedError` or `LeaseExpiredError` (with the reason), changing nothing. To describe a stale rejection it reads the task's generation without a lock.
- **WHY:** Heartbeats are the most frequent write. A transaction that holds a single row lock and never waits for another while holding it cannot take part in a lock cycle with the task-first operations. The heartbeat does not update `task_id`, so the foreign-key check takes no lock on the task, and the deadline trigger does not read `tasks`. Refusing to renew an expired lease makes eligibility monotonic: once recovery may revoke an attempt, nothing makes it ineligible again, and the worker learns at once that it must stop.
- **RUNNER-UP:** Lock the task, then the attempt, like the other writes: also free of deadlocks, but it contends with every writer of the task. Accept renewals until a revocation commits (proposed during the audit): saves a late heartbeat, but recovery eligibility could flip back.
- **TRADE-OFF:**
  - It is an exception to the lock order of DEC-021 and DEC-028 that later changes must preserve: no trigger on `attempts` may lock `tasks`, and `task_id` must stay unchanged.
  - A worker whose heartbeat is later than its lease can no longer renew, even if recovery has not run; its completion is still accepted until a revocation commits (DEC-037).
- **VERIFICATION:**
  - `test_a_heartbeat_does_not_lock_the_task`: a heartbeat succeeds under `lock_timeout` while another transaction holds the task row; a completion, as a control, cannot.
  - Rejection tests; both forced orders of heartbeat against recovery.
  - A 40-round unforced race of heartbeat, completion, recovery and cancellation that asserts no deadlock, exactly one of completion or recovery per round, and the invariants. Which one wins is not asserted; `test_completion_and_recovery_in_either_order` forces both orders.
  - Locally, adding a task lock after the attempt lock made that race fail with `DeadlockDetected`; removing each check failed its test.
- **Cancellation grace note:** the heartbeat also reads the task's `cancel_requested_at` without a lock, in the statement that reads the clock after the attempt lock, and refuses renewal with `CancellationRequestedError` once a request has committed. It still takes no task lock (DEC-040).
- **Session time zone note:** refusing an expired lease now also holds across a daylight-saving change in the session TimeZone, because the heartbeat compares UTC instants (DEC-067).

### DEC-037 — Recovery revokes one expired attempt per transaction; ownership ends at its commit
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented (`revoke_next_expired`)
- **WHAT:** `revoke_next_expired(conn, *, retry_policy, usage_capture=None)` works in five steps:
  1. Lock, with `FOR UPDATE OF t SKIP LOCKED`, the task of the most overdue running attempt whose lease has expired.
  2. Lock that attempt.
  3. Re-check, against the locked rows and a fresh clock reading, that the attempt is still the task's running generation and that its lease has expired.
  4. Record the attempt as `revoked`, with its reason and `finished_at` at that instant, and move the task as `next_task_state` decides. A requeue also sets the task's `available_at` (DEC-053), and a given `usage_capture` closes the attempt's usage capture (DEC-058).
  5. Commit.

  It returns `AttemptRevocation` or `None`. Afterwards `complete_attempt` raises `AttemptRevokedError` (a subclass of `AttemptFinishedError`), or `StaleAttemptError` once a newer generation exists; `renew_lease` raises `AttemptRevokedError`.
- **WHY:**
  - Recovery locks task-then-attempt like every other writer except the heartbeat.
  - `SKIP LOCKED` lets concurrent recoverers, and tasks that are busy, proceed without waiting.
  - Re-checking after the locks closes the gap between discovery and lock: a heartbeat or a completion that commits in between wins.
  - Expiry only makes an attempt eligible; the committed revocation is the single authority boundary. A completion that commits first therefore counts (R-EXEC-05), and completion needs no clock comparison.
  - One attempt per short transaction keeps locks brief; sweeping repeatedly belongs to the worker runtime.
- **RUNNER-UP:** Revoking a batch in one transaction: fewer round trips, but many task locks held at once. Folding recovery into `claim_next_attempt`: one call for workers, but two concerns in one, and slower claims.
- **TRADE-OFF:**
  - `None` means "nothing revoked by this call", including a candidate renewed or locked meanwhile, not "nothing expired".
  - The worker's sweep schedules recovery: up to `recovery_batch_limit` calls every `recovery_interval`, idle or busy (DEC-045).
  - Revocation does not stop the worker or an external provider call and does not un-bill it (R-COST-01, the worker runtime and later).
  - After a database outage longer than the lease, every live attempt is revocable at once (R-OPS-01).
- **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py` sections 19 and 20:
  - Exactly-once revocation, over 10 rounds of 6 concurrent recoverers.
  - Skipping a locked task without waiting.
  - A completion before revocation counts; after it, the old worker is fenced before and after a new claim.
  - Both forced orders of completion against recovery and of heartbeat against recovery.
  - A poisoned write, and a terminated connection, each roll back both rows.
  - An invariant that every revoked attempt's lease had expired when it was revoked.
  - Locally, mutants that skip the re-check, drop `SKIP LOCKED`, stop locking the task, or report a revoked completion as a plain finished one each fail tests.
- **Recovery correction note (DEC-050):** a recovery that waits for the attempt's row lock now waits at most `lock_timeout` and then fails with `55P03`; the sweep ends and is retried at the next interval. The lock order is unchanged.

### DEC-038 — Revoked attempts: a distinct state; the task follows the transient-failure rule
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented
- **WHAT:** `AttemptState.REVOKED` is terminal and carries `revoked_reason`; it has no failure kind or error details. For a revoked attempt, `next_task_state` returns `queued` while attempts remain (`cancelled` if a request is pending) and `failed` once none remain, whether or not a request is pending.
- **WHY:**
  - DEC-009: lost ownership is a recovery event, not a handler failure, so the attempt records what happened.
  - DEC-019: every started attempt uses budget, so exhaustion fails the task.
  - DEC-020: a pending request cancels only an outcome that would requeue. An exhausted revocation would not requeue, and the lease design's reasons (an expired lease, an exceeded deadline) are failures not caused by the request, so the task fails truthfully and its job reads `failed` (DEC-031).
- **RUNNER-UP:** Exhausted with a pending request ends `cancelled`: matches the user's intent, but hides a task that hung or lost every attempt. Recording a revocation as a failed attempt with failure kind `transient` (what the replaced test helper did): no new state, but it contradicts DEC-009.
- **TRADE-OFF:**
  - A user who requested cancellation can see `failed`; the attempt's `revoked_reason` explains why.
  - A deterministic hang is retried up to `max_attempts` times the execution timeout, with no backoff (DEC-033).
  - The cancellation-grace revocation of the grace-period design is caused by the request and will need its own rule.
- **VERIFICATION:** `tests/unit/test_policy.py` decision table, plus a check that a revocation moves the task exactly as a transient failure does for every count and request; the integration outcome table (six cases, including a single-attempt task with a pending request); invariant rules on attempt history.
- **Cancellation grace note:** the anticipated rule now exists: a `cancellation_grace_expired` revocation cancels the task whatever budget is left, and requires a pending request. `next_task_state` takes the revocation reason for every revoked outcome (DEC-040).
- **Retry scheduling note (DEC-053):** a `lease_expired` or `deadline_exceeded` revocation that requeues now also schedules a retry delay, exactly as a transient failure does, so a deterministic hang is no longer retried back to back. `next_task_state` is unchanged.

### DEC-039 — Migration 0002: additive schema and a quiescent upgrade with `NOWAIT` locks
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented (`0002_attempt_leases.sql`)
- **WHAT:** Migration 0002 runs in order:
  1. Require READ COMMITTED.
  2. `LOCK TABLE tasks IN EXCLUSIVE MODE NOWAIT`, then `LOCK TABLE attempts IN ACCESS EXCLUSIVE MODE NOWAIT`. If either fails, raise `lock_not_available` (`55P03`) with an operator message.
  3. Refuse (`55000`) while any task or attempt is running.
  4. Add the nullable columns `lease_expires_at`, `deadline_at` and `revoked_reason`, and replace `attempts_state_valid`.
  5. Add the named constraints `attempts_lease_iff_deadline`, `attempts_lease_required`, `attempts_lease_after_start`, `attempts_lease_within_deadline`, `attempts_revoked_reason_valid`, `attempts_revoked_reason_iff_revoked` and `attempts_revoked_reason_matches_lease`, and the trigger `attempts_deadline_immutable`.

  It adds no index. The operational precondition is: stop workers and claimers → let running attempts finish → migrate → deploy.
- **WHY:**
  - A probe of a blocking design (the same two locks without `NOWAIT`) showed it froze unrelated claims while it waited, and could deadlock with a session that read `attempts` and then locked a task.
  - With `NOWAIT` the migration never waits while holding a lock, so it can do neither, and a lock it cannot take means the database is not quiescent.
  - Refusing running work avoids silently reassigning attempts whose workers may still be alive.
  - `attempts_lease_required` makes claims from pre-0002 code fail closed afterwards.
  - The existing partial index on running attempts already serves recovery's lookup.
- **RUNNER-UP:** Blocking locks with a `lock_timeout`: waits out brief activity, but still freezes writers up to the timeout and needs a number. Expiring legacy running attempts, or giving them a grace lease: no drain needed, but it reassigns, or guesses about, work that may be live.
- **TRADE-OFF:**
  - Any session using the tables, even an idle reader, makes the upgrade fail; the operator retries.
  - Attempts of dead legacy workers must be resolved by hand before upgrading.
  - Old and new code cannot run side by side: old code cannot read revoked attempts.
  - The claim-ordering index stays deferred until measured.
- **VERIFICATION:**
  - `tests/integration/test_migration_0002.py`: upgrade over terminal history; refusal while an attempt or a task is running; prompt `55P03` with no effect while a claim, a completion or an idle reader holds the tables; the READ COMMITTED requirement; a live legacy worker's next claim rejected after the upgrade, with nothing changed; a 10-round race against legacy workers that asserts only outcomes valid under every schedule (applied or refused, no deadlock, no running attempt without a lease, the invariants).
  - A test for every new constraint and for the trigger, in `test_schema_constraints.py`; the domain/schema drift test now includes revocation reasons.
  - Locally, removing `NOWAIT` made a test fail after 10 s instead of refusing, and removing the refusal, the isolation check, the trigger or `attempts_lease_required` each failed tests.
- **Cancellation grace note:** migration 0003 follows the same locking rule, `NOWAIT` before any change, but needs neither the running-work refusal nor the isolation check, because it changes no data-dependent state (DEC-041).

## Bounded cancellation grace period

Entries DEC-040 and DEC-041 record the cancellation grace design the owner approved after an
architecture review and an independent review. The owner accepted them on
2026-11-07; "implemented" means verified by the tests named,
locally and in GitHub CI: they were merged in PR #7 (`d83afeb`), with both CI jobs
passing for the pull request (run 40271111779) and for `main` after the merge (run
40271850302). The worker's response to a cancellation, handler
interruption and scheduled recovery remain for the worker runtime.

### DEC-040 — A cancellation request sets the running attempt's lease to a grace period
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented
- **WHAT:** `request_cancellation(conn, task_id, *, grace)` requires a positive, representable `timedelta`, checked before any write, with no default. For a running task, in one transaction, it:
  1. locks the task, then its running attempt;
  2. records `cancel_requested_at` at one instant taken after those locks;
  3. if the attempt's lease still holds, sets it to `least(request + grace, deadline_at)`, which may shorten or lengthen it; an expired lease is left alone, never revived.

  The generation, worker, deadline and state of the attempt do not change. A repeated request changes nothing. From the request's commit on, `renew_lease` raises `CancellationRequestedError` (with the lease end). Recovery is unchanged except for the reason it records, decided in this order:
  - a lease at its deadline: `deadline_exceeded`;
  - else, if the request came strictly before the lease ended: `cancellation_grace_expired`;
  - else `lease_expired`.

  A grace expiry cancels the task whatever budget is left; the other reasons keep DEC-038's rule.
- **WHY:**
  - A grace period that is just the remaining lease was rejected in review: with `lease == execution_timeout` it lasts until the deadline, where the task would end `failed`, and a request near the end of a lease leaves almost no time to wind down.
  - Setting the lease bounds cancellation to the grace period independently of the lease and timeout, needs no new column (the grace lives in the lease) and no new recovery query.
  - Locking the attempt makes the race with heartbeats strict. A heartbeat that commits first is reset by the request, which reads the fresh lease. A request that commits first is seen by every later heartbeat.
  - The reason follows from stored facts, and the deadline still wins, so a hang that reached its limit stays visible as a failure.
- **RUNNER-UP:** The grace is the remaining lease, with no new parameter. A grace duration stored per attempt at claim (a policy field and a column). A separate grace deadline checked by recovery beside the lease.
- **WHY NOT:** The first fails the reviewed case on latency and outcome. The second needs a migration column, a fallback for attempts already running, and a policy field. The third needs a column and a second recovery predicate, for the same effect.
- **TRADE-OFF:**
  - A lease is no longer an absolute promise: a request can reset it, and the worker hears only at its next heartbeat, so the worker runtime must keep the heartbeat interval shorter than any grace in use.
  - If the worker is already dead, a grace longer than its remaining lease delays recovery.
  - A worker that died in the lease window before a request is recorded as a grace expiry, so the task is cancelled.
  - When the deadline falls inside the grace, a task with no attempts left ends `failed`.
  - `request_cancellation` gained a required argument.
- **VERIFICATION:**
  - Unit: `tests/unit/test_leases.py` (the reason at, before and after expiry; deadline precedence; shortening, lengthening, the deadline cap, no revival, overflow; grace validation) and `tests/unit/test_policy.py` (the grace outcome for every budget; required reasons).
  - Integration: `tests/integration/test_attempt_lifecycle.py` section 21:
    - the lease rules;
    - refused heartbeats;
    - the reviewed case, cancelled with one attempt allowed;
    - the deadline inside the grace;
    - a request after expiry;
    - completion within the grace counted, and rejected after revocation;
    - a request timed after a lock wait;
    - forced orders of heartbeat against request, and of request against recovery (request against completion in section 11);
    - failed writes and a lost connection rolling back;
    - a corrupted running task refused;
    - a 40-round stress test (unforced, seeded start windows) asserting no deadlock, exactly one of completion or recovery per round, and the outcome each implies; which one wins is not asserted, because the forced-order and sequential tests above force each.
  - Locally, 17 mutants, including a heartbeat that ignores the request, a request that does not lock the attempt or does not reset the lease, a request that revives an expired lease, swapped precedence, a budget-dependent outcome, a request timed by `now()`, and a heartbeat that locks the task, each fail tests.

### DEC-041 — Migration 0003 widens the revocation reasons; versions switch together
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented (`0003_cancellation_grace.sql`)
- **WHAT:** 0003 takes `LOCK TABLE attempts IN ACCESS EXCLUSIVE MODE NOWAIT` (`55P03` with an operator message if any session uses it), then replaces `attempts_revoked_reason_valid` to admit `cancellation_grace_expired`. It keeps `attempts_revoked_reason_matches_lease`, which already admits the new reason exactly when the lease ended before the deadline, and every other 0002 constraint and trigger. It adds no column or index, takes no `tasks` lock, has no running-work refusal and no isolation check. Rollout: stop all application processes → apply 0003 → start only grace-period code; draining first is preferable.
- **WHY:**
  - Every stored row satisfies the widened rule and running attempts are unaffected, so the schema change needs no quiescence.
  - The real hazard is mixed application versions after the migration: older code cannot read the new reason, and its heartbeats ignore cancellation.
  - A running-work refusal would not prevent that, so the rollout order is documented instead.
  - `NOWAIT` keeps the DEC-039 rule: never wait while holding a lock.
- **RUNNER-UP:** The 0002 contract again: lock both tables, require READ COMMITTED, refuse running work.
- **WHY NOT:** It adds checks that protect no data in this migration and still does not stop older processes from running afterwards.
- **TRADE-OFF:**
  - The database does not enforce the version switch.
  - Grace-period code on a 0002 schema fails closed: its grace revocations are rejected and roll back until 0003 is applied.
- **VERIFICATION:** `tests/integration/test_migration_0003.py`:
  - upgrade from 0002 keeps revoked history and running attempts, whose heartbeats then behave as before;
  - the new reason is rejected by 0002 (the revocation rolls back both rows) and accepted after;
  - an idle reader or a held attempt row makes it fail at once with no history row and the old reasons;
  - a held task row does not;
  - a 10-round race against live claims, heartbeats, completions and requests never deadlocks, and applies while work is still being claimed.

  The schema tests cover the new reason against `matches_lease`, and the drift test covers the reason values. Locally, removing the new value, removing `NOWAIT` (the test fails after 10 s instead of hanging), or also locking `tasks` each fail tests.

## Worker contracts

Entries DEC-042 to DEC-044 record the worker contracts agreed after the worker design
reconciliation (single-slot process, main-thread supervisor, handler thread, and the
C1–C5 reliability contracts). The owner accepted them on
2026-11-07; "implemented" means verified by the tests named, locally and in GitHub
CI: they were merged in PR #9 (`823530a`), with both CI jobs passing for the pull
request (run 40408021385) and for `main` (run 40408731149). These entries are the
contracts; the first runtime that uses them is the first worker runtime (DEC-045, DEC-046).

### DEC-042 — Handlers are synchronous functions; one pure boundary maps what they did to an outcome
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented locally (`orqestra.worker.handlers`)
- **WHAT:**
  - A handler is `Callable[[TaskContext, JsonValue], JsonValue]`. `TaskContext` is frozen and holds the task and attempt IDs, the attempt number, the job kind and a `CancellationSignal`. It has no database connection.
  - `CancellationSignal(event)` is a read-only view of a `threading.Event` the worker owns and sets once it has observed a committed cancellation request. It offers `is_set()` and `wait(timeout)` only; its event cannot be set, cleared, replaced or deleted through it, and `TaskContext` refuses anything but such a view, including the event itself.
  - `run_handler(handler, context, payload)` returns exactly one `HandlerOutcome` and never raises for the handler:
    - a valid JSON result is `AttemptSucceeded` holding a checked copy, JSON `null` included (DEC-030): new lists and dicts with the same keys, order and immutable scalar values, so it is equal to the result as JSON but is not the handler's object, and changes to that object afterwards cannot reach the outcome;
    - an invalid result is `AttemptFailed`, unclassified, with `error_type` `orqestra.worker.handlers.InvalidHandlerResult`;
    - an `Exception` goes through `AttemptFailed.from_exception` unchanged, except that `HandlerCancelled` is `AttemptCancelled` only when the cancellation signal is set;
    - a `BaseException` that is not an `Exception` (`SystemExit`, `KeyboardInterrupt`, `GeneratorExit`, `asyncio.CancelledError`) is a `ControlPlaneSignal`, for which the worker must record no outcome.
  - A valid result uses only `None`, `bool`, `int`, finite `float`, `str`, `list` and `dict` with `str` keys, as exact types, with no NUL character or unpaired surrogate in any string, no cycle, nesting within Python's recursion limit, and must encode with `json.dumps(allow_nan=False)`. Each container's entries are copied in one step, so another thread still changing it cannot break the copy. A returned coroutine or generator is reported and never run. An unstarted native coroutine is closed, which runs none of its code (it has no frame yet) and prevents the "never awaited" warning; a started coroutine, a generator or any other awaitable is left untouched, because closing it would run handler code.
  - Failure text (`error_type`, `error_message`) is stored with NUL written as `\u0000` and unpaired surrogates as backslash escapes, the same way every time, so the stored text is exactly what was sent.
- **WHY:**
  - Every value PostgreSQL would refuse in a result or failure text is caught or escaped before anything is written, so a valid outcome is never lost to a predictable database error, and no database error is ever turned into a handler failure (DEC-009, DEC-029).
  - Exact types and a copy mean the stored result is the JSON value the handler returned, fixed at the moment it was checked, which the worker will rely on to reconcile an uncertain completion. Without the copy, a handler (or a thread it started) could change its object after the check, so that an unchecked or different value would be written.
  - A handler holding the worker's event could set it and turn its own `HandlerCancelled` into a cancelled outcome without any request. The database would still refuse to record it (`next_task_state` requires a committed request), but the worker would be driven onto its defect path; the read-only view removes that path through the handler's interface.
  - Control-plane signals stay unclassified, as DEC-009 and `classify_failure` require; the existing domain policy is unchanged.
  - `HandlerCancelled` without an observed request would make `next_task_state` raise; mapping it to an ordinary failure keeps the outcome valid.
- **RUNNER-UP:**
  - Async handlers, or a handler class with set-up and tear-down: more surface than one synchronous function needs, and async conflicts with the synchronous psycopg choice (DEC-016).
  - Accepting anything `json.dumps` accepts: friendlier, but tuples, `str` enums and non-string keys would be silently converted, and `NaN` would be written as a value PostgreSQL rejects.
  - Recording control-plane signals as an unclassified failure: rejected in review, because it contradicts DEC-009.
  - Passing the `threading.Event` typed as a read-only protocol (the first implementation): rejected in review, because the event's `set()` is still callable at run time.
  - Keeping the handler's own object as the result: rejected in review, because it can change after validation.
  - Freezing results into tuples and read-only mappings: no copy needed later, but the stored types would no longer be the handler's `list` and `dict`.
- **TRADE-OFF:**
  - Stricter than `jsonb`: handlers must convert tuples, enums and dict subclasses, and nesting beyond about 1,000 levels is refused.
  - There is no size limit on results or failure messages yet. Copying a result costs time and memory proportional to it, and a container shared in several places is copied once per place (as JSON storage would write it).
  - The read-only view guards the intended interface only; deliberate introspection can still reach the event, so the database remains the authority for cancelled outcomes.
  - A handler that raises a control-plane signal records nothing; once the worker exists, its attempt is recovered by lease expiry and uses retry budget.
  - The boundary cannot interrupt a handler; interruption and fencing belong to the worker process.
- **VERIFICATION:** `tests/unit/test_worker_handlers.py`:
  - 16 valid results (JSON `null`, `false`, `0`, big integers, `-0.0`, nested, shared references) succeed as an equal copy of exactly the same types: every list and dict new, every scalar the same object, the sign of `-0.0` kept;
  - changing the original afterwards (appending NaN, setting infinity and NUL, adding a tuple under a non-string key, deleting a key) leaves the outcome equal to the value checked; a shared list is copied once per place;
  - 29 invalid results (NaN and infinities, nested; NUL and surrogates in values and keys; tuple, set, bytes, `Decimal`, date, UUID, `object`, `str` enum, dict subclass; non-string keys; cyclic lists and dicts; 5,000 levels of nesting; a 5,000-digit integer) are unclassified `InvalidHandlerResult` failures with a message naming the path;
  - an unstarted coroutine is reported, closed without running its body or `finally`, and leaves no "never awaited" warning (a control test shows the warning without the boundary); a started coroutine, an unstarted and a suspended generator, and a custom awaitable with its own `close()` are reported and left untouched;
  - the cancellation signal reports and waits, has no `set` or `clear`, refuses attribute assignment and deletion, and views only a `threading.Event`; a context refuses the raw event, `None` and other objects; a handler calling `set()` on its signal fails with `AttributeError` (an unclassified failure, never cancelled) and the worker's event stays unset; an event set by another thread while the handler waits on its signal is observed and cancels;
  - transient, permanent, unclassified and message-less exceptions match `from_exception`, with and without an observed request;
  - `HandlerCancelled` is cancelled only with an observed request;
  - five control-plane signals, with and without a request, are `ControlPlaneSignal`, and `from_exception` still refuses them;
  - NUL and surrogate escaping in messages and in a deliberately odd `error_type`;
  - `run_handler` passes context and payload, contains `SystemExit` and `KeyboardInterrupt`, and covers cooperative, ignored and mid-run cancellation.

  Locally, these mutants each failed tests: cancelling without an observed request (1), classifying control-plane signals (12), catching only `Exception` (1), accepting NaN (6), NUL (3), surrogates (2), tuples (2), non-string keys (4) or cycles (2), registering coroutine functions (2), and not escaping failure text (4). After review: a context accepting the raw event (3), a signal exposing `set()` (2), a signal with writable attributes (1), keeping the original object (7), not copying dicts (7) or lists (6), not closing an unstarted coroutine (1), closing every coroutine (1), and closing any awaitable with a `close()` (2).

### DEC-043 — A handler registry of exact job kinds, checked with the claim's own rule
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented locally (`HandlerRegistry`, `orqestra.domain.kinds`)
- **WHAT:**
  - `HandlerRegistry(mapping)` is an immutable `Mapping[str, Handler]` in kind order. It needs at least one kind; kinds are checked by `check_job_kinds`; every handler must be callable and not a coroutine, generator or async-generator function. Lookups are exact, and there is no fallback handler.
  - `registry.kinds` is the sorted tuple a worker passes to `claim_next_attempt(..., kinds=...)`.
  - `check_job_kinds` is the job-kind filter's claim check moved, unchanged, from `lifecycle._supported_kinds` to `orqestra.domain.kinds`, so the claim and the registry share one rule: a non-empty collection of non-blank `str` kinds (blank as `btrim` defines it: spaces only) without NUL characters. The claim SQL is unchanged.
- **WHY:** A worker must claim exactly the kinds it can run; any kind the registry accepts is one the claim accepts and a job can have. One function means the two rules cannot drift apart.
- **RUNNER-UP:** A plain `dict` checked at worker start-up — no new type, but nothing would stop a later change from mutating it or claiming kinds it does not hold. Duplicating the check in the registry — no change to `lifecycle.py`, but two copies of one rule.
- **TRADE-OFF:** A small change to `lifecycle.py` (an import and a deleted private function); its behaviour and messages are unchanged and covered by the 47 job-kind filter claim tests. Coroutine detection sees through functions and `functools.partial`, not through objects with an async `__call__`; those are still caught when they return a coroutine (DEC-042).
- **VERIFICATION:** `tests/unit/test_kinds.py` (accepted kinds including tab, newline, CRLF and no-break space; 17 refused inputs); `tests/unit/test_worker_handlers.py` (exact, sorted kinds usable by the claim; no fallback for case, spacing, prefix or wildcard variants; immutability; callable objects and partials accepted; 14 refused registries). The existing section-22 claim tests in `tests/integration/test_attempt_lifecycle.py` pass against the moved check.

### DEC-044 — Worker settings: every timing value required, with only the relationships safe operation needs
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction · implemented locally (`orqestra.worker.settings.WorkerSettings`)
- **WHAT:**
  - `WorkerSettings` (pydantic-settings, prefix `ORQESTRA_WORKER_`, frozen) requires `lease`, `execution_timeout`, `heartbeat_interval`, `db_operation_timeout`, `fence_margin`, `poll_interval`, `recovery_interval`, `recovery_batch_limit` and `shutdown_timeout`. None has a default. Environment durations are ISO 8601 (`PT30S`) or `HH:MM:SS`; a bare number has no unit and is refused.
  - Each duration is positive and representable, except `shutdown_timeout`, which may be zero. `recovery_batch_limit` is at least 1. `db_operation_timeout` is at least 1 ms, because PostgreSQL's `statement_timeout` counts whole milliseconds and 0 disables it.
  - Relationships: `LeasePolicy(lease, execution_timeout)` is valid (`lease_policy`); `db_operation_timeout < heartbeat_interval`; `heartbeat_interval + db_operation_timeout + fence_margin < lease`.
  - The database connection is the existing `ORQESTRA_DATABASE_URL`. The existing `Settings.worker_concurrency` (default 4) is unchanged and not read: the worker is single-slot by approved direction.
- **WHY:**
  - Operational values need the owner's approval (DEC-034), so none is invented.
  - The relationships are the ones a healthy worker needs: a database call must end before the next heartbeat is due, and a heartbeat sent on time must be confirmed before the worker's local fencing margin is reached, or every worker would fence itself.
  - Nothing promises recovery progress: no relationship involving `poll_interval`, `recovery_interval` or `recovery_batch_limit` is claimed.
- **RUNNER-UP:** Defaults derived from the lease (for example a heartbeat every third of it), or extra fields on the existing `Settings` — fewer variables, but an invented production policy, and the worker's required values would become optional for every other command.
- **TRADE-OFF:**
  - Operators must set twelve variables before a worker can start.
  - `ORQESTRA_WORKER_CONCURRENCY` shares the worker prefix but belongs to `Settings`; `WorkerSettings` ignores it.
  - Whether to require several heartbeats per lease, and how the cancellation grace relates to the heartbeat interval (DEC-040), are operator rules the settings cannot check.
- **VERIFICATION:** `tests/unit/test_worker_settings.py`: valid settings and their lease policy; frozen; nothing has a default; each field required on its own; environment parsing in ISO 8601 and `HH:MM:SS`; four unit-less values refused; `worker_concurrency` neither read nor affected; zero, negative and unrepresentable durations refused for each field; `shutdown_timeout` zero allowed and negative refused; batch limit 1 allowed, 0 and -1 refused, non-integers refused; `db_operation_timeout` exactly 1 ms allowed and 1 µs less refused; lease equal to the execution timeout allowed and 1 µs shorter refused; database timeout 1 µs under the heartbeat interval allowed and equal refused; the fencing relationship 1 µs over allowed and at or under refused; a relationship is not reported while a field is invalid. Locally, mutants dropping or weakening each check (5) each failed tests.
- **Graceful shutdown note:** `shutdown_timeout` is now read. It is the time a stopping worker gives the task it holds to settle (handler, completion and reconciliation), counted from the latch; `0` abandons a running task at once and stops an idle worker at once (DEC-049).
- **Retry scheduling note (DEC-053):** two more required settings, `retry_base_delay` and `retry_max_delay`, with no defaults; both positive and representable, the base no longer than the maximum, together forming `retry_policy`. Every worker must use the same values; nothing checks this.
- **Recovery correction note (DEC-050):** a tenth required setting, `lock_timeout`, at least 1 ms and, in whole milliseconds rounded up, strictly shorter than `db_operation_timeout`, which is therefore at least 2 ms. Millisecond rounding is now exact for every supervisor bound (floating point had made some, such as 2007 ms, 1 ms too long).

## The first worker runtime

Entries DEC-045 and DEC-046 record the first single-slot worker runtime and its local
watchdog, agreed after a read-only probe showed that a stalled database call can hold the
supervisor indefinitely. The owner accepted them on 2026-11-07;
"implemented" means verified by the tests named, locally and in GitHub CI: they
were merged in PR #10 (`d498698`), with both CI jobs passing for the pull request
(run 40487174691) and for `main` (run 40488034469). There is no command or service that
runs a worker yet.

### DEC-045 — A single-slot worker: a supervisor, a handler thread and a local watchdog that ends the process
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented locally (`orqestra.worker.runtime`, `orqestra.worker.fence`)
- **WHAT:**
  - **Supervisor and handler thread.** `Worker.run` is the supervisor, on the calling thread. It alone uses one autocommit connection (`connect_supervisor`: `statement_timeout` from `db_operation_timeout`, existing `options` kept). It claims with `kinds=registry.kinds`, reads the task and its job, and runs the handler with `run_handler` on one separate thread, which gets no connection. It claims again only after the task is settled.
  - **Schedule.** `next_step` is a pure function of the local clock:
    1. a finished handler's completion;
    2. a running handler's heartbeat every `heartbeat_interval`;
    3. a recovery sweep step: up to `recovery_batch_limit` calls of `revoke_next_expired` every `recovery_interval`, idle or busy. While a handler runs, a step starts only if it can take its full `db_operation_timeout` and still end before the next heartbeat is due;
    4. while idle, a stop or a claim every `poll_interval`.

    Every wait is finite.
  - **Watchdog.** One daemon thread with two guards, each armed with a token so an old arming can never touch a newer one. It ends the process with `end_process` when an armed guard expires. `end_process` writes one diagnostic line to stderr with `os.write`, best effort, and calls `os._exit` in a `finally`, so a failed write (stderr closed, a broken pipe) can never prevent the exit. The guards:
    - **Ownership** (exit 3), armed while a handler runs.
      - **Deadline:** the lease end mapped to the local clock from the claim (`local_deadline`: the local time read just before the claim was sent, plus the database's `lease_expires_at − started_at`), so it is never later than the real lease end, minus `fence_margin`.
      - **Moves later** only after a *confirmed* renewal; an unconfirmed heartbeat (any database error, a failed connect, a lost answer) leaves it unchanged.
      - **Moves earlier** when a heartbeat reports a cancellation request (the lease now ends with the grace period). Renewals then stop; the handler is not told by this runtime.
      - **Disarmed** by the handler thread itself the moment the handler's work has ended.
    - **Operation** (exit 4), armed around every supervisor database call, including connecting, with a deadline `db_operation_timeout` ahead. It is a liveness bound, not an ownership claim: a stalled call ends the process instead of hanging it.
  - **The supervisor ends the process itself:**
    - when a heartbeat shows the attempt can no longer be renewed (`LeaseExpiredError`, `AttemptRevokedError`, `StaleAttemptError`, `WorkerMismatchError`, `AttemptFinishedError`, `NotFoundError`) while the handler still runs (exit 3);
    - when the supervisor loop fails while a handler runs (exit 3);
    - when the handler raised a control-plane signal (exit 5, nothing recorded, DEC-009).
- **WHY:**
  - A Python thread cannot be stopped, so ending the process is the only way to stop a handler that may no longer own its task.
  - A read-only probe showed the supervisor cannot do it alone. With `statement_timeout = 500 ms`, `connect_timeout = 2` and keepalives every second, a psycopg call through a stalled TCP proxy was still blocked after 8 s. A separate thread calling `os._exit` ended that process at 1.00 s, and a `SIGALRM` timer at 1.11 s.
  - Mapping from the claim keeps the local deadline conservative without a database clock read. Moving it only on confirmed renewals keeps a lost answer from extending ownership.
  - Holding sweeps back for heartbeats lets a busy worker recover others without risking its own lease. The existing rule `db_operation_timeout < heartbeat_interval` leaves room for at least one sweep step per heartbeat interval.
- **RUNNER-UP:**
  - A parent supervisor process with a killable child per handler: stronger (it can stop GIL-holding handlers and kill process groups), but more to run and an overhead per task.
  - A `SIGALRM` timer instead of a thread: it worked in the probe, but it takes the process's single alarm and the main thread's signal handling, which shutdown handling will need.
  - Fencing from the main thread only, relying on libpq timeouts: rejected by the probe.
- **TRADE-OFF:**
  - **What the fence cannot stop:** a handler holding the GIL in native code starves the watchdog too, and processes a handler detached survive it.
  - **Clock assumptions:** the local and database clocks are assumed to run at the same rate, with `fence_margin` absorbing small differences. A step forward in the database clock can end the real lease before the local fence (fencing in the database still holds).
  - **Conservative exits:** any database call longer than `db_operation_timeout`, or a watchdog expiry racing a handler that has just finished, ends the process. The outcome is then lost and recovery decides, using one attempt of the budget.
  - **Single connection:** the supervisor is single-threaded, so a slow claim, read or completion delays the next heartbeat. The ownership guard bounds the consequence.
  - **Cancellation:** handlers are not yet told about a cancellation request; they are fenced at the end of the grace period instead.
  - **Recovery delay:** there is no guaranteed bound, only that any live, responsive worker sweeps.
- **VERIFICATION:**
  - **`tests/unit/test_worker_fence.py`** (injected clock and `terminate`):
    - the deadline mapping;
    - expiry exactly at the deadline, once;
    - `extend` only later and `restrict` only earlier;
    - obsolete tokens touch nothing and an old deadline never fires;
    - a disarmed guard never fires;
    - the earliest of two guards fires;
    - a real watchdog thread fires while another thread is blocked, and honours an extension;
    - `end_process` writes one JSON line and exits with the code, and still exits with the code when the write raises `OSError` or `BrokenPipeError`;
    - in a real subprocess whose main thread is blocked, the production watchdog exits with code 3 with stderr writable, closed, or a broken pipe;
    - a confirmed renewal applied after the old deadline but before the watchdog checks does not expire the worker, both on the watchdog and through `Worker._heartbeat`;
    - exit codes are stable.
  - **`tests/unit/test_worker_runtime.py`:**
    - the `next_step` decision table;
    - a 1,000-second simulation with an endless expired backlog and every call taking its full bound: every heartbeat runs exactly on time, with at least one revocation between consecutive heartbeats;
    - completion-error classification;
    - connection options;
    - against injected lifecycle functions:
      - unconfirmed heartbeats (connection loss, deadlock, timeout, failed connect) never move the fence and a confirmed one does;
      - the handler thread's disarm makes later renewals harmless;
      - each lost-ownership error ends the process while the handler runs and not after it ended;
      - a cancellation request shortens the fence;
      - every completion path;
      - a failing supervisor ends the process while a handler runs.
  - **`tests/integration/test_worker_runtime.py`** (in-process, real PostgreSQL):
    - supported tasks complete while others stay queued at generation 0;
    - three concurrent workers each hold exactly one task and never share a generation;
    - no second claim while a handler runs;
    - heartbeats keep a slow handler alive;
    - a busy worker revokes another worker's expired attempt while its own handler runs;
    - five handler failures keep the existing classification;
    - a killed supervisor connection is reconnected, with no handler failure;
    - a server rejection of a valid outcome ends the worker without recording a failure;
    - a deadlock on completion is retried without rerunning the handler;
    - refused renewals leave the fence at its last confirmed deadline, which expires before the database lease;
    - a stalled supervisor call while a handler runs (a heartbeat or a sweep step, whichever meets the stall) trips the operation guard;
    - ambiguous completions, committed or not: covered by the DEC-047 tests, which replaced the original tests of ending the worker.
  - **`tests/integration/test_worker_process.py`** (a real subprocess; exits measured against PostgreSQL's clock):
    - a stalled supervisor call exits 4 before the lease ends;
    - refused connections exit 3 before the lease ends;
    - a lease ended by the database exits 3 at the next heartbeat;
    - a cancellation request exits 3 at the end of the grace period, and recovery then cancels the task;
    - a `SystemExit` handler exits 5 with nothing recorded;
    - an ambiguous completion: covered by the DEC-047 tests, which replaced the original exit 6 test.
  - **Stability:** both integration files passed 8 of 8, then 5 of 5, consecutive local runs. The first full `make verify` showed that the stall tests had assumed which call would stall; they now accept either call a busy worker makes, and always resume the proxy.
  - **Adversarial audit correction:** before review, `end_process` called `os._exit` only after a successful `os.write`. With stderr closed or a broken pipe, the write raised: the watchdog thread died and the process kept running, so the handler went on unfenced (shown by a probe; the same raise could escape a supervisor exit). With the old `end_process` restored, the four new stderr tests fail and the writable control passes.
  - **Mutation checks:** locally, each of these mutants failed tests:
    - an unconfirmed heartbeat extending the fence;
    - no disarm by the handler thread;
    - tokens ignored when moving a deadline;
    - sweeps not held back for heartbeats;
    - no operation guard;
    - claims while a handler runs;
    - lost ownership ignored;
    - an ambiguous completion retried;
    - a cancellation not shortening the fence.
- **Cooperative cancellation note:** "the handler is not told by this runtime", and the trade-off that handlers are not yet told, no longer hold. When a heartbeat confirms a committed request, the supervisor still stops renewing and restricts the fence to the end of the grace period first, then sets the attempt's cancellation event for the handler (DEC-048). A handler that ignores it is still fenced at the end of the grace period.
- **Graceful shutdown note:** `run(stop)` is replaced by `run()` and `request_stop()`. The supervisor now waits on a self-pipe that the handler's end, `request_stop` and signals write to, rather than on the handler's `done` or the `stop` event; this also ended a busy wait while an outcome waited for a retry. A stop is latched and, while a task is held, drained rather than ignored; a third guard, drain, ends the process with exit 9. Signal handling, which the `SIGALRM` runner-up left to shutdown, is owned by `shutdown.run_until_signalled` on the main thread (DEC-049).
- **Recovery correction note (DEC-050):** the supervisor's session also carries `lock_timeout` and `idle_in_transaction_session_timeout`. Neither changes what the watchdog guards; together they keep a frozen worker's open transaction from blocking recovery, and keep a recovering worker from being ended by its own operation guard while it waits for that lock.
- **Provider usage note (DEC-059):** the schedule gains a `USAGE` step after the heartbeat: while a handler runs, the supervisor writes the provider usage record the handler waits on, under the same rule as a sweep step, so it never delays a heartbeat.
- **Session time zone note:** `local_deadline` subtracts in UTC and refuses naive instants, and the hold of a claimed but unstarted attempt (DEC-049) uses it, so the fence and the hold measure elapsed time in any session TimeZone (DEC-067).

### DEC-046 — Outcome policy: database errors are never handler failures; an ambiguous completion ends the process
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-26) · implemented locally (`Worker`, `classify_completion_error`); the ambiguous case is reconciled by DEC-047
- **WHAT:**
  - **The handler's outcome** is recorded with `complete_attempt`, the authoritative lifecycle operation. Lease expiry is never checked locally before completing, so a completion still counts until recovery commits.
  - **Ownership already ended** (`AttemptRevokedError`, `StaleAttemptError`): the outcome is discarded.
  - **A database error is never turned into a handler failure:**
    - **Retried:** a failed connect (nothing was sent), or a server error that certainly rolled back and may be retried (`40001`, `40P01`, `55P03`, `57014`). The retry comes after `db_operation_timeout` and continues until the attempt's local deadline (exit 8 then). The same rule bounds a provider usage record's retries (DEC-059), but those are written while a handler still runs: the ownership fence then ends the process (exit 3) before the attempt's deadline, so exit 8 is only a backstop behind it.
    - **Defect (exit 7):** a server rejection of a valid outcome with any other code, or a lifecycle invariant failure (`AttemptFinishedError`, `NotFoundError`, `ValueError`, `RuntimeError`).
    - **Ambiguous:** a broken connection or an error without a server code. The worker does not classify the event as a handler failure and does not rerun the handler; it reconciles the completion from PostgreSQL's evidence (DEC-047). Exit 6 means only that this could not be established before the attempt's deadline.
  - **Claims and reads:**
    - **A claim error that certainly rolled back, or a failed connect,** is retried after `poll_interval`.
    - **A claim whose answer was lost** (it may have committed) holds further claims for a full `lease`.
    - **A claimed attempt that cannot be started** (its task cannot be read, or too little lease is left) holds further claims until its lease has certainly ended: the local time after the claim returned, plus the lease. It is then recovered.

      In both of these cases the worker runs at most one handler at a time and never holds two attempts whose leases are both valid. The abandoned attempt can still read `running` in the database, beside the worker's next one, until recovery revokes it.
    - **A missing task, job or handler** is a defect (exit 7).
- **WHY:**
  - DEC-009 and DEC-029 already say that database and infrastructure errors are not handler failures.
  - Ending the process on ambiguity is the smallest correct policy without reconciliation. A retry could raise `StaleAttemptError` even though the earlier completion committed, and rerunning the handler would repeat its side effects.
- **RUNNER-UP:**
  - The C4 reconciliation now: re-read the attempt and task, and compare every stored detail. This is deferred to a later change so that it gets its own review and tests.
  - Treating an ambiguous completion like a retryable one: rejected, because a committed outcome would then be reported as stale or finished, or retried after recovery claimed the task again.
- **TRADE-OFF:**
  - An ambiguous completion that did not commit costs a revocation: one attempt of the budget, and a rerun by another worker after the lease expires.
  - A claim whose answer was lost costs the same, and keeps this worker from claiming for up to one lease.
- **VERIFICATION:**
  - **`classify_completion_error`** decision table (`tests/unit/test_worker_runtime.py`).
  - **Completion paths against injected failures:** discarded, ambiguous, rejected, refused, retried until the deadline, failed connect, control-plane signal never recorded, missing outcome.
  - **Claim paths:** a failed claim retried after the poll interval; an ambiguous claim held for a full lease; a failed read, and a lease too short to start, each held until the lease ends; a missing job.
  - **In-process PostgreSQL tests:**
    - a killed connection is not a handler failure;
    - a check-violation trigger on the completion ends the worker with the attempt still running and no failure kind;
    - a one-time deadlock is retried, with the handler run once;
    - ambiguous completions, committed and not, through a proxy that cuts the connection just after or just before the `COMMIT` reaches the server.
  - **The real-subprocess test:** exit 6, a committed success, the handler run once and the other task unclaimed.
  - **Mutation checks:** locally, not holding an unstarted claim (2 tests) and treating an ambiguous claim as a plain failure (1) each failed tests, as did retrying an ambiguous completion (DEC-045).
- **Completion reconciliation note:** the ambiguous case is no longer temporary. A completion whose answer was lost is now reconciled from PostgreSQL's evidence (DEC-047); exit 6 now means only that this could not be established before the attempt's deadline. The rest of this policy is unchanged: database errors are never handler failures, retryable errors are retried until the deadline, other rejections of a valid outcome are defects, and claims are handled as described. The first runtime's tests of ending the process on a lost answer (listed here and under DEC-045) were replaced by the DEC-047 tests.

## Reconciling a completion whose answer was lost

Entry DEC-047 records the C4 reconciliation agreed in the worker reliability design audit.
The owner accepted it on 2026-11-07; "implemented" means
verified by the tests named, locally and in GitHub CI: it was merged in PR #11
(`278cb74`), with both CI jobs passing for the pull request (run 40520647309) and for
`main` (run 40524368913). Cooperative cancellation (DEC-048) and graceful
shutdown (DEC-049) are not part of it.
- **Graceful shutdown note:** exit 9 is added for shutdown. `run` returns only through a controlled stop: an unexpected exception while a handler runs still ends the process (exit 3); `SystemExit` with a success code is raised as `UncontrolledExitError`; any other exception propagates, so the process exits nonzero (DEC-049).

### DEC-047 — A lost completion answer is reconciled from one consistent PostgreSQL read; the handler never runs again
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-27) · implemented locally (`read_completion_evidence`, `Worker`)
- **WHAT:**
  - **Evidence read.** `read_completion_evidence(conn, attempt_id, *, generation, outcome)` is read-only: one `SELECT` joins the attempt to its task, so both come from one statement snapshot. It returns a `CompletionEvidence`:
    - `RUNNING`: the attempt runs, and its task runs at this generation;
    - `REVOKED`: recovery ended it, and its task is queued, cancelled or failed at this generation (`next_task_state`, DEC-038) or has moved on to a later one;
    - `MATCHES`: it ended with exactly this outcome;
    - `CONFLICTS`: it ended otherwise;
    - `INCONSISTENT`: no such attempt, another generation, or a task that contradicts its attempt.
  - **What `MATCHES` needs:**
    - **success:** the attempt succeeded with no failure details, and the task succeeded at this generation with `result = expected::jsonb` evaluated by PostgreSQL. A JSON `null` result is sent as the `jsonb` value `null`, never SQL `NULL`.
    - **cancelled:** the attempt and the task cancelled at this generation.
    - **permanent or unclassified failure:** exactly this failure kind, error type and message, and the task failed at this generation.
    - **transient failure:** the attempt's failure details, and a task queued, cancelled or failed at this generation or moved on to a later one. It may since have been requeued, claimed again, cancelled or failed.
  - **Contradictions are `INCONSISTENT`.** A revoked or transient-failed attempt beside a task still running or succeeded at its generation is `INCONSISTENT`, never `REVOKED` or `MATCHES`. The schema allows it only through corruption: its constraints are single-row, and the fencing trigger stops a task re-entering `running` at the same generation, but not an attempt row ending while its task never left `running`.
  - **The worker's loop.** When `complete_attempt`'s answer is lost (`classify_completion_error` returns `AMBIGUOUS`), the supervisor:
    1. keeps the handler's outcome object and closes the broken connection;
    2. reads the evidence at once on a fresh connection (through `_call`, under the operation guard);
    3. acts on it:
       - `MATCHES`: settles, and the worker carries on;
       - `REVOKED`: discards the outcome;
       - `RUNNING`: writes the identical outcome again;
       - `CONFLICTS` or `INCONSISTENT`: ends the process (exit 7).
  - **Re-read, never infer.** While uncertain, any rejection of a rewrite (`AttemptRevokedError`, `StaleAttemptError`, `AttemptFinishedError`, `NotFoundError`) is decided by reading the evidence again.
  - **Bounded.** Failed reads, a second lost answer and retryable errors are spaced by `db_operation_timeout`. The first read after a lost answer always happens. Past the attempt's execution deadline on the local clock, an unresolved outcome ends the process (exit 6, `reconciliation_unresolved`); an outcome that certainly was not recorded still ends it as exit 8.
- **WHY:**
  - Only `complete_attempt` and `revoke_next_expired` end a running attempt, each in one transaction that also writes its task, and the attempt ID is private to its worker. So the attempt row says what happened, as long as the worker can recognise its own outcome.
  - Recognising it exactly needs `jsonb` equality. A read-only probe showed Python equality after a round trip calls `1e308` unequal to the 309-digit number `jsonb` stores for it, and calls `true` equal to `1`; DEC-025 already compares in SQL for the same reason.
  - A rejected rewrite cannot settle the question, because `complete_attempt` checks the generation before the finished state (DEC-029).
  - Rerunning the handler would repeat its side effects; keeping its outcome in memory never does.
- **RUNNER-UP:**
  - The first runtime's policy: end the process on any lost answer (DEC-046). That costs a revocation and a rerun of a handler whose outcome may have committed.
  - Comparing in Python after a JSON round trip: refuted by the probe.
  - Reading the attempt and the task in two statements: a concurrent change between them could mislead.
  - A fixed number of retries instead of the deadline: an invented value, with no relation to when recovery can act.
- **TRADE-OFF:**
  - **One new read function** in `lifecycle.py`, with no schema change.
  - **No renewals while reconciling:** a database outage longer than the remaining lease can let recovery revoke the attempt first, and the finished outcome is then discarded. Renewing while completing is a possible later optimisation.
  - **Bounded by the attempt's deadline,** so a long outage near it ends the process with the outcome unresolved; recovery or the committed completion then decides.
- **VERIFICATION:**
  - **`tests/integration/test_completion_evidence.py`** (real PostgreSQL, real completions and revocations):
    - a running attempt for every outcome;
    - success results matched only by `jsonb` equality: key order, JSON `null`, `1e308`, `1.0` vs `1`, `-0.0` vs `0` match; `true` vs `1`, `0` vs `false`, `null` vs `{}`, `"1"` vs `1` and different values conflict;
    - permanent and unclassified failures match only their exact kind, type and message;
    - cancellation;
    - a transient failure still matches after its task was claimed again and succeeded, was cancelled, or ran out of attempts;
    - revoked attempts, also after the task was claimed again;
    - after a revocation or a transient failure, every legitimate aftermath keeps its evidence: requeued, cancelled while queued, claimed again (running at generation 2), claimed again and succeeded, exhausted (failed), and cancelled by a pending request (including a grace expiry);
    - a revoked or transient-failed attempt beside a task still running at its generation (the attempt row ended behind the operations' back) or succeeded at it (the task row changed) is `INCONSISTENT`;
    - the fencing trigger refuses a task re-entering `running` at the same generation;
    - missing records, other generations and corrupted records are inconsistent;
    - the read writes nothing.
  - **`tests/unit/test_worker_runtime.py`** (injected clock and scripted answers):
    - a lost answer keeps the outcome, closes the connection and reads at once;
    - each evidence value leads to its action;
    - the rewrite is the identical outcome object and the handler thread is never restarted;
    - a rejected rewrite (revoked, stale, finished, not found) is decided by a second read;
    - failed reads are spaced and end at the deadline with exit 6;
    - the first read happens even past the deadline;
    - a second lost answer is spaced;
    - a retryable rewrite reads again before writing;
    - without a lost answer, a retryable error still ends as exit 8.
  - **`tests/integration/test_worker_runtime.py`** (in-process, through `PgProxy`):
    - a committed completion whose answer was lost is reconciled for an object, JSON `null`, `1e308`, a permanent and a transient failure, and the worker then runs the next task;
    - an uncommitted one is written again;
    - when a cancellation request commits while the worker is reconciling, the rewritten success still stands (DEC-020);
    - two lost answers in a row (rolled back, then committed) are reconciled;
    - recovery that revoked first wins and the outcome is discarded;
    - recovery that wins between the evidence and the rewrite is respected, also when another worker has claimed the task again;
    - a connection cut during the evidence read is retried;
    - a stored `true` changed to `1` behind the worker's back is a defect;
    - an unreachable database ends the worker at the deadline with exit 6.

    In every case the handler ran once for the attempt.
  - **`tests/integration/test_worker_process.py`** (real subprocess):
    - a committed completion whose answer was lost is reconciled, and the process carries on to the next task without exiting;
    - an unreachable database ends the process with exit 6 at a 4 s deadline, with nothing recorded as a failure.
  - **Mutation checks:** locally, each of these failed tests:
    - not comparing the result;
    - comparing it as text;
    - requiring a transient failure's original task state;
    - accepting `RUNNING` whatever the task;
    - returning `REVOKED`, or a transient `MATCHES`, whatever the task's state at that generation (found in review; the original code did this, and the four contradiction cases fail against it);
    - rejecting a task that moved on to a later generation;
    - rewriting blind after a lost answer;
    - inferring from a rejection instead of re-reading;
    - never ending an unresolved reconciliation;
    - writing again when the evidence matches.
- **Graceful shutdown note:** while a stopping worker drains, reconciliation is also bounded by the drain deadline; if that comes first, the process ends with exit 9 and PostgreSQL's evidence decides later (DEC-049).

## Cooperative cancellation

Entry DEC-048 records cooperative cancellation, agreed in the worker reliability design
audit. The owner accepted it on 2026-11-07;
"implemented" means verified by the tests named, locally and in GitHub CI: it was
merged in PR #12 (`308bb0a`), with both CI jobs passing for the pull request (run
40598713849) and for `main` (run 40599609791). Graceful shutdown (DEC-049) is
not part of it.

### DEC-048 — A heartbeat that confirms a committed request tells the handler, after fencing at the grace end
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-27) · implemented locally (`Worker`)
- **WHAT:**
  - **One event per attempt.** `_Running` keeps a `threading.Event`, created unset when the handler starts. The handler receives only the existing read-only `CancellationSignal` over it, in `TaskContext.cancellation`.
  - **Telling the handler.** When `renew_lease` raises `CancellationRequestedError`, which it does only after reading a committed `cancel_requested_at`, the supervisor:
    1. stops renewing;
    2. restricts the ownership fence to the end of the grace period, less `fence_margin`;
    3. only then sets the event, and logs `cancellation_requested`.

    Restricting first means fencing holds even if anything later fails.
  - **Nothing else sets it:** not an unconfirmed heartbeat (a database error or a failed connect), lost ownership (`LeaseExpiredError`, `AttemptRevokedError`, `StaleAttemptError`, …), or another attempt's request.
  - **No new mechanism.** There is no polling loop, no new lock and no new outcome mapping. `run_handler` already makes `HandlerCancelled` an `AttemptCancelled` only when the signal is set, and the database already accepts a cancelled outcome only after a committed request.
- **WHY:**
  - The pieces existed (DEC-040, DEC-042) but the event was created and discarded, so a handler that could stop cleanly was fenced instead.
  - Heartbeats already carry the committed request, so telling the handler there adds no database load and no new race.
  - Fencing first means that, once a request is confirmed, the local fence ends at the grace period even if notification goes wrong.
- **RUNNER-UP:**
  - A separate, more frequent cancellation poll: it would notice sooner, but adds database load and a second timing contract.
  - Giving the handler the event itself: rejected with the worker contracts, since the handler could forge an observed cancellation.
- **TRADE-OFF:**
  - **Notice comes only with a heartbeat that reaches the database.** There are three cases:
    - **Confirmed:** a heartbeat reads the committed request. The local fence moves to the end of the grace period, less `fence_margin`, and the handler is told.
    - **Grace ends first:** the next heartbeat finds the lease ended (`LeaseExpiredError`). The worker is fenced at once, with no notice, and recovery cancels the task.
    - **Unobserved, during an outage:** if the database is unreachable, the worker cannot learn of the request or its grace deadline. It keeps the existing local ownership fence: the end of the lease it last confirmed, less `fence_margin` (DEC-045). That can be later than the end of the grace period. The database's own fencing still rejects the worker's writes once the lease has ended, and recovery cancels the task.
  - **Any timing bound is conditional.**
    - **Responsive database:** if every heartbeat is answered within `db_operation_timeout`, the handler is fenced by roughly `max(request + grace - fence_margin, request + heartbeat_interval + db_operation_timeout)`. Otherwise the bound is the end of the last confirmed lease.
    - **What the fence itself assumes:** the watchdog thread is scheduled (no handler holding the GIL, no long scheduling stall), and the clocks run at the same rate (DEC-045). Exiting adds its own time.
    - **What the tests show:** the order of fencing and notice, the fence deadline, and the cases above. They do not measure an end-to-end bound.
  - **Cooperation is the handler's choice.** One that ignores the signal is ended only by the fence; one holding the GIL can delay even that (DEC-045).
  - **A spontaneous `HandlerCancelled` can still become cancelled.** If one is raised just as the supervisor sets the event, `run_handler` maps it to `AttemptCancelled`. The database accepts that, because the request had committed.
- **VERIFICATION:**
  - **`tests/unit/test_worker_runtime.py`:**
    - each attempt's event is fresh and unset, and a later attempt is unaffected by an earlier request;
    - the handler's view reads the worker's event and cannot set it;
    - a confirmed request restricts the fence, stops renewals and sets the event;
    - the fence is restricted even if setting the event fails;
    - unconfirmed heartbeats (connection lost, deadlock, statement timeout, refused connect) and lost ownership (grace expired, revoked, stale) never set it;
    - a request seen after the handler finished changes nothing recorded;
    - an unobserved `HandlerCancelled` is an unclassified failure;
    - a cooperative handler ends `AttemptCancelled`.
  - **`tests/integration/test_worker_runtime.py`** (in-process, real PostgreSQL):
    - a handler told of a committed request ends by the existing policy: cancelled; success stands; a transient failure cancels; a permanent failure fails. The attempt is never revoked and the worker carries on;
    - a request after the completion committed changes nothing (`already_finished`);
    - a success after a committed request still stands;
    - a 1 µs grace fences the worker with no notice, and recovery cancels the task;
    - a repeated request does not extend the lease or run the handler again;
    - a cancelled outcome whose answer was lost is reconciled (DEC-047).
  - **`tests/integration/test_worker_process.py`** (real subprocess):
    - a cooperative handler is cancelled and the process carries on;
    - refused connections fence the process (exit 3) with no cancellation event, request or recorded outcome;
    - the existing grace test still fences a handler that ignores the signal, and recovery cancels the task.
  - **Commit-order races** between cancellation and completion at the lock level stay covered by `test_cancellation_and_completion_in_either_order` and the race tests in `test_attempt_lifecycle.py`.
  - **Mutation checks:** locally, each of these failed tests:
    - never telling the handler;
    - telling it before restricting the fence;
    - telling it on an unconfirmed heartbeat;
    - telling it on lost ownership;
    - one event shared by every attempt.

## Graceful shutdown

Entry DEC-049 records graceful, bounded shutdown, agreed in the graceful shutdown design
audit and its clarification (owner decisions O1–O10). The owner accepted
it on 2026-11-07; "implemented" means verified by the tests named, locally
and in GitHub CI: it was merged in PR #13 (`155d319`), with both CI jobs passing for
the pull request (run 40669564020) and for `main` (run 40671619033). There is still no
command or service that runs a worker.

### DEC-049 — Graceful shutdown: a latched stop, a drain bounded by a watchdog guard, and lock-free signal handling
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-28) · implemented locally (`Worker.request_stop`, `orqestra.worker.shutdown`, `Guard.DRAIN`)
- **WHAT:**
  - **Stop requests.** `Worker.request_stop()` sets a flag and writes one byte to the worker's nonblocking self-pipe. It takes no lock, so a signal handler may call it. The supervisor empties the pipe before it reads any state and then waits on it (`poll`), so the handler's end, a stop request or a signal ends a wait at once and no wake is lost.
  - **The latch** is the supervisor's first read of the flag that finds it set: at the top of its loop; after each database call of a step that makes several (after the claim and after the task read, so a claimed task is not read further; between an evidence read and its rewrite); or in the pre-start check, the last read before `thread.start()`. So a stop is latched at most one database call, the one in flight, after it was requested. It is the shutdown's linearization point. From the latch on, the supervisor sends no claim, starts no handler and starts no sweep.
    - A handler that started after the request was stored but before the latch was already running, and it is drained.
    - A claim answered after the latch is not started (`claimed_task_not_started`). It is left to recovery.
  - **Draining.** If a task is held at the latch, the watchdog's drain guard is armed at `latch + shutdown_timeout`. Heartbeats, completion and reconciliation continue.
    - Once the task is settled (recorded, reconciled, or discarded because recovery revoked it), the guard is disarmed, `run` returns, and the process exits 0.
    - Otherwise the drain guard, or the supervisor's own `ABANDON` step, ends the process with exit 9 (`drain_deadline_passed`). This holds even while the supervisor is blocked in a database call.
    - `shutdown_timeout = 0` stops an idle worker at once and abandons a running task at once.
  - **Nothing is fabricated.** Shutdown never sets the cancellation event and writes nothing; an abandoned attempt stays `running` until recovery revokes it after its lease (`lease_expired`).
  - **Exit 0** means every handler the worker started has ended as PostgreSQL confirmed. It does not mean no attempt of this worker is still `running`. A held claim whose lease may not have ended is logged at the stop: `stopped_with_unstarted_claim`, with the attempt and task IDs, generation and lease end, or `stopped_with_uncertain_claim` after a lost claim answer.
  - **Signals.** `run_until_signalled(worker)` runs the worker on the main thread.
    - It refuses, changing nothing, off the main thread or when a previous SIGTERM or SIGINT handler was installed outside Python and so cannot be restored.
    - It sets the worker's pipe as the process's wakeup fd, and refuses, putting it back, if another wakeup fd was set. Then it installs one handler for SIGTERM and SIGINT.
    - The handler numbers its runs with `itertools.count`. The first requests a stop; any later one, before `run` has returned, ends the process with exit 9 (`shutdown_forced`).
    - Everything is restored in `finally`, however `run` ends; a partial installation is rolled back.
  - **No false success.** `run` returns only through the controlled stop. An exception while a handler runs ends the process (exit 3, as before). `SystemExit` with a success code is raised as `UncontrolledExitError`. Any other exception propagates, so the process exits nonzero (Python's exit 1).
  - **No busy wait.** The design audit found the supervisor spinning while an outcome waited for a retry (about 881,000 iterations in one second), because it waited on the handler's `done` event, already set. The pipe wait removes it.
- **WHY:**
  - `shutdown_timeout` existed but was unused. A stop while busy was ignored until the task settled, bounded only by the attempt's execution deadline. SIGTERM killed the process mid-task, and SIGINT raised `KeyboardInterrupt` wherever the main thread was.
  - A watchdog guard holds whatever the supervisor is doing, as the ownership and operation guards already do.
  - Read-only probes (Python 3.12, macOS) decided the signal design:
    - a handler calling `Event.set()` while the interrupted main thread held that event's lock deadlocked;
    - a main thread blocked in `join()` was not woken by a signal delivered to another thread: the handler ran 1.8 s late, when `join` returned;
    - with `set_wakeup_fd`, the same delivery woke a `select` in 10 ms;
    - handlers do nest, and `itertools.count` gave each run its own number.
  - Without a lock in signal context, no ordering between a stop request and `thread.start()` can be enforced; the supervisor's own read is the strongest point that can.
- **RUNNER-UP:**
  - **The supervisor on its own thread, the main thread joining it, and a handler that sets an `Event`:** the first audit's proposal. Rejected by the probes above.
  - **`pthread_sigmask` plus a `sigwait` thread:** no Python handler at all. Rejected because children of a thread with SIGTERM blocked inherit the block (a probe's child ignored `terminate()`), and any thread started before the mask would take the default action and kill the process.
  - **A supervisor-only drain check:** late by up to one database call.
  - **Starting a claim answered after the latch, and draining it:** may finish the task, but starts work after a stop, which at `shutdown_timeout = 0` is only harm.
  - **Releasing the lease on abandon:** a write while the handler thread may still run would let another worker start before this process has ended. A release that keeps the budget needs a new revocation reason, so a migration.
  - **Keeping `run(stop)` with a relay thread:** compatible, but a second way to stop, and a thread that leaks when the event is never set.
- **TRADE-OFF:**
  - **Timing is conditional.** From a signal, the process ends within about `db_operation_timeout + shutdown_timeout`, plus exit time, only while the watchdog and main threads can run. A handler holding the GIL delays the signal handler and the watchdog alike; SIGKILL, and an orchestrator kill grace shorter than this, end it like a crash; detached processes survive.
  - **An abandoned drain, or a claim that never started, uses one attempt** of the task's budget, and is recovered only after its lease ends by a live worker; a task on its last attempt fails.
  - **Exit 9 can race a completion that committed.** PostgreSQL decides.
  - **Signals:** two identical signals that arrive before the handler runs count once (POSIX). The worker's wake pipe (`select.poll`) and the helper are POSIX only, and the helper cannot share the process with another wakeup-fd user, such as an asyncio loop.
  - **Resources:** the pipe's descriptors are closed only when the worker is garbage, so no write can reach a closed or reused descriptor.
  - **API:** `run(stop)` became `run()` plus `request_stop()`: four test call sites and the README example changed.
  - **Handlers are not told about shutdown.**
- **VERIFICATION:**
  - **`tests/unit/test_worker_shutdown.py`** (fake clock, scripted database calls, threads synchronised by events):
    - **latch:** a stop before `run` claims nothing; the latch is permanent; a stop ends a 10-minute idle wait at once;
    - **draining:** a busy worker latches at once, arms the drain guard at the latch plus the timeout, keeps heartbeating, never tells the handler, records the outcome when the handler ends (woken by it, not a timer) and stops;
    - **drain guard:** fires on its own while the supervisor is uninvolved; settling disarms it at once; a passed deadline abandons a running task and an unrecorded outcome; `shutdown_timeout = 0`, idle and busy;
    - **latency:** a stop stored during the claim, the task read or the job read is latched before any further database call (none follows), and the attempt is held for recovery; one stored during an evidence read is latched, and the drain guard armed, before the rewrite;
    - **stop/start race:** a stop during a claim, or while the claimed task is read, leaves it unstarted with a warning naming the attempt and task; a stop stored after the pre-start check finds the handler running and drains it, with no further claim; a lost claim answer is reported; a hold whose lease has certainly ended is not;
    - **no busy wait:** at most 5 steps over a 0.5 s retry wait;
    - **no false success:** `SystemExit(0)`, `SystemExit(None)` and `SystemExit(False)` while idle raise `UncontrolledExitError`; `SystemExit(3)` passes through; `SystemExit(0)` while a handler runs exits 3; `SystemExit(0)`, `KeyboardInterrupt` and `KeyError` while completing or reconciling are never a success, and leave the task unsettled; a real process whose supervisor calls `sys.exit(0)` exits 1;
    - **signal handler:** the first signal requests a stop and a later one forces exit 9; a nested one still counts as the second; one after a controlled stop forces nothing; the handler returns while the watchdog's lock and the logging lock are held and the pipe is full;
    - **installation:** handlers and the wakeup fd are installed for the run and restored after; a signal after the helper returned reaches the previous handler, not the worker; a signal delivered to another thread ends a 30-second wait at once; refusal off the main thread, with an unrestorable handler, or with another wakeup fd changes nothing; a partial installation is rolled back; an exception from `run` restores everything and propagates.
  - **`tests/unit/test_worker_runtime.py`:** `next_step` while draining (heartbeat and completion go on, no sweep, wait for the drain deadline, abandon first), and every wait finite and positive across 120 combinations.
  - **`tests/unit/test_worker_fence.py`:** exit 9 is stable; the drain guard ends the process with `drain_deadline_passed`; with three guards armed, the earliest fires.
  - **`tests/integration/test_worker_runtime.py`** (real PostgreSQL, in process):
    - a draining worker renews, records, and leaves the second task queued;
    - a drain that runs out leaves the attempt `running`, untouched, then revoked `lease_expired`, requeued with budget left and failed on its last attempt;
    - a lost completion answer while draining is reconciled;
    - an outage while draining ends at the drain deadline, not the attempt's;
    - a supervisor blocked in a stalled call while draining is ended by the drain guard first;
    - a cooperative cancellation while draining ends cancelled;
    - a cancellation grace shorter than the drain fences first (ownership), and recovery cancels;
    - a stop during an in-flight claim (held by the proxy before its `INSERT` commits) leaves the attempt unstarted, warned about, and revoked `lease_expired` later;
    - `shutdown_timeout = 0` abandons a running task at once;
    - shutdown never tells the handler or records anything.
  - **`tests/integration/test_worker_process.py`** (a real subprocess, signals sent after `worker_started` is logged):
    - SIGTERM or SIGINT while idle exits 0, with a 1 s or a zero timeout;
    - a task settled within the drain exits 0, with the second task still queued;
    - a task not settled in time exits 9 two seconds after `shutdown_requested` by the worker's own log timestamps, untouched, then recovered (queued, or failed on its last attempt);
    - a second signal exits 9 at once (`shutdown_forced`);
    - `shutdown_timeout = 0` exits 9 at once.
  - **Mutation checks:** locally, 23 deliberate faults each failed tests, with the files restored by hash:
    - a latch that is not permanent; `request_stop`, or the handler's end, not waking the supervisor;
    - the drain guard never armed, or not disarmed on settling (caught only after a test was added for it);
    - shutdown telling the handler; no pre-start check; sweeps while draining; a stop before settling; no abandon at the drain deadline;
    - `SystemExit(0)` passing through; the busy wait restored; no unstarted-claim warning;
    - no wakeup fd; a handler that takes a lock; check-then-set instead of numbered runs; handlers or the wakeup fd not restored; another wakeup fd accepted;
    - **review correction F1:** before it, a stop that arrived during a claim was latched only in the pre-start check, after the task and job reads (up to three database calls), and one during an evidence read only after the rewrite. Removing the look after the claim, after the task read, or before the rewrite, or the pre-start check, each failed its own latency test.

## Recovery correction: workers frozen inside a transaction

Entry DEC-050 records a correction found while testing the worker command (DEC-051): a worker frozen inside a
database transaction blocked recovery of its attempt, and ended the workers that tried.
The owner accepted it on 2026-11-07; "implemented" means
verified by the tests named, locally and in GitHub CI: it was merged in PR #14 (`0eab699`),
with both CI jobs passing for the pull request (run 40711775654) and for `main` (run
40713618712).

### DEC-050 — Worker sessions bound their lock waits and their idle transactions
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-28) · implemented (`WorkerSettings.lock_timeout`, `supervisor_conninfo`)
- **WHAT:**
  - **A required setting**, `lock_timeout`: at least 1 ms and, in the whole milliseconds PostgreSQL uses (rounded up, by exact `timedelta` arithmetic), strictly shorter than `db_operation_timeout`. There is no default.
  - **Session options** on every worker supervisor connection, keeping existing `options` such as a `search_path`:
    - `statement_timeout` = `db_operation_timeout`, as before;
    - `lock_timeout` = `lock_timeout`;
    - `idle_in_transaction_session_timeout` = `db_operation_timeout`.
  - **No new behaviour in the worker.** A lock wait that times out raises `LockNotAvailable` (`55P03`), already a retryable code: an unconfirmed heartbeat, a completion retried, a failed sweep, or a failed claim. A session the server ended raises `IdleInTransactionSessionTimeout` (`25P03`) on its next use, with the connection closed, which the worker treats as a lost answer: unconfirmed, or reconciled for a completion (DEC-047).
- **WHY:**
  - **The defect, reproduced deterministically:**
    1. A worker frozen inside `renew_lease`, after it locks the attempt row, left its session `idle in transaction` holding the lock.
    2. Once the lease ended, a healthy worker's sweep locked the task (`SKIP LOCKED`), then waited for the attempt.
    3. Its operation guard, armed before its `statement_timeout`, ended it with exit 4.
    4. Nothing ended the frozen transaction.
  - **Neither bound suffices alone.**
    - The idle bound releases the lock, but up to `2 × db_operation_timeout` after the frozen heartbeat began. The lease may end before that whenever `db_operation_timeout > fence_margin`, and a recovering worker would then still wait and die.
    - The lock bound keeps a recovering worker that meets an ordinary lock wait alive, but the lock is never released.
  - **The idle bound can derive from `db_operation_timeout`.** Every supervisor transaction runs inside one call under the operation guard, so a running supervisor never sits idle in a transaction that long.
  - **The lock bound needs its own value.** Deriving it from `db_operation_timeout` would invent a ratio, and operational values need the owner's approval (DEC-034).
- **RUNNER-UP:**
  - **Recovery taking the attempt lock with `NOWAIT` or `SKIP LOCKED`** (with the idle bound): precise, but it changes `lifecycle.py` and DEC-037, and does not protect heartbeats or completions that wait for a lock other clients hold.
  - **Deriving `lock_timeout` as a fraction of `db_operation_timeout`:** one fewer variable, but an invented ratio.
  - **A server-wide `idle_in_transaction_session_timeout`:** it would cover every client, but it is a database-administration decision, not the worker's.
- **TRADE-OFF:**
  - **Operators set a tenth variable,** and `db_operation_timeout` can no longer be 1 ms.
  - **A paused but healthy worker loses its session.** A SIGSTOP, a suspended VM or a debugger paused between two statements of a transaction for `db_operation_timeout` has its transaction rolled back. That is safe, because the transaction is atomic, but it is not limited to defective workers.
  - **A long but legitimate lock wait is retried,** which costs liveness under contention, not correctness.
  - **Only worker supervisor sessions are bounded.** Other clients can still hold locks; workers then retry, and one whose heartbeat cannot lock its attempt for longer than its lease fences itself.
  - **No timing guarantee.** These are bounds, not a recovery-time SLA. `lock_timeout < db_operation_timeout` does not guarantee that a lock wait ends before the watchdog: the operation guard bounds the whole call, so earlier statements of the same transaction, the network and scheduling can use up the difference.
- **VERIFICATION:**
  - **`tests/unit/test_worker_settings.py`:**
    - `lock_timeout` is required, read from the environment in ISO 8601, at least 1 ms;
    - it must be strictly shorter than `db_operation_timeout` after rounding (1999 ms passes; 1999.001 ms, 2 s and 3 s are refused);
    - the exact-rounding case (2007 ms);
    - `db_operation_timeout` 2 ms with a 1 ms lock passes, and 1 ms is refused.
  - **`tests/unit/test_worker_runtime.py`:** both options appear in whole milliseconds, rounded up, after an existing `search_path`, including 2007 ms exactly.
  - **`tests/integration/test_worker_runtime.py`** (real PostgreSQL):
    - **idle bound:** a session opened with the worker's exact options shows both settings. It survives an idle half of the bound, then is ended about the bound after it was abandoned; its row lock is released, nothing committed, the next use raises `25P03`, and the connection reads closed.
    - **lock held during recovery:** with another session holding an expired attempt's row lock, a worker's sweeps fail with `LockNotAvailable` while no watchdog guard fires, the worker stays alive and the attempt is untouched. Once the lock is released, it revokes the attempt (`lease_expired`) and runs the task to success on attempt 2.
    - **lock held during a heartbeat:** the heartbeat is unconfirmed (`LockNotAvailable`), then renewed after the lock is released, with no guard firing.
    - **lock held during a completion:** the completion is retried (`LockNotAvailable`) with nothing partial, then recorded once.
  - **`tests/integration/test_worker_process.py`** (real processes, the deterministic reproduction turned around):
    1. Worker A's heartbeat is held at the proxy after its row lock, A is frozen with SIGSTOP, and the write is let through.
    2. The test proves the window was hit: the session is `idle in transaction` and a `NOWAIT` probe fails.
    3. PostgreSQL ends A's session, releasing the lock and rolling the renewal back (the lease is unchanged).
    4. Worker B, which never dies and never hits its operation guard, revokes attempt 1 (`lease_expired`) and runs attempt 2 to success, the only success recorded.
    5. Resumed, A ends through its watchdog (exit 4 or 3) or has its late write refused. The attempt rows and result are unchanged.

    It passed in 10 of 10 consecutive runs, each proving the window was hit.
  - **Mutation checks:** locally, each of these failed tests, with the files restored by hash:
    - without the idle bound, the process test and the idle test fail;
    - without `lock_timeout`, or with it equal to the statement bound, the lock-held recovery test fails;
    - without the relationship check, or with floating-point rounding, the settings tests fail.

## A command that runs a worker

Entry DEC-051 records `orqestra worker run`, agreed in the worker command design audit
(owner decisions 01–08). The owner accepted it on 2026-11-07;
"implemented" means verified by the tests named, locally and in GitHub CI: it was
merged in PR #15 (`71fab72`), with both CI jobs passing for the pull request (run
40760433076) and for `main` (run 40761239383).

### DEC-051 — `orqestra worker run`: explicit built-in kinds, fail-fast checks, then the existing runtime
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-28) · implemented (`orqestra.cli`, `orqestra.worker.builtin`, `persistence.migrate.verify_current`)
- **WHAT:**
  - **The command.** `orqestra worker run --kind KIND [--kind KIND …]`, a subcommand of the existing argparse CLI. `--kind` is required (argparse exits 2 without it) and repeatable; duplicates collapse.
  - **A fixed registry.** `orqestra.worker.builtin` holds the only handlers, two diagnostics for development and tests. Kinds are checked by `check_job_kinds` and matched exactly; nothing is imported dynamically.
    - `diagnostic.echo` returns its payload.
    - `diagnostic.wait` takes exactly `{"seconds": n}`: a finite, non-negative `int` or `float`, not a `bool`, at most `execution_timeout`. It waits on its read-only cancellation signal (an event wait, no polling) and returns `{"waited_seconds": n}`, or raises `HandlerCancelled` once told. Any other payload raises `PermanentError` before waiting.
  - **Startup, in order.** Each refusal is one JSON line on stdout and exit 1, before any worker exists:
    1. `Settings` and `WorkerSettings` validate. Errors carry each field's location, message and type, never its input.
    2. Every kind is built in.
    3. No `diagnostic.*` kind is named when `ORQESTRA_ENVIRONMENT=production`.
    4. `ORQESTRA_DATABASE_URL` is set and parses; a malformed one is reported generically.
    5. One bounded `connect_supervisor` connection runs `verify_current`.
  - **`verify_current`.** One `SET TRANSACTION READ ONLY` transaction of catalog and history `SELECT`s, with no migration lock. It requires the history to equal the packaged migrations exactly, refusing:
    - an uninitialized schema, or an empty history;
    - a history that is behind (a prefix), ahead, renamed, edited or has a gap;
    - no current schema.

    It never calls `migrate`.
  - **Database errors at startup** are reported by type and SQLSTATE only. There is no startup retry.
  - **Then the existing runtime.** `Worker` with `connect=functools.partial(connect_supervisor, …)`, so its sessions carry DEC-050's bounds, then `run_until_signalled`, then exit 0. The worker's exits 3 to 9 and its nonzero ends are unchanged.
- **WHY:**
  - It makes the verified runtime runnable without new infrastructure.
  - Explicit kinds mean a worker never runs work nobody asked for.
  - A diagnostic `wait` handler lets the real entrypoint show busy shutdown, crash recovery and cancellation.
  - Before the preflight, a worker against an unmigrated or older schema retried `claim_failed` forever.
  - Fail-fast startup surfaces misconfiguration rather than hiding it.
  - A history that is only a prefix is refused, unlike in `migrate`, because the code needs every migration applied.
- **RUNNER-UP:**
  - `python -m orqestra.worker` as a second entrypoint convention.
  - Dynamic `module:attr` handler registries: arbitrary code from configuration.
  - Retrying the database at startup.
  - Waiting for the first real handler before any command exists.
  - An `orqestra submit` command: deferred at the time; DEC-064 later added `orqestra job submit` and `orqestra job status`.
- **TRADE-OFF:**
  - **Diagnostic kinds ship in the package.** They run only when named, and are refused in production. An unset `ORQESTRA_ENVIRONMENT` defaults to `development`, so a production deployment must set it.
  - **Exit 1 has two meanings,** a refused start or an unexpected exception; the JSON line or the traceback tells them apart.
  - **The preflight connection is not under the watchdog.** It is bounded by `connect_timeout` and `statement_timeout` only. No task is held then, and signals are not yet installed, so SIGTERM ends it at once.
  - **The startup check is not a lock against change.** A migration applied after it is not detected until the worker's own calls meet it.
  - **Unsupported kinds stay queued** with no warning.
  - **There is no service, supervision or deployment.**
- **VERIFICATION:**
  - **`tests/unit/test_worker_builtin.py`** (29):
    - `echo` returns its payload unchanged;
    - `wait` returns what it waited, and accepts exactly its bound;
    - 18 malformed or unbounded payloads are permanent failures, refused at once: over the bound, `True`, `False`, a string, `None`, a list, negative, infinite, NaN, missing, extra or misspelt keys, non-objects;
    - a cancelled wait ends early as `AttemptCancelled`;
    - the registry is valid and bounded by the settings.
  - **`tests/unit/test_cli.py`:**
    - `--kind` is required (exit 2);
    - every missing worker setting is listed, `lock_timeout` included, without its input;
    - an invalid value is not echoed, a broken relationship is refused, and a lock wait not shorter than the database bound is refused (three values);
    - unsupported, wrongly cased and blank kinds are refused, and diagnostics are refused in production;
    - a missing, malformed or unreachable DSN is refused without the password or any part of the DSN;
    - a schema that is not current is refused;
    - no worker is created or run after any refusal;
    - a successful start runs the preflight first, registers exactly the deduplicated kinds, and connects as the preflight did, with the runtime's `connect_supervisor`.
  - **`tests/integration/test_migrations.py`:**
    - a current schema passes with nothing changed, on a connection whose transactions are all read-only;
    - the check sends exactly `SET TRANSACTION READ ONLY` and then only `SELECT`s;
    - it does not wait for a held migration lock;
    - seven bad histories (uninitialized, empty, behind, ahead, renamed, tampered, gap) and a missing schema are each refused with the schema left exactly as it was.
  - **`tests/integration/test_worker_cli.py`** (the installed `orqestra` script, real processes):
    - **refused starts:** a schema that is behind, an unreachable database with a password, and production with a diagnostic kind each exit 1 with a JSON error, never log `worker_started`, never print the password, and leave the task queued;
    - **two workers and ten tasks:** while one holds a 10 s wait, the other completes all nine echoes. Every task has exactly one attempt and its payload as its result, and an unsupported kind stays queued with no attempt;
    - **crash recovery:** a SIGKILLed worker's attempt is revoked (`lease_expired`) and run to success by another;
    - **a stale worker:** one stopped with SIGSTOP past its lease, then resumed, ends or has its write refused, and never changes the newer result;
    - **cancellation:** a committed request cancels a running wait, and the worker carries on;
    - **signals:** SIGTERM or SIGINT while idle exits 0; a busy worker that settles within the drain exits 0, and one that cannot exits 9;
    - **a lost database while busy** exits 3.

## Deterministic task partitioning

Entry DEC-052 records the partitioner, agreed in the partitioning read-only assessment. The owner
accepted it on 2026-11-07. It was merged in PR #16
(`da5b4b3`), with both CI lanes passing for the pull request (run 40770878527) and for
`main` (run 40771746389).

### DEC-052 — An ordered item list is partitioned into contiguous, fixed-size batch tasks with index-derived keys
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-10-28) · implemented locally (`orqestra.domain.partitioning.partition`)
- **WHAT:**
  - **The function.** `partition(items, *, batch_size, max_attempts, key_prefix="batch") -> tuple[TaskSpec, ...]` is pure, standard library only, and has no database, worker or invoice knowledge.
  - **Batches.** Contiguous slices of `batch_size` in input order; the last may be shorter. Repeated items are kept.
  - **Keys.** `"<key_prefix>-<index>"`, with the index zero-padded to the width of the last index, so code-point order (DEC-027) is batch order.
  - **Payloads.** `{"offset": <index of the batch's first item>, "items": [...]}`.
  - **Retries.** `max_attempts` is passed through; the database enforces its range (DEC-026).
  - **Refusals.**
    - **`ValueError`:** no items, a `batch_size` below 1, or a blank or NUL-containing `key_prefix` (the rules of `tasks_task_key_not_blank` and of PostgreSQL `text`).
    - **`TypeError`:** a `str` or `bytes` in place of a list, a non-`int` `batch_size` (`bool` and `float` included), or a non-`str` prefix.
  - **The caller's sequence is read once and never changed.** Items are not copied, following the read-only payload convention (DEC-026).
  - **No limits:** there is no default batch size and no maximum item count, batch size or payload size.
- **WHY:**
  - **Determinism is what idempotent replay needs.** Positional slicing is deterministic for any JSON item, and DEC-025 accepts a replay only when the resulting tasks are equal.
  - **Index-derived, zero-padded keys** need no item identity, never collide, and list in batch order.
  - **The offset** lets later aggregation map results back to input positions, whatever order the batches are claimed in (DEC-033).
  - **Mechanism now, policy later.** It is independent of any workload. Choosing batch sizes and limits needs the synthetic workload and its measurements.
- **RUNNER-UP:** keys derived from item identity: a caller-supplied id per item, with each batch keyed by its first item's id or by a hash of its content.
- **WHY NOT:**
  - it needs an item-identity contract the engine does not have;
  - hashed keys do not list in input order;
  - identical batches would collide;
  - it is harder to keep deterministic.
- **TRADE-OFF:**
  - **Positional keys tie a task to its place in the input.** Inserting an item shifts every later batch, so a changed input under the same idempotency key is a conflict, not a partial reuse. That is correct for an immutable job.
  - **Key width depends on the batch count,** which is stable for a given input.
  - **PostgreSQL compares tasks, not settings.** A different `batch_size` replays when it yields the same tasks (for example, any size at least the item count) and conflicts when it does not.
  - **Large jobs are easy to build before any size bounds are measured** (BS-05). A job is still one transaction, and a replay compares every task (DEC-024, DEC-025).
  - **Items within a batch are one handler call's work.**
- **VERIFICATION:**
  - **`tests/unit/test_partitioning.py`:**
    - **exhaustively, for every length from 1 to 200 and every batch size from 1 to n+2:** the batches reproduce the input in order with repeats; the count is ⌈n / size⌉; every batch but the last is full; the offsets are index × size; the keys are unique and already sorted; `max_attempts` is constant;
    - **key padding:** one, ten, eleven and 101 batches;
    - **exact values:** an exact multiple, a shorter last batch, a batch larger than the input, and exact `TaskSpec` values with repeats;
    - **determinism:** equal output for equal input, from a list or a tuple, with the caller's list unchanged; different batch sizes that make the same tasks give equal tasks;
    - **pass-through and validity:** `max_attempts` 1, 3 and 100 pass through, and the tasks form a valid `JobSubmission`;
    - **refusals:** empty input (list and tuple); seven non-sequences (`str`, `bytes`, `bytearray`, a set, a generator, a dict, an int); eight bad batch sizes (0, −1, `True`, `False`, 1.5, 2.0, `"2"`, `None`); five bad prefixes.
  - **`tests/integration/test_job_submission.py`** (real PostgreSQL):
    - 23 items in batches of 2 are stored as 12 tasks, which `list_tasks` returns as `batch-00` … `batch-11`, with the right offsets, every item in order and `max_attempts` stored;
    - under one idempotency key, batch sizes 6 and 100 replay a size-5 submission of five items (`created=False`, the same job), while size 2 raises `IdempotencyConflictError` and stores nothing.
  - **Mutation checks:** locally, each of these failed tests, with the file restored by hash:
    - no zero-padding;
    - per-batch instead of per-item offsets;
    - a dropped last batch;
    - reordered items;
    - accepting a `bool` batch size;
    - accepting empty input;
    - accepting a NUL prefix.

## Durable retry scheduling

Entry DEC-053 records retry backoff, agreed in the retry scheduling assessment (decisions D1–D4).
The owner accepted it on 2026-11-07. The original
GitHub Actions run 41538196569 passed both lanes on head `e4aa59b`; later checks are
recorded on PR #17.

### DEC-053 — A requeued task waits until a durable `available_at`, from a required policy with deterministic jitter
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (retry scheduling decisions D1–D4) · implemented on PR #17 (`orqestra.domain.retry`, migration 0004, `orqestra.persistence.lifecycle`)
- **WHAT:**
  - **Durable eligibility (D1).** Migration 0004 adds `tasks.available_at timestamptz NOT NULL DEFAULT now()`. A task may be claimed only when `available_at <= statement_timestamp()`. Both claim paths check it inside the atomic claim: discovery's `FOR UPDATE SKIP LOCKED` query and the claim's `UPDATE … WHERE`, so a refused claim updates no row.
  - **When a retry is scheduled (D2).** Only when the task is requeued: a transient failure with attempts left, or a `lease_expired` or `deadline_exceeded` revocation with attempts left and no cancellation request. Then `available_at` = the database clock read after the locks (DEC-035) + `retry_delay(policy, task_id, n)`, where *n* is the ended attempt's generation. A success, a permanent or unclassified failure, exhausted retries and any cancellation, including `cancellation_grace_expired`, leave it unchanged.
  - **The delay.** `min(max_delay, base_delay * 2**(n-1))`, scaled by a factor in `[0.5, 1]` from the first 64 bits of SHA-256(task UUID bytes ‖ *n* as 8 big-endian bytes). Integer microseconds throughout, standard library only; never below half the capped value (rounded up), never above `max_delay`, and no huge integers for large *n*. An *n* of 2⁶⁴ or more is a `ValueError`: that is a contract of the pure function, since stored generations are 1–100. A retry time past the representable range is also a `ValueError`; completion or recovery then raises it and changes nothing.
  - **The policy.** `RetryPolicy(base_delay, max_delay)` is frozen, with no defaults: both positive and representable (the `LeasePolicy` rule), and `base_delay <= max_delay`. `complete_attempt` and `revoke_next_expired` require it as a keyword. The worker builds it from two required settings, `ORQESTRA_WORKER_RETRY_BASE_DELAY` and `ORQESTRA_WORKER_RETRY_MAX_DELAY` (DEC-044 note).
  - **An early explicit start (D3).** `start_attempt` on a queued task that is not yet eligible raises `TaskNotEligibleError`, a subclass of `TaskNotStartableError` with `state = queued` and the task's `available_at`, and changes nothing.
  - **Ordering (D4).** Unchanged: among eligible tasks, `created_at` then `id`. No fairness rule and no index.
  - **Not exposed on `Task`.** `available_at` is a column only; the `Task` record and `TASK_COLUMNS` are unchanged.
- **WHY:**
  - **Durable, because recovery is the database's job.** The retry time must survive the worker that scheduled it: a crash, a restart or another worker claiming next. Anything held in a worker's memory would be lost exactly when retries matter.
  - **The database clock** is the one every claimant compares against, read after the locks as for leases (DEC-035), so waiting for a lock does not shorten the delay.
  - **Revocations are retried like transient failures (DEC-038),** so a hang or a dead worker is not retried back to back. A grace expiry ends the task, so there is nothing to delay.
  - **Deterministic jitter** spreads retries of many tasks failing together (a provider outage) without randomness that tests and replays cannot reproduce; the same task and attempt always get the same delay, whichever process computes it.
  - **A required policy** follows DEC-034: no production value is invented.
  - **A distinct subclass** lets callers tell "not yet" from "never" without breaking code that catches `TaskNotStartableError`.
- **RUNNER-UP:**
  - Delays held by the worker (sleep before releasing, or skip recently failed tasks in memory): lost on a crash and invisible to other workers.
  - Random jitter: not reproducible in tests or across processes.
  - A per-task or per-job-kind policy stored in the database: needs a policy model nobody has decided; deferred.
  - Ordering by `available_at`, or a claim index: changes fairness and planning without measurements (D4).
- **WHY NOT:** each runner-up either loses the retry time with its process, cannot be tested deterministically, or decides policy or performance questions that wait for measurements.
- **TRADE-OFF:**
  - **Policy consistency is not enforced.** The process that records a requeue decides the delay; workers configured differently schedule differently, and nothing detects it.
  - **A delay is an earliest time, not a schedule.** A task runs at the first poll of a live, supporting worker after it, behind any backlog.
  - **Claims scan delayed tasks too.** With no index, a large backlog waiting out its delays is scanned and sorted by every claim; not measured.
  - **Versions must switch together.** Code from before 0004 ignores `available_at` and would retry at once. `orqestra worker run`'s exact-history check refuses mismatched versions at startup (DEC-051), but does not stop a worker already running, and direct callers of the lifecycle functions are not checked.
  - **The delay does not depend on the error**, for example a rate limit's `Retry-After`.
  - **Many attempts revoked together after an outage** are spread by the jitter only.
- **VERIFICATION:**
  - **`tests/unit/test_retry.py`:** both delays required; zero, negative, unrepresentable and non-`timedelta` values refused for each; base above max refused, equal allowed; immutable; for 41 task IDs and every *n* from 1 to 100, the delay lies between half (rounded up) and all of the capped exponential; doubling to the cap; huge *n* (up to 10⁹) capped without overflow; a 1 µs policy always waits 1 µs; equal delays for an equal task and attempt, also in a fresh interpreter; no two of 41 tasks share a capped delay, and a new factor for every attempt; *n* below 1, 2⁶⁴ or more, `bool`, `float` and `str` refused, and 2⁶⁴ − 1 accepted within the same bounds; the first attempt never waits longer than `base_delay`; `retry_at` adds the delay, and an unrepresentable time is a `ValueError`.
  - **`tests/unit/test_worker_settings.py`:** both settings required and parsed from ISO 8601; base above max refused; the policy built from the two settings. Unit fakes check the runtime passes the configured policy to `complete_attempt` and to each recovery sweep.
  - **`tests/integration/test_attempt_lifecycle.py`, section 23 (real PostgreSQL):** a new task is eligible at once; a delayed older task is passed over for an eligible newer one, and only delayed tasks give `None`; the predicate both claims use is inclusive at fixed instants (equal is eligible, 1 µs before is not), and an explicit start succeeds 1 µs past it; an early explicit start raises `TaskNotEligibleError` (also a `TaskNotStartableError`) with every row unchanged, and succeeds once eligible; 8 concurrent claimants over 12 tasks, 4 delayed, take each eligible task once and no delayed one; a transient failure is delayed from a clock between the attempt's end and the moment after; `lease_expired` and `deadline_exceeded` revocations set exactly the recovery's time plus that generation's delay, for generations 1 and 2; a permanent failure, exhaustion, a success and a grace expiry leave `available_at` unchanged, and so do an unclassified failure, a transient failure or an acknowledged cancellation with a cancellation pending, a `lease_expired` or `deadline_exceeded` revocation with a cancellation pending, and a revocation on the last attempt (each asserting the final state and the revocation reason); a requeued task with a later `available_at` is claimed before a newer eligible one (claims follow `created_at`, not `available_at`); an unrepresentable retry time, made deterministic by pinning the instant fed to the real guard to the last minute of year 9999 under a one-hour policy, makes completion and recovery raise `ValueError` and change nothing, and the same call then succeeds; a task cancelled during its backoff is cancelled at once and never runs, and its refusal is not the eligibility subclass; a revoked worker's completion during the backoff raises `AttemptRevokedError`, and after the next generation started `StaleAttemptError`, changing nothing.
  - **`tests/integration/test_worker_runtime.py`:** a worker retries a transient failure three times, each attempt starting no earlier than the previous one's end plus its delay, and the exhausted attempt schedules nothing.
  - **`tests/integration/test_worker_process.py` (real subprocesses):** retries spaced by the stored delay; a worker killed with SIGKILL is recovered by a second process, which sets exactly the recovery's time plus the delay and claims no earlier; SIGTERM during a one-minute delay exits 0 and leaves the task and its attempt unchanged; two workers polling the same task both wait for its time.
  - **`tests/integration/test_migration_0004.py`:** pinned to migrations 0001–0004, so a later migration cannot change what it exercises; from an authentic, populated 0003 schema (no synthetic column), the upgrade keeps every task and attempt row, stamps one `available_at` between clock reads before and after, makes queued tasks claimable by current code and leaves the running attempt renewable; a repeated run is a no-op and the history verifies; it fails at once (`55P03`, "could not lock tasks") while a reader or a row lock holds `tasks`, leaving no history row and no column; it does not lock `attempts`; the column is `timestamptz NOT NULL` with a `now()` default, and the set of indexes on `tasks` is the same before and after.
  - **Historical upgrade tests.** Migrations 0002 and 0003 are still tested against genuine pre-0004 schemas, pinned to the migrations they exercise. Because current lifecycle code reads `available_at`, those tests reproduce the claim, start, success and revocation of that time in a test-only module (`tests/integration/historical.py`), raw SQL in the same lock order, deciding with the production pure functions. One test now shows the 0002 schema refusing the `cancellation_grace_expired` reason and its transaction leaving nothing changed, rather than current code failing closed on an old schema, which is no longer possible by design.
  - **Mutation checks (the author's earlier local run, not repeated since):** each of these failed tests, with the file restored and checked by hash: discovery ignoring `available_at` (6 cases); the claim's `UPDATE` ignoring it; an exclusive boundary; completion or recovery scheduling no retry; completion or recovery scheduling on every ending; no jitter; an exponent off by one; no cap.
- **Note (2026-11-09):** if the database clock steps back between discovery and the claim, so the task discovery locked is no longer eligible at the claim instant, `claim_next_attempt` returns `None` and changes nothing, with a warning naming the task; an explicit `start_attempt` still raises `TaskNotEligibleError`. `tests/integration/test_attempt_lifecycle.py` section 23.

## Worker architecture

### DEC-054 — Independent single-slot workers without an Orqestra-owned service
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · owner-approved direction (2026-11-03, worker architecture revision r2 `8d656bd386ecee65e8db5214ca59a2879bed02b2c10c8a2f6effc0387a8c0a46`)
- **WHAT:**
  - One `orqestra worker run` process has one handler slot. Scale by running N independent worker processes against PostgreSQL; do not add in-process threads or asyncio slots.
  - This architecture adds no Orqestra-owned worker service or restart supervisor. The operator starts workers with identical settings and owns restart policy; deployment is out of scope.
  - Keep lease, execution-timeout, retry-delay and cancellation-grace settings explicit and required, with no defaults. Numeric operating values and per-kind policies remain undecided pending measurement (BS-05 or the synthetic workload).
  - Cover the deadline boundary at worker level: let a handler run beyond `execution_timeout`, verify that the worker is fenced, then verify recovery revokes the attempt as `deadline_exceeded` (implemented in PR #18).
- **WHY:**
  - DEC-044/045 already define the single-slot process model and require process termination when ownership cannot be confirmed. Multiple slots in one process would reopen that design.
  - Before PR #18, worker tests covered multi-process execution and crash recovery but not a handler that outlives its execution deadline and the resulting `deadline_exceeded` revocation. PR #18 closes that coverage gap without changing production code.
  - No service or default timing values are needed to verify the approved worker behavior; both require separate operational decisions or measurement.
- **RUNNER-UP:** Multiple worker slots in one process (threads or asyncio), or an Orqestra-owned worker supervisor. These reopen the DEC-045 stop model or the undecided hosting boundary.
- **WHY NOT:** The existing runtime can end a process when a handler may have lost ownership; it cannot safely isolate sibling slots inside that process. DEC-051 also records that Orqestra owns no service or supervisor.
- **TRADE-OFF:** The operator must launch enough workers for progress and keep their settings consistent. Orqestra provides no restart or recovery-time guarantee while no compatible worker is live.
- **VERIFICATION:** `tests/integration/test_worker_process.py::test_a_handler_outliving_the_execution_timeout_is_fenced_and_revoked_as_deadline_exceeded` (PR #18) holds a handler open past its six-second test deadline, confirms the worker exits with the fenced result before the lease ends, and checks that recovery records `deadline_exceeded` and requeues the task. The test also rules out connection-loss, lease-loss, and cancellation as the reason for fencing. This is integration coverage; it does not add production behavior. Existing tests continue to cover stale-worker fencing, SIGKILL recovery, and one authoritative result.

## Invoice verification

### DEC-055 — Invoices are verified by deterministic checks first; a provider only reads an ambiguous amount string, and the same checks decide
- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **WHAT:**
  - **Statuses.** Each synthetic invoice becomes `verified`, `rejected`, `needs_review` or `malformed`. The schema is closed: `invoice_id`, non-empty `lines` of `line_id`, `quantity` and `unit_amount`, and `total`. Identifiers match `[A-Za-z0-9._-]{1,64}` and are never sent to a provider or stored in a result. An amount is an integer below 10^64 in opaque minor units, an ASCII digit string, or an *ambiguous* non-blank printable string of up to 64 characters. There is no tax, currency, rounding, date or vendor rule.
  - **Rules.** `quantity_positive` per line and `total_matches_lines`. Any `FAIL` is `rejected` and all `PASS` is `verified`, both without a provider call.
  - **Provider step.** Otherwise one route is chosen for the invoice and its provider is called once per ambiguous amount, in document order. The request carries only the capability, the route, the field kind and the raw string. A reply must be a digit string (`re.fullmatch("[0-9]{1,64}", s, re.ASCII)` is the only digit test). The value is substituted and the same checks run again, so the checks decide the status and the result is marked `provider_assisted`. An unacceptable reply is `needs_review` (`provider_response_invalid`); no eligible route is `needs_review` (`no_eligible_route`). Empty and blank amounts are `malformed` and never reach a provider.
  - **Routing.** `choose_route` is pure. A route is eligible if it declares `interpret_amount`, has an integer estimate of at least 0, and the estimate is at most `max_cost_units` when one is given. The lowest `(estimated_cost_units, route_id)` wins. A missing estimate is excluded, never read as zero. There is no failover.
  - **Handler.** `invoice_verification_handler(providers, routes, timeout, max_cost_units=None)` takes the DEC-052 batch payload and returns `{"offset", "results"}`. It is not registered in `worker/builtin.py` or the CLI (DEC-051). Cancellation is checked before each item and before each provider call. *Amended by DEC-057:* the factory now also requires `max_provider_calls_per_attempt` and accepts `recorder`.
  - **Errors.** Provider errors carry a code from a closed set; `timeout`, `rate_limited` and `unavailable` are transient, `rejected` and `unsupported_request` permanent. Any other exception from a provider, or a return that is not a `ProviderResponse`, becomes a `PermanentError` with the fixed text `provider_protocol_violation`. A bad batch payload is `PermanentError("batch_payload_invalid")`.
  - **Stored result.** Per item: `index`, `status`, `provider_assisted`, `findings` (rule, verdict, positional path), `issues` (code, positional path), `reason`, and `routing`, which is `{"route_id"}` or `null`. Estimates, call counts and provider-reported usage are never stored in the task result. No migration, ledger, API or CLI change.
- **WHY:**
  - The provider holds no information beyond the string it is given, so it can only interpret that string. Re-parsing its reply and rerunning the same checks keeps provider output validated (`development.md`, "Invoice and provider workloads") and makes outcomes replayable.
  - The engine already provides retry with durable backoff (DEC-053), cancellation and result validation, so none of it is reimplemented.
  - Provider errors are stored as `str(exc)` (`AttemptFailed.from_exception`), so only closed codes may reach them.
  - Providers, prices, budgets and operating timings are undecided (DEC-034); estimates are opaque inputs.
- **RUNNER-UP:** Let the provider return a consistent or inconsistent verdict on indeterminate findings.
- **WHY NOT:** The provider has no information beyond the ambiguous string, its verdict could not be re-checked, and outcomes would not be replayable. Failover across routes was also rejected because it multiplies billable calls; so were registering the handler in the CLI (DEC-051) and a validation library (heavier than DEC-026's style).
- **TRADE-OFF:**
  - `rejected`, `malformed` and `needs_review` items are item results, so the task still succeeds and the job derives as `succeeded` (DEC-031). Aggregation must read the item results.
  - A provider error fails the whole batch task, and a retry runs the whole batch again, repeating calls that already succeeded. Deterministic `request_id`s let a real adapter deduplicate; no exactly-once claim is made.
  - A provider could return a plausible number that makes the arithmetic hold. `provider_assisted` marks every such result; the risk is bounded, not removed.
  - The stored `routing` is an audit of which route the decision chose, for the attempt that completed. It is not usage accounting: calls made by failed or revoked attempts are not represented, and provider-reported usage is not part of it; the usage ledger records it (migration 0005, DEC-059). *Amended by DEC-057:* with a recorder, every call's intent is recorded per `attempt_id` before the call is made, including the calls of failed or revoked attempts. Its result is recorded only when the outcome is known: `answered` with the provider's `usage`, if it reported one, for a `ProviderResponse`; `failed` with its code and no usage for a provider error. Any other exception, a return that is not a `ProviderResponse`, a `BaseException`, a failed `record_result` or a process interrupted before the result leaves only the intent. The durable `UsageSink` of DEC-059 writes these records to the usage ledger, and the stored result still excludes them.
  - The schema is tiny and synthetic. A different schema changes only `model.py` and `checks.py`.
  - There is no cap on provider calls per invoice; it would be an invented policy and remains undecided. *Amended by DEC-057:* there is still no per-invoice cap, but the caller must now set a per-attempt cap.
- **VERIFICATION:** Unit tests cover normalization, both rules, routing (including an exhaustive small-space comparison and permutation invariance), the provider contract and mock, every path of verification, the handler through `run_handler`, a golden replay, the persisted-result shape, an import scan and a runtime network guard. One integration test runs partition, submit, claim, handler and completion against PostgreSQL and reads the retry time after a transient provider error. The results of the fixed test profile are recorded on the pull request, not here.

## Invoice job summary

### DEC-056 — A job's outcomes are summarized in process from one consistent snapshot, and counts are final only when the summary is complete
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · the architecture was owner-approved (revision `D6REVIEW-r2`, SHA-256 `628dd2cc4f0c17332b4c881fad8280676e4190a0a04cde2b9ef75dfc08474582`). This entry treats DEC-055's stored result shape as validated input.
- **WHAT:**
  - **Entry point.** `orqestra.aggregation.invoice.summarize_invoice_job(conn, job_id)` returns an `InvoiceJobSummary`, `None` for an unknown job, `WrongJobKindError` for another kind (before any task is read) and `ValueError` for a job without tasks. A caller already inside a transaction gets `RuntimeError`, as for every operation that owns its transaction. Content problems are reported, never raised. No migration, table, command, HTTP API or dependency is added.
  - **Snapshot.** `orqestra.persistence.snapshots.job_batch_snapshot` runs one `REPEATABLE READ, READ ONLY` transaction whose first statement is `SET TRANSACTION`. It reads the job row, then streams the job's tasks through a named server-side cursor ordered by `task_key COLLATE "C"`, in chunks of `fetch_size` rows. It takes no row locks, does not block workers, and ends the transaction (commit on a normal exit, rollback on an error) however the block is left, including an abandoned stream.
  - **Bounded projection.** Per task: `id`, `task_key`, `state`, the JSON type of the payload and, only for an object payload, its key count, whether `offset` and `items` exist, the text of a numeric `offset` and the length of an array `items`; and `result` only for a succeeded task. Every payload expression sits behind `jsonb_typeof` and nothing is cast, so a payload that is not a batch cannot raise a database error. The payload itself, its items and any other key are never returned.
  - **Pure builder.** `orqestra.workloads.invoice.summary.SummaryBuilder` (no I/O, import-scanned like the other workload modules) folds the rows. A payload is a valid batch by the handler's rule (exactly `offset` and `items`, offset a non-negative integer, items an array). Result indices are absolute, and `offset` is never added. The valid ranges, sorted by offset, must start at 0 and each begin where the previous one ended.
  - **Which outcomes count.** Items of a task that did not succeed are `unprocessed_items`, and its result is never read. A succeeded task's result must be an object with exactly `offset` and `results`, with the payload's offset and one entry per item; otherwise the whole batch is rejected, its items are `invalid_items` and one batch-level anomaly is reported (a stored JSON `null` is such a mismatch). In an accepted envelope an entry is judged by the index it claims: an unusable entry or a status outside the four, or a `provider_assisted` that is not a bool, makes its index invalid; an index outside the task's own range is `index_out_of_range` and claims nothing; an index claimed twice is `index_duplicate` and counts no occurrence, so the outcome does not depend on the order of the entries; a valid entry at an unexpected position is counted and flagged `index_out_of_order`. Nothing is inferred from a job's or task's status.
  - **Contract.** `job_status` and `task_counts` are folded from the same rows with `derive_job_status`. When `expected_items` is not `None`, `processed + unprocessed + missing + invalid == expected`. `complete` needs a succeeded job, `expected_items >= 1`, every item processed, and no anomaly. Anomalies are a closed set of codes with a task key and an index, never a value from the data, and at most `MAX_SAMPLES` of them are kept, chosen by `(task_key, index, code)` so the choice does not depend on arrival order.
- **WHY:**
  - DEC-055 makes `rejected`, `malformed` and `needs_review` item results inside a succeeded task, so a job's status says nothing about its items (DEC-031). Only the item results can be aggregated.
  - A coherent read of the job row and all its tasks needs one snapshot. `REPEATABLE READ, READ ONLY` gives it without locks, and the pure builder keeps every counting rule unit-testable without a database.
  - Task payloads are any JSON value and the README itself submits a non-batch payload to this kind, so the projection must be total. Counts are per-task tallies and are not a final job tally unless `complete`; overlapping ranges can count one absolute index in multiple tasks while also making the summary incomplete.
  - The aggregation package exists because `workloads/` must open no connection and `persistence` must not know a workload.
- **RUNNER-UP:** Compute the aggregate in SQL with jsonb aggregation and return only counters.
- **WHY NOT:** Per-entry validation, duplicate detection and anomaly samples are awkward and hard to test in SQL; it would restate the handler's payload rule in a second language; and it would put summary semantics in the persistence layer. Client-side streaming keeps memory at one fetch chunk plus a counter and one `(offset, length, task_key)` triple per task. The database may still need a blocking sort over result rows for the requested order, potentially spilling to temporary files.
- **TRADE-OFF:**
  - The named cursor is the first in `src/`, one more pattern to review. The snapshot holds back the vacuum horizon for as long as the read lasts; the duration is an observation, with no threshold.
  - `ORDER BY task_key COLLATE "C"` may not use the default-collation unique index. The order only makes the stream repeatable (the partition check sorts by offset itself), and its cost is measured, not asserted.
  - `observed_at` is the `statement_timestamp()` of the first query, not the snapshot instant.
  - Counts are per-task tallies, not a final job tally unless `complete`. `provider_assisted_items` counts items, not provider calls or spend: a retried batch repeats calls, and usage is not part of the summary; see the usage ledger and DEC-061.
  - A `complete` summary is stable only if the engine's contracts hold (`SUCCEEDED` has no outgoing transition and only a completion writes a result). The schema does not enforce that, so a manual edit could change it. A summary that is not complete is a point-in-time observation.
  - A batch with `items: []` is valid but is never `complete`. Batch size and limits stay undecided (BS-05).
- **VERIFICATION:** Unit tests (`tests/unit/test_invoice_summary.py`) cover absolute-index mapping at offsets 0, 3 and 100 with the offset-added and relative-index regressions, every anomaly code and every batch-rejection condition, the partition rules, a conservation check of `processed + unprocessed + missing + invalid == expected` and order independence over every combination of up to three tasks drawn from twelve shapes, `complete` in each negative case, the deterministic sample bound, frozen and zero-filled output, payload-shape parity with the handler, and that no value from the data reaches a summary. Integration tests (`tests/integration/test_invoice_summary_pg.py`, real PostgreSQL) run the real lifecycle for happy paths and use raw SQL only for corrupt fixtures: payloads `5`, `"x"`, JSON `null`, `[1]`, `{"invoice_id": "INV-1"}`, `{"offset": 1.5, "items": []}` and `{"offset": 0, "items": "x"}` are first shown to fail an unguarded projection and are then reported without a database error; the projection is compared with the Python description of each payload; a completion committed after the job row is read is invisible to the stream, and a companion test asserts that weakening the isolation level to `READ COMMITTED` makes it visible, so the first test is sensitive to the level; partial jobs and nine kinds of corrupt result; a write inside the snapshot fails; table contents are unchanged by a read; the connection ends outside a transaction after success, an error and an abandoned stream, in autocommit and manual-commit mode; a caller-open transaction is refused; and fetch sizes 1, 2 and 1,000 give identical summaries. Three tests marked `performance` (never in a default lane) seed with raw SQL and record time, `tracemalloc` peak and RSS at 50,000 invoices for batch sizes 1, 10, 100 and 1,000, the ordering and fetch-size cost, and reads made while tasks complete, asserting correctness only. The results of the fixed test profile are recorded on the pull request, not here.

## Provider call recording

### DEC-057 — Every invoice provider call is numbered under a required per-attempt cap and recorded before and after it is made
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · the design was agreed as revision `RECORDER-r2` (SHA-256 `0ac89d9e569b6f246be23f8173597002038346ae59be4fa2b5a2c464932e25a3`). It amends DEC-055's handler signature and its "no cap" trade-off.
- **WHAT:**
  - **Records.** `orqestra.calls.recording` is pure: it imports only `dataclasses`, `enum`, `typing`, `uuid` and `orqestra.calls.provider`. A `ProviderCallIntent` holds `attempt_id`, `call_seq` (0 to `MAX_CALL_SEQ = 2**31 - 1`), the absolute `item_index` (0 to `MAX_CALL_SEQ`), `route_id`, `capability`, `field_kind` and `estimated_cost_units` (0 to `MAX_COST_UNITS = 2**63 - 1`). These are the usage ledger's `INTEGER` and `BIGINT` ranges (`orqestra.persistence.provider_usage`). `route_id` and `capability` are only checked to be strings. On the verification path, `route_id` comes from a checked `RouteOption`, which uses the ledger's identifier pattern, and `capability` is the constant `interpret_amount`, so the ledger accepts every intent made there once a durable recorder adds the provider. An intent built directly can carry a `route_id` the ledger refuses; a durable recorder then raises in `record_intent`, before the call. A `ProviderCallResult` holds `attempt_id`, `call_seq`, `outcome` (`answered` or `failed`, the ledger's values), `failure_code` (set exactly when failed) and the provider's own `usage`. Its numbering and outcome fit the ledger, but `usage` is bounded only by `ProviderUsage` (up to 64 characters, any number of decimal places), which is looser than the ledger's amount range (below `1E+20`, at most 18 decimal places). Both are frozen and keyword-only, and both refuse a wrong type or value without quoting it. Neither carries a raw string, a request id, the reply's value, a prompt or `reported_cost_units`.
  - **What a durable recorder adds.** A record does not name the provider, job, task or prompt version; the ledger's intent requires the provider. A durable recorder adds those from its own wiring, so `orqestra.calls` never depends on the worker.
  - **Recorder.** `ProviderCallRecorder` is a runtime-checkable protocol with `record_intent` (it returns only once the intent is recorded; if it raises, no call is made) and `record_result`. `InMemoryCallRecorder` keeps every record in order and raises `ValueError` if, within an attempt, `call_seq` does not start at 0 and go up by 1, if a result has no intent, or if a call gets a second result. It is not durable and takes no lock.
  - **Cap and numbering.** `ProviderCalls(attempt_id=, cap=, recorder=)` in `workloads/invoice/verify.py` is the only source of an attempt's `call_seq` and enforces its cap. Each provider call goes: `cancelled()`, `record_intent`, `interpret`, `record_result`, then the reply is judged as before. An intent outside the ledger's ranges raises `ValueError` when it is built, before `record_intent`, so the task fails and no call is made. Recorder calls sit outside the provider's exception handling, and whatever they raise propagates unchanged. A `ProviderResponse`, valid or not, is `answered` with its `usage`. A provider error is `failed` with its code and is raised again fresh. Any other exception, a return that is not a `ProviderResponse`, or a `BaseException` leaves the call without a result. Every `interpret` invocation, including a mock's cached repeat, gets its own `call_seq`.
  - **Verification order.** `verify_invoice` requires `calls` and `item_index`. DEC-055's steps 1–3 are unchanged. Then, if `calls.remaining` is less than the invoice's number of ambiguous amounts, the invoice is `needs_review` (`provider_call_cap_reached`), with `routing` naming the chosen route, and no call is made.
  - **Handler.** `invoice_verification_handler(providers, routes, timeout, max_cost_units=None, *, max_provider_calls_per_attempt, recorder=None, non_billable_routes=frozenset())`. The cap has no default. It is an `int` from 0 to `MAX_CALL_SEQ`: a `bool` or non-integer raises `TypeError`, and a value out of range raises `ValueError`. With a billable provider (`billable_routes`) and no recorder, the factory raises `ValueError`; a recorder of the wrong type raises `TypeError`. Only the offline `MockProvider` itself (`type(p) is MockProvider`) may be marked non-billable. A recorder, when given, records every call, a marked mock's too. Each `handle()` is one attempt and builds one `ProviderCalls` for `context.attempt_id`, shared by every item and route of the batch. The stored result's shape is unchanged; only the new `reason` value is added.
  - **Isolation.** The import scan allows `uuid`. A new rule forbids `orqestra.persistence` and `orqestra.aggregation` in `calls/` and `workloads/`, and `orqestra.workloads` and `orqestra.worker` in `calls/`.
- **WHY:**
  - A call that is billed must be attributable even when its attempt fails, is revoked or crashes. An intent recorded before the call, and a result after it, is the ledger's model: a call with an intent and no result is visibly unresolved, never silently missing.
  - An attempt-scoped `call_seq` from a single authority is the ledger's key `(attempt_id, call_seq)`. Bounding the cap, the item index and the estimate by the ledger's ranges means a durable recorder can always store a record, and a value it could not store fails before a call is made rather than after.
  - A required cap with no default is the smallest policy that bounds spend per attempt without inventing a production value (DEC-034).
  - Deny-by-default billability is only useful if billable calls cannot go unrecorded, so a billable provider without a recorder is refused at construction.
- **RUNNER-UP:** Wrap each provider in a recording decorator, so the recorder lives with the provider instead of in verification.
- **WHY NOT:** A wrapper cannot see `item_index` or the attempt's `call_seq` unless it is told them on every call; it spreads cap authority across providers; and a wrapped provider is no longer `type(p) is MockProvider`, which breaks the mock exemption. Writing directly to the PostgreSQL ledger from the handler was also rejected: the handler has no connection, and `workloads/` opens none (DEC-056). A per-invoice cap was rejected because it does not bound an attempt's total.
- **TRADE-OFF:**
  - It is a deliberate, source-incompatible change for callers. Nothing in `src/` constructs the handler, and it is registered nowhere (DEC-051).
  - The cap is checked per invoice, so capping is not monotone: after one invoice is capped, a later invoice that needs fewer calls can still be called. That is the cost of never spending billed calls on an invoice the cap would leave undecided.
  - The cap is checked against the remaining count only. Whether cap × (per-call timeout + the recorder's write bounds) fits in `execution_timeout − fence_margin` is not checked: it needs `WorkerSettings` and the durable sink's bounds, so it belongs to worker wiring and registration. **Cap validation is not complete in this change.**
  - The in-memory recorder is not durable. Until a durable recorder exists, a recorder failure is a handler failure (unclassified, not retried), and a failed `record_result` leaves the call unresolved. *Superseded by DEC-059 for sink-wired kinds:* with the durable sink, a ledger failure ends the worker process and never fails the task.
  - A provider can report a `usage` amount that the ledger cannot store. It is known only after the call, so a durable recorder that refuses it leaves a billed call unresolved; how such an amount is handled is left to the durable recorder and is not decided here.
  - A retry is a new attempt with its own cap and its own `call_seq` from 0, so a batch that keeps failing still repeats calls across attempts (DEC-055).
- **VERIFICATION:** Unit tests run offline under `no_network`. `tests/unit/test_calls_recording.py` covers the ledger's outcome values and ranges, frozen keyword-only records without raw or reply fields, every refused type and value (including `item_index` `2**31` and an estimate of `2**63`, with `2**31 - 1` and `2**63 - 1` accepted), messages that never quote a value, and each in-memory recorder rule. `tests/unit/test_invoice_verify.py` covers the call order (intent before the call, result after it), the result for every kind of provider outcome, calls recorded before a later failure, cancellation before the first and before a later call (no intent for the stopped call, earlier calls resolved), a `BaseException`, recorder failures on intent (no call made, the recorder's own error propagates) and on result, an intent outside the ledger's ranges (no call made), cached repeats with their own `call_seq`, records without raw text, the cap at, below and above what an invoice needs, cap 0, the per-invoice cap over invoices needing 2, 2 and 1 calls under cap 3, and `ProviderCalls` validation including `2**31 - 1` accepted and `2**31` refused. `tests/unit/test_invoice_handler.py` covers the same per-invoice cap through `handle()`, the recorder after a cancellation before an item, between items and between calls, one attempt per run, absolute indices in the records, the factory's cap validation (`True`, `False`, `-1`, `1.0`, `"1"`, `None` and a missing cap refused; 0 accepted), the billable refusal, the recorder type check, a recorder failure as the task's failure, an out-of-range estimate or item index failing the task before any call, and a golden result that is byte-identical with a marked mock, with and without a recorder. `tests/unit/test_invoice_isolation.py` covers the layer rule with meta-cases. The integration tests only gain the new wiring. The results of the fixed test profile are recorded on the pull request, not here.


## Provider usage closure

### DEC-058 — An attempt's usage capture is closed in the transaction that ends it: by the worker's own closure, or by recovery at a tier declared per job kind
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · the architecture was agreed in design revision `CLOSURE-r2` (SHA-256 `9311d3256c2c8f5926d2eb78885df71c66b3596ad40efe3f86dd103433f2f8c0`). *Numbering:* it was proposed alongside DEC-057 under the same number and is renumbered DEC-058. The `DEC-057` references in `src/orqestra/persistence/lifecycle.py`, `src/orqestra/persistence/provider_usage.py` and `tests/integration/test_usage_closure_pg.py` mean this entry; those in `src/orqestra/workloads/invoice/handler.py` and `tests/unit/test_invoice_isolation.py` mean DEC-057.
- **WHAT:**
  - **Worker closure.** `complete_attempt(..., usage_closure=None)` takes an optional `UsageClosure`. Before any I/O it must be a `UsageClosure` (`TypeError`), name this attempt and be closed by `WORKER` (`ValueError`). Inside the completion's own transaction the order is: task lock, attempt lock, fencing checks, attempt update, task update, then the closure on the same cursor, by `record_usage_closure`'s rule: an identical stored closure is a replay (`created=False`), a different one raises `UsageReplayConflictError`, and a complete closure that does not count every intent, each with its result, raises `CaptureNotCompleteError`. Any error rolls back the transition too. Every outcome closes, a requeuing transient failure included: the closure belongs to the attempt. `AttemptCompletion.usage_closure` returns it. `CaptureNotCompleteError` is a defect of the caller's count; nothing was recorded, and the caller may complete again with an `incomplete` closure.
  - **Recovery closure.** `revoke_next_expired(..., usage_capture=None)` takes a mapping from job kind to `CaptureTier`, checked before any I/O and copied: not a mapping, or a tier that is not a `CaptureTier`, is a `TypeError`; the kinds are checked by `check_job_kinds`, so an empty mapping, a blank kind, a NUL or a non-`str` kind is refused. After the revocation's attempt and task updates, in the same transaction, it reads the job's kind without a lock (a kind never changes, as discovery assumes). An unmapped kind gets no closure. At T3, the capture is `complete` with the number of intents when none lacks a result, `complete(0)` for none, and `incomplete` otherwise; at T1 and T2 it is `incomplete`. It is always closed by `RECOVERY`. Its write rule never raises a replay conflict: an existing closure is kept and returned with `created=False`; otherwise it inserts with `ON CONFLICT DO NOTHING`, and if a concurrent closure committed first, that one is re-read and returned. Waiting behind an in-flight closure insert for the same attempt is bounded by `lock_timeout` (DEC-050). `AttemptRevocation.usage_closure` returns it.
  - **Accurate late flag.** The intent insert reads its attempt `FOR KEY SHARE OF a`, the lock the foreign key takes anyway, taken before the row is read. An intent that waited behind a lifecycle lock re-reads the committed `attempts.state`, so it is recorded late exactly when the attempt had ended. The task row stays unlocked and the heartbeat is unchanged.
  - **Late results.** `record_late_usage(conn, result)` runs `record_call_result`'s insert-or-match and then reads the attempt's closure, in one READ COMMITTED transaction, and returns `LateUsage(result, closure)` with `after_closure`. It never writes a closure and does not refuse a running attempt: the stored state is the authoritative late flag.
  - **Classification.** The pure `capture_of(AttemptUsage)` says, in order: `OPEN` for a running attempt, even with a closure; `CLOSED` for a complete closure whose `call_count` equals the calls read, each with a result; `LOWER_BOUND` for any other closure, and for a finished attempt with calls but no closure; `NOT_CAPTURED` for a finished attempt with neither. It computes no totals.
  - **Imports.** `NotFoundError` moves to `persistence/_shared.py`; `lifecycle` re-exports the same class. Without the new arguments, completion and recovery behave exactly as before and write no ledger row. No migration, runtime change or dependency.
- **WHY:**
  - The transaction that ends the attempt already holds the attempt row `FOR UPDATE`. Every new intent needs `KEY SHARE` on that row through `provider_calls_attempt_fk`, so the intent set is frozen from the lock to the commit, and results are only added. A count taken after the lock therefore stays true until commit, and atomicity needs no new lock.
  - The lock order is unchanged: task, attempt, then the closure's primary-key slot. The closure trigger reads attempts and the ledger without row locks and never reads tasks (DEC-036), and a standalone closure writer holds no engine row lock, so none of them can form a cycle with the lifecycle.
  - A closure written in a second transaction could be lost in a crash between the commits, and a revoked attempt without a closure looks the same as one whose calls were never captured.
  - The offline mock and other kinds may record nothing. One fleet-wide T3 would close such an attempt as `complete(0)`, an over-claim no function could detect; declaring the tier per kind makes it a per-kind choice that recovery applies.
- **RUNNER-UP:** `complete_attempt` derives the worker's closure from the ledger, and recovery closes the capture in a separate step after the revocation commits.
- **WHY NOT:** A derived worker closure would hide a writer whose count disagrees with the ledger, and cannot tell that a result write failed. A separate recovery step leaves a revoked attempt unclosed after a crash between the two commits, indistinguishable from `not_captured`.
- **TRADE-OFF:**
  - A defect in a worker's closure blocks that completion; the documented fallback is to complete again with an `incomplete` closure.
  - Completion and revocation keep their locks for one more count and one insert, both bounded by a primary key.
  - The kind-to-tier map is configuration that must match what each kind's handlers record. A kind wrongly mapped to T3 is the only way to over-claim.
  - Closures are immutable, so later facts can contradict them. A call recorded after a complete closure, or results after an incomplete recovery closure, leave the attempt `LOWER_BOUND`; history is never rewritten.
  - The equality between a complete lifecycle closure's `call_count` and the attempt's on-time intents holds only for closures the lifecycle wrote where none existed before. A closure recorded by `record_usage_closure` while the attempt runs can be followed by on-time intents.
  - The merged ledger's intent insert changes: it now takes `FOR KEY SHARE OF a`.
  - **Integration with DEC-057.** No durable recorder exists yet: the only recorder, `InMemoryCallRecorder`, writes nothing to the ledger, so a closure cannot see calls recorded there. Until a durable recorder writes a kind's calls to the ledger, that kind must stay unmapped in `usage_capture` and its attempts must not be closed as `complete`; mapping it to T3 would close a revoked attempt as `complete(0)`, an over-claim. Such an attempt reads `not_captured`, or `lower_bound` when calls or an incomplete closure exist, never `closed`. *(DEC-059 adds that durable recorder for the worker; the rule stands for every kind not wired to it.)*
- **VERIFICATION:** Unit tests (`tests/unit/test_usage_closure.py`) cover the `capture_of` table for every finished attempt state and a running attempt, and refuse every bad closure and tier map before the connection is touched. Integration tests (`tests/integration/test_usage_closure_pg.py`, real PostgreSQL, invariants checked after each test) cover: no ledger row without the new arguments, and `NOT_CAPTURED` for an attempt from before migration 0005; each outcome committing its closure with its transition; rollback of attempt, task and ledger for a miscounted or unresolved complete closure, a conflicting stored closure, a failing trigger on the task update or the closure insert, and a backend terminated at the closure insert, for completion and recovery; completion evidence `MATCHES` exactly when the closure exists; fencing; recovery by kind (an unmapped kind gets no closure) and tier, for every revocation reason, keeping an existing closure, and waiting behind an uncommitted closure until it commits or rolls back; late intents and results after each kind of closure; an intent that waited behind a completion recorded as late; eight concurrent completions and late results; a completion behind an uncommitted closure; completion and recovery in both forced orders; per-attempt attribution across a retry; a gated completion refusing heartbeats at `lock_timeout` while results and recovery do not wait; and a 40-round unforced race of heartbeat, completion, recovery, a call writer and cancellation. The results of the fixed test profile are recorded on the pull request, not here.


## Provider usage sink

### DEC-059 — The worker writes each provider call's intent before the call and its result after it, through its supervisor, and closes those kinds at T3
- **Status:** Accepted (owner, 2026-11-06; was Proposed) · its architecture is design revision `SINK-r2` (SHA-256 `e798b77feb09ae0d2d9c49d17a876880a44c822c4ece67140d4b8f5dc50e81df`). Later corrections made while verifying the implementation (wording, the exit code of a refused usage record, and test evidence) change no architecture and do not supersede `SINK-r2`. It is the durable recorder DEC-057 and DEC-058 left open. No migration.
- **WHAT:**
  - **Sink.** `orqestra.worker.usage.UsageSink(kinds, provider_for_route, model_for_route=None, prompt_version=None)` is checked at construction against the ledger's identifier and text rules and does no I/O. `sink.recorder` is a `ProviderCallRecorder` for handler factories; `Worker(usage=sink)` binds the sink to one worker, and every sink kind must be a registered kind.
  - **The T3 contract.** Every handler registered for a sink kind must record every provider call through `sink.recorder`. Otherwise both the worker's closure and recovery's would claim `complete` for fewer calls than were made (DEC-058's over-claim). This change documents the contract and enforces nothing; checking it at registration is a required future step. Every worker of a fleet must use the same kind-to-tier map.
  - **Handoff.** One lock and condition guard a single request slot. `record_intent` converts the record purely (adding the provider, model and prompt version), and raises `ValueError` before posting for an unmapped route, an attempt other than the bound running one, a `call_seq` that does not start at 0 and go up by 1, or a value outside the ledger's rules: as in DEC-057, the task fails and no call is made. A second post while one waits is a `RuntimeError`. If cancellation is already known it raises `HandlerCancelled`; otherwise it posts, wakes the supervisor and waits. `ACKED` means the row committed and the call is made; `CANCELLED` raises `HandlerCancelled`. `record_result` converts the amount to an exact `Decimal`; an amount the ledger cannot hold (`1E+20` or more, more than 18 decimal places) writes no row, clamps and zeroes nothing, leaves the call unresolved, logs `usage_result_unrepresentable` with ids and `call_seq` only, and lets the handler go on. Otherwise it posts and waits; a result never raises for a ledger failure. A result that names no acknowledged, unanswered intent is refused with `ValueError`, as `InMemoryCallRecorder` refuses it.
  - **Cancellation.** The supervisor withdraws (`CANCELLED`) an intent still waiting when it sets the cancellation event, and never takes an intent after that; both are supervisor actions on one thread, so the order is exact. An acknowledged intent is a call in flight: it goes ahead, and its result is never withdrawn. An intent whose earlier write may have committed (a lost answer) and is then withdrawn makes the closure `incomplete`.
  - **Which exit ends an unrecordable record.** While a handler waits, the ownership guard stays armed at the local lease expiry minus `fence_margin`, and the lease is capped at `deadline_at` (`initial_lease`, `renewed_lease`). In a real process the fence therefore always ends the process first: exit 3, `ownership_deadline_passed`, no call, and nothing recorded for the attempt; an unresolved call is closed `incomplete` by recovery at T3. The supervisor's exit 8 (below) stays as defense in depth behind the fence.
  - **Backstop.** A handler waits at most until the attempt's local deadline D plus `usage_handoff_bound(settings) + 2 × db_operation_timeout`, that is D + 5 bounds, then raises `UsageHandoffExpiredError`, a `BaseException` that is never classified (exit 5, no call). While heartbeats succeed or are not due, that is strictly later than the supervisor's worst case, D + 4 bounds: a retry due at D may still wait for (1) a step in progress and (2) a heartbeat, then (3) lose a write's answer and (4) fail the immediate replay, which has no deadline check, before it reaches exit 8. Heartbeats that keep failing are retried within one bound, so the reserve rule holds usage writes back as it holds sweeps and exit 8 may never come; the fence (exit 3, first in a real process) or this backstop (exit 5) then ends the process. Safety is unchanged: no call is made without its committed intent.
  - **Limits (pure, tested, not wired).** `check_provider_call_budget(settings, *, max_provider_calls_per_attempt, provider_call_timeout)` raises `ValueError` unless `cap × (provider_call_timeout + 2 × usage_handoff_bound) ≤ execution_timeout − fence_margin`, compared exactly in whole microseconds; the cap is an `int` from 0 to `MAX_CALL_SEQ` (a `bool` is refused). `WorkerSettings` gains no field. Both `usage_handoff_bound` and this budget are limits for the case with no failures; retries and replays are bounded by the attempt's deadline and by the ownership fence, not by them.
  - **Scheduling.** `Step.USAGE` and `Timing.usage_due` (default `None`, so a `Timing` built without it still works). While a handler runs: a due heartbeat first; then a waiting record if `heartbeat_due is None` or `heartbeat_due − now ≥ sweep_reserve` (the sweep rule); then a sweep. Records are written while a stopping worker drains.
  - **Serving.** Each record is one guarded `_call` of `record_call_intent` or `record_call_result`, classified by `classify_completion_error`. A retryable SQLSTATE or failed connect is retried every bound, the last try at the attempt's local deadline. A lost answer closes the connection and replays the same keyed write at once, then every bound. An intent unconfirmed at the deadline exits 8 (`usage_intent_unrecorded`), with no call; a result, exit 8 (`usage_result_unrecorded`). A rejecting SQLSTATE, `NotFoundError`, `CallIntentNotFoundError`, `UsageReplayConflictError`, a `RuntimeError`, or a first (non-replay) intent returning `created=False` exits 7 (`usage_write_rejected`). An intent stored late exits 3 (`usage_intent_late`) without a call; its row stays. Lost ownership is unchanged. None of these is ever the handler's outcome.
  - **Closures.** For a sink kind, the completion passes the worker's closure at T3: `complete(n)` exactly when every confirmed intent has a confirmed result, otherwise `incomplete`; the same closure on every write and rewrite, so a DEC-047 replay is identical. `CaptureNotCompleteError` and `UsageReplayConflictError` at completion exit 7 (closing a catch-list gap). Sweeps pass `usage_capture={kind: T3}` for the sink's kinds. Without a sink nothing changes and the CLI is unchanged.
  - **Diagnostics** carry worker, task and attempt ids, `call_seq`, the exception type and the SQLSTATE; never message text, an amount, a unit, a prompt or provider text. Exit 8's meaning now includes an intent or result that could not be recorded.
- **WHY:**
  - The supervisor stays the only database client, so the watchdog's operation guard, the server-side timeouts and the reconnect paths bound ledger writes unchanged.
  - Heartbeat priority is a pure, testable rule shared with sweeps.
  - The intent's `FOR KEY SHARE` late flag (DEC-058) lets the worker refuse a call once it has lost ownership.
  - `ProviderCalls` writes each result before the next intent, so its closures are normally `complete(n)`. What keeps `complete(n)` exact is the closure rule (every confirmed intent has a confirmed result), not that order, which the sink does not enforce.
- **RUNNER-UP:** A dedicated ledger writer thread with its own connection.
- **WHY NOT:** It breaks the rule that the supervisor alone uses the database; it needs a second operation guard, and the watchdog holds one deadline per guard (`fence.py`); and its locks would interleave with heartbeats and completions outside heartbeat priority. Re-checking cancellation after an acknowledgement and skipping the call was also rejected: it leaves a committed intent for a call never made, so the ledger shows an unresolved call that did not happen.
- **TRADE-OFF:**
  - Each call waits for up to two supervisor writes, plus a step in progress or a heartbeat; sweeps may be postponed while records wait, bounded by the cap. When PostgreSQL is slow, calls are delayed, never made unrecorded.
  - A late intent leaves a row for a call not made, read as `lower_bound`: it may overcount, never undercount.
  - An unrepresentable amount leaves a billed call unresolved by design.
  - The T3 contract and the fleet-wide tier map are documented, not enforced.
  - The handler's backstop raises a `BaseException`: an attempt whose supervisor never answered is never recorded by this worker, and recovery decides.
- **VERIFICATION:** All tests use synthetic provider doubles; none calls a real provider. Unit tests (`tests/unit/test_worker_usage.py`) cover `next_step` with a waiting record (heartbeat first, the reserve, `heartbeat_due is None`, before sweeps, while draining, every wait finite, and a simulation in which heartbeats are never late and records always progress), a `Timing` built the old way, the handoff (withdrawal on cancellation, no post once cancelled, a result never withdrawn, an acknowledged call going ahead, a second post refused, the backstop as a never-classified signal at D + 5 bounds, more than the supervisor's 4-bound worst case, which a fake-clock test walks through), every refusal before a post, the conversions (exact `Decimal`s and scales, every failure code, `1E+20` and 19 decimal places left unresolved), the budget check and its boundaries, the wiring rules with messages that quote no value, and the supervisor's step with injected ledger writes (retry spacing, the deadline, lost answers replayed, every defect, the late intent, the closure passed to the completion, the tier map passed to sweeps, ids-only diagnostics). PostgreSQL tests (`tests/integration/test_worker_usage_pg.py`, a real `Worker` and, where needed, `PgProxy`, whose `drop_commit` generalizes `drop_completion` to any marked statement) cover every call with its two rows and `complete(n)`, cancellation while an intent waits (no row, no call, `complete(0)`) and during a call (the result written, `complete(1)`, cancelled), lost intent and result answers committed and rolled back (one row and one call each), a retryable error, intents unrecordable until the deadline (exit 8, never 5, no call, after the ownership guard had already expired, which in a real process is exit 3 first), a rejection (exit 7, nothing failed), a late intent (exit 3, no call, `lower_bound`) and a late result (accepted), an unrepresentable amount (the task succeeds, `incomplete`, ids-only logs), and heartbeat cadence while records are written. Process tests (`tests/integration/test_worker_process.py`, a sink-wired `metered` kind) include the named disproof test, in which sweeps and heartbeats compete with waiting intents (orphan leases staggered so at least one is revoked after the metered attempt started and before the stall) and the database then stalls: renewals keep their cadence, no call runs without its committed row, nothing is failed, the process exits 4 or 3, recovery closes the attempt at T3 and the retry numbers its calls from 0; plus a fence during a stalled call (exit 3, unresolved, `incomplete`), a stalled intent write (exit 4, no call) and SIGKILL mid-batch. `tests/integration/test_worker_usage_performance.py` (marked `performance` and `integration`) measures the per-call cost against the in-memory recorder and direct ledger writes and asserts correctness only. The results of the fixed test profile are recorded on the pull request, not here.

## OpenRouter adapter

### DEC-060 — An unwired OpenRouter adapter over an injected HTTP transport, with closed failures and a deadline checked before every blocking step
- **Status:** Accepted (owner, 2026-11-06; was Proposed) · it follows the owner's resolved direction for a future outbound adapter (OpenRouter, the standard library only, no default model, no live calls, never log credentials, prompts or responses).
- **WHAT:**
  - **Package.** A new `orqestra.providers` package, outside the offline-scanned provider-contract and workload packages. `transport.py` holds `Deadline`, the `HttpTransport` and `HttpExchange` protocols and the fixed-text `TransportTimeout` and `TransportFailure`. `stdlib_transport.py` holds `HttpsTransport` (`http.client` and `ssl`). `openrouter.py` holds `OpenRouterConfig`, `OpenRouterProvider`, `CallReport`, `ResponseDefect` and `OpenRouterProtocolError`. The package imports only the standard library, the provider contract module, `orqestra.domain` and itself; only `stdlib_transport.py` may import `http`, `ssl` or `socket`. No other module imports the package, so the adapter is unwired.
  - **Configuration.** `OpenRouterConfig(credential, route_models)` is frozen and has no defaults. The credential is any object with `get_secret_value()` (pydantic's `SecretStr` fits, with no pydantic import); its value must be 1 to 512 visible ASCII characters, no message quotes it, and `repr` shows `***`. `route_models` must be non-empty, maps route ids (the routing rule) to model slugs (`[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}`, a project rule), and is copied into an immutable mapping. There is no default model and no constructor from `Settings`. `Settings` gains an optional `openrouter_api_key: SecretStr`, masked as `***` in `redacted()` and read by nothing yet.
  - **Call.** `call(request) -> CallReport` (`response`, `model_served`, `generation_id`, `input_tokens`, `output_tokens`, `latency_ms`); `interpret` returns `call(request).response`, and the `Provider` protocol is unchanged. A capability other than `interpret_amount`, an unmapped route or mistyped request fields are `unsupported_request` before any I/O. The body is exactly `model`, `messages` (a fixed system prompt carrying `PROMPT_VERSION`, and the canonical JSON of `field_kind` and `raw`) and `"stream": false`. The headers are exactly `Authorization`, `Content-Type` and `Accept`. There is one POST per call, no retry, and the package logs nothing.
  - **Deadline.** `Deadline` uses an injected monotonic clock. `remaining()` raises `TransportTimeout` when no time is left, so a socket never gets a timeout of zero. The adapter checks it before the POST and before every read. `HttpsTransport` connects with the time left as its timeout, and sets the socket timeout to the time left before sending, before reading the response head and before each `read1`.
  - **Transport.** `HttpsTransport` fixes the host, port and path, follows no redirect, reads no proxy setting from the environment, leaves the debug level at 0 and closes the connection on every path. A `TimeoutError` becomes `TransportTimeout`. An `OSError` (including `ssl.SSLError`), an `http.client.HTTPException`, a `ValueError` or an `OverflowError` becomes `TransportFailure`. Each is raised after the `try` statement, so it has no exception chain. An `OverflowError` comes from a huge budget: it overflows the socket timeout after the name lookup but before any connection is made or request bytes are sent, and it is reported as `unavailable`. Checking the cap, timeout and fence together remains the wiring change's job.
  - **Status.** 400 is `unsupported_request`; 401, 402 (even with `Retry-After`) and 403 are `rejected`; 408 is `timeout`; 429 is `rate_limited`; 502 and 503 are `unavailable`. Any other status, including 3xx, other 2xx, 500 and 504, is `OpenRouterProtocolError(unknown_status)`. A status other than 200 is closed without its body being read. `TransportTimeout` is `timeout`, and `TransportFailure` is `unavailable`.
  - **Body.** It is read in chunks up to 256 KiB (`body_too_large`). It must decode as strict UTF-8 and parse as one JSON object, with exact decimals, no `NaN` or `Infinity`, no duplicate keys, and `-0` kept signed (`malformed_json` otherwise, including a recursion or digit-limit error). An embedded `error` with an integer `code` is mapped like a status; an unlisted code or a malformed envelope is `malformed_error`, and its message and metadata are never read. Then `id` (1 to 128 visible ASCII characters), `model` (the slug rule) and exactly one choice object are required (`invalid_schema`), with `finish_reason` `"stop"` and string content (`unsupported_content`). The value is the content with surrounding whitespace removed.
  - **Usage.** Absent usage leaves usage and token counts `None`. Token counts are integers (not bools) from 0 to 2^63-1. `cost` is an integer or exact decimal that is finite, unsigned (`-0` included), has at most 18 decimal places and is below 1E+20. It becomes `ProviderUsage(format(amount, "f"), "openrouter_credits")`, which keeps both value and scale. Anything else is `invalid_usage`. The limits mirror the usage ledger's (`AMOUNT_MAX_SCALE`, `AMOUNT_LIMIT`, `TOKEN_MAX`, `ID_MAX_LENGTH`, `MODEL_MAX_LENGTH`), so every answer the adapter accepts can be recorded.
  - **Errors.** Every error carries a code from a closed set and is raised outside the handler that detected it, so `__cause__` and `__context__` are `None` at every layer. `OpenRouterProtocolError` is a `PermanentError` with the fixed text `provider_protocol_violation` and a `defect` code.
- **WHY:**
  - An injected transport means every adapter test runs against a fake response, and only one module can open a socket.
  - Checking the time left before each blocking step bounds the overrun to one operation.
  - A closed failure surface with empty exception chains means no credential, prompt or remote text can escape through an error. Standard-library messages need the same care: `putheader` quotes an illegal header value, credential included, in its `ValueError`.
  - Mirroring the ledger's limits means the change that wires the adapter in will never receive an answer it cannot record.
- **RUNNER-UP:** `urllib.request`, with the route-to-model map kept in `Settings`.
- **WHY NOT:** `urllib.request` reads proxy settings from the environment, which could send the Bearer credential to a proxy. It also follows redirects and has no clean per-read timeout. A map in `Settings` would force `config` either to import `providers` or to duplicate the slug rule; the wiring change can build the map and let `OpenRouterConfig` validate it.
- **TRADE-OFF:**
  - The standard library has no timeout for name lookup (`getaddrinfo`). Every other blocking step (connecting, including the TLS handshake; sending; reading the response head; each body read) is given the time left when that step starts, and the time is checked again only before the next step. Reading the response head (the status line, up to 100 header lines, and any `100 Continue` responses) and reading one body chunk can each wait on the socket several times, and sending writes the head and the body separately. So a slow server can make one call overrun its budget by more than one wait.
  - The handler thread cannot be interrupted, and cancellation is checked only between provider calls (`verify.py`), so the hard bound is the worker's watchdog and fence (DEC-045, DEC-048). If a worker is fenced mid-call, that call stays unresolved in the usage ledger.
  - A failure after the request was sent may still have been billed. It is reported as `unavailable`, and the wiring change must treat it as ambiguous.
  - 500 and 504 are not retried, and strict parsing may reject a valid future response shape.
  - The ledger's `CallFailure` has no protocol-violation code. The change that wires the adapter in decides how to store that outcome; no schema changes here.
- **VERIFICATION:**
  - `tests/unit/test_openrouter_adapter.py` uses a fake transport and a fake clock. It covers every documented status and ten others; embedded errors; malformed bodies, including bad UTF-8, `NaN`, duplicate keys, 100,000-level nesting and an integer over the digit limit; schema and identifier failures; the size cap; usage limits and fixed-point cost text; the deadline, including a slow chunked body, no reads after expiry and an expired budget that never reaches the transport; the route map and configuration; and redaction of the credential, prompt and response from `str`, `repr`, `args`, formatted tracebacks and logs, with empty exception chains.
  - `tests/unit/test_provider_transport.py` runs the real `HttpsTransport` against a stand-in for `HTTPSConnection`. It covers the fixed endpoint, the timeout set before each step, the debug level and proxy settings, closing on every path, and the mapping of each kind of standard-library exception. Through a subclass of the real class, it also checks real request framing and an illegal header value, and that a credential-quoting header error and a `BadStatusLine` carrying remote text both reach the adapter only as `unavailable`.
  - `tests/unit/test_provider_boundaries.py` adds an import scan of the package with its own self-tests, a check that nothing outside the package imports it, a fresh-interpreter check that importing the adapter loads neither `http.client` nor `ssl`, equality of the mirrored limits with the ledger's, and a round trip of an answer at every limit into `CallIntent` and `CallResult`.
  - `tests/unit/test_config.py` and `tests/unit/test_cli.py` check that the new key defaults to `None` and is masked.
  - The results of the fixed test profile are recorded on the pull request, not here.

## Provider usage report

### DEC-061 — A job's provider usage is reported from its ledger in the summary's snapshot, with exact per-unit totals and explicit finality
- **Status:** Accepted (owner, 2026-11-06; was Proposed) · the architecture was owner-approved.
- **WHAT:**
  - **Entry point.** `orqestra.aggregation.usage.report_job_usage(conn, job_id)` returns a `JobUsageReport` for an invoice job, `None` for an unknown job, `WrongJobKindError` for another kind (before anything else is read), `ValueError` for a job without tasks and `RuntimeError` inside a caller's transaction, exactly as DEC-056. `summary.py`, `invoice.py`, the migrations, the runtime and the adapters are unchanged.
  - **Snapshot.** `JobBatchSnapshot` gains `usage_totals()` and `attempt_usage()`, run in its one `REPEATABLE READ, READ ONLY` transaction after the job-row read, so the totals, the attempt rows and the task rows describe one state. `usage_totals()` is one `GROUPING SETS ((unit, attempt state), (unit))` query over `provider_calls` (filtered on its indexed `job_id`) joined to results and attempts, summing `reported_amount` in `numeric` and grouping on `reported_unit COLLATE "C"`. `attempt_usage()` streams one row per attempt through a second named cursor, ordered by `task_key COLLATE "C"` and `attempt_number`: intents, answered, failed and unresolved calls, settled calls without an amount, late intents and results, the estimates of unresolved calls and how many had none, and the closure. `tasks()` is unchanged, and every open cursor is closed when the block ends.
  - **Pure builder.** `UsageReportBuilder` (in `aggregation`, because the `workloads` import scan does not allow `decimal`) folds totals, then attempts, then tasks. An attempt is `OPEN` while running; `NOT_CAPTURED` when finished with no closure and no intent; `LOWER_BOUND` when finished with intents but no closure (also counted in `unclosed_attempts`), with an incomplete closure, or with a complete closure that the snapshot contradicts (its `call_count` differs from the intents, or a call is unresolved, reported as `closure_contradicted`); and `CLOSED` otherwise. The job is `OPEN` if its derived status is unfinished or any attempt is open, `CLOSED` if every attempt is closed (including a finished job without attempts), and `LOWER_BOUND` otherwise.
  - **Report.** Frozen, with zero-filled read-only mappings: capture counts; call counts; `reported_totals` keyed by the exact unit with `UnitTotal(amount, calls)`; `by_attempt_state` with the same figures per current attempt state; late counts; `estimated_exposure_units` and `unresolved_without_estimate`; the cross-check counts; and a closed set of anomaly codes with at most `MAX_SAMPLES` samples ordered by `(task_key, attempt_number, code)`.
  - **Cross-check.** For each succeeded task, a fresh `SummaryBuilder` counts its `provider_assisted` items, so `assisted_items` equals the summary's `provider_assisted_items` by construction. When the task's succeeded attempt is `CLOSED`, fewer answered calls than assisted items is the anomaly `assisted_exceeds_answered`; otherwise the task is counted as unchecked. Nothing is raised.
- **WHY:**
  - Reusing DEC-056's snapshot keeps every figure, the job status and the cross-check on one past state; without it an attempt and its task could be compared as seen at different times.
  - A stored amount can have 38 significant digits and two of them sum to 39, more than Python's default 28-digit `Decimal` context keeps. Summing in SQL keeps reported figures exact, and Python never adds amounts.
  - The ledger accepts late writes (an intent after a complete closure, a result after revocation), so finality must be re-derived from the snapshot and an absent capture must never read as zero.
  - Reusing `SummaryBuilder` per task applies DEC-056's rules exactly, with no change to summary code.
- **RUNNER-UP:** A separate `persistence/usage_snapshot.py` with its own read-only transaction and its own copy of the task projection, or reading each attempt with `read_attempt_usage` and summing in Python.
- **WHY NOT:** A separate module either duplicates `_STREAM_TASKS` (two copies of a total-projection rule) or imports private names across modules. `read_attempt_usage` opens one transaction per attempt, which breaks the single snapshot, and Python sums round silently under the default context.
- **TRADE-OFF:**
  - `JobBatchSnapshot` has a second named-cursor stream, and the snapshot holds back vacuum for as long as the read lasts.
  - The builder keeps one `(attempt number, answered, capture)` entry per succeeded attempt until its task is read, comparable to the summary's per-task triples, and builds one throwaway `SummaryBuilder` per succeeded task.
  - Tokens and closure tiers are not totalled in this version; units are not normalized or converted, and there is no price, currency or cost.
  - Invoice jobs only (`WrongJobKindError`), matching the report's place beside the invoice summary. `CLOSED` describes the snapshot only, since a later write can contradict a closure.
- **VERIFICATION:** Unit tests (`tests/unit/test_usage_report.py`) cover the capture table over every finished attempt state and closure, intent and unresolved combination, a running attempt in every case, an exhaustive property check of `CLOSED`, `NOT_CAPTURED` and `OPEN`, job finality, not-captured versus zero, contradicted closures, every cross-check path including parity with `SummaryBuilder` on corrupt results, per-state totals and conservation, the sample bound and order independence, frozen and zero-filled output, and that no value from the data reaches an anomaly. Integration tests (`tests/integration/test_job_usage_report_pg.py`, real PostgreSQL) run a task revoked through lease expiry, failed transiently and then succeeded, and check the per-state split, totals, exposure and finality, then a late result; a missing closure, not captured versus a true zero, a contradicted closure and a job cancelled before any attempt; distinct units (`credits`, `Credits`, `µ-credits`), preserved scale (`0.25 + 1.50 = 1.75`) and an exact 39-digit sum, a failed call with usage and calls without usage; open finality for a running attempt and a queued task; the cross-check with real handler results from `MockProvider`, against `summarize_invoice_job`; a late result and a late intent committed after the job read are not seen, and a companion test with `READ COMMITTED` sees them; two fixed-order gates commit on another session between two reads of the same report: a late answer committed after the totals and before the attempts leaves the totals' call count equal to the answered calls, and a completion committed after the attempts and before the tasks leaves a running job with an open attempt, while a `READ COMMITTED` companion for each shows the mismatch (credits counted once beside two answered calls; a succeeded task beside a running attempt), so both tests are sensitive to the level; reports read while a writer thread records calls each satisfy conservation and never go backwards; three seeded random ledgers match an independent oracle built from `read_attempt_usage` and summed under an exact context; and the snapshot contract (isolation and read-only inside it, no table changed, the connection idle after success, an error and an abandoned stream in both commit modes, a caller transaction refused, a missing job, another kind refused before any ledger read, identical reports at fetch sizes 1, 2 and 1,000). A test marked `performance` seeds 50,000 invoices at batch sizes 1, 100 and 1,000 with set-based SQL that respects the fencing guard, and records the environment (including the test process's CPU affinity and cgroup CPU quota where available), row counts, wall time, `tracemalloc` peak, RSS and `EXPLAIN (ANALYZE, BUFFERS)` summaries of both ledger queries on standard output and in JUnit properties, asserting correctness only. The results of the fixed test profile are recorded on the pull request, not here.

## Provider use boundaries

### DEC-062 — Before live provider use: no inbound API, authorized live calls only, no prices in the ledger, synthetic data until retention is reviewed, gated registration and a measured claim index
- **Status:** Accepted (owner, 2026-11-06; was Proposed) · the inbound-API direction is the owner's recorded guidance of 2026-11-03. It narrows the "Costs" guidance in `docs/development.md` and adds registration conditions to DEC-057 and DEC-058 without changing their text. None of it is implemented by this entry.
- **WHAT:**
  - **No inbound API.** No inbound Orqestra API or web framework is planned. Jobs are submitted and read through the Python interfaces; the CLI runs migrations and workers. Any future inbound service needs its own decision, covering its framework, authentication and hosting.
  - **Live calls.** Neither the change that builds the provider adapter (DEC-060) nor the change that wires it into a worker makes a live provider call. Every live call needs the maintainer's separate, explicit authorization, which names its scope. Ordinary tests stay offline.
  - **No prices.** The usage ledger keeps the provider-reported amount with the unit label the provider declared, plus the provider, model and token counts. It records no price, published price version or currency, and nothing converts a reported amount into money. Provider-reported figures stay separate from route estimates.
  - **Retention.** Version 1 has no deletion or retention mechanism: the ledger refuses `UPDATE` and `DELETE`, and no function removes its rows. Only synthetic data is recorded until a retention policy is reviewed and accepted.
  - **Per-job spend.** A per-job spend gate, if one is added, is advisory. Only the per-attempt call cap (DEC-057) is enforced.
  - **Registration.** Invoice verification is registered with `orqestra worker run` (DEC-051) only when all of these hold:
    - a durable recorder writes its calls to the ledger;
    - the per-attempt cap is required and set;
    - the cap has been validated against the worker's timing, so that cap × (per-call timeout + the recorder's write bound) fits within `execution_timeout − fence_margin` (DEC-057 leaves that check to the wiring);
    - the job kind's tier in `usage_capture` matches what its recorder writes (DEC-058);
    - live calls are authorized as above.
  - **Claim index.** Claim ordering (DEC-033) is measured against the CI PostgreSQL image (`postgres:16.15`, pinned by digest), and the observations are recorded on the pull request that makes them. The partial index DEC-033 left open is added only if repeated measurements show that the scan-and-sort cost matters and the index's write overhead is acceptable. No fixed threshold is set.
- **WHY:**
  - Orqestra runs as independent single-slot workers with no service of its own (DEC-054). An inbound API would add an authentication boundary, a framework dependency and a hosting model that no requirement asks for. Recording that stops "API framework" reading as a pending choice.
  - A live call is billable and sends data off the machine, so it needs the same explicit approval as any other spending.
  - A price table would be invented data that drifts from what the provider actually bills. The provider's own figure and unit are the only usage facts the system can observe.
  - An append-only ledger with no deletion path is safe only for data whose retention needs no policy.
  - Workers claim independently, so a job-wide limit can't be enforced without a shared count taken on every call.
  - Each registration condition closes a known gap:
    - without a durable recorder, a billed call can go unrecorded;
    - without the timing check, a capped attempt can still outlive its fence;
    - without a matching tier, recovery can over-claim a revoked attempt as `complete(0)`.
  - DEC-033 records that each claim scans and sorts queued tasks. Measurement, not assumption, decides whether that cost justifies an index.
- **RUNNER-UP:** Record none of these now, and decide each boundary in the change that first needs it: adapter wiring, registration, retention and the claim index.
- **WHY NOT:**
  - The header would keep listing an API framework as undecided, and `docs/development.md` would keep asking for a price version the ledger doesn't store.
  - The change that registers invoice verification could land without the conditions it must meet.
  - An enforced per-job spend limit was also considered: a row-locked counter on the job, taken before every billable call. It was not chosen. It serializes every call of a job on one row and adds a lock outside the task, attempt and closure order that DEC-058 keeps cycle-free. It would also still need a price to turn calls into spend.
- **TRADE-OFF:**
  - With no inbound API, external systems can't submit or read jobs, so any integration waits for a new decision.
  - The ledger can't give a money figure. A cost report needs a later decision on a price source and its versioning.
  - Ledger rows accumulate with no deletion path, and real data can't be recorded until retention is reviewed.
  - Concurrent workers can exceed an advisory per-job gate. A task's provider calls are bounded only by its attempts × the per-attempt cap.
  - Invoice verification stays unavailable from `orqestra worker run` until every registration condition is met.
  - Until the measurements justify an index, each claim keeps scanning and sorting the queued tasks.
- **VERIFICATION:** This entry changes documentation only, with no code, schema or test changes. Each condition is verified by tests in the change that implements it. The registration change shows each condition refusing registration when unmet. The claim-index choice rests on recorded measurements, not on a fixed threshold.

## Provider call package name

### DEC-063 — The provider call package is `orqestra.calls`, and the repository's wording names providers instead of the old two-letter term
- **Status:** Accepted (owner, 2026-11-06; was Proposed) · it follows the owner's naming rule of 2026-11-06. It changes no earlier decision: entries and ADRs that used the old term or package path are reworded for that term only, with their numbers, dates, statuses and meaning unchanged.
- **WHAT:**
  - **Rename.** The package that holds the provider-neutral call contract (request and response types, the scripted mock, call recording and route selection) moves from its old two-letter path to `orqestra.calls`, keeping its modules (`provider`, `mock`, `recording`, `routing`) and their history. Its unit tests become `tests/unit/test_calls_*.py`, and every import, string module path and layer name in the isolation and boundary tests follows.
  - **Wording.** The README, `pyproject.toml`, the package docstring, the docs and the ADRs describe the work in provider terms: "provider adapters", "provider call", "provider work", "provider budget" and "cost-aware provider orchestration". The `development.md` section is now "Invoice and provider workloads". OpenRouter's host name stays as it is, because the adapter must name the host it calls.
  - **No behaviour change.** No logic, signature, error code, setting name, stored value or migration changes.
- **WHY:** The owner's naming rule of 2026-11-06 removes the term from the repository, the old package path included, apart from OpenRouter's host name.
- **RUNNER-UP:** `orqestra.inference`.
- **WHY NOT:** It implies model inference only, while the package also covers route selection and call recording.
- **TRADE-OFF:**
  - Any import of the old package path from outside the repository breaks. There is none inside it.
  - Earlier decision entries and ADRs no longer read word for word as first written; only the term changed.
- **VERIFICATION:** On the change's head, a case-insensitive `git grep` for the term as a standalone token matches only lines naming OpenRouter's host, and no tracked path contains it. The full suite passes with the same number of tests as before the rename.

## CLI job submission and status

### DEC-064 — `orqestra job submit` sends a JSON file of tasks through `submit_job`, and `orqestra job status` prints a job's derived status
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · it adds two commands to the argparse CLI of DEC-051 and changes no persistence, schema or dependency; the CLI framework choice stays open.
- **WHAT:**
  - **Commands.** `orqestra job submit --kind KIND [--idempotency-key KEY] [--max-attempts N] TASKS_FILE` and `orqestra job status JOB_ID`, under a new `job` subcommand.
  - **Input.** `TASKS_FILE` is a path, or `-` for standard input: a UTF-8 JSON array of task objects with exactly `task_key` (a string), `payload` (any JSON value) and an optional integer `max_attempts`. A task without `max_attempts` takes `--max-attempts`, which has no default (DEC-034), so a task with neither is refused. Unknown fields, repeated keys in an object, numbers that are not finite (`NaN`, `Infinity`, or a literal such as `1e400` that overflows a double), strings and object keys anywhere in a task that cannot be encoded as UTF-8 (a lone surrogate, which JSON allows as an escape), and nesting deeper than the parser can follow are refused before the database is reached, with messages that name the task index and field, never a value. `--kind` and `--idempotency-key` values that are not UTF-8 (a byte such as `\xff` on the command line arrives as a lone surrogate) are refused the same way. Numbers with a fraction or an exponent are read as doubles, as Python's `json` reads them. `--kind` is checked with `check_job_kinds`, which refuses only a blank kind and a NUL, as the `jobs_kind_not_blank` constraint does for blanks. Everything else is `JobSubmission` and `submit_job`'s own validation and idempotency (DEC-025), unchanged.
  - **Sequence.** Settings are loaded, the connection string is checked without echoing it (as for `orqestra migrate`; a connection string that is not UTF-8 is now reported as invalid by every command instead of ending in a traceback), and an autocommit connection is opened; `verify_current` then refuses a schema that is not current before any write or read. Submit calls `submit_job` once. Status calls `get_job` and `get_job_progress`; a job row never changes and the counts are one statement, so the two reads need no shared snapshot.
  - **Output.** One JSON line on stdout. Submit: `status`, `job_id`, `kind`, `idempotency_key`, `created` and `created_at`; `created` is `false` when an identical earlier submission with the same key is returned. Status: `status`, `job_id`, `kind`, `idempotency_key`, `created_at`, `job_status` (DEC-022) and `task_counts` with every task state, zeros included. Payloads and results are never printed.
  - **Exit codes and errors.** 0 on success; 1 for a refusal, with the existing `{"status": "error", …}` line: invalid settings, a missing or malformed URL, an unreadable or invalid file, an invalid kind, an idempotency conflict (naming the job that holds the key), a schema that is not current, an unknown job id, or a database error; 2 for argparse's usage errors. A database error is reported by its type, SQLSTATE and, for an integrity error, the violated constraint's name, because driver messages can quote the connection string or the submitted values. A value that still cannot be encoded for the database is reported as an error line too, never a traceback.
  - **Kinds.** Any non-blank kind may be submitted, including one no worker runs; such a job stays queued.
- **WHY:**
  - The README has listed CLI job submission and status as planned, and DEC-062 rules out an inbound API, so the CLI is the operator's way to submit and follow a job without writing Python.
  - Going through `submit_job` keeps one validation and idempotency path for every caller.
  - The kind and key are the choices that vary from run to run; as flags, like `orqestra worker run --kind`, they keep the command self-describing and let one task file be submitted under different keys.
  - Strict parsing turns a typo, a value PostgreSQL would refuse, or a string that cannot be sent to it at all into a clear refusal before the database is reached, instead of a driver error or a traceback.
- **RUNNER-UP:** One JSON document holding the whole submission: `kind`, `idempotency_key` and `tasks`.
- **WHY NOT:** It ties the kind and key to the file, so submitting the same tasks under another key means another file, and the command line no longer says what is being submitted. A payload-only array with generated task keys was also rejected, because the keys would depend on order while DEC-025 makes order irrelevant to a submission's identity; so was JSON Lines, which nothing needs to stream yet.
- **TRADE-OFF:**
  - The whole file is read into memory and submitted in one transaction, as `submit_job` requires.
  - The commands set no connection or statement timeout of their own, as `orqestra migrate` does not; a caller bounds the connection attempt with `connect_timeout` in the URL or `PGCONNECT_TIMEOUT`.
  - A job of a kind no worker runs is accepted and stays queued; refusing it would tie submission to one worker's registry.
  - There is no command to list jobs, cancel one or show its tasks.
- **VERIFICATION:** Unit tests (`tests/unit/test_cli.py`) cover argparse's usage errors, every file refusal (not UTF-8, invalid JSON, `NaN`, `-Infinity`, a repeated key, deep nesting, not an array, no tasks, a non-object task, an unknown or missing field, a non-string key, a `max_attempts` that is a `bool`, a float or a string or that is missing without `--max-attempts`, duplicate task keys, `Infinity`, `1e400`, `-1e400` and `1.5E+309`, a lone surrogate in a task key, in a nested payload string and in a payload object key, and one 500 levels deep) with no database access, a valid surrogate pair and the largest finite double accepted, `--kind` and `--idempotency-key` that are not UTF-8, an encoding failure at the database mapped to an error line, a connection string that is not UTF-8 refused by `job status`, `migrate` and `worker run`, a missing file, an invalid kind, invalid and missing settings and a malformed URL for both commands, an unreachable database without the password or address, a schema that is not current, the conflict, a constraint violation reported by name without its values, both outputs and an unknown job. PostgreSQL tests (`tests/integration/test_cli_jobs_pg.py`) submit a file and read it back as stored, replay it with the same key in another task order, refuse the same key with different content and store nothing, create two jobs without a key, follow the status through a claim and a completion, refuse an unknown job, name `tasks_max_attempts_range` for a retry limit of 101, report a NUL in a payload by SQLSTATE without its value, report a NUL in the idempotency key and a retry limit of 2⁷⁰ without their values, refuse a lone surrogate in a task key or payload and an overflowing number before the database and store nothing, refuse a command-line byte that is not UTF-8 in a real process with one error line and no traceback, accept a kind no worker runs and refuse an unmigrated schema before any write.

## Provider call budget

### DEC-065 — A job's provider calls are checked against a provable bound, cap × Σ `max_attempts`, assumed by the operator and read in one snapshot with the usage report
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · it adds a read-only check over the usage report of DEC-061, within DEC-062: no price, no currency and no enforcement. No migration and no new dependency.
- **WHAT:**
  - **Bound.** `call_bound(max_attempts_total, cap)` in `orqestra.aggregation.budget` returns `cap × max_attempts_total`, the most provider calls a job's attempts can make. Every attempt is held to `cap` calls (DEC-057), and a task starts at most `max_attempts` attempts (DEC-019; `tasks_attempt_count_range`). The bound can be computed before submission, from the task file. `cap` follows the per-attempt cap's own rule (`check_call_cap`: an integer, not a boolean, from 0 to `MAX_CALL_SEQ`), and `max_attempts_total` must be a non-negative integer.
  - **Check.** `check_call_budget(report, max_attempts_total=…, cap=…)` returns a `CallBudgetCheck`: `cap`, `max_attempts_total`, `bound`, the report's `calls` and `finality`, a verdict, `unknown_reason` and `cap_source = "operator"`.
    - `exceeded` when `calls > bound`, at any finality. Recorded calls only grow, so even a lower bound over the limit proves the overrun: the assumed cap is wrong, a worker ran with a larger one, or there is a defect.
    - `within` when the report is `CLOSED` and `calls ≤ bound`. It holds for that snapshot only: the ledger accepts late writes, which can contradict a closed report.
    - `unknown` otherwise, with the report's finality (`open` or `lower_bound`) as the reason. A job whose attempts recorded nothing and closed nothing is a lower bound, so it is `unknown`, never `within`.
  - **One snapshot.** `read_job_call_budget(conn, job_id, cap=…)` opens one `job_batch_snapshot`. It folds the usage first, through `read_job_usage` (the former private `_read` of `report_job_usage`, renamed with no change in behavior), so another kind of job raises `WrongJobKindError` before any ledger row or task is read. Then it reads `JobBatchSnapshot.max_attempts_total()`, a new bounded query (`sum(max_attempts)` of the job's tasks), in the same `REPEATABLE READ, READ ONLY` transaction. The snapshot takes no row lock. An invalid `cap` is refused before anything is read.
  - Reported amounts and `estimated_exposure_units` stay in the usage report and are never added to the check.
- **WHY:**
  - The blueprint asks for a bounded budget policy, and DEC-062 rules out prices, currencies and an enforced per-job counter. A count of calls is the one budget figure that needs neither.
  - The bound follows from two mechanisms that are already enforced and tested: the per-attempt cap and the bounded attempts per task. It holds before any call is made.
  - Reading the retry limits in the report's snapshot keeps the bound and the counted calls about the same database state.
  - Keeping `unknown` for an open or lower-bound report follows the rule that unknown usage is never read as zero.
- **RUNNER-UP:** Advisory per-unit limits over `reported_totals`, with a `within`, `exceeded` or `unknown` verdict per unit label.
- **WHY NOT:** Unit labels are whatever a provider declares, so a limit per label is a policy over values the system cannot validate. Per-unit totals are already shown by the usage report, and a limit on them still says nothing until a provider reports. An enforced per-job counter was rejected by DEC-062.
- **TRADE-OFF:**
  - The cap is not stored anywhere: it lives in each worker's wiring, and the ledger checks only `call_seq >= 0`. The bound holds only if every worker of the job used the cap the operator passes, and nothing enforces that, as with the retry settings.
  - The check is advisory and after the fact: it reports an overrun and prevents none.
  - Calls are counted, not weighted: a call that reported a large amount counts the same as one that reported none.
- **VERIFICATION:** Unit tests (`tests/unit/test_call_budget.py`) cover the bound and its argument refusals (without echoing the value), and every verdict over reports built by the real `UsageReportBuilder`. That includes `exceeded` from `CLOSED`, `LOWER_BOUND` and `OPEN` reports, `within` only when `CLOSED`, `calls == bound` reading `within`, `unknown` for `OPEN` and `LOWER_BOUND` with the reason, a not-captured job reading `unknown`, and `cap_source`. PostgreSQL tests (`tests/integration/test_call_budget_pg.py`) show:
  - a task row and a call committed after the job read are seen by neither the retry-limit sum nor the report, and a fresh read sees both;
  - another session takes `FOR UPDATE NOWAIT` on the job's tasks and attempts while the snapshot is open, so the snapshot holds no lock;
  - exact bounds and verdicts over a real ledger: within at the cap and exceeded below it for a one-attempt task, the bound counting every attempt a task may start, and unknown for a job with no attempt, a running attempt and a revoked attempt with an unresolved call;
  - a missing job reads `None`; another kind of job is refused before any read; an open caller transaction and an invalid cap are refused.

## CLI job summary and usage

### DEC-066 — `orqestra job summary` and `orqestra job usage` print an invoice job's summary and usage report, with the call budget check on request
- **Status:** Accepted (owner, 2026-11-07; was Proposed) · it adds two read-only commands to the `job` subcommand of DEC-064 and changes no persistence, schema or dependency.
- **WHAT:**
  - **Commands.** `orqestra job summary JOB_ID` and `orqestra job usage JOB_ID [--max-provider-calls-per-attempt N]`.
  - **Sequence.** Exactly DEC-064's: settings, the connection string checked without echoing it, an autocommit connection, then `verify_current` before any read. Summary calls `summarize_invoice_job` (DEC-056). Usage calls `report_job_usage` (DEC-061) or, with `--max-provider-calls-per-attempt`, `read_job_call_budget` (DEC-065), so the report and the check come from one snapshot and the job is read once.
  - **Output.** One JSON line on stdout with `status`, `job_id`, `kind`, `observed_at` and `job_status`, then every other field of the library object one to one. Summary adds the four derived properties (`processed_items`, `finished`, `integrity_ok`, `complete`); usage adds `finished`. Counts keyed by an enum list every member, zeros included. Reported amounts are plain decimal strings (`format(amount, "f")`), so no digit is lost to a float and none is written in exponent form. Anomaly samples carry a code, a task key and an index or attempt number. With a cap, usage adds `budget`: `cap`, `cap_source`, `max_attempts_total`, `bound`, `calls`, `finality`, `verdict` and `unknown_reason` (`null` unless the verdict is `unknown`). Payloads, results, invoice values and the idempotency key are never printed.
  - **Exit codes and errors.** 0 on success; 1 with DEC-064's `{"status": "error", …}` line for invalid settings, a missing or malformed URL, a cap that `check_call_cap` refuses (before the database is reached, without its value), a schema that is not current, an unknown job id, a job of another kind (`WrongJobKindError`, named by its id only, never by its stored kind), a job without tasks (which `submit_job` never creates), or a database error (type and SQLSTATE only); 2 for argparse's usage errors, such as a cap that is not an integer.
- **WHY:**
  - The blueprint asks for the minimum CLI controls to run and inspect jobs, and the summary and usage report were readable only from Python. A clean-clone check and the demonstration need them from the shell.
  - One command per library function, as for `job status`, keeps the output a faithful copy that tests can compare with the library, with no second interpretation in the CLI.
  - Reading the budget through `read_job_call_budget` keeps the report and the retry-limit sum about the same database state, which two separate reads would not.
  - Naming a wrong-kind job by its id only, and printing counts and identifiers only, keeps the commands within DEC-056's rule that no value from the data is reported.
- **RUNNER-UP:** One `orqestra job inspect JOB_ID` command printing the status, summary and usage together.
- **WHY NOT:** It would read the job three times, in three snapshots, or need a new combined snapshot read; it would refuse a job of another kind that `job status` can read; and its output could not be compared one to one with a single library function.
- **TRADE-OFF:**
  - The output is long: every enum member is listed, and the usage report repeats its per-state figures.
  - Task keys appear in anomaly samples. They are the submitter's identifiers, not payload values, and the summary already carries them.
  - The cap is the operator's assumption, as in DEC-065; the command cannot check it.
  - As in DEC-064, the commands set no timeout of their own, and there is still no command to list jobs or show their tasks.
- **VERIFICATION:** Unit tests (`tests/unit/test_cli.py`) print a summary and a usage report built field by field, compare each document's keys with the dataclass fields so a new field cannot be dropped silently, check a small amount printed without an exponent, the budget for `exceeded`, `unknown` and `within`, a cap of 0 and of `MAX_CALL_SEQ`, the refusal of a negative cap and of one above `MAX_CALL_SEQ` before the database without the value, argparse errors for a missing or malformed `JOB_ID` or cap, and for each command an unknown job, another kind without its name, a job without tasks, a schema that is not current, a database error by type and SQLSTATE, invalid and missing settings, a malformed URL and an unreachable database without secrets. PostgreSQL tests (`tests/integration/test_cli_jobs_pg.py`) compare each command's output, except `observed_at`, with the library's own result serialized the same way: a complete job run through the real handler (no invoice value in the output), a job with succeeded, failed and running tasks, a retried task's usage, the budget `exceeded` and `unknown` on that task and `within` on a closed job. They also refuse an unknown job, an echo job (without its kind), an unmigrated schema, and report a missing ledger table by type and SQLSTATE.

## Session time zone

### DEC-067 — Library transactions decode instants in UTC, so lease, deadline and retry arithmetic measures elapsed time
- **Status:** Accepted (owner full-auto rule, 2026-11-08) · implemented (`owned_transaction`, `local_deadline`, the claim hold) · cites ADR-0003 (the database clock decides)
- **WHAT:**
  - `owned_transaction` runs `SET LOCAL TimeZone = 'UTC'` as its own statement, right after the isolation statement. Every `timestamptz` decoded inside a library-owned transaction is therefore a `datetime.UTC` value. The setting is transaction-local: commit or rollback restores the caller's session zone.
  - `local_deadline` converts both instants to UTC before subtracting, and refuses naive ones. The hold of a claimed but unstarted attempt (DEC-049) goes through it.
  - Reads outside `owned_transaction` (`get_attempt`, `get_job`, `list_tasks`, the snapshots and the other `tuple_cursor` reads) keep the session zone. They are a reviewed list, and none feeds lease, deadline or retry arithmetic.
- **WHY:**
  - psycopg decodes `timestamptz` in the session TimeZone, all values sharing one `ZoneInfo`, and Python compares, subtracts and adds datetimes that share a zone by wall time.
  - In a zone with daylight saving, a lease or a retry delay computed across a clock change was an hour long or short (DEC-034, DEC-053). An expired lease could compare as live, so a heartbeat could renew it (DEC-036). And the ownership fence of a lease crossing a spring change was armed an hour late.
  - Examples: the Europe/London fold of 2026-10-25, where a lease that ended at 01:30 BST compared as live at 01:10 GMT, 40 minutes later; and the America/New_York changes of 2026-03-08 and 2026-11-01, where a 30 s lease mapped to an hour more or an hour less.
  - CI and the Compose server run in UTC, so no test saw it.
- **RUNNER-UP:**
  - `-c TimeZone=UTC` in `supervisor_conninfo`.
  - Refusing sessions whose TimeZone is not UTC.
  - `.astimezone(UTC)` in every domain rule.
  - A custom UTC `timestamptz` loader.
  - Doing the arithmetic in SQL.
- **WHY NOT:**
  - The connection option covers the worker's connection only, not library callers, and it changes DEC-050's option list.
  - A refusal would stop deployments behind poolers or with `PGTZ` set.
  - Converting in every rule spreads the rule over five files, and every future site must remember it.
  - A custom loader replaces psycopg's C loader, and one registered globally would change user code in the same process.
  - SQL `interval` days follow wall time too.
- **TRADE-OFF:**
  - One more statement per owned transaction, heartbeats and claims included. The measurement campaign measures this code.
  - In a session whose TimeZone is not UTC, `orqestra job submit` prints `created_at` with `+00:00` while `orqestra job status` keeps the session's offset; the instant is the same.
  - Correctness relies on psycopg reading the session TimeZone for each query's loaders, and on PostgreSQL reporting the revert of `SET LOCAL`. The integration tests check both.
  - Open: a CI lane whose test sessions use a TimeZone with daylight saving (needs the owner).
- **VERIFICATION:**
  - `tests/integration/test_attempt_lifecycle.py`, section 24:
    - with the session in Europe/London, instants read on the 2026-10-25 fold inside `owned_transaction` are UTC; the expired lease is `lease_expired`, 40 minutes past; a 2 h lease from before the fold is exactly 2 h long;
    - the same values read outside it keep London's zone and show the defect;
    - in America/New_York, Australia/Lord_Howe and Europe/London, every instant in the records and errors of submission, claim, renewal, completion, an ineligible start, cancellation, an expired renewal and recovery is UTC, and the session keeps its zone afterwards.
  - `tests/unit/test_worker_fence.py`: `local_deadline` maps a 30 s lease across the New York spring and autumn changes to 30 s, and refuses naive instants.
  - `tests/unit/test_worker_runtime.py`: across both changes, the hold of an unreadable claim and the ownership fence each wait one real lease.
  - `tests/unit/test_persistence_boundaries.py` (AST):
    - cursors are made only by the shared helpers;
    - connections run statements directly only in migrations;
    - the reads outside `owned_transaction` are a fixed list;
    - lifecycle applies the lease and retry rules only inside the operations that own their transaction.
  - Mutation checks: locally, each of these failed tests:
    - removing the `SET LOCAL`;
    - merging it into the isolation statement, which the prepared-statement variant of the claim test refuses;
    - reverting `local_deadline` to a plain subtraction;
    - reverting the hold to a plain subtraction;
    - a new lifecycle read outside `owned_transaction` that applies a lease rule.
