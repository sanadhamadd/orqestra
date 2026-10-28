# Orqestra — engineering decision ledger

Material engineering decisions in WHAT / WHY / RUNNER-UP form. A proposal is not a
decision: every entry stays **Proposed** until the owner marks it **Accepted** in a
reviewed change. Trivial edits are not logged. Cross-cutting choices also get an ADR.

Deliberately **undecided** (do not infer a choice from the code): API framework, worker
hosting, AI provider and models, asyncio vs threads for worker slots, operational lease
and execution-timeout values and any per-job-kind policy (DEC-034). The DB driver,
migration tool and task queue are no longer open: see DEC-016, DEC-017 and DEC-033.
Operational cancellation grace periods are undecided too (DEC-040).

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
- **Day 3A Slice 2 clarification:** an attempt whose lease or deadline expired is recorded as `revoked` with a revocation reason, never as a failed attempt with a failure kind or error details (DEC-038). A task that uses up its budget through revocations ends `failed` because its retries are exhausted, not because a handler failed permanently.

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
- **Status:** Proposed · owner-approved direction · **ADR:** 0003 · guard implemented; generation-checked completion implemented in Step 2B (DEC-029)
- **WHAT:** Attempt *N* holds generation *N*. The count starts at 0, changes only on a `queued → running` claim, by exactly +1, never decreases and never exceeds `max_attempts`. A narrow trigger (`tasks_attempt_count_fencing`) rejects any other change; it does not restate the transition table.
- **WHY:** A state-only conditional write accepts a stale worker's result after the task was requeued and reclaimed. The generation guard is the one invariant whose violation would be silent, so the database enforces it.
- **RUNNER-UP:** A separate `generation` column, or relying on write-path discipline and tests alone.
- **TRADE-OFF:** Every started attempt consumes retry budget, including attempts lost to crashes; returning budget later needs a separate counter.
- **Day 3A Slice 2 note:** revoked attempts use budget like any other, and recovery fails a task whose budget is used up (DEC-038); no separate counter was added.
- **VERIFICATION:** trigger tests for legal claims, decreases, resets, skipped generations, unrelated increments, frozen terminal tasks and retry exhaustion.

### DEC-020 — Cooperative cancellation
- **Status:** Proposed · owner-approved direction · policy, schema and writes implemented (Step 2B, DEC-032); grace period later
- **WHAT:** A request cancels a queued task immediately. A running task records `cancel_requested_at` and keeps running. A legitimate success may still win; an outcome that would requeue cancels instead; other terminal outcomes are recorded truthfully. A future recovery step must revoke ownership after a bounded grace period.
- **WHY:** Attempt and usage records stay truthful while work is in flight, and no retries start after a request.
- **RUNNER-UP:** Immediate cancellation of running tasks, fencing off late results — simpler state, but the database would show no live attempt while a worker still spends.
- **TRADE-OFF:** Running tasks are not terminal the moment cancellation is requested. Revoking database ownership is not proof that an external AI call stopped or was not billed.
- **VERIFICATION:** `next_task_state` and `state_after_cancel_request` decision tables; `tasks_cancel_request_not_queued`, `tasks_cancelled_was_requested`. Race tests are Step 2; the grace period needs its own design and tests.
- **Day 3A Slice 2 note:** a revocation counts as an outcome that would requeue while attempts remain, so a pending request cancels the task instead; an exhausted revocation fails it, as an exhausted transient failure does (DEC-038). Until the grace period exists, the execution deadline bounds how long a running attempt can ignore a request. A revocation caused by the request itself (Slice 3) will need its own rule.
- **Day 3A Slice 3 note:** the bounded grace period now exists (DEC-040). A request carries a grace duration and sets the running attempt's lease to end that long after it; heartbeats stop renewing it, and recovery then cancels the task whatever budget is left. The cooperative rules above are unchanged within the grace period.

### DEC-021 — READ COMMITTED, short transactions, task-before-attempt locking (direction)
- **Status:** Proposed · owner-approved direction · implemented: job submission (DEC-024) and the attempt lifecycle (DEC-028)
- **WHAT:** Authoritative writes are single conditional statements or lock-then-decide transactions (`SELECT … FOR UPDATE` on the task, then its attempt) under READ COMMITTED. No transaction is held open during external or AI work.
- **WHY:** Row locks plus re-checked conditions give compare-and-set without serialization-failure retry loops; one lock order prevents deadlocks.
- **RUNNER-UP:** SERIALIZABLE isolation with retries.
- **TRADE-OFF:** Cross-table invariants rely on the write paths and their tests.
- **VERIFICATION:** Step 2 concurrency tests (competing starts, stale generations, cancellation races).
- **Day 3A Slice 2 note:** the heartbeat is the one write that locks only the attempt row (DEC-036).

### DEC-022 — Job status is derived from task states (direction)
- **Status:** Proposed · owner-approved direction · implemented; the rules are DEC-031
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

## Day 2 — Step 2B: transactional attempt lifecycle

Entries DEC-028 to DEC-032 record implementation choices within the approved Day 2
direction (ADR-0003, DEC-019 to DEC-022). They stay **Proposed** until the owner accepts
them in a reviewed change. No schema change, migration or dependency was needed, and the
lifecycle policy functions and transition tables are unchanged.

### DEC-028 — Lifecycle writes lock the task, then the attempt, decide with the policy, and commit
- **Status:** Proposed · implemented (`orqestra.persistence.lifecycle`)
- **WHAT:** `start_attempt`, `complete_attempt` and `request_cancellation` each own one short READ COMMITTED transaction (the guard shared with `submit_job`). Each one locks the task row (`SELECT … FOR UPDATE`) before anything else; completion then locks its attempt row. The domain functions decide (`can_transition`, `next_task_state`, `state_after_cancel_request`, `transition`, `transition_attempt`), the writes follow, and the transaction commits. Starting a task claims it (`queued → running`, `attempt_count + 1`) and inserts attempt *N = attempt_count* in the same transaction. No transaction spans task execution: a start commits before the work and a completion is a separate transaction.
- **WHY:** One lock order (task, then attempt) on every path means no deadlocks. Every writer of a task serialises on its row, so each decision sees the latest committed state. Deciding in Python keeps the retry and cancellation rules in one place (DEC-008, DEC-020), and the lock gives a precise reason for every rejection.
- **RUNNER-UP:** Single conditional `UPDATE … WHERE state = … AND attempt_count = …` statements — fewer round trips, but the policy would be restated in SQL and a rejection would need a second query to explain itself. SERIALIZABLE with retries — no explicit locks, but retry loops on every write path.
- **TRADE-OFF:** Three to five statements per operation, and a hot task row serialises its writers (by design). A Python check that a queued task has attempts left was removed as unreachable: `tasks_queued_has_attempts_left` already guarantees it (DEC-026).
- **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py`: 20 rounds × 8 concurrent starters yield exactly one attempt each; a gated test observes the losing starter blocked on the winner's row lock (`pg_locks`), then refused; a failing attempt insert (constraint, or a test-only trigger reporting the task already `running` at generation 1) rolls the claim back; 5 rounds × 6 competing workers start exactly `max_attempts` attempts, numbered 1..5. A whole-schema invariant check runs at the end of every test and after every race round. Locally, removing the task lock on start failed 8 tests; removing it on completion failed 5; removing the start or completion transaction failed 10 and 13; disabling the transition-table check on start failed 19. Removing only the attempt lock failed none: the task lock alone serialises every writer, so the attempt lock is defence in depth.

### DEC-029 — Completion is accepted only from the current generation; rejections are typed
- **Status:** Proposed · implemented
- **WHAT:** `complete_attempt(conn, attempt_id, generation=N, outcome=…)` takes effect only when the attempt's number is *N*, the task's `attempt_count` is *N*, and the attempt is still running. Otherwise it changes nothing and raises:
  - `StaleAttemptError` — the generation does not match, or a newer one exists.
  - `AttemptFinishedError` — the current attempt already ended (a duplicate completion).
  - `NotFoundError` — an unknown task or attempt.

  Starting a non-queued task raises `TaskNotStartableError`. A running attempt whose task is not running is reported as an invariant violation (`RuntimeError`) instead of being completed. Outcomes are typed records — `AttemptSucceeded(result)`, `AttemptFailed(kind, error_type, error_message)` and `AttemptCancelled()` — and `AttemptFailed.from_exception` classifies handler exceptions with `classify_failure`.
- **WHY:** The attempt ID alone cannot tell a current worker from one whose task was requeued and restarted. The generation can, and requiring both catches mismatched pairs. Typed errors let a worker distinguish "my result was recorded earlier" from "someone else owns this task now", and neither is a task failure (DEC-009). Completion never classifies exceptions itself, and database errors during completion roll back and propagate, so infrastructure failures are never recorded as permanent handler failures.
- **RUNNER-UP:** Accepting an identical duplicate completion as an idempotent success — friendlier to retried calls, but it needs a result comparison and hides which call took effect. Result enums instead of exceptions — they would force a check at every call site of an operation that normally succeeds.
- **TRADE-OFF:** A worker that loses a completion's response (for example, its connection drops after the commit) learns the recorded outcome only through `AttemptFinishedError` or by reading the attempt. Nothing in this step revokes ownership of a running attempt; that is Day 3's lease recovery.
- **Day 3A Slice 2 note:** a completion of a revoked attempt raises `AttemptRevokedError`, a subclass of `AttemptFinishedError`, so a worker can tell "my outcome was recorded" from "I lost ownership and nothing I report counts" (DEC-037).
- **VERIFICATION:** a generation older than the current one, its attempt already failed and requeued, cannot record success, permanent or transient failure, and the newer attempt's result stands. A test stand-in for future lease recovery revokes a running attempt; the slow worker then cannot overwrite the new result. Mismatched generations (0, older, future) are rejected. Five kinds of duplicate completion change nothing, including after a requeue. 20 rounds × 4 concurrent completions of one attempt record exactly one result. A completion that fails at the attempt update or at the task update leaves both untouched and can be retried. A connection terminated mid-completion records nothing. Locally, removing the generation check failed 5 tests, removing the finished check failed 10, and ignoring a pending cancellation request failed 4.

### DEC-030 — Result contract: SQL NULL is "no result"; JSON null is a valid result
- **Status:** Proposed · implemented; no schema change
- **WHAT:** A succeeded attempt's result is stored as `jsonb` on its task, always as a JSON value: a result of `None` is stored as the jsonb value `'null'`, never SQL `NULL`. `Task.result` is meaningful only when `Task.state` is `SUCCEEDED`; every other task has SQL `NULL`, enforced by `tasks_result_iff_succeeded`.
- **WHY:** The schema's existing constraint already makes the task's state the unambiguous signal of whether a result exists, so a result that is JSON null needs no new column and is not silently confused with "no result".
- **RUNNER-UP:** Forbid JSON null results — simpler to read, but it rejects a legitimate JSON value. Wrap results (`{"value": …}`) or add a `has_result` column — unambiguous on their own, but either changes the stored format or needs a migration.
- **TRADE-OFF:** Readers must check `state` before interpreting `result`; `None` alone does not mean "no result".
- **VERIFICATION:** `test_a_success_stores_its_result_and_json_null_is_a_result` checks `result IS NULL` and `jsonb_typeof` for six result kinds, including `null`, `false` and `0`; storing JSON null as SQL NULL fails it.

### DEC-031 — Job status rules
- **Status:** Proposed · implemented (`derive_job_status`, `get_job_progress`)
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
- **Status:** Proposed · implemented (`request_cancellation`)
- **WHAT:** `request_cancellation` returns one of four outcomes:
  - `cancelled` — the task was queued; it now has `cancel_requested_at = finished_at`.
  - `requested` — the task is running and keeps running (DEC-020).
  - `already_requested` — the earlier request's timestamp is kept.
  - `already_finished` — the task is terminal and nothing is written.
- **WHY:** All four are normal outcomes, not errors. Recording a request on a finished task would falsely suggest it was pending while the task ran.
- **RUNNER-UP:** Exceptions for repeated or late requests — they would turn a safe, idempotent call into an error path.
- **TRADE-OFF:** There is no job-level cancellation yet, and no grace period after which a running attempt that ignores the request loses ownership (Day 3).
- **VERIFICATION:** Cancelling a queued task makes it unstartable and keeps any earlier attempt history. A pending request turns a transient failure into cancellation, while a success, a permanent failure or an exhausted retry stands. Repeated requests keep the first timestamp, and finished tasks are unchanged. Deterministic gated tests force both commit orders for success and for transient failure, and 30-round races check the invariants.

## Day 3A — Slice 1: PostgreSQL task discovery

### DEC-033 — PostgreSQL-native queue discovery using `SKIP LOCKED`
- **Status:** Proposed · owner-approved direction (2026-10-25) · implemented (`claim_next_attempt`)
- **WHAT:** PostgreSQL is the task queue; there is no separate broker. `claim_next_attempt(conn, worker_id)` locks the first claimable task with `SELECT … WHERE state = ANY(<sources of running>) ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`, then claims it in the same transaction through the private `_claim` step it shares with `start_attempt`: transition check, generation advance and attempt insert. It returns the new attempt, or `None` when nothing is claimable. The claimable states come from the transition table (`allowed_sources`), not a literal.
- **WHY:** Tasks already live in PostgreSQL (DEC-015), so claiming a row and starting its attempt is one transaction with no second system to keep consistent. `SKIP LOCKED` lets concurrent claimants take distinct tasks without waiting for each other or for a cancellation in progress. Sharing `_claim` keeps one authoritative implementation of the claim, guarded by the existing fencing trigger.
- **RUNNER-UP:** An external broker (Redis, RabbitMQ) delivering task IDs — it pushes work and scales independently, but it is a second system that must agree with the authoritative rows (DEC-015's runner-up). Within PostgreSQL, a single `UPDATE … WHERE id = (SELECT … FOR UPDATE SKIP LOCKED) RETURNING` saves a round trip, but it would restate the claim SQL and bypass the domain transition check.
- **TRADE-OFF:**
  - The order by `created_at`, then `id`, is a preference, not strict FIFO: locked tasks are skipped, and the tasks of one job share `created_at`.
  - A requeued task keeps its `created_at`, so it is claimed again straight away. There is no backoff, and a transient failure can use up its retries back to back once a worker loop exists.
  - Each claim scans and sorts the queued tasks until a partial index is added (planned with the lease migration).
  - There is no lease, so a claimed task whose worker dies stays `running` (Day 3A Slice 2).
  - `None` means "nothing claimable right now", not "the queue is empty".
- **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py`, section 16 (20 tests):
  - Claims and empty queues; running and finished tasks are never claimed; one claim per generation; older tasks first.
  - 20 rounds × 8 concurrent claimants over 8 tasks yield 8 distinct attempts; 20 rounds × 8 claimants over 3 tasks yield exactly 3.
  - Gated tests hold a claimant mid-claim and show another claimant taking the next task, or returning `None`, under `lock_timeout = 200ms`, so no wait occurred.
  - A task being cancelled is skipped and never claimed; a claim before a cancellation becomes a recorded request; 30-round claim-versus-cancel races leave no invalid claim.
  - A failing attempt insert rolls the claim back.
  - 6 competing claimants never exceed `max_attempts` across 10 tasks.

  Locally, replacing `SKIP LOCKED` with a waiting `FOR UPDATE` failed 3 tests, removing the row lock failed 7 (the fencing trigger rejected the colliding claims), claiming from any state failed 11, removing the ordering failed 2, and removing the owned transaction failed 11.
- **Day 3A Slice 2 note:** the lease migration (0002, DEC-039) does not add the claim-ordering index: it is a performance change and stays deferred until measured. Claims now take a lease policy (DEC-034), and a revoked task is requeued with its original `created_at`, so it too is claimed again without backoff.
- **Day 3B Slice 1A note (job-kind filter):** Proposed · owner-approved scope (2026-10-26) · implemented; merged in PR #8 (`46acdd9`), with both CI jobs passing for the pull request (run 40397331066) and for `main` (run 40397973812). Day 3B Slice 1B-A moved the kind check, unchanged, to `orqestra.domain.kinds.check_job_kinds`, shared with the handler registry (DEC-043).
  - **WHAT:** `claim_next_attempt(conn, worker_id, *, kinds, policy)` takes a required, non-empty collection of supported job kinds. Discovery adds `AND EXISTS (SELECT FROM jobs WHERE jobs.id = tasks.job_id AND jobs.kind = ANY(<kinds>))` to the same `FOR UPDATE SKIP LOCKED` query, so the filter is part of the atomic claim; ordering, the task-before-attempt lock order, `_claim`, fencing and the owned transaction are unchanged. Kinds match exactly. A bare string or bytes, a non-collection, an empty collection, a non-string kind, a blank kind or a kind with a NUL character raises `TypeError` or `ValueError` before any statement is sent; duplicates are accepted and collapsed. "Blank" is the database's rule for job kinds, `jobs_kind_not_blank` (`btrim(kind) <> ''`, where `btrim` trims spaces only), checked in Python as `kind.strip(" ")`, so every kind a job can have can be claimed, including one containing a tab or a newline. There is no unfiltered claim.
  - **WHY:** A worker can only run the kinds it has handlers for. Without the filter it would claim a task it cannot run, and could only fail or release it: that uses an attempt of the task's budget, records an outcome that says nothing about the task, and, because a requeued task keeps its place, can hold up the work behind it. Filtering inside the locking query means a claimant never locks, counts or reorders a task it cannot run.
  - **RUNNER-UP:** Denormalising the kind onto `tasks` (a `tasks.kind` column, possibly indexed with the claim order) avoids the per-candidate join, but needs a migration and a copy of an immutable job fact; deferred until claim cost is measured. Also rejected: one catch-all handler per worker, or filtering after the claim (which uses budget), or an optional `kinds` defaulting to "any" (a silent, unsafe fallback).
  - **TRADE-OFF:**
    - Breaking API change: every caller must name its kinds.
    - The SQL guarantees only which task is locked: the first queued task of a supported kind, by `created_at` then `id`, not locked elsewhere. How the planner evaluates the `EXISTS` (for example per candidate or as a semi-join), and what a backlog of unsupported tasks costs, has not been measured; it should be judged from reproducible query plans and a benchmark when claim cost matters, before any index or denormalisation.
    - The job row is read without a lock and is not re-checked once the task is locked. That is correct only while a job's kind never changes. No application code path updates `jobs.kind`, but the database does not enforce it: there is no trigger or permission against it (adding one needs a migration, out of scope here).
    - A task whose kind no worker supports waits forever; detecting that is left to monitoring, which does not exist yet.
    - Within a claimant's kinds the order is still `created_at`, then `id`; there is no per-kind fairness or priority.
  - **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py`, section 22 (13 tests, 47 cases): only supported kinds are claimed; a failed filtered claim rolls back completely; a kind is refused as blank exactly when PostgreSQL's own `btrim` rule refuses it (8 samples: only spaces are refused; tabs, newlines, CRLF and a no-break space are kinds); tabs and newlines match exactly and are never trimmed or confused with spaces; NUL characters are refused before any statement; a claim takes no lock on the job row, in either direction, under `lock_timeout` and `NOWAIT`; an eligible task is claimed past earlier candidates held mid-claim by two other claimants and an unsupported task; an older unsupported task with one attempt left never holds up newer supported work and keeps its state, generation and budget, then is claimed at generation 1 by a claimant supporting it; several kinds keep the age order; exact matching (case, whitespace, wildcard, prefix, literal `%`); 18 invalid inputs refused before any database work, also inside a caller's transaction, leaving every row unchanged; repeated kinds accepted; a gated claim is skipped without waiting by claimants of the same and another kind; 20 rounds of concurrent claimants of disjoint kinds each take only their own kind, and 20 rounds of overlapping kinds never claim a task twice and, as holds for every schedule, claim every task. The existing section 16 and Day 3A tests pass with `kinds` added.

    Locally, each of these mutations failed at least one section-22 test: blank checked with `strip()` (8 tests), no NUL check (3), the `EXISTS` taking `FOR SHARE` on `jobs` (1), a `FOR KEY SHARE` lock on the job after discovery (1), a waiting `FOR UPDATE` (3), and no kind filter (28).

## Day 3A — Slice 2: attempt leases, execution deadlines and recovery

Entries DEC-034 to DEC-039 record the Slice 2 design the owner approved on 2026-10-26 after
an architecture audit, an independent review and a PostgreSQL locking investigation. They
stay **Proposed** until the owner accepts them in a reviewed change. "Implemented" means
verified by the tests named, locally and in GitHub CI: Slice 2 was merged in PR #6
(`a031666`), with both CI jobs passing for `main` (run 40260902718). These are
building blocks: there is no worker loop, nothing schedules recovery, and a revoked worker
is not stopped (Day 3B).

### DEC-034 — Attempt leases with a non-renewable execution deadline, under an explicit policy
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented (`orqestra.domain.leases`, migration 0002)
- **WHAT:** Each attempt stores `lease_expires_at` and `deadline_at`, set at claim from one instant: the deadline is the claim instant plus `execution_timeout`, the lease the claim instant plus `lease`, capped at the deadline. A heartbeat sets the lease to `policy.lease` from now, capped at the deadline and never lower than before. The deadline never changes, enforced by the trigger `attempts_deadline_immutable`. An attempt may be revoked once the database time reaches `lease_expires_at`. The reason follows from the stored facts: a lease that reached its deadline cap means `deadline_exceeded`, any other expired lease `lease_expired`. `LeasePolicy(lease, execution_timeout)` is a required keyword argument of `start_attempt` and `claim_next_attempt`. Both values must be positive `timedelta`s within the representable range, `lease` cannot exceed `execution_timeout`, and a deadline beyond `datetime`'s range rolls the claim back with `ValueError`. There are no defaults.
- **WHY:** A heartbeat proves that a worker is alive, not that its work progresses: a worker whose handler hangs can renew its lease forever. A deadline that no heartbeat can extend bounds that case (R-EXEC-04). Capping the lease at the deadline leaves recovery with one predicate. Keeping leases on the attempt keeps, with each attempt's history, the facts its revocation was decided on. The policy is required because an optional "no lease" would bring back work that nobody recovers, and numeric defaults are an operational decision not yet made.
- **RUNNER-UP:** The lease on the task row: one place to look, but it loses per-attempt history. An independent deadline checked beside the lease: two predicates, and a precedence rule when both have expired. Defaults in `Settings`: convenient, but they invent operational values.
- **TRADE-OFF:**
  - A slow but healthy attempt is revoked at its deadline even while it heartbeats; choosing the timeout for each kind of work is open.
  - A worker that dies in its last lease window before the deadline is recorded as `deadline_exceeded`.
  - Every caller must pass a policy; the existing test call sites were updated.
  - Operational bounds (maximum lease and timeout, heartbeat interval, per-job-kind policies) are undecided and need owner approval before Day 3B.
- **VERIFICATION:** `tests/unit/test_leases.py` (validation, expiry at the exact instant and one microsecond either side, the cap, never shortening, representability); `tests/integration/test_attempt_lifecycle.py` sections 17 and 18, including a lease kept alive by heartbeats until its deadline and then revoked as `deadline_exceeded`.
- **Day 3A Slice 3 note:** the revocation reason has a third case. In order: a lease at its deadline is `deadline_exceeded`; otherwise a lease that ended after a cancellation request made while it held is `cancellation_grace_expired`; otherwise `lease_expired`. A cancellation request is the one write besides a heartbeat that sets the lease, and it may shorten it (DEC-040).

### DEC-035 — One claim instant, and a database clock read after the locks
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented
- **WHAT:** The claim's `UPDATE tasks … SET updated_at = statement_timestamp() … RETURNING updated_at` is the claim instant. It becomes the attempt's `started_at` and the base of its lease and deadline. `renew_lease` and `revoke_next_expired` read `statement_timestamp()` in a separate statement after taking the row locks their decision depends on, and decide expiry in Python against the freshly locked rows. Other timestamps still use `now()`.
- **WHY:** `now()` is fixed when the transaction starts, before any lock wait. A claim that waited for a task row would get a shorter lease, possibly one that had already expired by its commit. The claim `UPDATE` runs after the task lock is held on both claim paths, so its statement timestamp leaves the wait out. One value keeps `started_at`, the lease, the deadline and the task's `updated_at` consistent. Reading the clock after the locks means a decision is never older than the rows it compares.
- **RUNNER-UP:** `clock_timestamp()` captured once after the lock: equally fresh, but evaluated on every call, so a later edit could give the columns different instants. `statement_timestamp()` of the locking statement: taken before that statement's own wait.
- **TRADE-OFF:** A claimant that stalls between its claim `UPDATE` and its commit still loses that time from its lease, which is right: it was not running the task. The claim's `updated_at` moves from transaction start to the claim statement. Expiry depends on one database clock: a step forward can expire leases early (fencing still holds), and a step back is absorbed because a lease never shrinks.
- **VERIFICATION:** `test_a_claim_that_waited_for_the_task_lock_still_gets_its_full_lease`, for `start_attempt` and `claim_next_attempt`, with and without server-side prepared statements. A holder keeps the claimant waiting (observed in `pg_locks`) and reads `clock_timestamp()` before releasing; the attempt must start after that. Locally, deriving the instant from `now()` fails it.
- **Day 3A Slice 3 note:** a cancellation request's instant (`cancel_requested_at`, and for a queued task `finished_at` and `updated_at`) is now `statement_timestamp()` of its task update, taken after its locks, because the revocation reason compares it with the lease (DEC-040).

### DEC-036 — The heartbeat renews under the attempt lock alone and refuses expired leases
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented (`renew_lease`)
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
- **Day 3A Slice 3 note:** the heartbeat also reads the task's `cancel_requested_at` without a lock, in the statement that reads the clock after the attempt lock, and refuses renewal with `CancellationRequestedError` once a request has committed. It still takes no task lock (DEC-040).

### DEC-037 — Recovery revokes one expired attempt per transaction; ownership ends at its commit
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented (`revoke_next_expired`)
- **WHAT:** `revoke_next_expired(conn)` works in five steps:
  1. Lock, with `FOR UPDATE OF t SKIP LOCKED`, the task of the most overdue running attempt whose lease has expired.
  2. Lock that attempt.
  3. Re-check, against the locked rows and a fresh clock reading, that the attempt is still the task's running generation and that its lease has expired.
  4. Record the attempt as `revoked`, with its reason and `finished_at` at that instant, and move the task as `next_task_state` decides.
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
  - Nothing schedules recovery yet.
  - Revocation does not stop the worker or an external AI call and does not un-bill it (R-COST-01, Day 3B and later).
  - After a database outage longer than the lease, every live attempt is revocable at once (R-OPS-01).
- **VERIFICATION:** `tests/integration/test_attempt_lifecycle.py` sections 19 and 20:
  - Exactly-once revocation, over 10 rounds of 6 concurrent recoverers.
  - Skipping a locked task without waiting.
  - A completion before revocation counts; after it, the old worker is fenced before and after a new claim.
  - Both forced orders of completion against recovery and of heartbeat against recovery.
  - A poisoned write, and a terminated connection, each roll back both rows.
  - An invariant that every revoked attempt's lease had expired when it was revoked.
  - Locally, mutants that skip the re-check, drop `SKIP LOCKED`, stop locking the task, or report a revoked completion as a plain finished one each fail tests.

### DEC-038 — Revoked attempts: a distinct state; the task follows the transient-failure rule
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented
- **WHAT:** `AttemptState.REVOKED` is terminal and carries `revoked_reason`; it has no failure kind or error details. For a revoked attempt, `next_task_state` returns `queued` while attempts remain (`cancelled` if a request is pending) and `failed` once none remain, whether or not a request is pending.
- **WHY:**
  - DEC-009: lost ownership is a recovery event, not a handler failure, so the attempt records what happened.
  - DEC-019: every started attempt uses budget, so exhaustion fails the task.
  - DEC-020: a pending request cancels only an outcome that would requeue. An exhausted revocation would not requeue, and Slice 2's reasons (an expired lease, an exceeded deadline) are failures not caused by the request, so the task fails truthfully and its job reads `failed` (DEC-031).
- **RUNNER-UP:** Exhausted with a pending request ends `cancelled`: matches the user's intent, but hides a task that hung or lost every attempt. Recording a revocation as a failed attempt with failure kind `transient` (what the replaced test helper did): no new state, but it contradicts DEC-009.
- **TRADE-OFF:**
  - A user who requested cancellation can see `failed`; the attempt's `revoked_reason` explains why.
  - A deterministic hang is retried up to `max_attempts` times the execution timeout, with no backoff (DEC-033).
  - The cancellation-grace revocation of Slice 3 is caused by the request and will need its own rule.
- **VERIFICATION:** `tests/unit/test_policy.py` decision table, plus a check that a revocation moves the task exactly as a transient failure does for every count and request; the integration outcome table (six cases, including a single-attempt task with a pending request); invariant rules on attempt history.
- **Day 3A Slice 3 note:** the anticipated rule now exists: a `cancellation_grace_expired` revocation cancels the task whatever budget is left, and requires a pending request. `next_task_state` takes the revocation reason for every revoked outcome (DEC-040).

### DEC-039 — Migration 0002: additive schema and a quiescent upgrade with `NOWAIT` locks
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented (`0002_attempt_leases.sql`)
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
- **Day 3A Slice 3 note:** migration 0003 follows the same locking rule, `NOWAIT` before any change, but needs neither the running-work refusal nor the isolation check, because it changes no data-dependent state (DEC-041).

## Day 3A — Slice 3: bounded cancellation grace period

Entries DEC-040 and DEC-041 record the Slice 3 design the owner approved after an
architecture review and an independent review. They stay **Proposed** until the owner
accepts them in a reviewed change; "implemented" means verified by the tests named,
locally and in GitHub CI: Slice 3 was merged in PR #7 (`d83afeb`), with both CI jobs
passing for the pull request (run 40271111779) and for `main` after the merge (run
40271850302). The worker's response to a cancellation, handler
interruption and scheduled recovery remain Day 3B.

### DEC-040 — A cancellation request sets the running attempt's lease to a grace period
- **Status:** Proposed · owner-approved direction · implemented
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
  - A lease is no longer an absolute promise: a request can reset it, and the worker hears only at its next heartbeat, so Day 3B must keep the heartbeat interval shorter than any grace in use.
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
- **Status:** Proposed · owner-approved direction · implemented (`0003_cancellation_grace.sql`)
- **WHAT:** 0003 takes `LOCK TABLE attempts IN ACCESS EXCLUSIVE MODE NOWAIT` (`55P03` with an operator message if any session uses it), then replaces `attempts_revoked_reason_valid` to admit `cancellation_grace_expired`. It keeps `attempts_revoked_reason_matches_lease`, which already admits the new reason exactly when the lease ended before the deadline, and every other 0002 constraint and trigger. It adds no column or index, takes no `tasks` lock, has no running-work refusal and no isolation check. Rollout: stop all application processes → apply 0003 → start only Slice 3 code; draining first is preferable.
- **WHY:**
  - Every stored row satisfies the widened rule and running attempts are unaffected, so the schema change needs no quiescence.
  - The real hazard is mixed application versions after the migration: older code cannot read the new reason, and its heartbeats ignore cancellation.
  - A running-work refusal would not prevent that, so the rollout order is documented instead.
  - `NOWAIT` keeps the DEC-039 rule: never wait while holding a lock.
- **RUNNER-UP:** The 0002 contract again: lock both tables, require READ COMMITTED, refuse running work.
- **WHY NOT:** It adds checks that protect no data in this migration and still does not stop older processes from running afterwards.
- **TRADE-OFF:**
  - The database does not enforce the version switch.
  - Slice 3 code on a 0002 schema fails closed: its grace revocations are rejected and roll back until 0003 is applied.
- **VERIFICATION:** `tests/integration/test_migration_0003.py`:
  - upgrade from 0002 keeps revoked history and running attempts, whose heartbeats then behave as before;
  - the new reason is rejected by 0002 (the revocation rolls back both rows) and accepted after;
  - an idle reader or a held attempt row makes it fail at once with no history row and the old reasons;
  - a held task row does not;
  - a 10-round race against live claims, heartbeats, completions and requests never deadlocks, and applies while work is still being claimed.

  The schema tests cover the new reason against `matches_lease`, and the drift test covers the reason values. Locally, removing the new value, removing `NOWAIT` (the test fails after 10 s instead of hanging), or also locking `tasks` each fail tests.

## Day 3B — Slice 1B-A: worker contracts

Entries DEC-042 to DEC-044 record the worker contracts agreed after the Day 3B design
reconciliation (single-slot process, main-thread supervisor, handler thread, and the
C1–C5 reliability contracts). They stay **Proposed** until the owner accepts them in a
reviewed change; "implemented" means verified by the tests named, locally and in GitHub
CI: Slice 1B-A was merged in PR #9 (`823530a`), with both CI jobs passing for the pull
request (run 40408021385) and for `main` (run 40408731149). These entries are the
contracts; the first runtime that uses them is Slice 1B-B1 (DEC-045, DEC-046).

### DEC-042 — Handlers are synchronous functions; one pure boundary maps what they did to an outcome
- **Status:** Proposed · owner-approved direction · implemented locally (`orqestra.worker.handlers`)
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
- **Status:** Proposed · owner-approved direction · implemented locally (`HandlerRegistry`, `orqestra.domain.kinds`)
- **WHAT:**
  - `HandlerRegistry(mapping)` is an immutable `Mapping[str, Handler]` in kind order. It needs at least one kind; kinds are checked by `check_job_kinds`; every handler must be callable and not a coroutine, generator or async-generator function. Lookups are exact, and there is no fallback handler.
  - `registry.kinds` is the sorted tuple a worker passes to `claim_next_attempt(..., kinds=...)`.
  - `check_job_kinds` is the Slice 1A claim check moved, unchanged, from `lifecycle._supported_kinds` to `orqestra.domain.kinds`, so the claim and the registry share one rule: a non-empty collection of non-blank `str` kinds (blank as `btrim` defines it: spaces only) without NUL characters. The claim SQL is unchanged.
- **WHY:** A worker must claim exactly the kinds it can run; any kind the registry accepts is one the claim accepts and a job can have. One function means the two rules cannot drift apart.
- **RUNNER-UP:** A plain `dict` checked at worker start-up — no new type, but nothing would stop a later change from mutating it or claiming kinds it does not hold. Duplicating the check in the registry — no change to `lifecycle.py`, but two copies of one rule.
- **TRADE-OFF:** A small change to `lifecycle.py` (an import and a deleted private function); its behaviour and messages are unchanged and covered by the 47 Slice 1A claim tests. Coroutine detection sees through functions and `functools.partial`, not through objects with an async `__call__`; those are still caught when they return a coroutine (DEC-042).
- **VERIFICATION:** `tests/unit/test_kinds.py` (accepted kinds including tab, newline, CRLF and no-break space; 17 refused inputs); `tests/unit/test_worker_handlers.py` (exact, sorted kinds usable by the claim; no fallback for case, spacing, prefix or wildcard variants; immutability; callable objects and partials accepted; 14 refused registries). The existing section-22 claim tests in `tests/integration/test_attempt_lifecycle.py` pass against the moved check.

### DEC-044 — Worker settings: every timing value required, with only the relationships safe operation needs
- **Status:** Proposed · owner-approved direction · implemented locally (`orqestra.worker.settings.WorkerSettings`)
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
  - Operators must set nine variables before a worker can start.
  - `ORQESTRA_WORKER_CONCURRENCY` shares the worker prefix but belongs to `Settings`; `WorkerSettings` ignores it.
  - Whether to require several heartbeats per lease, and how the cancellation grace relates to the heartbeat interval (DEC-040), are operator rules the settings cannot check.
- **VERIFICATION:** `tests/unit/test_worker_settings.py`: valid settings and their lease policy; frozen; nothing has a default; each field required on its own; environment parsing in ISO 8601 and `HH:MM:SS`; four unit-less values refused; `worker_concurrency` neither read nor affected; zero, negative and unrepresentable durations refused for each field; `shutdown_timeout` zero allowed and negative refused; batch limit 1 allowed, 0 and -1 refused, non-integers refused; `db_operation_timeout` exactly 1 ms allowed and 1 µs less refused; lease equal to the execution timeout allowed and 1 µs shorter refused; database timeout 1 µs under the heartbeat interval allowed and equal refused; the fencing relationship 1 µs over allowed and at or under refused; a relationship is not reported while a field is invalid. Locally, mutants dropping or weakening each check (5) each failed tests.
- **Day 3B Slice B2b note:** `shutdown_timeout` is now read. It is the time a stopping worker gives the task it holds to settle (handler, completion and reconciliation), counted from the latch; `0` abandons a running task at once and stops an idle worker at once (DEC-049).

## Day 3B — Slice 1B-B1: the first worker runtime

Entries DEC-045 and DEC-046 record the first single-slot worker runtime and its local
watchdog, agreed after a read-only probe showed that a stalled database call can hold the
supervisor indefinitely. They stay **Proposed** until the owner accepts them in a reviewed
change; "implemented" means verified by the tests named, locally and in GitHub CI: Slice
1B-B1 was merged in PR #10 (`d498698`), with both CI jobs passing for the pull request
(run 40487174691) and for `main` (run 40488034469). There is no command or service that
runs a worker yet.

### DEC-045 — A single-slot worker: a supervisor, a handler thread and a local watchdog that ends the process
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented locally (`orqestra.worker.runtime`, `orqestra.worker.fence`)
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
      - **Moves earlier** when a heartbeat reports a cancellation request (the lease now ends with the grace period). Renewals then stop; the handler is not told in this slice.
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
    - ambiguous completions, committed or not, end the worker with no rerun and no further claim.
  - **`tests/integration/test_worker_process.py`** (a real subprocess; exits measured against PostgreSQL's clock):
    - a stalled supervisor call exits 4 before the lease ends;
    - refused connections exit 3 before the lease ends;
    - a lease ended by the database exits 3 at the next heartbeat;
    - a cancellation request exits 3 at the end of the grace period, and recovery then cancels the task;
    - a `SystemExit` handler exits 5 with nothing recorded;
    - an ambiguous completion exits 6 with the handler run once and the other task unclaimed.
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
- **Day 3B Slice B2a-2 note:** "the handler is not told in this slice", and the trade-off that handlers are not yet told, no longer hold. When a heartbeat confirms a committed request, the supervisor still stops renewing and restricts the fence to the end of the grace period first, then sets the attempt's cancellation event for the handler (DEC-048). A handler that ignores it is still fenced at the end of the grace period.
- **Day 3B Slice B2b note:** `run(stop)` is replaced by `run()` and `request_stop()`. The supervisor now waits on a self-pipe that the handler's end, `request_stop` and signals write to, rather than on the handler's `done` or the `stop` event; this also ended a busy wait while an outcome waited for a retry. A stop is latched and, while a task is held, drained rather than ignored; a third guard, drain, ends the process with exit 9. Signal handling, which the `SIGALRM` runner-up left to shutdown, is owned by `shutdown.run_until_signalled` on the main thread (DEC-049).

### DEC-046 — Slice 1B-B1 outcome policy: database errors are never handler failures; an ambiguous completion ends the process
- **Status:** Proposed · owner-approved direction (2026-10-26) · implemented locally (`Worker`, `classify_completion_error`); temporary for the ambiguous case until C4 reconciliation
- **WHAT:**
  - **The handler's outcome** is recorded with `complete_attempt`, the authoritative Day 3A operation. Lease expiry is never checked locally before completing, so a completion still counts until recovery commits.
  - **Ownership already ended** (`AttemptRevokedError`, `StaleAttemptError`): the outcome is discarded.
  - **A database error is never turned into a handler failure:**
    - **Retried:** a failed connect (nothing was sent), or a server error that certainly rolled back and may be retried (`40001`, `40P01`, `55P03`, `57014`). The retry comes after `db_operation_timeout` and continues until the attempt's local deadline (exit 8 then).
    - **Defect (exit 7):** a server rejection of a valid outcome with any other code, or a lifecycle invariant failure (`AttemptFinishedError`, `NotFoundError`, `ValueError`, `RuntimeError`).
    - **Ambiguous (exit 6):** a broken connection or an error without a server code. The worker does not classify the event as a handler failure, does not report the outcome as recorded, does not rerun the handler and does not claim again. Either the completion committed, or recovery decides.
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
  - The C4 reconciliation now: re-read the attempt and task, and compare every stored detail. This is deferred to Slice 1B-B2 so that it gets its own review and tests.
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
- **Day 3B Slice B2a-1 note:** the ambiguous case is no longer temporary. A completion whose answer was lost is now reconciled from PostgreSQL's evidence (DEC-047); exit 6 now means only that this could not be established before the attempt's deadline. The rest of this policy is unchanged: database errors are never handler failures, retryable errors are retried until the deadline, other rejections of a valid outcome are defects, and claims are handled as described. The B1 tests of ending the process on a lost answer (listed here and under DEC-045) were replaced by the DEC-047 tests.

## Day 3B — Slice B2a-1: reconciling a completion whose answer was lost

Entry DEC-047 records the C4 reconciliation agreed in the Day 3B Slice 1B-B2 design audit.
It stays **Proposed** until the owner accepts it in a reviewed change; "implemented" means
verified by the tests named, locally and in GitHub CI: Slice B2a-1 was merged in PR #11
(`278cb74`), with both CI jobs passing for the pull request (run 40520647309) and for
`main` (run 40524368913). Cooperative cancellation (Slice B2a-2, DEC-048) and graceful
shutdown (Slice B2b) are not part of it.
- **Day 3B Slice B2b note:** exit 9 is added for shutdown. `run` returns only through a controlled stop: an unexpected exception while a handler runs still ends the process (exit 3); `SystemExit` with a success code is raised as `UncontrolledExitError`; any other exception propagates, so the process exits nonzero (DEC-049).

### DEC-047 — A lost completion answer is reconciled from one consistent PostgreSQL read; the handler never runs again
- **Status:** Proposed · owner-approved direction (2026-10-27) · implemented locally (`read_completion_evidence`, `Worker`)
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
  - The B1 policy: end the process on any lost answer (DEC-046). That costs a revocation and a rerun of a handler whose outcome may have committed.
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
- **Day 3B Slice B2b note:** while a stopping worker drains, reconciliation is also bounded by the drain deadline; if that comes first, the process ends with exit 9 and PostgreSQL's evidence decides later (DEC-049).

## Day 3B — Slice B2a-2: cooperative cancellation

Entry DEC-048 records cooperative cancellation, agreed in the Day 3B Slice 1B-B2 design
audit. It stays **Proposed** until the owner accepts it in a reviewed change;
"implemented" means verified by the tests named, locally and in GitHub CI: Slice B2a-2 was
merged in PR #12 (`308bb0a`), with both CI jobs passing for the pull request (run
40598713849) and for `main` (run 40599609791). Graceful shutdown (Slice B2b, DEC-049) is
not part of it.

### DEC-048 — A heartbeat that confirms a committed request tells the handler, after fencing at the grace end
- **Status:** Proposed · owner-approved direction (2026-10-27) · implemented locally (`Worker`)
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
  - Giving the handler the event itself: rejected in Slice 1B-A, since the handler could forge an observed cancellation.
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

## Day 3B — Slice B2b: graceful shutdown

Entry DEC-049 records graceful, bounded shutdown, agreed in the Day 3B Slice B2b design
audit and its clarification (owner decisions O1–O10). It stays **Proposed** until the owner
accepts it in a reviewed change; "implemented" means verified locally by the tests named,
not yet in GitHub CI. There is still no command or service that runs a worker.

### DEC-049 — Graceful shutdown: a latched stop, a drain bounded by a watchdog guard, and lock-free signal handling
- **Status:** Proposed · owner-approved direction (2026-10-28) · implemented locally (`Worker.request_stop`, `orqestra.worker.shutdown`, `Guard.DRAIN`)
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
