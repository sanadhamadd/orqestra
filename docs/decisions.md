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
- **Status:** Proposed · owner-approved direction · **ADR:** 0003 · guard implemented; generation-checked completion implemented in Step 2B (DEC-029)
- **WHAT:** Attempt *N* holds generation *N*. The count starts at 0, changes only on a `queued → running` claim, by exactly +1, never decreases and never exceeds `max_attempts`. A narrow trigger (`tasks_attempt_count_fencing`) rejects any other change; it does not restate the transition table.
- **WHY:** A state-only conditional write accepts a stale worker's result after the task was requeued and reclaimed. The generation guard is the one invariant whose violation would be silent, so the database enforces it.
- **RUNNER-UP:** A separate `generation` column, or relying on write-path discipline and tests alone.
- **TRADE-OFF:** Every started attempt consumes retry budget, including attempts lost to crashes; returning budget later needs a separate counter.
- **VERIFICATION:** trigger tests for legal claims, decreases, resets, skipped generations, unrelated increments, frozen terminal tasks and retry exhaustion.

### DEC-020 — Cooperative cancellation
- **Status:** Proposed · owner-approved direction · policy, schema and writes implemented (Step 2B, DEC-032); grace period later
- **WHAT:** A request cancels a queued task immediately. A running task records `cancel_requested_at` and keeps running. A legitimate success may still win; an outcome that would requeue cancels instead; other terminal outcomes are recorded truthfully. A future recovery step must revoke ownership after a bounded grace period.
- **WHY:** Attempt and usage records stay truthful while work is in flight, and no retries start after a request.
- **RUNNER-UP:** Immediate cancellation of running tasks, fencing off late results — simpler state, but the database would show no live attempt while a worker still spends.
- **TRADE-OFF:** Running tasks are not terminal the moment cancellation is requested. Revoking database ownership is not proof that an external AI call stopped or was not billed.
- **VERIFICATION:** `next_task_state` and `state_after_cancel_request` decision tables; `tasks_cancel_request_not_queued`, `tasks_cancelled_was_requested`. Race tests are Step 2; the grace period needs its own design and tests.

### DEC-021 — READ COMMITTED, short transactions, task-before-attempt locking (direction)
- **Status:** Proposed · owner-approved direction · implemented: job submission (DEC-024) and the attempt lifecycle (DEC-028)
- **WHAT:** Authoritative writes are single conditional statements or lock-then-decide transactions (`SELECT … FOR UPDATE` on the task, then its attempt) under READ COMMITTED. No transaction is held open during external or AI work.
- **WHY:** Row locks plus re-checked conditions give compare-and-set without serialization-failure retry loops; one lock order prevents deadlocks.
- **RUNNER-UP:** SERIALIZABLE isolation with retries.
- **TRADE-OFF:** Cross-table invariants rely on the write paths and their tests.
- **VERIFICATION:** Step 2 concurrency tests (competing starts, stale generations, cancellation races).

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
