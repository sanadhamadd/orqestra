# ADR-0003: Authoritative execution state in PostgreSQL with generation fencing

- **Status:** Proposed — owner-approved direction (2026-10-24), recorded after an
  architecture audit and an independent review; awaiting the owner's acceptance of this
  implementation in a reviewed change.
- **Date:** 2026-10-24
- **Ledger entries:** DEC-015 … DEC-023; implemented through DEC-024 … DEC-050

## Context

Orqestra must run jobs as tasks across independent workers with at-least-once execution,
bounded retries, crash recovery and cancellation. Every task may be executed more than
once, so the system needs one durable record of who owns a task, what each execution
attempt did and which result is authoritative. Workers can stall and resume after their
work was handed to someone else; a write from such a stale worker must not overwrite the
current owner's state. A conditional write that checks only the task's state cannot
detect this: after `running → queued → running`, the state matches again.

## Decision

- PostgreSQL is the authoritative store for jobs, tasks and attempts. Any future queue
  delivers work; it does not own state.
- A task is the unit of work; an attempt is one execution of it by one worker. Attempts
  have their own lifecycle (`running → succeeded | failed | cancelled`); failed attempts
  record a `FailureKind` and error details.
- `tasks.attempt_count` is the task's ownership generation. Claiming a task
  (`queued → running`) advances it by exactly one and creates attempt *N = attempt_count*.
  The database rejects any other change to it (trigger `tasks_attempt_count_fencing`).
- Every authoritative write about a running task must present its generation and succeed
  only if the task is still `running` at that generation; otherwise it is rejected as
  stale. A stale rejection is an expected outcome, not a task failure.
- The domain transition tables remain the single source of legal transitions; the schema
  adds invariants that must hold regardless of the writer: valid states, at most one
  running and at most one succeeded attempt per task, results only on succeeded tasks,
  bounded attempts, a queued task always has attempts left, terminal timestamps, and
  cancellation provenance.
- Cancellation is cooperative (DEC-020): queued tasks are cancelled at once; running tasks
  record a request and finish through their current attempt or, once a bounded grace
  period given with the request has run out, through recovery (DEC-040).
- Every claim gives its attempt a lease and a non-renewable execution deadline. An attempt
  whose lease has expired may be revoked; ownership ends only when a recovery transaction
  commits the revocation, after which the attempt's writes are rejected like any other
  stale write (DEC-034 … DEC-038).
- Writes use short READ COMMITTED transactions, locking a task before its attempts, and
  never hold a transaction open across external or AI work (DEC-021).
- The schema is managed by forward-only, checksummed SQL migrations applied under an
  advisory lock, with transaction control rejected by PostgreSQL (DEC-017).

## Consequences

- Implemented and verified locally and in GitHub CI (PR #2): the schema, its constraints,
  the fencing guard, the migration runner and the lifecycle policy functions (see
  DEC-015 … DEC-020).
- Implemented and verified locally and in GitHub CI (PR #3): atomic, idempotent job
  submission and job and task reads (DEC-024 … DEC-027). Submission creates queued tasks
  at generation 0 and does not touch ownership.
- Implemented and verified locally and in GitHub CI (PR #4): starting an explicitly
  identified task's attempt, generation-checked completion with rejection of stale and
  duplicate results, cooperative cancellation writes, and derived job status
  (DEC-028 … DEC-032), with concurrency tests between separate connections.
- Implemented and verified locally and in GitHub CI (PR #5): discovering and claiming the
  next queued task with `FOR UPDATE SKIP LOCKED`, sharing the claim with `start_attempt`
  (DEC-033).
- Implemented and verified locally and in GitHub CI (PR #6): attempt leases and execution
  deadlines from one claim instant; lease renewal that locks only the attempt; revocation
  of one expired attempt per transaction, moving its task by the retry and cancellation
  policy; and migration 0002 with its quiescent-upgrade contract (DEC-034 … DEC-039).
- Implemented and verified locally and in GitHub CI (PR #7): a bounded cancellation grace
  period set by the request on the running attempt's lease, refused heartbeats once a
  request has committed, the `cancellation_grace_expired` revocation reason that cancels
  the task, and additive migration 0003 (DEC-040, DEC-041).
- Implemented and verified locally and in GitHub CI (PR #8): claims filtered by an explicit,
  required set of job kinds inside the same atomic `SKIP LOCKED` claim (DEC-033 note).
- Implemented and verified locally and in GitHub CI (PR #9): worker contracts: the
  synchronous handler protocol with a read-only cancellation signal, the job-kind handler
  registry, the pure mapping from what a handler did to an attempt outcome (a checked copy
  of a valid result; control-plane signals are never recorded), and validated worker
  timing settings with no defaults (DEC-042 … DEC-044).
- Implemented and verified locally and in GitHub CI (PR #10): a first single-slot worker
  runtime. It claims supported kinds, runs one handler thread, renews the lease, sweeps for
  expired attempts while idle or busy, and records outcomes through the Day 3A operations.
  A local watchdog ends the process when the worker cannot establish that it still owns a
  running task, or when a database call stalls (DEC-045, DEC-046).
- Implemented and verified locally and in GitHub CI (PR #11): a completion whose answer was
  lost is reconciled from one consistent PostgreSQL read of the attempt and its task, with
  results compared by `jsonb` equality: it committed, the same outcome is written again, or
  recovery revoked it. The handler never runs again; an outcome unresolved by the attempt's
  deadline ends the process (DEC-047).
- Implemented and verified locally and in GitHub CI (PR #12): cooperative cancellation. A
  heartbeat that confirms a committed request restricts the fence to the end of the grace
  period and then tells the handler through its read-only signal; a handler that
  cooperates ends the attempt cancelled. Notice is only as timely as heartbeats (DEC-048).
- Implemented and verified locally and in GitHub CI (PR #13): graceful, bounded shutdown. A
  stop request (SIGTERM or SIGINT) is latched by the supervisor, which then claims and
  starts nothing; the task it holds is drained and settled through PostgreSQL, or a drain
  guard ends the process (exit 9) and recovery takes the attempt after its lease, using
  one attempt of its budget. Shutdown writes nothing and never cancels or fails a task
  (DEC-049).
- Implemented and verified locally only (recovery correction, DEC-050): worker supervisor
  sessions carry a `lock_timeout` shorter than `db_operation_timeout` and an
  `idle_in_transaction_session_timeout` equal to it. A worker frozen inside a transaction
  no longer holds its attempt's row lock indefinitely, and a recovering worker that meets
  a held lock retries instead of being ended by its watchdog. Other clients' sessions are
  not bounded by this.
- Not yet implemented, and not proven by the current tests:
  - a command or service that runs workers;
  - stopping handlers that hold the GIL or detach processes;
  - bounded worker slots;
  - retry backoff.

  A task whose worker dies is recovered only while some worker runs. These must be
  verified by their own tests before being described as working.
- Cross-table invariants (a running task has exactly one running attempt whose number
  equals `attempt_count`) are the responsibility of the transactional operations, not the
  schema; the integration tests check them across the whole schema after every test.
- Lease renewal is the one write that locks only the attempt row; it never waits for a task
  lock while holding it, so it cannot deadlock with the task-first writes (DEC-036).
- Every started attempt consumes retry budget, including attempts lost to crashes.
- Revoking an attempt's database ownership does not stop or un-bill an external AI call;
  usage records must be attributable to the attempt that incurred them even after it
  loses ownership.

**Runner-up:** a broker or queue as the system of record with state-only status updates —
simpler delivery, but no transactional history and no protection against stale writes.
