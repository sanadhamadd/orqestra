# ADR-0004: Independent single-slot workers over a PostgreSQL queue

- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **Date:** 2026-11-07
- **Ledger entries:** DEC-033, DEC-044, DEC-045 … DEC-051, DEC-053, DEC-054

## Context

[ADR-0003](0003-authoritative-state-in-postgresql.md) makes PostgreSQL the authoritative
store for jobs, tasks and attempts, with generation fencing, leases, execution deadlines and
revocation. It does not say how work reaches a worker, what a worker process is, or what
happens when the process itself misbehaves. Those choices decide how the system scales, how
a stalled or crashed worker is contained, and what an operator has to run.

Three facts constrain them:

- A Python thread cannot be stopped. A handler that may no longer own its task can only be
  stopped by ending its process.
- A database call can stall indefinitely: a stalled network still acknowledges TCP, so
  neither `statement_timeout` nor keepalives end the call on the client.
- Recovery is the database's job. Anything a worker keeps in memory, such as when a task
  may be retried, is lost exactly when a worker crashes.

## Decision

- **PostgreSQL is the queue** (DEC-033). There is no broker. `claim_next_attempt` locks the
  first claimable task of a kind the worker runs, by `created_at` then `id`, with
  `FOR UPDATE SKIP LOCKED`, and starts its attempt in the same transaction. A claim never
  waits for a row lock, so concurrent workers take distinct tasks; the order is a
  preference, not strict FIFO.
- **One process, one task** (DEC-045, DEC-054). A worker process is a supervisor, one
  handler thread and a watchdog thread.
  - The supervisor alone uses the database, on one autocommit connection. It claims, starts
    the handler, renews the lease, sweeps for expired attempts while idle or busy, and
    records the outcome. It never claims again until the previous task is settled.
  - The handler thread gets no connection.
  - The watchdog ends the process when an armed deadline passes: the ownership guard (the
    lease end on the local clock, less `fence_margin`; exit 3) while a handler runs, and
    the operation guard around each supervisor database call (exit 4).
  - Scale is N independent processes against the same database. There are no in-process
    slots and no Orqestra-owned service or restart supervisor.
- **Database errors are never handler failures** (DEC-046). A retryable error is retried, a
  defect ends the process (exit 7), and an outcome whose recording is uncertain is never
  reported as recorded and never causes the handler to run again.
- **A lost completion answer is reconciled** (DEC-047). One read of the attempt and its task
  says whether the completion committed, the attempt still runs (the identical outcome is
  written again), or recovery revoked it. Results are compared with PostgreSQL's `jsonb`
  equality. An outcome still unresolved at the attempt's deadline ends the process (exit 6).
- **Cancellation reaches the handler through heartbeats** (DEC-048). When a heartbeat reads
  a committed request, the supervisor stops renewing, moves the fence to the end of the
  grace period, and only then sets the handler's read-only cancellation signal.
- **Shutdown is graceful and bounded** (DEC-049). SIGTERM or SIGINT requests a stop without
  taking a lock. From the latch on, the supervisor claims nothing and starts nothing; a task
  it holds is drained, or the drain guard ends the process at `shutdown_timeout` (exit 9).
  Shutdown writes nothing; an abandoned attempt is left to recovery.
- **Supervisor sessions bound their waits** (DEC-050): `statement_timeout` and
  `idle_in_transaction_session_timeout` equal `db_operation_timeout`, and a required
  `lock_timeout` is shorter, so a frozen worker cannot hold a row lock that a recovering
  worker waits on until its own watchdog ends it.
- **`orqestra worker run` is the entry point** (DEC-051). Kinds are named explicitly and
  matched against a fixed registry of built-in diagnostic handlers, refused in production.
  Startup fails fast, with one JSON line and exit 1, unless the settings validate and the
  schema's migration history equals the packaged one exactly; it never migrates.
- **Retry timing is durable** (DEC-053). A requeued task stores `available_at`, the
  database time from which it may be claimed: the clock read after the locks plus a
  bounded exponential delay with deterministic jitter from a required `RetryPolicy`. Both
  claim paths refuse a task before then, inside the same atomic claim.
- **Every timing value is explicit and required** (DEC-044, DEC-054): lease, execution
  timeout, retry delays, cancellation grace and the worker's own intervals have no
  defaults. Numeric operating values stay undecided until they are measured.

## Consequences

- Implemented and verified on the merged pull requests the ledger entries name: discovery
  and kind filtering (PR #5, PR #8), the worker runtime and its watchdog (PR #10),
  reconciliation (PR #11), cooperative cancellation (PR #12), graceful shutdown (PR #13),
  session bounds (PR #14), `orqestra worker run` (PR #15), durable retry eligibility
  (PR #17) and the deadline boundary at worker level (PR #18). The worker's exit codes are
  `ExitCode` in [`worker/fence.py`](../../src/orqestra/worker/fence.py).
- Workers need nothing but PostgreSQL. Adding capacity means starting another process with
  the same settings; every worker must use the same retry policy and timing values, and
  nothing enforces that.
- Execution is at least once. A handler can run again after a crash, a lost lease or an
  uncertain claim, and every started attempt uses retry budget, so handlers must tolerate
  repeats.
- Ending the process is the containment for every case where ownership cannot be
  established. The cost is conservative exits: a slow database call or a watchdog racing a
  handler that has just finished ends the process, and recovery decides after the lease.
- The fence cannot stop a handler that holds the GIL in native code, or processes a handler
  detached. The local and database clocks are assumed to run at the same rate, with
  `fence_margin` absorbing small differences; the database's own fencing still rejects a
  stale write.
- Recovery needs a live worker. There is no bound on recovery delay while no compatible
  worker runs, and the operator owns starting, restarting and sizing the workers.
- The only shipped handlers are diagnostics. Running a real workload, such as invoice
  verification, needs a handler registered with a worker, which DEC-062 keeps gated.
- Each claim scans and sorts the claimable tasks: there is no index for the claim query.
  Its cost at depth is a measurement question, not a decided one.

**Runner-up:** an external broker delivering task ids, with multi-slot worker processes and an
Orqestra-owned supervisor. It scales delivery independently and uses fewer processes, but it
adds a second system that must agree with the authoritative rows, and sibling slots in one
process cannot be stopped one at a time when a handler loses ownership.
