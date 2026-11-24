# Known limitations


- `orqestra worker run` is a command, not a service: nothing restarts, supervises or
  deploys it, and it can run only the built-in diagnostic handlers. A task whose worker dies is
  recovered only while some worker is running: any live worker sweeps, idle or busy.
  There is no guaranteed recovery delay: it depends on live workers, their sweep
  interval and batch limit, and the backlog of expired attempts. It is never shorter than
  the attempt's current lease: an attempt becomes revocable only when that lease ends
  (normally a full lease after its last renewal, sooner after a cancellation request's
  grace, DEC-040), and only a live worker's sweep revokes it.
- Revoking an attempt ends its database ownership only, and does not cancel or un-bill an
  external provider call already made. Calls recorded in the usage ledger stay attributed to the
  revoked attempt through its `attempt_id`. Recovery closes an attempt's usage capture only
  for kinds given a tier in `usage_capture`; an unmapped kind with no recorded calls reads
  `not_captured`, never "zero", and an unrecorded call is not known to the ledger. A
  worker passes a closure and a tier map only for the kinds of its `UsageSink` (DEC-059).
- The OpenRouter adapter is wired only into the benchmark worker, behind an opt-in, and
  never into `orqestra worker run` (DEC-071); no live run has been made. Its own limits are
  listed in [OpenRouter adapter](usage.md#openrouter-adapter-benchmark-opt-in-only).
- Invoice verification has no live provider and no packaged worker: `orqestra worker run`
  builds its worker without a `UsageSink`. Only unpackaged workers wire the ledger: the
  benchmark workers (`benchmarks/worker.py`, `benchmarks/faults.py`) register invoice
  verification behind a `UsageSink` at tier T3, and the process tests' worker
  (`tests/integration/worker_process.py`) does so for its `metered` test kind. With
  `InMemoryCallRecorder` calls stay in memory. The worker's `UsageSink` is durable, but nothing checks that a sink kind's
  handlers record every call through it (the T3 contract), or that every worker of a fleet
  uses the same tier map. The ledger, and so the usage report, holds only what is
  recorded through `orqestra.persistence.provider_usage`. The stored `routing` shows only
  which route was chosen. A provider error
  repeats the whole batch on retry, so billable calls could repeat (each attempt keeps
  its own cap); deterministic request ids would let a real adapter deduplicate them, and
  no exactly-once claim is made.
- A provider call can be left unresolved: an intent with no result. This happens when
  the provider raises something that is not a provider error, returns something that is
  not a `ProviderResponse`, the process is interrupted, or the result is not written (the
  worker's sink writes none for a reported amount outside the ledger's range). With
  another recorder, a recorder failure is a handler failure: an unclassified error that
  is not retried; with the worker's sink, a ledger failure ends the process instead. The
  cap is checked against the worker's execution timeout only by
  `check_provider_call_budget`, which nothing calls for you (DEC-059).
- The invoice job summary is read in process or with `orqestra job summary` (DEC-066), one
  job per call; no API exposes it. Its snapshot read grows with the number of tasks and the
  size of their stored results, and holds back vacuum while it lasts; the cost of the
  ordering, the effect of the fetch size and the behaviour under concurrent completions
  are measured by tests that no default lane runs, and no threshold or batch-size limit is
  decided. Counts are per-task tallies, not a final job tally unless `complete`.
- The provider usage report is read in process or with `orqestra job usage` (DEC-066), one
  job per call; no API exposes it. Its read
  grows with the number of tasks, attempts and ledger rows; it is measured at 50,000
  invoices by a test that no default lane runs, and no threshold is decided. Tokens and
  closure tiers are not totalled.
- Every integration test and benchmark run uses one machine and one local PostgreSQL server
  (in Docker for the Compose setup and in CI), reached over a Unix socket or the loopback
  network: the integration suite refuses any
  other target (`check_test_target` in `tests/integration/postgres.py`), and so does the
  benchmark database guard (`benchmarks/database.py`). There is no multi-host result.
- `orqestra worker run` runs the diagnostic handlers only (`KINDS` in
  `src/orqestra/worker/builtin.py`), so the shipped command never runs the invoice handler,
  and no invoice result says anything about it.
- The offline provider double for benchmarks, `SeparatorReader` (`benchmarks/provider.py`),
  answers at once and makes no network call, so nothing here measures a real provider's
  latency, cost or reliability.
- The demonstration's timings (`python -m benchmarks.demo`) describe one machine and one
  run; they are printed and recorded, never checked, and never enter the performance
  tables.
- There is no purchase-order matching: the invoice rules check each invoice on its own
  (`src/orqestra/workloads/invoice/verify.py`).
- Usage is totalled per exact unit that a provider reports, such as `tokens`, with no
  price, currency or money figure (DEC-062).
- The call budget check (`--max-provider-calls-per-attempt`, DEC-065) assumes that every
  worker of the job used that cap; the cap is not stored, so nothing confirms it.
- **The worker's local fence is limited.**
  - It cannot stop a handler that holds the GIL in native code: the watchdog thread then
    cannot run either.
  - It cannot stop processes a handler started and detached.
  - It assumes the local and database clocks run at the same rate, with `fence_margin`
    absorbing small differences. A step forward in the database clock can end the real
    lease before the local fence.
  - It has been shown under the tested conditions only: a stalled proxy, refused
    connections, a cancellation request, and a lease ended by the database.
- **Worker runtime limits.**
  - **Cancellation notice:** a handler is told of a request only at a heartbeat. With a
    grace period shorter than the time to the next heartbeat, or during a database
    outage, it is fenced without notice. Handlers that hold the GIL cannot react, and
    nothing interrupts a handler that ignores the signal before the fence.
  - **Unresolved completions:** a completion whose answer was lost, and which cannot be
    reconciled before the attempt's deadline (for example, the database stays
    unreachable), ends the process; recovery or the committed completion then decides.
    The worker does not renew the lease while reconciling, so a long outage can let
    recovery revoke the attempt first, discarding a finished outcome.
  - **Lost claims:** a claim whose answer was lost, or whose task could not then be
    read, is never started and is recovered when its lease expires, using one attempt
    of its budget. The worker claims nothing else until that lease has certainly ended
    (a full `lease` after a lost answer). So a worker process runs at most one handler at
    a time and never holds two attempts whose leases are both valid; but the abandoned
    attempt can still read `running` in the database, beside the worker's next one, until
    recovery revokes it.
  - **Shutdown timing is conditional.** From a signal, the process ends within about
    `db_operation_timeout + shutdown_timeout`, plus exit time, only while the
    watchdog and main threads can run: a handler holding the GIL delays the signal
    handler and the watchdog alike. SIGKILL, and an orchestrator whose kill grace is
    shorter than this, end the process like a crash. Processes a handler started and
    detached survive. The worker's wake pipe (`select.poll`) and the signal helper are
    POSIX only.
  - **Shutdown costs budget when it abandons work.** An attempt left by exit 9, or a
    claim that never started, uses one attempt and is recovered only after its lease
    ends, by a live worker. Handlers are not told about shutdown.
- Partitioning (DEC-052) has no production batch size and no size limits: a whole job,
  however many batches, is still one submission transaction, and replaying it compares
  every task. The large-scale benchmark (BS-05) has run
  ([docs/performance.md](performance.md)); no bound is decided from it yet.
- There are no default or maximum lease values: callers choose them. Operational bounds
  and per-job-kind policies are not decided yet. `WorkerSettings` requires every timing
  value and has no defaults.
- `run_handler` cannot interrupt a handler. The existing `Settings.worker_concurrency`
  (default 4) is validated but not read by anything; the worker is single-slot.
- Handler results are checked more strictly than `jsonb` requires: only exact built-in
  types are accepted (a `tuple`, a `str` enum or a `dict` subclass is refused rather than
  converted), and nesting deeper than about 1,000 levels is refused. Results and failure
  messages have no size limit yet.
- **Lock and idle-transaction bounds (DEC-050) are bounds, not a recovery-time promise.**
  - They protect worker supervisor sessions only. Other clients (a cancellation request
    in progress, a migration, a person in `psql`) can still hold task or attempt locks
    for as long as their sessions allow; workers then retry, and a worker whose heartbeat
    cannot get its lock for longer than its lease fences itself.
  - `idle_in_transaction_session_timeout` ends any supervisor session paused between two
    statements of a transaction for `db_operation_timeout`, whatever the cause. A
    supervisor that is running cannot pause that long inside its transaction without its
    operation guard ending the process first; a paused process (SIGSTOP, a suspended VM
    or container, a debugger) loses its session and its transaction rolls back.
  - Timing still depends on scheduling and the network: the settings relationships do not
    guarantee that a delay cannot outlast the watchdog's deadline.
- A heartbeat proves the worker is alive, not that its work progresses: a stuck handler
  keeps renewing its lease until the deadline, and only then is revoked.
- A database outage longer than the lease makes every live attempt revocable once the
  database returns, and each revocation uses one attempt of its task's budget.
- A retry delay is the earliest time a requeued task may run, not a schedule: it runs
  when a live worker supporting its kind next polls after that time, and behind any
  backlog. A retried task keeps its original `created_at`, so once eligible it is claimed
  ahead of newer tasks. Each claim reads the queued tasks in order through the partial
  index of migration 0006 (DEC-069), so with every task eligible its cost no longer grows
  with the queue. It still steps over queued tasks that are waiting out a delay or are of a
  kind the worker does not run, so a long run of those ahead of the eligible ones costs more,
  and the index adds a measured write cost; both are in
  [docs/performance.md](performance.md#the-claim-index-with-and-without).
  `None` from `claim_next_attempt` or `revoke_next_expired` means only that nothing was
  claimed or revoked by that call.
- Retry policy consistency is not enforced: each process uses its own settings, and a
  worker configured differently schedules different delays for the tasks it requeues.
  There are no production retry values (DEC-034), no per-job-kind policy, and the delay
  does not depend on the error (for example, a rate limit's `Retry-After`). A database
  outage longer than the lease still revokes every live attempt when it returns; their
  retries are spread by the jitter only.
- There is no rate limiting or backpressure: nothing limits how fast workers claim tasks or
  call a provider, or slows submission under load. The OpenRouter adapter maps a provider's
  `429` to the transient `rate_limited` code (DEC-060), but no worker uses that adapter yet; a
  transient provider error fails the batch, which is retried after the ordinary retry delay,
  which ignores `Retry-After`.
- A task whose job kind no claimant supports stays `queued` indefinitely: nothing reports
  or expires it yet. Each claim now also reads the job of the queued tasks it considers; the
  claim cost of an unsupported backlog, and the plan PostgreSQL chooses for it, are measured in
  [docs/performance.md](performance.md) ("Claim cost against queue depth", the
  `unsupported` variant).
- The claim reads a job's kind without locking the job, which relies on kinds never
  changing. No code path updates a job's kind, but the database does not forbid it.
- Cancellation is cooperative and bounded by the grace period. A worker learns of a
  committed request only when a heartbeat reports it; it then tells its handler through
  the read-only cancellation signal (DEC-048). Notice is not guaranteed before the grace
  period ends, for example during a database outage or when the grace is shorter than the
  time to the next heartbeat. Nothing can forcibly interrupt a handler that ignores the
  signal: the worker is fenced at the end of the grace period, and the attempt is revoked
  only when a live worker's recovery sweep runs. There are no default or maximum grace
  values, and the heartbeat interval must be shorter than any grace used for a worker to
  hear in time. There is no job-level cancellation.
- A request resets a valid lease to one grace period, so if the worker is already dead,
  recovery waits for the grace period instead of the lease it had left; a grace longer
  than that lease delays recovery.
- Execution is at-least-once only where workers are running. Running workers claim the
  kinds they support and recover expired attempts, and generation fencing records only
  the current generation's result. A task makes no progress while no running worker
  supports its kind or no worker is running at all, so eventual completion is not
  guaranteed. Neither execution nor external effects are exactly-once: a handler may run
  again after recovery.
- Migration 0002 needs a quiescent database: stop workers and claimers, let running
  attempts finish, migrate, then deploy. It refuses while any task or attempt is running,
  and fails at once while another transaction holds any lock on `attempts`, or a lock on
  `tasks` stronger than a plain read's (a write, or `SELECT … FOR UPDATE` or `FOR SHARE`);
  a transaction that has only read `tasks` does not stop it. Code from before 0002 cannot
  run against the new schema: it cannot read revoked attempts, and its claims are
  rejected.
- Migration 0003 is additive and may run while attempts are running, but it fails at once
  while any session uses `attempts`. Code from before 0003 must not run once the new code
  can write `cancellation_grace_expired`: it cannot read that reason, and its heartbeats
  ignore cancellation. Stop all application processes, apply 0003, then start only
  code that includes migration 0003; draining first is preferable. The database does not enforce this order.
- Migration 0004 is additive and may run while attempts are running, but it fails at once
  while any session uses `tasks`. Code from before 0004 ignores `available_at` and would
  retry requeued tasks at once. Stop all application processes, apply 0004, then start
  only code that includes migration 0004, with one shared retry policy. `orqestra worker run` refuses to start
  against any other migration history, but a worker already running is not stopped, and
  direct callers of the lifecycle functions are not checked.
- A job whose started tasks were all requeued reads as `queued`; one failed task makes a
  whole job `failed` (the per-state counts give the detail).
- Idempotency keys are global (not scoped per job kind or client) and never expire.
  Keys match exactly, including case and whitespace. Payload equality follows `jsonb`:
  object key order is ignored, numbers compare by value (`1` equals `1.0`) and strings
  compare exactly, without Unicode normalisation.
  Replay detection relies on submission columns never being updated; no code path
  updates them, but the database does not forbid it.
- A submission is one transaction regardless of its size; each benchmark record times its
  submission (`timing.submission`), but no size limit is decided from it. Tasks are listed by `task_key` in code-point order; the order in which
  they were submitted is not stored.
- Cross-table invariants (for example, a running task has exactly one running attempt
  whose number equals `attempt_count`) are maintained by the lifecycle operations and
  checked by the integration tests, not enforced by the schema.
- Migrations are forward-only and transactional; statements that cannot run in a
  transaction, such as `CREATE INDEX CONCURRENTLY`, are rejected.
