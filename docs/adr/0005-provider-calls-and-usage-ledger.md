# ADR-0005: Provider calls: deterministic checks decide; intent-first usage ledger; no prices

- **Status:** Accepted (owner, 2026-11-07; was Proposed)
- **Date:** 2026-11-07
- **Ledger entries:** DEC-055, DEC-057 … DEC-062; applied by DEC-065 and DEC-066

## Context

The invoice workload can ask an external provider to read an amount that deterministic code
cannot parse. A provider call costs money, can fail or time out, and can happen in an attempt
that later loses ownership. Under at-least-once execution
([ADR-0003](0003-authoritative-state-in-postgresql.md),
[ADR-0004](0004-independent-workers-over-a-postgresql-queue.md)) a call can be made more than
once for the same item, and revoking an attempt in the database neither stops nor un-bills a
call already made.

So the system needs answers to four questions before any live call: what a provider is
allowed to decide, how many calls an attempt may make, how each call is recorded so that a
crash cannot hide it, and what a report of those calls can honestly claim.

## Decision

- **Deterministic checks decide** (DEC-055). Each synthetic invoice is `verified`,
  `rejected`, `needs_review` or `malformed` by two arithmetic checks. A provider is called
  only for an ambiguous amount string, once per such amount, with the capability, route,
  field kind, raw string, `request_id` and `timeout` and nothing else. Its reply must be a digit string; the value is
  substituted and the same checks run again, so the checks, not the provider, decide the
  status. Route choice is pure: the lowest estimate among eligible routes, and a missing
  estimate is never read as zero.
- **Every call is numbered under a required per-attempt cap** (DEC-057). An attempt's calls
  are numbered from 0 by one source that enforces the cap: an invoice that needs more calls
  than the attempt has left is `needs_review` (`provider_call_cap_reached`) and the provider
  is not called. Each call records an intent before it is made and a result after it; if
  the intent cannot be recorded, the call is not made.
- **Usage capture is closed with the attempt** (DEC-058). The transaction that ends an
  attempt also closes its capture: the worker's own closure on completion, or recovery's on
  revocation at the capture tier declared for the job kind. A closure is `complete` only
  when every intent has its result, and recovery closes `complete` only at tier T3;
  otherwise it is `incomplete`.
- **The worker writes the ledger through its supervisor** (DEC-059). A `UsageSink` hands
  each record from the handler thread to the supervisor, which alone uses the database. The
  call is made only once its intent row has committed. A ledger failure is never the
  handler's outcome: the supervisor retries, replays a lost answer, or ends the process.
- **The ledger is append-only and has no prices** (DEC-062). `provider_calls`,
  `provider_call_results` and `attempt_usage_closures` refuse `UPDATE`, `DELETE` and
  `TRUNCATE`. They keep the amount and unit the provider reported, never a price, a
  currency or a conversion to money. Route estimates stay separate from reported figures.
- **Reports say how final they are** (DEC-061). A job's usage is read from the ledger in the
  same `REPEATABLE READ, READ ONLY` snapshot as its tasks, with exact per-unit totals summed
  in PostgreSQL, and a finality of `open`, `lower_bound` or `closed`. Unknown usage is never
  read as zero.
- **A real provider adapter exists but is not wired in** (DEC-060). The OpenRouter adapter
  has no third-party dependency, works over an injected transport, has no default model, makes
  one POST per call with a deadline checked before every blocking step, and logs nothing.
  No other module imports it.
- **Live use is gated** (DEC-062). There is no inbound API. Every live provider call needs
  the maintainer's separate authorization; only synthetic data is recorded until a retention
  policy is accepted; and invoice verification is registered with a worker only when a
  durable recorder, a validated cap, a matching capture tier and authorized live calls are
  all in place.

## Consequences

- Implemented and verified on the merged pull requests the ledger entries name: the
  verification workload and routing (PR #19), the per-attempt cap and call recorder
  (PR #25), the usage ledger (PR #24) and its closures (PR #26), the usage report (PR #27),
  the OpenRouter adapter (PR #29), the worker's usage sink (PR #30) and the boundaries for
  live use (PR #31). Two later entries apply them: the call budget check over the usage
  report (DEC-065, PR #37) and the `orqestra job summary` and `orqestra job usage` commands
  (DEC-066, PR #39).
- A provider can never change a status by itself: a wrong reply is either refused or checked
  by the same arithmetic as everything else.
- Calls per attempt are bounded by the cap, and calls per task by the cap times the task's
  retry limit. The cap is not stored; the bound holds only if every worker of a job used
  the same one.
- The ledger records the calls of every attempt, including attempts that later lost
  ownership, so a job's calls include its repeats. A call whose intent committed cannot be
  dropped silently: without a result it stays unresolved, and the report is a lower bound.
- The T3 contract is documented, not enforced: every handler registered for a kind the sink
  records must record every call through the sink, or a closure would claim `complete` for
  fewer calls than were made. Checking this at registration is a required future step.
- No report gives a money figure. A cost report needs a later decision on a price source.
- Ledger rows are never deleted; there is no retention mechanism yet, which is why only
  synthetic data may be recorded.
- Not yet implemented, and not proven by the current tests: invoice verification registered
  with `orqestra worker run`, the OpenRouter adapter wired into any worker, and any live
  provider call.

**Runner-up:** recording usage after the fact from the provider's billing data, with prices in
the ledger and the provider allowed to return a verdict. It needs less code in the worker, but
a crash between a call and its record would hide the call, a price table would need its own
versioning, and the provider would decide statuses that deterministic checks can decide.

## Notes

- **2026-11-10, DEC-071.** The OpenRouter adapter is now wired into one worker, the
  unpackaged benchmark worker. Only a single `benchmarks.run` cell can select it, it is off by
  default, and every call reserves one of the cell's calls before its intent, failing
  closed once they are spent. Its tests and its dry run use an in-memory transport. No `src`
  module imports the adapter, invoice verification is still not registered with
  `orqestra worker run`, and no live provider call has been made; each one still needs the
  owner's separate authorization (DEC-062). The text above is unchanged.
