# ADR-0001: Record architecture decisions

- **Status:** Proposed
- **Date:** 2026-10-22

## Context

Orqestra is developed by a single maintainer, and design is discussed in several places
outside the repository. A discussion that is not recorded in the repository is not
binding. Decisions therefore need a durable, reviewable home.

## Decision

- Material choices are logged as short WHAT / WHY / RUNNER-UP entries in
  `docs/decisions.md`.
- Cross-cutting or expensive-to-reverse choices get an ADR in `docs/adr/` using this
  format: Status, Date, Context, Decision, Consequences.
- ADRs and ledger entries are created as **Proposed** and become **Accepted** only when
  the owner says so in a reviewed change. Superseded records are kept and linked.

## Consequences

- Every pull request lists its material decisions or states "No material design
  decisions".
- Contributors must read existing ADRs before proposing conflicting designs and flag
  conflicts instead of silently overriding them.

**Runner-up:** free-form design notes in a wiki or ad hoc messages — faster to write, impossible to
supersede cleanly or audit.
