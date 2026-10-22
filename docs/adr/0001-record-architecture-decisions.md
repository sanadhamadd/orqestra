# ADR-0001: Record architecture decisions

- **Status:** Proposed
- **Date:** 2026-10-22

## Context

Orqestra is built by one engineer with AI assistants. Design is discussed across several
chats; the charter rules that nothing discussed in chat is binding until recorded in the
repository. Decisions therefore need a durable, reviewable home.

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
- Assistants must read existing ADRs before proposing conflicting designs and flag
  conflicts instead of silently overriding them.

**Runner-up:** free-form design notes in the wiki or chat — faster to write, impossible to
supersede cleanly or audit.
