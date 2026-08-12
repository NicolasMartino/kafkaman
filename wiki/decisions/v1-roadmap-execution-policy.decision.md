# V1 Roadmap Execution Policy

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-21
- Category: Delivery process
- Scope: Defines how V1 design decisions are ratified and how parallel worktrees may be used without breaking roadmap dependencies.
- Sources:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/decisions/runtime-composition-and-topology.decision.md
  - wiki/decisions/message-consumption-and-handler-model.decision.md
  - wiki/decisions/library-test-strategy.decision.md
  - wiki/decisions/consumer-test-tooling.decision.md
  - User direction in Codex conversation on 2026-06-21

## Decision

1. **Split accepted design choices from validation status.**
   A page may contain accepted sub-decisions while the overall decision remains
   `Draft` until its milestone validates the behavior in code and tests.
2. **Do not blanket-accept the remaining Draft decisions.**
   Receive runtime, consumer test tooling, library test strategy, and receive
   portions of runtime composition stay Draft until their milestone exits prove
   them.
3. **Accept only the subset that has been decided or validated.**
   For example, M1 can ratify send-side runtime composition; M3 can ratify
   durable receive after it passes its exit gates; M7 can ratify full test
   pyramid/tooling claims.
4. **Use dependency-aware parallel worktrees.**
   Work may run in parallel when it has clear boundaries and does not require
   unvalidated downstream APIs.

## Options Considered

Decision status options:

1. **Accept every Draft decision now.**
   Rejected because it would promote unvalidated receive/test/runtime claims.
2. **Keep every Draft decision untouched until implementation exit.**
   Rejected because some choices, such as required idempotency keys and retry
   policy shape, need to guide implementation now.
3. **Use informal "accepted direction" wording only.**
   Rejected because implementers still need to know which choices are settled.
4. **Split accepted sub-decisions from milestone validation.**
   Accepted. It is precise about what is settled and honest about what is not
   yet proven.

Parallelization options:

1. **One linear branch.**
   Low merge risk, but too slow for V1.
2. **Parallelize all milestones immediately.**
   High apparent throughput, but likely to create API churn because M3 depends
   on M2 and M4 depends on M3 seams.
3. **Dependency-aware worktrees.**
   Accepted. Parallelize M1 closeout, M2 substrate work, M3 API/test sketches,
   retry/DLQ documentation, and docs/status updates. Serialize merges around
   shared migration/config/runtime APIs.

## Worktree Rules

- Each worktree states its lane, scope, files likely to change, and merge
  prerequisite.
- M1 closeout merges before broader M2/M3 code that depends on durable-send
  schema shape.
- M2 change-engine/config work may start in parallel but owns the migration and
  config substrate.
- M3 work may start as Harness-level failing tests and API sketches, but deep
  receive implementation waits for M2 substrate.
- M4 implementation waits for M3 receive seams, but the retry/DLQ policy
  decision is accepted now.
- Docs/status cleanup may proceed independently when it does not alter code
  contracts.

## Revisit When

- Worktree merge conflicts repeatedly land in the same crate/module, showing the
  lane boundaries are too broad.
- A milestone proves a Draft decision wrong and the accepted sub-decision needs
  to be superseded.
