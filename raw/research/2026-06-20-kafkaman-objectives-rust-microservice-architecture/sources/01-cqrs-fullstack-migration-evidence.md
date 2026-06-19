# Source Note: cqrs-fullstack Migration Evidence

Original project: `/Users/nicolasmartino/Documents/workout/cqrs-fullstack`
(copied locally as `sources/01-cqrs-fullstack/`, which is gitignored due to size
and because it is a separate private project).
Retrieved: 2026-06-20
Mode: Local path copy; this note is a committed excerpt of the relevant files so
the provenance for `wiki/decisions/messaging-scope-and-receive-model.decision.md`
survives in the tracked repo.

## Why This Note Exists

The messaging-scope decision rests on the claim that the reference architecture
**tried commands-over-Kafka and deliberately retired them in favor of a durable
HTTP dispatch engine**. The full source is not committed, so the load-bearing
evidence is excerpted verbatim below.

## Evidence 1 — Kafka command path was built, then dropped

File: `code/exercise-api/migrations/010_drop_legacy_command_transport_tables.sql`

```sql
-- Remove Kafka command-path tables left over from the pre-cutover architecture.

DROP TABLE IF EXISTS commands_inbox;
DROP TABLE IF EXISTS processed_commands;
```

The presence of `code/exercise-api/migrations/006_commands_inbox.sql` (creating
`commands_inbox`) followed by migration 010 dropping it confirms the path was
created and later removed.

## Evidence 2 — Replacement is a durable HTTP dispatch engine (same machinery, different verb)

File: `code/frontend/app/migrations/008_create_mutation_jobs.sql`

```sql
-- Durable internal HTTP mutation dispatch queue.

CREATE TABLE IF NOT EXISTS mutation_jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    correlation_id UUID NOT NULL,
    -- ...
    service TEXT NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    body JSONB NOT NULL,
    mutation_id UUID NOT NULL,
    sequence_no INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- ...
    locked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE mutation_jobs IS 'Durable queue for internal HTTP mutation delivery';
COMMENT ON COLUMN mutation_jobs.status IS 'Pending | Dispatching | Applied | Rejected | RetryableFailure | TerminalFailure';
```

The dispatcher claims rows with `FOR UPDATE SKIP LOCKED`
(`code/frontend/app/src/server/mutations.rs:345`) — the same claiming pattern an
outbox relay uses, with HTTP POST as the dispatch verb instead of Kafka produce.

## Evidence 3 — Synchronous outcome correlation is a host/BFF concern

File: `code/frontend/app/migrations/002_create_outcome_waiters.sql`

```sql
-- BFF Outcome Waiters Table
-- Tracks pending wait-for-outcome requests for efficient correlation.
-- When a client submits a mutation or sync request, the BFF may wait for
-- the outcome from the target service before responding.

CREATE TABLE IF NOT EXISTS outcome_waiters (
    correlation_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    batch_ids TEXT[] NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    outcomes JSONB
);
```

The expiry/batch/partial-outcome policy lives in the BFF, not in a reusable
library primitive — supporting the decision that kafkaman provides outcome
*primitives*, not the wait *policy*.

## Evidence 4 — Idempotent receive on the target service

File: `code/exercise-api/migrations/009_create_processed_mutations.sql`

```sql
CREATE TABLE IF NOT EXISTS processed_mutations (
    actor_id UUID NOT NULL REFERENCES user_cache(id) ON DELETE CASCADE,
    mutation_id UUID NOT NULL,
    outcome_status TEXT NOT NULL,
    response_status INT NOT NULL,
    response_body JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (actor_id, mutation_id)
);
```

The receive handler (`code/exercise-api/src/adapters/handlers/http/mutations.rs`)
uses an `Idempotency-Key` header and `tx_processed_mutation_find/record/lock` to
make redelivery safe and return the stored response — i.e. duplicate dispatches
are absorbed at the consumer. This is the at-least-once + idempotent-consumer
model, not transport-level exactly-once.
