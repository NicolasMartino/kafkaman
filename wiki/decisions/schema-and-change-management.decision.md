# Schema and Change-Management Model

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-20
- Category: Persistence and operations
- Scope: How kafkaman lays out its tables, owns its schema, and manages structural + operational change across environments.
- Sources:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md (the design discussion this decision records)
- Related:
  - wiki/proposals/01-kafkaman-objectives.proposal.md
  - wiki/decisions/messaging-scope-and-receive-model.decision.md
  - wiki/decisions/runtime-builder-and-axum-composition.decision.md
  - wiki/plans/first-poc-outbox-publisher.plan.md

## Amendment, 2026-08-26: never edit a table template in place

`wiki/decisions/runtime-builder-and-axum-composition.decision.md` adds a
standing rule this decision does not currently carry, and it belongs here
because it constrains every future schema change:

> Never edit an existing table template. Add an upgrade changeset and bump that
> template's `template_version`.

The reason is a gap in the migration engine rather than a style preference.
`descriptor_changeset!`'s `checksum_material` is
`version;name;message_type;topic` — it does not cover the DDL at all. An
in-place template edit is therefore invisible: existing databases keep the old
shape, fresh ones get the new shape, and the checksum matches either way. That
has already happened — `create_outbox_table_sql` contains `idempotency_key`,
`idempotency_source`, and `entity_key`, which is exactly what
`AddIdempotencyKey`, `AddIdempotencySource`, and `AddOutboxEntityKey` exist to
catch up. A fresh database replaying create-then-alter costs a few extra
statements and is always correct.

Generated changelogs additionally identify changesets by
`(role, message_type, template_version)` rather than registration order, since
the version is a durable primary key in the history table.

## Decision

1. **Dedicated Postgres schema `kafkaman`** (name configurable for `public`-only
   shops). No `kfkmm_` table-name prefix — the schema provides the namespace.
2. **Distinct table per message type**, generated from one uniform template (a
   `CreateMessageTable` changeset), so there is no schema drift between types.
   Not one generic table; not partitioned (for now).
3. **Idempotency** is enforced by a per-table `UNIQUE` on message identity. This
   is simpler than partitioning, which would force the partition key into every
   unique constraint.
4. **Purge/retention** is `DELETE`/`TRUNCATE`-based per type (acceptable at
   moderate volume with regular purging). Partitioning is **deferred** as a
   per-table optimization, applied only to a specific type if its volume ever
   makes `DELETE`-based purge painful.
5. **kafkaman ships its own change-management engine in Rust**, Flyway/Liquibase-
   inspired (`refinery` is the Rust precedent). One **explicit, ordered changelog**
   of **versioned changesets**, recorded in `kafkaman.changelog_history` with
   **checksum + applied_at + applied_by** (audit trail), applied **once per
   environment**.
6. **Two kinds of changeset, one mechanism.** Structural changes
   (`CreateMessageTable`) and one-shot operations (`Replay`) are the *same*
   versioned, audited mechanism. *Tunable settings* (retention, batch sizes, rate
   limits) are **per-env runtime configuration, not changesets** — see the
   [configuration & environment decision](configuration-and-environment-model.decision.md).
   There are **no SQL stored functions**.
7. **Two phases, both in the app process:**
   - `kafkaman::migrate(&pool)` = **DB convergence**, run **at app startup**
     (in `main`, before `Runtime::start()`) — mirroring how the reference runs
     `sqlx::migrate!` in `main` (`user-api/src/main.rs`). It is **advisory-locked**
     (concurrent replicas in a rolling deploy do not double-apply) and
     **idempotent** (changesets recorded once per env, so restarts/scale-ups
     re-run nothing). Applies pending changesets in version order; records them.
     The *same* function may instead be invoked as a standalone pre-deploy job for
     shops wanting DDL-role separation — but at-startup is the default.
   - **One-shot operational changesets stay fast so they never block startup:**
     `Replay` does a bounded state change only (flips affected rows to
     re-process / bumps a reprocess epoch). The heavy, rate-limited work
     (re-draining replayed messages) is done by the runtime subsystems **after**
     boot, not inside `migrate()`.
   - `kafkaman::Runtime::start()` = **runtime subsystems**: consumer schedulers,
     retry processor, and the **purge enforcer**.
   - Retention/purge is **runtime config, not a changeset**: the purge enforcer
     re-reads the per-env value (from `kafkaman.toml`) at each boot and acts, so
     changing it is a config + redeploy — not a new changeset. See the
     [configuration & environment decision](configuration-and-environment-model.decision.md).
8. **Plain tables, documented state machine.** No logic is hidden in DB
   functions, so break-glass manual fixes in prod remain possible (e.g. requeue
   by setting `status='Pending', next_attempt_at=now(), locked_at=NULL` — while
   skipping rows holding a live `locked_at` lease). Versioned changesets are the
   disciplined path; raw-table intervention is the documented escape hatch.
9. **Changeset authoring:** an explicit ordered Rust `changelog!` of changesets;
   kafkaman-provided declarative types (`CreateMessageTable`, `Replay`) plus a
   object-safe `Changeset` trait — `fn build(&self, cfg, b) -> Result<()>` in M1,
   or an intentionally boxed/generic async shape later if needed — for bespoke logic, where
   `cfg` is the resolved config bag (typed **value** selection only — no env
   identity to branch on; see point 11) and `b` is a `ChangeBuilder` that
   expresses the operations. Changesets use an explicit
   ordered list (complete, ordered, single-source history is the feature);
   annotations are reserved for *handlers*, not changesets.
10. **Versioning & placement:** changesets use **sequential integer versions**
    (`V001`, `V002`, …), **not** timestamps. This is deliberate: two branches
    adding the same number **collide on merge**, forcing devs to reconcile rather
    than silently interleave — a guard against undoing each other's work. (The
    collision check enforces ordering/awareness; semantic interference between
    differently-numbered changesets is still caught by changeset code review.)
    The changelog lives in its **own module** (e.g. `messaging/changelog.rs`),
    out of `main`, one changeset per file. Deriving the changelog from a
    directory (`embed_changelog!`) is deferred — not now.
11. **`apply(cfg)` may select values, not branch structure.** A changeset may
    read a per-env value from `cfg`, but must not branch *structural* logic on
    configuration — that causes schema drift and means the prod path was never
    exercised in lower envs. `cfg` exposes typed values, not an environment
    identity, so this is enforced by the contract rather than left to discipline.
    See the
    [configuration & environment decision](configuration-and-environment-model.decision.md).

## Guardrails for Operational Changesets

One-shot operational changesets (`Replay`) ride the same auto-applied `migrate()`
stream as structural changes. (Retention is *not* here — it is per-env runtime
config; see decision point 6.) The unified *ledger* (ordering + audit) holds;
these guardrails keep auto-apply safe:

- **Apply mode — auto-apply (decided 2026-06-20):** all changesets, structural
  and operational, apply automatically during `migrate()`. Rationale: anything
  reaching prod has already converged through lower environments.
- **Env/context targeting is the opt-out** (Liquibase-style contexts/labels): a
  changeset may declare which environments it applies to; this is how you hold a
  specific operation out of an env when you don't want it to auto-fire there.
- **Blast-radius limits (MANDATORY for bulk ops):** because operations run at each
  environment's *own* data scale, bulk operations — `Replay`, and the runtime
  purge enforcer — must batch and rate-limit, never one unbounded transaction.
  This is the safety net that makes auto-apply sound: lower-env testing validates
  a changeset's *correctness*, but not prod's *volume*, since the data differs
  per environment.
- **Dry-run / preview:** `migrate()` reports what a pending changeset *would* do
  (structural: the DDL; operational: e.g. "Replay will requeue ~N rows in table
  X"), surfaced in deploy logs so an automatic op is still *seen*.
- **Idempotency is already covered:** changesets run once per environment
  (tracked in `kafkaman.changelog_history`); re-deploys do not re-run them.

**Residual risk accepted:** a correct-but-large operation runs at full prod scale
on deploy; replay's downstream amplification (re-processing fans out to other
services) is inherent. Mitigated by mandatory batching/rate-limiting, dry-run
visibility, and env-targeting as a per-operation opt-out — not eliminated.

## Why

- **SQL stored functions were rejected:** untestable, undebuggable, and they lock
  kafkaman to one database. Logic belongs in Rust (unit-testable, type-safe,
  debuggable, portable).
- **CI/CD multi-environment reliability** requires changes that are versioned,
  ordered, applied-once, and checksummed — i.e. the Flyway model. A CLI is a
  manual per-env action and does not satisfy this; a versioned changelog applied
  by `migrate()` as a deploy step does.
- **Distinct per-type tables** were chosen for simplicity, operational clarity
  (named, inspectable tables), scoped reconsume, and *simpler* idempotency
  (per-table `UNIQUE`). They avoid partitioning's strong con (see alternatives).
- **Auditability:** the changelog history records who/what/when for every change,
  structural or operational.

## Alternatives Considered

- **Single generic table + `message_type` column** (the reference architecture's
  approach): rejected for weaker operational clarity and reconsume ergonomics.
- **Partitioning (list-by-type and/or range-by-time):** rejected for now. Strong
  con: a primary key / unique constraint on a partitioned table must include all
  partition-key columns — so **time-range partitioning breaks idempotency**
  (`UNIQUE(created_at, message_id)` does not prevent the same `message_id` at a
  different time). Also adds partition-maintenance burden (inserts fail without a
  matching partition). Reserved as a per-table scale optimization.
- **SQL functions for replay/purge:** rejected (lock-in, untestable, hard to
  debug). Replaced by Rust changesets.
- **Extending the host's `sqlx ./migrations` / `_sqlx_migrations`:** rejected.
  The `sqlx::migrate!` macro embeds the consumer crate's directory and uses a
  fixed, single-sequence `_sqlx_migrations` table — kafkaman's versions would
  collide with the host's. kafkaman owns its own `kafkaman.changelog_history`
  instead, run as its own CI/CD step parallel to the host's migrations.

## Consequences / Tradeoffs Accepted

- kafkaman implements its own change engine (build cost) — bought with
  testability, audit, and DB portability.
- Purge is `DELETE`-based, not instant partition-drop; fine at moderate volume,
  revisit per-table if a hot type forces it.
- kafkaman runs as its own deploy step (`kafkaman::migrate()`), separate from the
  host's app migrations; no shared migration table, no collision.

## Revisit When

- A single message type's volume makes `DELETE`-based purge painful → partition
  that one table.
- A need for config-file-authored changesets (non-developer ops) emerges.
- A second database backend appears (the change engine's portability pays off).
