# M2 Implementation Plan - Change Engine Maturity + Config/Env (code-level)

- Document Class: Plan
- Status: Completed
- Date: 2026-06-21
- Category: Implementation plan
- Scope: Concrete engineering plan for M2: maturing the M1 minimal `migrate()` into the real change-management engine (the `changelog!` macro, per-changeset checksums + `applied_by` audit, the already-landed advisory-locked concurrency, and the operational `Replay` changeset under its mandatory guardrails) and adding the thin `kafkaman.toml` configuration loader with typed, fail-fast-at-boot validation. Builds directly on the M1 send-side substrate. Consume/inbox, handler routing, and the retry/backoff/DLQ *processor* remain later milestones; M2 only validates retry/DLQ *config parsing*.
- Sources:
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/configuration-and-environment-model.decision.md
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - wiki/decisions/v1-roadmap-execution-policy.decision.md
  - wiki/plans/m1-durable-send-implementation.plan.md
- Related:
  - wiki/roadmaps/path-to-v1.roadmap.md
  - wiki/specs/m1-durable-send.spec.md
  - wiki/compatibility/m1-durable-send-schema-and-api-changes.compatibility.md

## How To Read This

This plan is the **how** for M2. The roadmap's M2 entry is the **what/why** and owns
the exit criteria. Signatures below are implementation targets, not public API
promises beyond whatever M2 actually validates and promotes to a spec at exit.

Two roadmap exit criteria anchor everything here:

1. **Versioned changesets apply once-per-env with audit** — checksums + `applied_by`,
   the `changelog!` macro, and `Replay` running under the guardrails.
2. **A misconfigured/missing required key fails fast at boot** — the `kafkaman.toml`
   loader with typed validation, before `migrate()` and before `Runtime::start()`.

### Already landed in M1 (do not re-build)

The M1 review-hardening pass already delivered two things the roadmap lists under
M2. M2 **formalizes and tests** them rather than building them from scratch:

- **Advisory-locked `migrate()`** — `pg_advisory_lock(advisory_lock_key(schema))`
  is held for the whole run on one connection (`kafkaman-sqlx/src/lib.rs:294`).
  M2 keeps this; the only change is adding a concurrency *gate test* and ensuring
  the checksum/audit writes happen inside the same locked run.
- **Duplicate-version rejection** — `ensure_unique_versions` already errors on a
  repeated `version()` (`kafkaman-sqlx/src/lib.rs:364`). M2's `changelog!` macro
  builds on this; the merge-collision discipline (decision point 10) is a
  source-level property the macro should make obvious, not a new runtime check.

## Scope

In scope (M2):

- `changelog!` declarative macro replacing hand-rolled `vec![Box::new(...)]`.
- Per-changeset **checksum** recorded in history + verified on re-run
  (immutable-changeset enforcement).
- `applied_by` (and `applied_at`, already present) audit columns.
- Operational `Replay` changeset + the **mandatory guardrails**: env/context
  targeting (opt-out), blast-radius batching/rate-limiting, dry-run/preview.
- The thin `kafkaman.toml` loader: discovery by convention, typed accessors,
  fail-fast validation at boot, `kafkaman.example.toml`.
- Retry/backoff/DLQ **config parsing + validation** (defaults + per-message
  overrides). The *processor* that consumes it is M4.
- `ResolvedConfig` becomes the bag *produced by* the loader, while staying usable
  hand-built (the Harness and unit tests must not need a TOML file).

**Public API break (called out up front, not buried in docs):** `migrate()` changes
from `Result<()>` to `Result<MigrationReport>` (lib.rs:282). This is a deliberate,
pre-V1 breaking change to the only stable `migrate` signature; callers must adapt.
It is recorded in the M2 compatibility note, but it is in scope and intended *now*,
not a surprise.

### Phase gates (sub-milestones within M2)

M2 is large; land it in two independently-shippable phases so review stays tractable
and a regression in one half never blocks the other:

- **Phase A — config + audit + macro:** `kafkaman-config` loader, fail-fast
  validation, checksum/`applied_by` history upgrade, `changelog!`, `MigrationReport`
  + dry-run. Closes the "fails fast at boot" and "audit" exit criteria.
- **Phase B — operational `Replay` + guardrails:** the `Replay` changeset,
  `MigrationContext` env-targeting, bounded-flip semantics. Closes the "`Replay`
  runs under guardrails" exit criterion. Depends on Phase A's `MigrationReport`/
  dry-run plumbing but nothing in Phase A depends on Replay.

Retry/DLQ **config parsing** rides Phase A (it is part of the loader's typed
surface); the retry *processor* remains M4.

Out of scope (later milestones, do not build):

- Consume/inbox, `MessageRouter`, `FromMessage`, `#[derive(KafkaMessage)]` — M3.
- The retry/backoff/DLQ **processor**, DLQ surface, poison handling — M4.
- `embed_changelog!` (directory-derived changelog) — explicitly deferred by the
  decision (point 10).
- Layered config sources / `[profile]` sections / precedence merging — explicitly
  rejected by the config decision. M2 reads one already-rendered flat file.
- `kafkaman-axum` admin routes, metrics, tracing layers — M6.

## Crate Impact

```text
crates/
├── kafkaman-core/      # + ChangesetMeta/checksum surface if the trait grows; no toml dep
├── kafkaman-config/    # NEW: thin kafkaman.toml loader + typed Config bag + validation
├── kafkaman-sqlx/      # migrate() maturity: checksums, applied_by, Replay, guardrails
├── kafkaman-worker/    # unchanged in M2 (retry processor is M4); reads resolved RelayConfig
├── kafkaman-test/      # Harness gains config-driven construction + Replay/dry-run assertions
└── kafkaman/           # facade re-exports kafkaman-config as `config`
apps/
└── axum-outbox/        # adopts changelog! macro, loads kafkaman.toml, ships kafkaman.example.toml
```

**New crate `kafkaman-config`** (recommended placement — see Decisions To Confirm):
owns `toml` + `serde` parsing and the typed property bag, so `kafkaman-core` stays
a pure-types crate with no TOML/file-IO dependency. `ResolvedConfig` is built
*from* a loaded `Config` but remains hand-constructable for tests.

## Config Loader (`kafkaman-config`)

### Discovery and shape

```rust
/// Thin loader for the single, already-rendered, flat kafkaman.toml.
/// No profiles, no layering, no precedence — CI/CD renders one file per env.
pub struct Config {
    table: toml::Table,
}

impl Config {
    /// Discover `kafkaman.toml` upward from the cwd (sqlx-style), or accept an
    /// explicit path. Absent file is allowed only for trivial, feature-free use.
    pub fn discover() -> Result<Option<Self>, ConfigError>;
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError>;
    pub fn from_str(toml: &str) -> Result<Self, ConfigError>;

    /// Typed required access — missing or mistyped key is a hard error.
    pub fn get<T: FromConfigValue>(&self, key: &str) -> Result<T, ConfigError>;
    /// Typed optional access — used where a genuine default applies.
    pub fn get_opt<T: FromConfigValue>(&self, key: &str) -> Result<Option<T>, ConfigError>;
}
```

- **No `env_name()` / environment identity is exposed** — enforced by the API, per
  config decision point 7 and schema decision point 11. A changeset can read a
  value but cannot fork structure on environment. (Env *targeting* for `Replay` is
  handled by `MigrationContext`, **not** by reading the env from `cfg` — see the
  Replay section.)
- `FromConfigValue` is implemented for `String`, `Duration` (humantime-style
  `"5m"`/`"500ms"`), `i64`, `f64`, `bool`.
- Dotted keys (`retry.defaults.max_attempts`, `retry.messages.order_created.*`)
  resolve through the nested TOML tables.

#### Typed sections, not just stringly-typed access

`get::<T>("dotted.key")` is the **escape hatch**, not the primary contract. The
key names and error taxonomy of a stringly-typed accessor would otherwise become
compatibility surface. Known sections deserialize into **serde-derived typed
structs** (`#[serde(deny_unknown_fields)]`) so the contract is the struct, not a
set of string keys:

```rust
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelaySection { /* lease, batch_size, poll_interval … */ }

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrySection { pub defaults: RetryPolicy, #[serde(default)] pub messages: BTreeMap<String, RetryPolicyOverride> }

impl Config {
    pub fn relay(&self) -> Result<RelaySection, ConfigError>;
    pub fn retry(&self) -> Result<RetrySection, ConfigError>;
}
```

`get`/`get_opt` remain for genuinely dynamic or host-specific keys. The decision's
`cfg.get::<Duration>("…")` example (config decision point 4) is preserved as that
escape hatch; the typed sections are how kafkaman's *own* config is consumed.

### Fail-fast validation at boot

```rust
/// Validate every key a used feature requires, in one pass, before migrate().
/// Returns ALL problems, not just the first, so an operator fixes the rendered
/// file once. A missing/mistyped required key is an error with a clear message
/// (key path, expected type, what was found).
pub fn validate(&self, schema: &ConfigSchema) -> Result<(), ConfigErrors>;
```

The boot sequence the example and docs prescribe becomes:

```rust
let cfg_file = Config::discover()?;                 // Option<Config>
let resolved = ResolvedConfig::from_config(cfg_file, /* registered messages */)?; // fails fast
let ctx = MigrationContext::from_env();             // deploy-supplied targeting metadata (see Replay)
migrate(&pool, &resolved, &ctx, &changelog()).await?; // then converge schema
worker::run(...);                                   // then runtime subsystems
```

`MigrationContext` is **deploy tooling's** input (e.g. `KAFKAMAN_CONTEXTS=staging`),
not a field of `cfg`. This is deliberate: it keeps env identity out of `cfg` (so a
changeset still cannot fork *structure* on environment) while giving `migrate()` the
targeting labels it needs to skip a `Replay` in an env it was not meant for — the
same split Flyway/Liquibase draw between config and `--contexts`.

`ResolvedConfig::from_config` is where "validate required keys + types at boot and
refuse to start with a clear message, rather than failing mid-migration" lives.

### Retry/DLQ config (parse + validate only)

Parse the shape from the retry/backoff/DLQ decision (`[retry.defaults]`,
`[retry.messages.<type>]`) into:

```rust
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: f64,
    pub errors_limit: usize,
    pub dlq: DlqMode,            // table (only V1 variant)
}
pub struct RetryConfig {
    pub defaults: RetryPolicy,
    pub overrides: BTreeMap<String, RetryPolicyOverride>, // per message_type
}
```

Resolution (`defaults` merged with a per-type override) and validation live here.
Because M2 **locks this TOML format in**, validation is complete, not just the
obvious numeric relationships:

- **Numeric relationships:** `max_attempts >= 1`, `initial_backoff <= max_backoff`,
  `errors_limit >= 1`.
- **Finite, sane multiplier:** `multiplier >= 1.0` **and** `multiplier.is_finite()`
  (reject `NaN`/`inf` from a malformed float).
- **Duration bounds:** humantime parse errors are config errors; reject values that
  overflow `Duration` (e.g. absurd `"99999999999w"`) rather than panicking.
- **Unknown keys are rejected** via `#[serde(deny_unknown_fields)]` on the retry
  section — a typo in a tunable name fails fast instead of silently defaulting.
- **Per-type overrides for unregistered messages:** a `[retry.messages.<type>]`
  whose `<type>` is not a registered message descriptor is a **boot error** (it can
  never take effect and almost always signals a typo). The registered set is known
  at boot from `ResolvedConfig`'s descriptors, so this is checkable in the same
  fail-fast pass.
- **DLQ variants:** `dlq` must be one of the allowed `DlqMode` variants; for V1 the
  only variant is `table`, so anything else is rejected with the allowed list in the
  message.

**No processor consumes this in M2** — M4 does. M2 proves it parses, merges, and
rejects bad policy at boot. The spec states plainly that M2 validates-but-does-not-
enforce retry policy.

## Change Engine Maturity (`kafkaman-sqlx`)

### `changelog!` macro

Replaces the hand-rolled `vec![Box::new(InitSchema), Box::new(CreateOutboxTable::new(2, ...))]`:

```rust
let changelog = changelog![
    InitSchema,                                          // V1
    CreateOutboxTable::new(2, OrderCreated::descriptor()?),
    Replay::outbox::<OrderCreated>(3).since(/* ... */),
];
```

Design constraints from the decision:

- **Versions stay explicit in source** (point 10). The macro does **not**
  auto-number by position — explicit `V00n` is what makes two branches adding the
  same number *collide on merge*. The macro's job is to remove `Box::new`
  boilerplate, return `Vec<Box<dyn Changeset>>`, and assert ascending unique
  versions at construction (reusing `ensure_unique_versions`).
- One changeset per file, changelog in its own module (point 10) — the macro is
  invoked from `messaging/changelog.rs`, not `main`.
- Annotations are reserved for handlers, not changesets — the macro is a plain
  declarative list, no attribute magic.

**Claim scope (do not over-state):** the macro is *ergonomic boilerplate removal +
a construction-time ascending/unique-version assertion* (reusing the existing
`ensure_unique_versions`, lib.rs:364). It does **not** — and cannot, at macro
expansion — enforce "one changeset per file" or detect a branch's version
collision; those are a **convention** (one-per-file) and a **source-merge
property** (explicit integer versions force a git conflict when two branches reuse
a number). The runtime duplicate-version check is the backstop, not the primary
guarantee. The plan claims only what the macro actually delivers.

### Checksums + `applied_by`

Grow `changelog_history`. The fresh-install shape is:

```sql
CREATE TABLE IF NOT EXISTS <schema>.changelog_history (
    version    BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    checksum   TEXT,                          -- NEW (see upgrade note below)
    applied_by TEXT,                          -- NEW
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### Migration-safe history upgrade (do NOT `ADD COLUMN ... NOT NULL`)

Any environment that ran M1 already has rows in `changelog_history` whose only
columns are `version/name/applied_at` (current bootstrap, lib.rs:358). Postgres
**rejects** `ALTER TABLE ... ADD COLUMN checksum TEXT NOT NULL` on a non-empty
table (*"column contains null values"*). The upgrade is therefore staged:

1. `ADD COLUMN IF NOT EXISTS checksum TEXT` (**nullable**) and
   `ADD COLUMN IF NOT EXISTS applied_by TEXT` (**nullable**).
2. **Backfill** pre-existing rows: `applied_by = 'unknown'` (historical applier is
   genuinely unknown) and leave `checksum` **NULL** to mark "applied before
   checksums existed."
3. Do **not** add a `NOT NULL` constraint in M2. New rows are written with both
   values populated; old rows keep a NULL checksum. The checksum-verify step
   (below) **skips rows with a NULL stored checksum** — a pre-checksum changeset is
   not retroactively suspected of mutation. New changesets get full enforcement.

This keeps the upgrade safe on existing installs and is recorded in the M2
compatibility note.

- **Checksum source — canonical, config-independent.** `build()` receives
  `ResolvedConfig` (lib.rs:320) and the schema name is configurable (schema
  decision pt 1), so the **rendered** SQL embeds env-specific identifiers and is
  *not* a stable cross-deploy key. The checksum is computed over the **logical
  changeset declaration** — a canonical serialization of `(version, name, the
  changeset's own structural parameters)` — explicitly **excluding** any
  config-derived value (schema name, and especially a `Replay` selector like
  `.since(<cfg value>)`). This is what makes the immutable-changeset rule mean
  "the *changeset* was edited," not "a tunable changed between deploys." Hash is
  SHA-256, hex.
- **Enforcement.** On re-run, if a version is already recorded with a **non-NULL**
  checksum that differs from the recomputed one, `migrate()` fails with
  `ChecksumMismatch { version, name }` — enforcing that original changesets are
  never edited in place rather than trusting it.
- **`applied_by`** = a configurable applier identity (default: `user@host` or a
  `KAFKAMAN_APPLIED_BY` value), recorded for audit. This is the "who" in
  who/what/when.

To produce a config-independent checksum, the `Changeset` trait gains an explicit
`fn checksum(&self) -> String` (default impl over the changeset's declared
parameters, overridable per type). This is preferred over hashing `ChangeBuilder`
output precisely because the builder output is config-dependent and
ordering-sensitive. Confirm the exact canonical form during task 3.

### Operational `Replay` + guardrails

`Replay` rides the same auto-applied versioned stream as structural changes
(decision: "two kinds of changeset, one mechanism"), but is a **bounded state
change only** — it must never be one unbounded transaction.

M2 demonstrates `Replay` on the **send-side outbox** (the only runtime that exists
pre-consume): flip a bounded, rate-limited batch of already-`Published` rows back
to `Pending` so the relay re-publishes them. The consume-side replay (re-process
received messages) lands in M3 on the same mechanism.

```rust
pub struct Replay { /* version, target table, selector, max_rows, contexts */ }

impl Replay {
    pub fn outbox<P: KafkaMessage>(version: i64) -> Self;
    pub fn since(self, occurred_after: OffsetDateTime) -> Self; // bounded selector
    pub fn max_rows(self, cap: i64) -> Self;                    // mandatory blast-radius cap
    pub fn contexts(self, contexts: &[&str]) -> Self;           // env-targeting opt-out (matched against MigrationContext)
}
```

#### Bounded *single* flip — it must not block startup

The decision (schema pt 7, 51-55) is explicit: a `Replay` does a **bounded state
change only** and "stays fast so it never blocks startup"; the heavy, rate-limited
re-drain is done by the **runtime relay after boot**. The M1 plan's earlier
`.batch(rows, per: Duration)` shape contradicted this — a rate-limited multi-batch
loop *inside* `migrate()` would block startup for the duration of the rate limit.
So M2 corrects it:

- `Replay` inside `migrate()` does **one bounded `UPDATE`** capped by `max_rows`
  (`UPDATE … SET status='Pending' … WHERE … LIMIT <cap>` via a CTE / `ctid` subquery)
  — a single fast statement, not a paced loop. This is the *bounded state flip*.
- The actual re-publish throughput is the **runtime relay's** job after boot, which
  already batches and rate-limits (M1 `RelayConfig`). That is where blast-radius
  *pacing* lives; `migrate()` only flips the bounded set.
- `max_rows` is the **mandatory** blast-radius cap: a `Replay` may never flip an
  unbounded set in one statement. If the intent is "replay everything," that is
  expressed by re-running successive capped changesets across deploys, not one
  unbounded flip.

#### Crash / resume / failure semantics (previously unspecified)

`Replay` runs in the same per-changeset transaction as every other changeset
(lib.rs:323-334), under the run-wide advisory lock (lib.rs:294-299), and is
recorded in `changelog_history` only on commit:

- **Crash mid-flip:** the changeset transaction rolls back; `version` is **not**
  recorded, so the next `migrate()` re-attempts the whole bounded flip. The flip is
  **idempotent** — re-flipping rows already `Pending` is a no-op (`WHERE status =
  'Published'` predicate), so re-running is safe.
- **Failure:** a failing `Replay` statement aborts the run exactly like a failing
  DDL changeset — `migrate()` returns the error, nothing past it applies, and the
  advisory lock is released in all paths.
- **No kill-switch needed beyond targeting + bounding:** because the flip is one
  capped idempotent statement gated by `contexts`, the opt-out is "don't include the
  changeset's context for this env" (or dry-run first), consistent with the
  decision's accepted auto-apply model.

#### Env/context targeting via `MigrationContext` (NOT `cfg`)

`Replay::contexts(["staging"])` is matched against the **`MigrationContext`** passed
to `migrate()` by deploy tooling — not against any env value read from `cfg`. This
reconciles the two decisions that otherwise collide: the config decision (pt 7/11)
forbids exposing env identity through `cfg`, while the schema decision's guardrails
require Liquibase-style context targeting. The targeting label is **deploy
metadata** the `migrate()` runner evaluates, so a changeset still cannot fork
*structure* on environment — it never sees the env at all; the runner simply skips a
changeset whose `contexts` do not intersect the active `MigrationContext`. A
changeset with no `contexts` applies everywhere (unchanged behavior).

#### Dry-run / preview

`migrate_dry_run()` reports, per pending changeset, what it *would* do — structural
changesets report their DDL; `Replay` reports "would requeue ~N rows in table X".
Surfaced via the returned `MigrationReport` and `tracing` so an auto-applied op is
still *seen* in deploy logs. Exact dry-run semantics are pinned below.

```rust
pub async fn migrate(pool, cfg, ctx, changesets) -> Result<MigrationReport>;
pub async fn migrate_dry_run(pool, cfg, ctx, changesets) -> Result<MigrationReport>; // applies nothing
```

`MigrationReport` lists `{version, name, action, preview, applied|skipped|would_apply}`.

##### Dry-run semantics (pinned — do not leave to task 5)

`migrate_dry_run`:

- **Acquires the advisory lock** and **bootstraps history** read-only context the
  same way (so it sees the true applied set), but **writes nothing** to
  `changelog_history` and **commits no** changeset transaction.
- **Checks checksums** of already-applied changesets and reports a would-be
  `ChecksumMismatch` as a finding rather than erroring the run (preview surfaces
  drift without failing a non-mutating call).
- For `Replay`, runs the **`COUNT`** matching its selector + `max_rows` cap inside a
  rolled-back transaction to produce the "~N rows" estimate; for structural
  changesets, renders the DDL it *would* run.
- Returns the same `MigrationReport` shape with every pending changeset marked
  `would_apply`.

**Residual risk** (accepted in the decision) is documented, not eliminated: a
correct-but-large `Replay` runs at full prod scale on deploy; the bounded `max_rows`
flip + dry-run + env-targeting are the mitigations, and the post-boot relay paces
the actual re-publish.

## Harness / Test Impact (`kafkaman-test`)

Per dogfooding-first, M2 opens with the Harness test we wish we could run. The
Harness gains:

- **Config-driven construction** — `Harness::connect_with_config(database_url,
  Config::from_str(toml))` so config-validation behavior is exercised at the
  Harness boundary, plus the existing hand-built `ResolvedConfig` path stays for
  feature-free tests.
- **Migration report assertions** — `harness.migrate_report()` to assert checksum
  recording, `applied_by`, skip-on-second-run, and `ChecksumMismatch` on a mutated
  changeset. Also: an existing M1-shaped history table (NULL checksum rows) upgrades
  without error and those rows are **not** flagged as mismatched (the
  nullable-checksum backfill path).
- **`MigrationContext` assertions** — drive `migrate()` with an explicit context so
  the test controls targeting without touching `cfg`.
- **Replay assertions** — enqueue + relay to `Published`, run a `Replay`
  changeset, assert the bounded `max_rows` flip returned exactly the capped set to
  `Pending` and that a second `relay_once` re-publishes them; assert a `Replay`
  whose `contexts` do not intersect the active `MigrationContext` is **skipped**;
  assert re-running a committed `Replay` is a no-op (idempotent flip); assert
  dry-run applies nothing but reports the `~N rows` estimate.

White-box config-loader unit tests (typed access, fail-fast, retry-policy
merge/validate) stay inside `kafkaman-config`. The dogfooding boundary rule from
M1 still holds: prefer the Harness at/above its boundary, keep DDL/SQL/loader unit
tests in their crates.

## Example Impact (`apps/axum-outbox`)

- `changelog.rs` adopts `changelog![ ... ]`.
- `main.rs` boot order becomes: `Config::discover()` → `ResolvedConfig::from_config`
  (fail-fast) → `MigrationContext::from_env()` → `migrate()` (now returns and logs a
  `MigrationReport`) → `worker::run`.
- Ship a committed **`kafkaman.example.toml`** at the repo root listing every key
  (schema, relay tunables, retry defaults + an `order_created` override) with dummy
  values — the config decision requires this artifact.
- The HTTP integration test gains a config-file path so the example's fail-fast
  wiring is covered (keeps the coverage gate honest as new code lands).

## Implementation Tasks (outside-in TDD)

Each behavior task opens with a failing Harness (or example) test; primitive
layers add white-box tests where the Harness is built from the thing under test.

0. **`kafkaman-config` crate skeleton + facade re-export.** `cargo check
   --workspace` fails only because the loader API is intentionally missing.
1. **Headline failing test (config fail-fast).** Harness test: a `kafkaman.toml`
   missing a required key (or with a mistyped `Duration`) makes
   `ResolvedConfig::from_config` return a clear, aggregated error *before* any DB
   work. Drives the loader's public shape.
**Phase A — config + audit + macro:**

2. **Config loader.** `Config` discovery/parse, `FromConfigValue`, `get`/`get_opt`,
   serde-typed sections (`relay()`/`retry()`, `deny_unknown_fields`), aggregated
   `validate`, full retry-policy parse/merge/validate (numeric + finite multiplier +
   duration overflow + unknown keys + unregistered-message override + DLQ variant).
   White-box unit tests. No env identity exposed.
3. **Checksum + `applied_by` in `migrate()`.** Migration-safe history upgrade:
   `ADD COLUMN IF NOT EXISTS` **nullable**, backfill `applied_by='unknown'`, leave
   old `checksum` NULL (no `NOT NULL` constraint in M2). Add `Changeset::checksum()`
   (config-independent, over declared params); store on apply; verify on re-run
   **skipping NULL-checksum rows**; `ChecksumMismatch` error on a mutated changeset.
   Harness gate: second run is a no-op, a mutated changeset is rejected, M1-shaped
   history upgrades without flagging old rows.
4. **`changelog!` macro.** Boilerplate-free declarative list returning
   `Vec<Box<dyn Changeset>>`, asserting ascending unique versions; explicit
   versions retained (claim scope: ergonomics + assertion, not merge-discipline
   enforcement). Migrate idempotency unaffected. Adopt it in the example.
5. **`MigrationReport` + dry-run (pinned semantics).** `migrate` returns a report;
   `migrate_dry_run` acquires the lock, writes nothing, reports checksum drift as a
   finding (not an error), and runs a rolled-back `COUNT` for `Replay` estimates.
   Harness asserts both.

**Phase B — operational `Replay` + guardrails:**

6. **`MigrationContext` + operational `Replay`.** Thread `MigrationContext` through
   `migrate()`; context-targeting skip is evaluated by the runner, not `cfg`.
   Send-side outbox replay: one bounded `max_rows` `UPDATE` flipping
   `Published → Pending` (single statement, not a paced loop); idempotent re-run;
   crash rolls back and re-attempts. Harness gates for `max_rows` bound, context
   skip, idempotent re-flip, re-publish, dry-run estimate.

**Closeout:**

7. **Example adoption.** `Config::discover` + `MigrationContext::from_env` boot path,
   `kafkaman.example.toml`, HTTP test covers the config-loaded path.
8. **Docs.** Promote the M2-validated subset to a spec; compatibility note for the
   history-schema columns **and the `migrate()` `Result<MigrationReport>` signature
   break**; update roadmap M2 status + log.

## Verification Gates

- `just test` (all tiers) and `cargo clippy --all-targets --all-features -D
  warnings` and `cargo fmt --check` are clean; coverage stays ≥ 80%.
- A missing or mistyped **required** config key fails fast at boot with a clear,
  aggregated message — proven before any `migrate()` DB call.
- `migrate()` records checksum + `applied_by` + `applied_at` per applied
  changeset; a second run applies nothing.
- An existing M1-shaped `changelog_history` (NULL checksum rows) upgrades without
  error and old rows are not flagged as mismatched.
- A changeset whose declared parameters changed after being applied is rejected
  with `ChecksumMismatch` (immutable-changeset enforcement); a tunable config value
  changing between deploys does **not** trigger a false mismatch.
- `changelog!` produces the same applied result as the M1 hand-rolled vec and
  rejects duplicate/disordered versions at construction.
- `Replay` flips only a bounded `max_rows` set in a single statement; never one
  unbounded transaction and never a paced loop inside `migrate()`; a `Replay` whose
  `contexts` miss the active `MigrationContext` is skipped; re-running a committed
  `Replay` is a no-op; dry-run applies nothing but reports the `~N rows` estimate;
  a follow-up `relay_once` re-publishes the requeued rows.
- Retry/DLQ config parses, merges defaults with per-type overrides, and rejects
  invalid policy at boot (numeric, non-finite multiplier, duration overflow,
  unknown keys, unregistered-message override, bad DLQ variant) — **without** any
  processor consuming it yet (M4).
- Advisory-locked concurrency (from M1) still holds under a concurrent-migrate
  gate test, now with checksum/audit writes inside the locked run.

## Decisions (confirmed 2026-06-21)

1. **Config crate placement — dedicated `kafkaman-config` crate.** Owns
   `toml`/`serde` parsing and the typed `Config` bag so `kafkaman-core` stays
   TOML/file-IO-free; the facade re-exports it as `config`. (Confirmed.)
2. **`Replay` surface in M2 — mechanism + guardrails + send-side demonstration.**
   Build the operational-changeset mechanism and all guardrails now and demonstrate
   it end-to-end on the outbox (re-publish `Published → Pending` in bounded
   batches), giving a real Replay gate at M2 exit; the consume-side replay reuses
   the same mechanism in M3. (Confirmed.)
3. **Checksum source — config-independent logical declaration**, via an explicit
   `Changeset::checksum()` over `(version, name, declared structural params)`,
   **excluding** config-derived values (schema name, `Replay` selectors). Revised
   from the original "hash rendered statements" after review: rendered SQL embeds
   the configurable schema name and config-derived selectors, so hashing it would
   make a tunable change between deploys trip a false `ChecksumMismatch`. (Confirmed
   via 2026-06-21 plan review; exact canonical form finalized at task 3.)

### Review-driven corrections (2026-06-21)

A plan review surfaced cross-decision and migration-safety issues, now folded in:

4. **History upgrade is migration-safe** — new columns are added **nullable** with a
   backfill, not `ADD COLUMN ... NOT NULL` (which fails on existing M1 installs with
   history rows). NULL-checksum legacy rows are skipped by the verify step.
5. **Env targeting uses `MigrationContext`, not `cfg`** — reconciles the config
   decision (no env identity in `cfg`) with the schema decision's context-targeting
   guardrail. The runner evaluates targeting; changesets never see the env.
6. **`Replay` is one bounded `max_rows` flip**, not a rate-limited loop inside
   `migrate()` — honoring the decision's "never blocks startup." Crash/idempotency/
   failure semantics are specified; the post-boot relay paces the re-publish.
7. **Auto-apply is retained** (schema decision pt 7) — the review's "explicit
   activation/kill-switch" was *not* adopted where it would reopen that accepted
   decision; instead the residual risk is mitigated by bounding + targeting +
   dry-run + idempotency, as the decision prescribes.
8. **`migrate()` signature break** (`Result<()>` → `Result<MigrationReport>`) is
   stated up front in Scope and the compatibility note, not buried.

## What Closes This Plan

All gates green. Promote only the M2-proven subset to a spec (the matured
`migrate()` contract: checksums/audit/`changelog!`/dry-run/`Replay`-guardrails, and
the `kafkaman.toml` loader + fail-fast validation contract). Do **not** promote the
retry/backoff/DLQ *runtime* behavior (that is M4 — M2 only validated its config
parsing) or any consume-side contract.
