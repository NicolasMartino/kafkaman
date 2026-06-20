# Configuration and Environment Model

- Document Class: Decision
- Status: Accepted
- Date: 2026-06-20
- Category: Configuration
- Scope: How kafkaman is configured, how per-environment values are supplied, and how that config reaches changesets and the runtime.
- Sources:
  - raw/design/2026-06-20-kafkaman-architecture-discussion.md
  - wiki/decisions/schema-and-change-management.decision.md
- Related:
  - wiki/decisions/schema-and-change-management.decision.md
  - wiki/decisions/retry-backoff-dlq-policy.decision.md
  - wiki/proposals/01-kafkaman-objectives.proposal.md

## Decision

1. **One flat config file: `kafkaman.toml`.** A single property bag at the
   project root, discovered by convention (à la `sqlx`). **No `[profile]`
   sections** — environment differences are not modeled inside the file.
2. **CI/CD renders the file per environment**, injecting values and secrets from
   the vault / config manager at deploy time. The running instance reads one
   already-resolved flat file; it has no notion of "profiles."
3. **Secrets never live in git.** DB URL, broker credentials, etc. are injected
   by CI/CD from the vault. The repository contains at most a committed
   **`kafkaman.example.toml`** listing every key with dummy values, so developers
   and the CI/CD template know what to fill.
4. **Typed access, validated at startup.** kafkaman loads `kafkaman.toml` into a
   resolved property bag with typed accessors (`cfg.get::<Duration>("…")`). With
   no profile fallback, a missing or mistyped key for a *used* feature is a hard
   error — kafkaman validates required keys + types at boot and refuses to start
   with a clear message, rather than failing mid-migration. Sensible defaults
   apply where a key is genuinely optional; the file may be absent for trivial,
   feature-free use.
5. **One config, two consumers, different timing:**
   - **Changesets** receive the resolved config bag and a builder as
     `apply(&self, cfg, b)` — for *one-time* value selection during a versioned
     change. `cfg` exposes typed **values** (`cfg.get::<…>("…")`), **not** an
     environment **identity**: there is deliberately no `cfg.env_name()` to switch
     on, so a changeset can select a value but *cannot* fork on which environment
     it is. Subject to the rule below.
   - **Runtime subsystems** (purge enforcer, schedulers) **re-read it at every
     boot**. This is why tunable settings live here.
6. **Tunable settings are runtime config, not changesets.** Retention windows,
   batch sizes, rate limits, and retry/backoff/DLQ policies are read by the
   runtime at boot. Because the
   file is re-rendered per env and re-read each boot, **changing a value +
   redeploying takes effect** — no new changeset required. (A run-once changeset
   could not do this: once applied, it never re-runs.) This supersedes the
   earlier "`SetRetention` changeset" idea.
   Per-message retry policy uses common defaults plus message-type overrides in
   `kafkaman.toml`; these are environment-rendered runtime values, not schema
   history.
7. **Config selects values, not structure — enforced by the contract.** A
   changeset may pull a *value* from `cfg` but must not branch *structural* logic
   on configuration. Because `cfg` hands over typed values rather than an
   environment discriminant, this is enforced by the API, not just convention:
   there is no env identity to `match` on. Branching schema by environment causes
   drift and means the prod code path was never exercised in lower envs. With the
   flat-file-per-env model the common case has no branching at all: the code path
   is identical everywhere and only the injected value differs, which fully
   preserves "tested downstream ⇒ safe in prod."

## Why

- **Per-env policy doesn't fit a versioned changeset.** Changesets are
  versioned-and-identical-everywhere; retention is env-specific and mutable. The
  clean split: structure + one-shot ops are changesets; tunable config is
  per-env configuration.
- **Flat-file-per-env over in-app profiles:** simpler, and *safer* — no profile
  branching in the app means an identical code path across environments, only
  the value differs. It also matches how the reference already ships per-env
  config (`config/runtime/{env}/...`) and how vaults inject at deploy time.
- **Convention (sqlx-style root file)** is discoverable and tooling-friendly.

## Alternatives Considered

- **In-file `[profile]` sections + a runtime profile selector** (Spring-style):
  rejected as more complex than needed once CI/CD renders the file per env. The
  app would carry profile-selection logic that the deploy pipeline already does.
- **kafkaman owns a full config *framework* (profiles, layered sources,
  precedence rules):** rejected — a library should not impose a heavyweight
  config system. kafkaman owns only a *thin* loader for the single flat
  `kafkaman.toml` (discovery by convention + typed validation at boot); it does
  **not** layer sources, merge profiles, or resolve precedence. CI/CD renders the
  one already-resolved file, so there is nothing for kafkaman to merge.
- **Retention as a `SetRetention` changeset:** rejected — run-once semantics make
  it non-tunable without authoring a new changeset, defeating per-env config
  injection.

## Consequences / Tradeoffs Accepted

- Per-env values live in the vault / CI-CD templates, not visible side-by-side in
  one repo file. Visibility/audit moves to the config manager (matches existing
  practice).
- kafkaman must validate config at boot and fail fast; missing keys are a hard
  error rather than a silent default when a feature needs them.

## Revisit When

- A need for in-app profile selection appears (e.g. one binary serving multiple
  envs at once) — reintroduce profiles then.
- A value currently treated as runtime config turns out to need change-control
  (versioning/review) — move it to a changeset.
