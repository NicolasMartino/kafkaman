# M5 Outbox Retention Compatibility

- Document Class: Compatibility Note
- Status: Active
- Date: 2026-08-24
- Category: Public API, schema, and runtime behavior compatibility
- Scope: The outbox retention surface — purge API, worker loop, config section, and
  the schema index that makes the age scan bounded.
- Sources:
  - wiki/decisions/outbox-retention-policy.decision.md
  - wiki/proposals/11-restore-retention-and-schema-boundaries.proposal.md
  - review.md
- Related:
  - wiki/compatibility/m5-code-audit-remediation.compat.md

## Behavior Change

Nothing purged any kafkaman table: no `DELETE` existed anywhere in the workspace,
so `Published` outbox rows accumulated for the life of the application. Retention
now exists, but is **opt-in** — absent `[retention]` config, no purger runs and
growth remains the status quo. Making it default-on would turn an upgrade into a
silent deletion of operational history.

Scope is the outbox alone. Received tables are the only durable record of what was
done, and their dedupe window *is* their retention window — purging a processed row
lets a Kafka redelivery re-run the handler. Cache tables are the state.

## Additive API

- `PurgeConfig` — `older_than`, `batch_size`, `poll_interval`, `include_failed`.
  `Default` retains one week, 1,000-row batches, a 60-second sweep, and spares
  `Failed` rows.
- `PurgeStats { deleted }`.
- `Error::InvalidPurgeConfig { field, reason }` on `kafkaman_core::Error`.
- `kafkaman_sqlx::purge_outbox_once(pool, table, &PurgeConfig) -> PurgeStats` —
  one bounded batch; the caller loops. Same contract as `claim_batch` and
  `dispatch_once`.
- `kafkaman_worker::run_purger(pool, table, cfg, shutdown)` — sweeps continuously
  while batches come back non-empty, then sleeps. Same shape as `run_dispatcher`.
- `kafkaman_config::RetentionSection` and `Config::retention() -> Option<_>`.
- `AddOutboxRetentionIndex` changeset and `create_outbox_retention_index_sql`.

## Schema

A partial index on `created_at`, restricted to terminal statuses:

```sql
CREATE INDEX ... ON <outbox> (created_at)
    WHERE status IN ('Published', 'Superseded', 'Failed')
```

Keyed on `created_at` rather than `published_at` because `Superseded` rows never
receive a `published_at` — they were collapsed, not published — so a single age
column has to be one both statuses carry. Keying on one column also keeps the scan
a plain range: a `COALESCE` or an `OR` across two age columns is exactly the shape
measured as unservable by any index (R14).

The predicate lists `Failed` even though the default purge excludes it, so the
opt-in variant uses the same index — a query filtering on a subset of these statuses
implies the predicate and stays index-served.

Verified index-served rather than assumed: `retention_scan_is_index_served` shows an
Index Scan, no sort, 0.47 ms over a 200k-row table.

## Migration

1. `CreateOutboxTable` now emits the retention index, so **fresh tables get it
   automatically**. `Changeset::checksum_material` hashes
   `version;name;message_type;topic` and not the SQL, so this does not trip a
   checksum mismatch — and equally, it does not reach tables that already applied
   that changeset.
2. **Existing tables need `AddOutboxRetentionIndex` added to the changelog.**
3. ⚠️ **The index build blocks writes.** Changesets apply inside a transaction, so
   `CREATE INDEX CONCURRENTLY` is unavailable and the build takes a `SHARE` lock for
   its duration. On an outbox that has grown without retention — the situation this
   changeset exists to fix — that is a real outage window, and because `enqueue`
   runs inside the caller's business transaction it propagates into application
   requests. Apply it during a maintenance window on a large table.
4. Add a `[retention]` section to actually reclaim anything. A service booted
   through `RuntimeBuilder` starts purgers for its published roles automatically
   when that section is present. A low-level service that wires loops manually
   must still spawn `run_purger` itself. Without a configured purger, the index
   is built and nothing uses it.

`kafkaman.example.toml` documents every knob the crate reads, with an inline
comment per field. It is now loaded and resolved by a unit test
(`the_shipped_example_config_resolves_and_covers_every_section`), so a knob renamed
in code, a section added without being documented, or a documented knob that does
not exist all fail the build. Nothing previously exercised that file, which is how
`[retention]` came to be absent from it.
