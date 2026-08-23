//! Query-plan measurement for the statements a relay cycle runs.
//!
//! `#[ignore]`d: these seed six-figure backlogs and are diagnostics, not
//! assertions. Run them deliberately:
//!
//! ```text
//! cargo test -p durable-send-tests --test outbox_claim_cost -- --ignored --nocapture
//! ```
//!
//! # Why measured rather than reasoned
//!
//! Whether a statement is bounded depends on which index the planner can use, and
//! that is not visible from reading the SQL. An earlier round of this work argued
//! the cost from index definitions and got both the mechanism and the conclusion
//! partly wrong; these tests exist so the next such claim is evidence.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use durable_send_tests::{start_harness, ProductSnapshot, TestResult};
use kafkaman_sqlx::{collapse_stale_pending_rows_sql, OutboxTable};
use sqlx::{PgPool, Row};

/// The relay's default `batch_limit`.
const BATCH_LIMIT: i64 = 100;

/// The two shapes an outbox table takes, which need measuring separately because
/// the answer differs between them.
#[derive(Clone, Copy, Debug)]
struct Shape {
    label: &'static str,
    /// `Pending` rows, all due.
    pending: i64,
    /// `Published` rows. Nothing purges them, so a running system accumulates
    /// these without bound — see the retention decision.
    published: i64,
    /// Entities holding a second, newer `Pending` row: the state a failed publish
    /// leaves behind when an update was already queued.
    overtaken: i64,
}

/// The pathological case: a drained-behind relay, which is when throughput matters
/// most and when the claim's sort is largest.
const BACKLOG: Shape = Shape {
    label: "backlog (200k Pending)",
    pending: 200_000,
    published: 0,
    overtaken: 10,
};

/// What a running system actually looks like: a large terminal bulk and a small
/// live set. The index under test must not regress this shape.
const STEADY_STATE: Shape = Shape {
    label: "steady state (200k Published + 200 Pending)",
    pending: 200,
    published: 200_000,
    overtaken: 0,
};

#[tokio::test]
#[ignore = "diagnostic: seeds a large backlog and prints query plans"]
async fn claim_candidate_ordering_cannot_be_index_served() -> TestResult {
    // R14, and its negative result.
    //
    // `claim_batch` orders candidates by `created_at`, and no index can serve that
    // ordering: the predicate is an OR across two statuses, so the planner has to
    // examine every row and then sort. Measured at a 200k backlog that is a Seq
    // Scan plus a 7,840 kB external merge sort spilling to disk.
    //
    // Two things this measurement establishes, both of which narrow R14 from how
    // it was first written up:
    //
    // 1. The steady-state shape is already fine — a Bitmap Index Scan and a 35 kB
    //    quicksort, sub-millisecond. R14 bites only when the relay is behind.
    // 2. A partial index on `created_at` does not fix the backlog shape. The plan
    //    is unchanged by it. Fixing this needs the OR removed from the statement,
    //    which is a semantic change to the correctness-critical claim and belongs
    //    in its own change — Postgres rejects `FOR UPDATE` with `UNION`, so the
    //    locking would have to move outside the merge and `SKIP LOCKED` would then
    //    apply after the limit.
    //
    // The pass condition is structural — whether a `Sort` node survives — because
    // timing alone misled an earlier round of this work. Note also that most of
    // the backlog-shape time is JIT compilation triggered by the inflated cost
    // estimate, not the scan itself.
    println!("\n=== claim candidate selection, by table shape ===\n");

    for shape in [BACKLOG, STEADY_STATE] {
        let (_postgres, harness) = start_harness().await?;
        let table = harness.outbox_table::<ProductSnapshot>().await?;
        seed(harness.pool(), &table, shape).await?;

        let name = table.qualified_name();
        let plan = explain(harness.pool(), &claim_candidate_sql(&name)).await?;
        println!("--- {} : before ---\n{plan}", shape.label);
        report(&format!("{} BEFORE", shape.label), &plan);

        // The obvious fix, RECORDED AS REFUTED so it is not tried again.
        //
        // A partial index on the ordering column looks like it should let the
        // claim stream rows in `created_at` order and stop at the limit. It does
        // not: the candidate predicate is an OR across two different statuses, so
        // the planner must evaluate every row to decide membership, and an index
        // covering only the `Pending` branch cannot order the union. The plan is
        // byte-identical with and without this index — same Seq Scan, same
        // external merge sort — so adding it would buy nothing and cost a write
        // amplification on every insert and status transition.
        sqlx::query(&format!(
            "CREATE INDEX idx_claim_order ON {name} (created_at) WHERE status = 'Pending'"
        ))
        .execute(harness.pool())
        .await?;
        sqlx::query(&format!("ANALYZE {name}"))
            .execute(harness.pool())
            .await?;

        let plan = explain(harness.pool(), &claim_candidate_sql(&name)).await?;
        println!("--- {} : after ---\n{plan}", shape.label);
        report(&format!("{} AFTER ", shape.label), &plan);
    }

    Ok(())
}

#[tokio::test]
#[ignore = "diagnostic: seeds a large backlog and prints query plans"]
async fn stale_pending_collapse_stays_bounded() -> TestResult {
    // R1's collapse. Recorded so anyone tempted to "simplify" the shipped
    // statement back to the obvious `EXISTS` form can see the cost in one command.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    seed(harness.pool(), &table, BACKLOG).await?;

    println!("\n=== stale-pending collapse, {} ===\n", BACKLOG.label);
    for (label, sql) in [
        (
            "shipped, window-driven",
            collapse_stale_pending_rows_sql(&table),
        ),
        (
            "rejected, EXISTS scan",
            rejected_collapse_sql(&table.qualified_name()),
        ),
    ] {
        let plan = explain(harness.pool(), &sql).await?;
        println!("--- {label} ---\n{plan}");
        report(label, &plan);
    }

    Ok(())
}

#[tokio::test]
#[ignore = "diagnostic: seeds a large backlog and prints query plans"]
async fn retention_scan_is_index_served() -> TestResult {
    // Checked rather than assumed, because R14 is the record of assuming exactly
    // this and being wrong. The retention index only earns its write cost if the
    // purge's age scan actually uses it; if this shows a Seq Scan, the index is
    // pure overhead on every insert and status transition.
    let (_postgres, harness) = start_harness().await?;
    let table = harness.outbox_table::<ProductSnapshot>().await?;
    seed(harness.pool(), &table, STEADY_STATE).await?;

    let plan = explain(harness.pool(), &retention_scan_sql(&table.qualified_name())).await?;
    println!(
        "\n=== retention age scan, {} ===\n{plan}",
        STEADY_STATE.label
    );
    println!(
        "  index used: {}",
        if plan.contains("_retention") {
            "YES"
        } else {
            "NO — the index is pure write overhead, reconsider it"
        }
    );
    report("retention scan", &plan);

    Ok(())
}

/// The victim selection inside `purge_outbox_once`, which is the half that has to
/// be index-served; the delete itself is by primary key.
fn retention_scan_sql(name: &str) -> String {
    format!(
        "SELECT message_id FROM {name}
          WHERE status IN ('Published', 'Superseded')
            AND created_at < now() - interval '7 days'
          ORDER BY created_at
          LIMIT 1000"
    )
}

/// `claim_batch`'s candidate selection, verbatim apart from the inlined limit.
fn claim_candidate_sql(name: &str) -> String {
    format!(
        "SELECT candidate.message_id FROM {name} candidate
          WHERE (
                 candidate.status = 'Pending'
                 AND candidate.next_attempt_at <= now()
                 AND (
                     candidate.entity_key IS NULL
                     OR NOT EXISTS (
                         SELECT 1 FROM {name} inflight
                         WHERE inflight.entity_key = candidate.entity_key
                           AND inflight.status = 'Publishing'
                     )
                 )
              )
             OR (candidate.status = 'Publishing' AND candidate.claim_expires_at <= now())
          ORDER BY candidate.created_at
          FOR UPDATE SKIP LOCKED
          LIMIT {BATCH_LIMIT}"
    )
}

/// The formulation rejected for R1: scan every `Pending` row and look forward.
/// Identical results, executed as a hash semi join over two full table scans.
fn rejected_collapse_sql(name: &str) -> String {
    format!(
        "UPDATE {name} AS stale
            SET status = 'Superseded', claim_id = NULL, claimed_by = NULL,
                claim_expires_at = NULL, last_error = 'superseded'
          WHERE stale.entity_key IS NOT NULL AND stale.status = 'Pending'
            AND EXISTS (
                SELECT 1 FROM {name} newer
                WHERE newer.entity_key = stale.entity_key
                  AND newer.status = 'Pending'
                  AND (newer.created_at, newer.message_id)
                      > (stale.created_at, stale.message_id)
            )"
    )
}

/// `EXPLAIN (ANALYZE, ...)` *executes* the statement, so each measurement runs in
/// its own transaction and rolls back. Without that an `UPDATE` under measurement
/// would drain the rows the next plan is supposed to find.
async fn explain(pool: &PgPool, sql: &str) -> TestResult<String> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(&format!("EXPLAIN (ANALYZE, BUFFERS, VERBOSE false) {sql}"))
        .bind(BATCH_LIMIT)
        .fetch_all(&mut *tx)
        .await?;
    tx.rollback().await?;

    let mut out = String::new();
    for row in rows {
        out.push_str(row.get::<String, _>(0).as_str());
        out.push('\n');
    }
    Ok(out)
}

/// The pass condition is structural, so report it structurally.
fn report(label: &str, plan: &str) {
    let sorted = plan.contains("->  Sort") || plan.contains("Sort Method");
    let spilled = plan.contains("Disk:") || plan.contains("temp read");
    let time = plan
        .lines()
        .find_map(|line| line.strip_prefix("Execution Time: "))
        .unwrap_or("?");
    println!(
        "  {label}: sort={} spill={} execution={time}\n",
        if sorted { "PRESENT" } else { "none" },
        if spilled { "yes" } else { "no" },
    );
}

async fn seed(pool: &PgPool, table: &OutboxTable, shape: Shape) -> TestResult<()> {
    let name = table.qualified_name();

    // `md5(...) || md5(...)` is 64 lowercase hex characters, which is what the
    // table's `idempotency_key` CHECK constrains.
    let insert = |status: &str, extra: &str, count: i64, salt: i64, prefix: &str, age: &str| {
        format!(
            "INSERT INTO {name} (
                 message_id, idempotency_key, status, attempts, next_attempt_at,
                 topic, partition_key, entity_key, correlation_id, headers, payload,
                 occurred_at, created_at{extra_cols}
             )
             SELECT gen_random_uuid(),
                    md5((i * {salt})::text) || md5((i * {salt} + 3)::text),
                    '{status}', 0, now() - interval '1 second',
                    'products', '{prefix}' || i, '{prefix}' || i, gen_random_uuid(),
                    '{{}}'::jsonb,
                    jsonb_build_object('product_id', '{prefix}' || i, 'name', 'seed'),
                    now(), {age}{extra}
               FROM generate_series(1, {count}) AS i",
            extra_cols = if extra.is_empty() {
                ""
            } else {
                ", published_at"
            },
        )
    };

    if shape.published > 0 {
        // Terminal bulk, older than everything live, as a real table would be.
        sqlx::query(&insert(
            "Published",
            ", now()",
            shape.published,
            7,
            "done-",
            "now() - interval '30 days' - (i * interval '1 millisecond')",
        ))
        .execute(pool)
        .await?;
    }

    sqlx::query(&insert(
        "Pending",
        "",
        shape.pending,
        11,
        "p-",
        "now() - (i * interval '1 millisecond')",
    ))
    .execute(pool)
    .await?;

    // Overtaken pairs are seeded at the *head* of the queue. Both statements order
    // by `created_at` ascending, so a pair at `now()` would sort to the far end of
    // the backlog and the window-driven form would correctly find nothing — making
    // the comparison a no-op measured against real work.
    for (salt, age) in [(13, 400), (17, 399)] {
        sqlx::query(&insert(
            "Pending",
            "",
            shape.overtaken,
            salt,
            "q-",
            &format!("now() - ({age} * interval '1 second')"),
        ))
        .execute(pool)
        .await?;
    }

    // Without fresh statistics the planner works from defaults, and the measured
    // plan is not the one production would get.
    sqlx::query(&format!("ANALYZE {name}"))
        .execute(pool)
        .await?;
    Ok(())
}
