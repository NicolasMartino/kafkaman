use std::time::Instant;

use kafkaman_core::{LifecycleSampler, MarkOutcome, RelayConfig, RelayStats};
use kafkaman_sqlx::{claim_batch, mark_publish_failed, mark_published, OutboxTable};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::metrics::RelayMetrics;
use crate::run_loop::sleep_or_shutdown;
use crate::{Publisher, Result};

/// Claim one batch, publish it, and record what happened to each row.
///
/// The claim commits before any publishing starts, so a slow broker does not
/// hold a transaction open across network calls. Publishing is sequential
/// because claim order is publish order, and per-entity ordering is what the
/// convergence guard downstream depends on.
#[tracing::instrument(level = "debug", target = "kafkaman::internal", skip_all)]
pub async fn relay_once<P: Publisher>(
    pool: &PgPool,
    publisher: &P,
    table: &OutboxTable,
    cfg: &RelayConfig,
) -> Result<RelayStats> {
    relay_once_inner(pool, publisher, table, cfg, None, None).await
}

/// The body of [`relay_once`], with the loop's instruments threaded in.
///
/// `metrics` and `lifecycle` are `None` when the public helper is called
/// directly, which is how the scheduler counters stay out of a deployment's
/// series when a test drives one cycle by hand — the same reason `relay_stats`
/// lives on the loop rather than here. The per-publish latencies and the
/// per-message success events can only be produced inside this function, so they
/// travel the same way rather than getting a second mechanism.
///
/// The sampler is borrowed mutably because its count carries across cycles:
/// sampling one in ten successes has to mean one in ten over the stream, not one
/// per batch that happens to contain ten.
async fn relay_once_inner<P: Publisher>(
    pool: &PgPool,
    publisher: &P,
    table: &OutboxTable,
    cfg: &RelayConfig,
    metrics: Option<&RelayMetrics>,
    mut lifecycle: Option<&mut LifecycleSampler>,
) -> Result<RelayStats> {
    cfg.validate()?;

    // Debug rather than info, beside `claim_batch`'s own span: this transaction
    // opens and closes on every cycle whether or not a row is due, so at the
    // default filter it would be most of what an idle service exports.
    let mut tx = pool
        .begin()
        .instrument(kafkaman_core::db_poll_span!(
            "BEGIN",
            table.qualified_name(),
            "open outbox claim transaction"
        ))
        .await
        .map_err(kafkaman_sqlx::Error::from)?;
    let claimed = claim_batch(
        &mut tx,
        table,
        &cfg.worker_id,
        cfg.lease_for,
        cfg.batch_limit,
    )
    .await?;
    tx.commit()
        .instrument(kafkaman_core::db_poll_span!(
            "COMMIT",
            table.qualified_name(),
            "commit outbox claim transaction"
        ))
        .await
        .map_err(kafkaman_sqlx::Error::from)?;

    let mut stats = RelayStats {
        claimed: claimed.len(),
        ..RelayStats::default()
    };

    for row in claimed {
        // The span the consumer will link to, and the one that closes the gap
        // the outbox opens: parented from the context stored at enqueue, so a
        // publish minutes later still belongs to the transaction that caused it.
        // Without a stored context it is a root span, which is what an
        // uninstrumented enqueue should produce.
        let span = tracing::info_span!(
            "kafkaman.relay.publish",
            "otel.kind" = "producer",
            "otel.status_code" = tracing::field::Empty,
            "otel.status_description" = tracing::field::Empty,
            message_type = table.descriptor.message_type.as_str(),
            messaging.system = "kafka",
            messaging.destination.name = row.row.topic.as_str(),
            messaging.operation.name = "send",
            messaging.message.id = %row.message_id(),
        );
        if let Some(trace) = &row.row.trace {
            kafkaman_core::set_parent(&span, trace);
        }

        let started = Instant::now();
        // Instrumented rather than entered: the publish awaits, and a span guard
        // held across an await attributes whatever else the runtime schedules on
        // this thread to this message.
        let published = publisher.publish(&row).instrument(span.clone()).await;
        if let Some(metrics) = metrics {
            let outcome = if published.is_ok() {
                "published"
            } else {
                "failed"
            };
            metrics.publish(&row.row.topic, outcome, started.elapsed());
            if published.is_ok() {
                metrics.published(row.row.occurred_at);
            }
        }

        // A mark that finds no claim is not an error: the lease expired and
        // another worker owns the row now. Counted, not failed.
        let outcome = match published {
            Ok(_) => {
                let marked = mark_published(pool, table, row.message_id(), row.claim_id)
                    .instrument(span.clone())
                    .await;
                if let Err(err) = &marked {
                    kafkaman_core::record_error(&span, err);
                }
                let outcome = marked?;
                if outcome == MarkOutcome::Updated {
                    stats.published += 1;
                }
                outcome
            }
            Err(err) => {
                // Rendered once: it is both the span's status description and
                // the `last_error` column the retry writes.
                let description = err.to_string();
                kafkaman_core::record_error(&span, &description);
                let marked = mark_publish_failed(
                    pool,
                    table,
                    row.message_id(),
                    row.claim_id,
                    &description,
                    cfg.retry_after,
                )
                .instrument(span.clone())
                .await;
                if let Err(err) = &marked {
                    kafkaman_core::record_error(&span, err);
                }
                let outcome = marked?;
                if outcome == MarkOutcome::Updated {
                    stats.failed += 1;
                }
                outcome
            }
        };

        match outcome {
            MarkOutcome::Updated => {
                // Emitted here, per row, rather than per cycle from the loop —
                // and inside the row's span. An event about one message that
                // cannot be traced back to that message is a line in a log file;
                // one that can is the pivot `sample_success` exists to provide.
                if let Some(sampler) = lifecycle.as_deref_mut() {
                    if sampler.take(1) > 0 {
                        // Attached, not merely parented: the log appender stamps
                        // records from the OpenTelemetry context, which the
                        // `tracing` span stack does not set on its own.
                        let _scope = kafkaman_core::attach(&span);
                        tracing::info!(
                            parent: &span,
                            message_type = table.descriptor.message_type.as_str(),
                            "outbox message published"
                        );
                    }
                }
            }
            MarkOutcome::StaleClaim => stats.stale += 1,
            MarkOutcome::Missing => stats.missing += 1,
        }
    }

    Ok(stats)
}

/// Run the outbox relay until `shutdown` is cancelled.
///
/// Returns `Err` only for a configuration that can never succeed. Transient
/// per-cycle failures are logged and retried, because a database blip must not
/// take the relay down.
///
/// Unlike [`run_dispatcher`](crate::run_dispatcher) and
/// [`run_purger`](crate::run_purger), this sleeps after every cycle rather than
/// continuing immediately on a non-empty batch. That means a backlog drains at
/// one batch per poll interval. It is left as-is deliberately: changing it is a
/// throughput change with its own timing consequences, not a tidy-up.
pub async fn run<P: Publisher>(
    pool: PgPool,
    publisher: P,
    table: OutboxTable,
    cfg: RelayConfig,
    shutdown: CancellationToken,
) -> Result<()> {
    // Validate once, up front. Previously the only validation happened inside
    // `relay_once`, so an invalid config produced an error every cycle and then
    // slept on the very `poll_interval` that had just been rejected — a zero
    // interval span the loop at full speed while logging an error each pass.
    cfg.validate()?;

    let metrics = RelayMetrics::new(table.descriptor.message_type.as_str());
    let mut lifecycle = cfg.lifecycle.sampler();

    loop {
        // Checked before the cycle, not only after it. Without this a relay
        // handed an already-cancelled token still claims and publishes one
        // batch — harmless to the data, since claims are leased and marks are
        // idempotent, but it starts broker calls a shutdown has already asked
        // it not to start. `run_dispatcher` and `run_purger` both guard here.
        if shutdown.is_cancelled() {
            break;
        }

        match relay_once_inner(
            &pool,
            &publisher,
            &table,
            &cfg,
            Some(&metrics),
            Some(&mut lifecycle),
        )
        .await
        {
            Ok(stats) => {
                metrics.relay_stats(&stats);
                if stats.claimed > 0 {
                    tracing::debug!(
                        claimed = stats.claimed,
                        published = stats.published,
                        failed = stats.failed,
                        stale = stats.stale,
                        missing = stats.missing,
                        "relay cycle complete"
                    );
                }
            }
            Err(err) => {
                metrics.error();
                tracing::error!(
                    error = %err,
                    "relay cycle failed; retrying after poll interval"
                );
            }
        }

        if !sleep_or_shutdown(cfg.poll_interval, &shutdown).await {
            break;
        }
    }

    Ok(())
}
