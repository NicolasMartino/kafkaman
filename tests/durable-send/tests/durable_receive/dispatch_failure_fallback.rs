//! Recording a dispatch failure when the savepoint rollback itself fails.
//!
//! `rollback_handler_and_record_received_failure` has two paths. The normal one
//! unwinds the handler's writes to the savepoint and records the failure in the
//! same transaction as the claim, which is what keeps attempts and error history
//! consistent with the row's status. When that rollback fails the transaction is
//! unusable, so it is abandoned and the failure is recorded on a fresh
//! connection — losing the atomicity, keeping the record.
//!
//! The second path had never run. It is the path a *panic* is most likely to
//! reach, since a panic can leave the connection in states a returned error
//! cannot, and "the failure is silently not recorded" is the worst outcome the
//! dispatcher has: the row keeps its old status and is claimed again forever.
//!
//! No hooks needed to provoke it. A handler that releases kafkaman's savepoint
//! destroys the thing the rollback names, so `ROLLBACK TO SAVEPOINT` fails for
//! real, against a real connection, exactly as it would if the transaction had
//! been broken some other way.
use super::*;

/// The savepoint kafkaman opens before the handler, released out from under it.
const DISPATCH_SAVEPOINT: &str = "kafkaman_dispatch_handler";

#[tokio::test]
async fn a_failed_savepoint_rollback_still_records_the_failure() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-lost-savepoint".to_owned(),
    })
    .with_idempotency_key("idem-lost-savepoint");
    assert!(
        harness
            .insert_received(&envelope, 9, 120, Some(b"order-lost-savepoint"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, _msg| {
        Box::pin(async move {
            sqlx::query(&format!("RELEASE SAVEPOINT {DISPATCH_SAVEPOINT}"))
                .execute(&mut *conn)
                .await?;
            Err(kafkaman_sqlx::Error::Handler(
                "failed after releasing the savepoint".to_owned(),
            ))
        })
    });

    let now = OffsetDateTime::now_utc();
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.claimed, 1);
    assert_eq!(stats.processed, 0);
    assert_eq!(
        stats.failed, 1,
        "a failure that could not be recorded atomically is still a failure, and \
         reporting it as anything else would let the loop treat the cycle as idle"
    );

    // The assertion the fallback exists for. Without it the transaction is
    // abandoned with nothing written, the row keeps its pending status, and the
    // next cycle claims it again — a row that fails forever with an empty error
    // history and an attempt count that never moves.
    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-lost-savepoint")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.errors.len(), 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Handler);
    assert!(
        row.errors[0]
            .detail
            .contains("failed after releasing the savepoint"),
        "the handler's own message survives the fallback: {}",
        row.errors[0].detail
    );

    Ok(())
}

/// The same fallback, reached by a panic rather than a returned error.
///
/// Worth pinning separately: the panic boundary drops the handler future mid-
/// flight, so the connection reaches the rollback in a state a returned error
/// never produces, and the panic message has to survive a path that abandons the
/// transaction carrying it.
#[tokio::test]
async fn a_panic_that_loses_the_savepoint_still_records_the_failure() -> TestResult {
    let _test_guard = receive_test_lock().lock().await;
    let (_postgres, harness) = start_harness().await?;
    let table = harness.received_table::<OrderCreated>().await?;

    let envelope = Envelope::new(OrderCreated {
        order_id: "order-lost-savepoint-panic".to_owned(),
    })
    .with_idempotency_key("idem-lost-savepoint-panic");
    assert!(
        harness
            .insert_received(&envelope, 9, 121, Some(b"order-lost-savepoint-panic"))
            .await?
    );

    let router = MessageRouter::new().handler::<OrderCreated>(|conn, _meta, _msg| {
        Box::pin(async move {
            sqlx::query(&format!("RELEASE SAVEPOINT {DISPATCH_SAVEPOINT}"))
                .execute(&mut *conn)
                .await?;
            panic!("panicked after releasing the savepoint");
        })
    });

    let now = OffsetDateTime::now_utc();
    let stats = dispatch_once(harness.pool(), &table, &router, now).await?;
    assert_eq!(stats.failed, 1);
    assert_eq!(stats.panicked, 1);
    assert_eq!(
        stats.panicked_message_id,
        Some(envelope.message_id),
        "the breaker needs the row's identity even on the fallback path, or a \
         panic recorded this way would not count towards it"
    );

    let row = harness
        .received_row_by_idempotency_key::<OrderCreated>("idem-lost-savepoint-panic")
        .await?;
    assert_eq!(row.status, ReceiveStatus::Retryable);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.errors[0].kind, ReceivedFailureKind::Handler);
    assert!(
        row.errors[0].detail.contains("handler panicked"),
        "{}",
        row.errors[0].detail
    );
    assert!(row.errors[0]
        .detail
        .contains("panicked after releasing the savepoint"));

    Ok(())
}
