use kafkaman_core::{ReceiveStatus, ReceivedFailureKind};
use time::OffsetDateTime;

use crate::replay::replay_received_filter_sql;
use crate::tests::OrderCreated;
use kafkaman_core::KafkaMessage;

use crate::{ChangeBuilder, Changeset, Error, Replay, ResolvedConfig};

#[test]
fn received_replay_filter_targets_only_terminal_rows() {
    let replay = Replay::received::<OrderCreated>(1).unwrap();
    let filter = replay_received_filter_sql(&replay).unwrap();
    // Retryable rows recover on their own; replaying them would double-run
    // work that is already scheduled.
    assert!(filter.contains(&ReceiveStatus::Failed.sql_literal()));
    assert!(!filter.contains(&ReceiveStatus::Retryable.sql_literal()));

    let narrowed = Replay::received::<OrderCreated>(1)
        .unwrap()
        .failure_kind(ReceivedFailureKind::Handler)
        .since(OffsetDateTime::UNIX_EPOCH);
    let filter = replay_received_filter_sql(&narrowed).unwrap();
    assert!(filter.contains("last_failure_kind = 'Handler'"));
    assert!(filter.contains("last_failed_at >= "));
}

#[test]
fn clear_history_changes_the_replay_checksum() {
    // Checksum material is what stops a silently-edited changeset from
    // re-running under an already-applied version, so every option that
    // changes emitted SQL must change the digest.
    let base = Replay::received::<OrderCreated>(9).unwrap().max_rows(5);
    let cleared = Replay::received::<OrderCreated>(9)
        .unwrap()
        .max_rows(5)
        .clear_history();
    assert_ne!(base.checksum(), cleared.checksum());

    let narrowed = Replay::received::<OrderCreated>(9)
        .unwrap()
        .max_rows(5)
        .failure_kind(ReceivedFailureKind::InvalidPayload);
    assert_ne!(base.checksum(), narrowed.checksum());
}

#[test]
fn a_replay_without_a_row_cap_refuses_to_build() {
    // An unbounded redrive of a terminal backlog is the one shape that can turn
    // a repair into a second outage.
    let cfg = ResolvedConfig::default().with_message(OrderCreated::descriptor().unwrap());
    let replay = Replay::received::<OrderCreated>(3).unwrap();

    assert!(matches!(
        replay.dry_run_preview(&cfg),
        Ok(message) if message.contains("would requeue")
    ));
    assert!(matches!(
        replay.build(&cfg, &mut ChangeBuilder::new()),
        Err(Error::InvalidReplay { .. })
    ));
    assert!(matches!(
        Replay::received::<OrderCreated>(3)
            .unwrap()
            .max_rows(0)
            .build(&cfg, &mut ChangeBuilder::new()),
        Err(Error::InvalidReplay { .. })
    ));
}

#[test]
fn outbox_replay_is_rejected_before_it_can_build_sql() {
    assert!(matches!(
        Replay::outbox::<OrderCreated>(3),
        Err(Error::UnsafeOutboxReplay { .. })
    ));
}
