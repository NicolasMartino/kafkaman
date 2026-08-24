//! Unit tests for [`crate::topics`].

use crate::topics::{reconcile, CleanupPolicy, ObservedTopic, TopicAction, TopicMode, TopicSpec};
use crate::Error;

#[test]
fn parses_the_policies_a_broker_reports() {
    assert_eq!(CleanupPolicy::parse("compact"), CleanupPolicy::Compact);
    assert_eq!(CleanupPolicy::parse("delete"), CleanupPolicy::Delete);
    assert_eq!(
        CleanupPolicy::parse("compact,delete"),
        CleanupPolicy::CompactAndDelete
    );
}

#[test]
fn policy_order_and_spacing_are_not_significant() {
    // Kafka does not promise an order, so a spelling difference must not be
    // mistaken for a configuration difference.
    assert_eq!(
        CleanupPolicy::parse("delete,compact"),
        CleanupPolicy::CompactAndDelete
    );
    assert_eq!(
        CleanupPolicy::parse(" compact , delete "),
        CleanupPolicy::CompactAndDelete
    );
    assert_eq!(CleanupPolicy::parse("COMPACT"), CleanupPolicy::Compact);
}

#[test]
fn an_unknown_policy_is_kept_verbatim_rather_than_guessed() {
    assert_eq!(
        CleanupPolicy::parse("compact,quantum"),
        CleanupPolicy::Unrecognized("compact,quantum".to_owned())
    );
    assert_eq!(
        CleanupPolicy::parse(""),
        CleanupPolicy::Unrecognized(String::new())
    );
}

#[test]
fn policies_round_trip_through_their_broker_spelling() {
    for policy in [
        CleanupPolicy::Compact,
        CleanupPolicy::Delete,
        CleanupPolicy::CompactAndDelete,
    ] {
        assert_eq!(CleanupPolicy::parse(policy.as_broker_value()), policy);
    }
}

#[test]
fn only_compact_alone_counts_as_compacted() {
    assert!(CleanupPolicy::Compact.is_compact_only());
    assert!(!CleanupPolicy::Delete.is_compact_only());
    assert!(!CleanupPolicy::CompactAndDelete.is_compact_only());
    assert!(!CleanupPolicy::Unrecognized("compact ".to_owned()).is_compact_only());
}

#[test]
fn the_default_spec_is_compacted() {
    let spec = TopicSpec::default();
    assert_eq!(spec.cleanup_policy, CleanupPolicy::Compact);
    assert_eq!(spec.partitions, None);
    assert_eq!(spec.replication_factor, None);
}

fn observed(policy: CleanupPolicy, partitions: i32) -> ObservedTopic {
    ObservedTopic {
        name: "products".to_owned(),
        cleanup_policy: policy,
        partitions,
    }
}

#[test]
fn a_compacted_topic_passes() {
    let drift = TopicSpec::compacted()
        .check(&observed(CleanupPolicy::Compact, 1))
        .expect("compacted topic is accepted");
    assert_eq!(drift, None);
}

#[test]
fn a_delete_topic_is_rejected() {
    let err = TopicSpec::compacted()
        .check(&observed(CleanupPolicy::Delete, 1))
        .expect_err("delete retention must not be accepted");
    let message = err.to_string();
    assert!(message.contains("products"), "{message}");
    assert!(message.contains("compact"), "{message}");
    assert!(message.contains("delete"), "{message}");
}

#[test]
fn compact_and_delete_is_rejected_just_as_firmly_as_delete() {
    // The case most likely to be mistaken for correct: compaction *is*
    // enabled, but records still age out, so the log cannot rebuild an
    // entity.
    TopicSpec::compacted()
        .check(&observed(CleanupPolicy::CompactAndDelete, 1))
        .expect_err("compact,delete must not be accepted");
}

#[test]
fn partition_drift_is_reported_but_not_an_error() {
    let spec = TopicSpec::compacted()
        .with_partitions(6)
        .expect("6 partitions is valid");
    let drift = spec
        .check(&observed(CleanupPolicy::Compact, 3))
        .expect("drift must not fail the check")
        .expect("drift must be reported");
    assert_eq!(drift.declared, 6);
    assert_eq!(drift.found, 3);
    assert!(drift.to_string().contains("republishing"), "{drift}");
}

#[test]
fn a_matching_partition_count_reports_nothing() {
    let spec = TopicSpec::compacted().expect_partitions(3);
    assert_eq!(
        spec.check(&observed(CleanupPolicy::Compact, 3))
            .expect("matching count is fine"),
        None
    );
}

#[test]
fn an_undeclared_partition_count_never_drifts() {
    // Verification does not need a partition count, so leaving it out must
    // not manufacture a warning on every boot.
    assert_eq!(
        TopicSpec::compacted()
            .check(&observed(CleanupPolicy::Compact, 12))
            .expect("no declared count means nothing to compare"),
        None
    );
}

#[test]
fn creation_refuses_to_guess_a_partition_count() {
    let err = TopicSpec::compacted()
        .partitions_for_create("products")
        .expect_err("creation must not invent a partition count");
    assert!(err.to_string().contains("products"), "{err}");
}

#[test]
fn creation_uses_the_declared_partition_count() {
    let spec = TopicSpec::compacted().expect_partitions(6);
    assert_eq!(
        spec.partitions_for_create("products")
            .expect("declared count is used"),
        6
    );
}

#[test]
fn partition_and_replication_counts_must_be_positive() {
    TopicSpec::compacted()
        .with_partitions(0)
        .expect_err("zero partitions is not a topic");
    TopicSpec::compacted()
        .with_partitions(-1)
        .expect_err("negative partitions is not a topic");
    TopicSpec::compacted()
        .with_replication_factor(0)
        .expect_err("zero replicas is not durable");
}

#[test]
fn a_spec_survives_a_serde_round_trip() {
    let spec = TopicSpec::compacted()
        .expect_partitions(3)
        .with_replication_factor(2)
        .expect("2 replicas is valid");
    let json = serde_json::to_string(&spec).expect("spec serializes");
    // The policy travels as a plain string, so an unrecognised one from a
    // newer build round-trips instead of failing to deserialize.
    assert!(json.contains("\"compact\""), "{json}");
    let back: TopicSpec = serde_json::from_str(&json).expect("spec deserializes");
    assert_eq!(back, spec);
}

#[test]
fn an_unrecognized_policy_survives_a_serde_round_trip() {
    let policy = CleanupPolicy::Unrecognized("compact,quantum".to_owned());
    let json = serde_json::to_string(&policy).expect("policy serializes");
    let back: CleanupPolicy = serde_json::from_str(&json).expect("policy deserializes");
    assert_eq!(back, policy);
}

#[test]
fn off_does_nothing_even_when_the_topic_is_wrong() {
    // The escape hatch has to be a real escape hatch: a cluster that denies
    // metadata reads cannot answer any of these questions, so nothing here
    // may fail.
    for observed in [
        None,
        Some(observed(CleanupPolicy::Delete, 1)),
        Some(observed(CleanupPolicy::CompactAndDelete, 9)),
    ] {
        let outcome = reconcile(
            "products",
            &TopicSpec::compacted(),
            observed.as_ref(),
            TopicMode::Off,
        )
        .expect("off never fails");
        assert_eq!(outcome.action, TopicAction::Satisfied);
        assert_eq!(outcome.drift, None);
    }
}

#[test]
fn verify_fails_on_a_missing_topic() {
    // Letting it through would mean the broker auto-creates it on first
    // publish — with `delete` retention, which is the whole bug.
    let err = reconcile("products", &TopicSpec::compacted(), None, TopicMode::Verify)
        .expect_err("a missing topic must not be shrugged at");
    assert!(matches!(err, Error::TopicMissing { .. }), "{err}");
    assert!(err.to_string().contains("products"), "{err}");
}

#[test]
fn verify_never_asks_to_create_anything() {
    let spec = TopicSpec::compacted().expect_partitions(3);
    // Even with everything needed to create it, `verify` must stay
    // read-only: the mode exists for clusters where creating is forbidden.
    reconcile("products", &spec, None, TopicMode::Verify).expect_err("verify must not create");
}

#[test]
fn create_creates_a_missing_topic_to_spec() {
    let spec = TopicSpec::compacted()
        .expect_partitions(6)
        .with_replication_factor(3)
        .expect("3 replicas is valid");
    let outcome = reconcile("products", &spec, None, TopicMode::Create)
        .expect("create is permitted to create");
    assert_eq!(
        outcome.action,
        TopicAction::Create {
            partitions: 6,
            replication_factor: Some(3),
        }
    );
    assert_eq!(outcome.drift, None);
}

#[test]
fn create_refuses_a_missing_topic_with_no_declared_partition_count() {
    let err = reconcile("products", &TopicSpec::compacted(), None, TopicMode::Create)
        .expect_err("create must not invent a partition count");
    assert!(
        matches!(err, Error::TopicPartitionsUndeclared { .. }),
        "{err}"
    );
}

#[test]
fn an_existing_correct_topic_satisfies_every_mode() {
    for mode in [TopicMode::Verify, TopicMode::Create] {
        let outcome = reconcile(
            "products",
            &TopicSpec::compacted(),
            Some(&observed(CleanupPolicy::Compact, 1)),
            mode,
        )
        .expect("a compacted topic is fine");
        assert_eq!(outcome.action, TopicAction::Satisfied, "{mode:?}");
        assert_eq!(outcome.drift, None, "{mode:?}");
    }
}

#[test]
fn create_does_not_repair_an_existing_wrong_topic() {
    // "create" means create what is missing, never rewrite what is there.
    // Silently changing a live topic's cleanup policy at boot would be a
    // data-retention change nobody asked for.
    let err = reconcile(
        "products",
        &TopicSpec::compacted(),
        Some(&observed(CleanupPolicy::Delete, 1)),
        TopicMode::Create,
    )
    .expect_err("an existing wrong topic is still an error under create");
    assert!(matches!(err, Error::TopicPolicyMismatch { .. }), "{err}");
}

#[test]
fn verify_fails_on_an_existing_wrong_topic() {
    let err = reconcile(
        "products",
        &TopicSpec::compacted(),
        Some(&observed(CleanupPolicy::Delete, 1)),
        TopicMode::Verify,
    )
    .expect_err("delete retention must fail boot");
    assert!(matches!(err, Error::TopicPolicyMismatch { .. }), "{err}");
}

#[test]
fn drift_is_carried_out_of_reconcile_without_failing_it() {
    let spec = TopicSpec::compacted().expect_partitions(6);
    let outcome = reconcile(
        "products",
        &spec,
        Some(&observed(CleanupPolicy::Compact, 3)),
        TopicMode::Verify,
    )
    .expect("drift must not fail boot");
    assert_eq!(outcome.action, TopicAction::Satisfied);
    let drift = outcome.drift.expect("drift is reported");
    assert_eq!((drift.declared, drift.found), (6, 3));
}

/// Test-only sugar so the partition-count builder does not need unwrapping
/// at every call site.
trait ExpectPartitions {
    fn expect_partitions(self, partitions: i32) -> TopicSpec;
}

impl ExpectPartitions for TopicSpec {
    fn expect_partitions(self, partitions: i32) -> TopicSpec {
        self.with_partitions(partitions)
            .expect("partition count is valid")
    }
}
