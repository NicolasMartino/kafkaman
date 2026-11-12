//! The `[recovery]` section and the named host overrides.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::num::NonZeroUsize;
use std::time::Duration;

use kafkaman_config::{Config, ConfigError, RecoveryConfig};
use kafkaman_core::MAX_REDRIVE_ROWS;

fn recovery(toml: &str) -> Result<RecoveryConfig, ConfigError> {
    Config::parse(toml).expect("the fixture parses").recovery()
}

fn refusal(toml: &str) -> String {
    match recovery(toml) {
        Err(ConfigError::InvalidValue { key, message }) => {
            assert_eq!(key, "recovery");
            message
        }
        other => panic!("expected an invalid [recovery] section, got {other:?}"),
    }
}

#[test]
fn an_absent_section_takes_every_default() {
    assert_eq!(
        recovery("").unwrap(),
        RecoveryConfig {
            batch_size: NonZeroUsize::new(100).unwrap(),
            pause: Duration::ZERO,
            max_redrive_rows: MAX_REDRIVE_ROWS,
        }
    );
    assert_eq!(MAX_REDRIVE_ROWS, 10_000);
}

#[test]
fn a_partial_section_defaults_only_what_it_leaves_out() {
    let cfg = recovery("[recovery]\npause_ms = 100\n").unwrap();
    assert_eq!(cfg.batch_size.get(), 100);
    assert_eq!(cfg.pause, Duration::from_millis(100));
    assert_eq!(cfg.max_redrive_rows, MAX_REDRIVE_ROWS);
}

#[test]
fn explicit_values_are_kept() {
    let cfg =
        recovery("[recovery]\nbatch_size = 25\npause_ms = 200\nmax_redrive_rows = 1\n").unwrap();
    assert_eq!(cfg.batch_size.get(), 25);
    assert_eq!(cfg.pause, Duration::from_millis(200));
    assert_eq!(cfg.max_redrive_rows, 1);
}

#[test]
fn the_ceiling_is_bounded_on_both_sides() {
    assert_eq!(
        recovery("[recovery]\nmax_redrive_rows = 10000\n")
            .unwrap()
            .max_redrive_rows,
        10_000
    );
    for value in [0, -1, 10_001] {
        let message = refusal(&format!("[recovery]\nmax_redrive_rows = {value}\n"));
        assert!(
            message.contains("between 1 and 10000"),
            "{value}: {message}"
        );
    }
}

#[test]
fn a_nonpositive_batch_or_negative_pause_is_refused() {
    for value in [0, -5] {
        let message = refusal(&format!("[recovery]\nbatch_size = {value}\n"));
        assert!(message.contains("batch_size must be > 0"), "{message}");
    }
    let message = refusal("[recovery]\npause_ms = -1\n");
    assert!(message.contains("pause_ms must be >= 0"), "{message}");
}

#[test]
fn wrong_types_and_unknown_keys_are_refused() {
    for toml in [
        "[recovery]\nbatch_size = \"100\"\n",
        "[recovery]\npause_ms = \"100ms\"\n",
        "[recovery]\nbatch = 100\n",
        "recovery = 3\n",
    ] {
        assert!(
            matches!(recovery(toml), Err(ConfigError::InvalidValue { .. })),
            "{toml:?} must be refused"
        );
    }
}

#[test]
fn an_integer_past_i64_is_refused_when_parsed() {
    assert!(matches!(
        Config::parse("[recovery]\nbatch_size = 9223372036854775808\n"),
        Err(ConfigError::Parse(_))
    ));
}

#[test]
fn the_overrides_replace_the_files_schema_and_worker() {
    let cfg = Config::parse(
        "[database]\nschema = \"kafkaman\"\n[relay]\nworker_id = \"user-local\"\nbatch_limit = 100\n",
    )
    .unwrap()
    .with_schema("kafkaman_slot1")
    .unwrap()
    .with_worker_id("user-republish-pod")
    .unwrap();
    assert_eq!(
        cfg.get::<String>("database.schema").unwrap(),
        "kafkaman_slot1"
    );
    assert_eq!(
        cfg.get::<String>("relay.worker_id").unwrap(),
        "user-republish-pod"
    );
    // Only the named key changes.
    assert_eq!(cfg.get::<i64>("relay.batch_limit").unwrap(), 100);
}

#[test]
fn an_override_creates_a_missing_section_and_refuses_a_non_table() {
    let cfg = Config::parse("").unwrap().with_schema("kafkaman").unwrap();
    assert_eq!(cfg.get::<String>("database.schema").unwrap(), "kafkaman");

    let err = Config::parse("relay = \"worker\"\n")
        .unwrap()
        .with_worker_id("pod")
        .unwrap_err();
    assert!(
        matches!(err, ConfigError::WrongType { ref key, expected: "table", found: "string" } if key == "relay"),
        "{err:?}"
    );
}

#[test]
fn the_example_documents_a_valid_recovery_section() {
    let example = Config::parse(include_str!("../../../kafkaman.example.toml")).unwrap();
    assert!(
        example.contains("recovery"),
        "the example must document the [recovery] section"
    );
    assert_eq!(
        example.recovery().unwrap(),
        RecoveryConfig {
            batch_size: NonZeroUsize::new(100).unwrap(),
            pause: Duration::from_millis(100),
            max_redrive_rows: MAX_REDRIVE_ROWS,
        }
    );
}
