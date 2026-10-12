//! Unit tests for the `[topics]` section.

use kafkaman_core::TopicMode;

use crate::{Config, TopicsSection};

#[test]
fn topics_defaults_to_verify_when_the_section_is_absent() {
    // The load-bearing default. An absent section must not mean "skip": that
    // would preserve the silent misconfiguration this check exists to catch.
    let cfg = Config::parse("[relay]\nworker_id = \"w\"").unwrap();
    assert_eq!(cfg.topics().unwrap().mode, TopicMode::Verify);
}

#[test]
fn topics_defaults_to_verify_when_the_section_is_present_but_empty() {
    let cfg = Config::parse("[topics]").unwrap();
    assert_eq!(cfg.topics().unwrap().mode, TopicMode::Verify);
}

#[test]
fn every_topic_mode_parses() {
    for (raw, expected) in [
        ("verify", TopicMode::Verify),
        ("create", TopicMode::Create),
        ("off", TopicMode::Off),
    ] {
        let cfg = Config::parse(&format!("[topics]\nmode = \"{raw}\"")).unwrap();
        assert_eq!(cfg.topics().unwrap().mode, expected, "mode = {raw}");
    }
}

#[test]
fn an_unknown_topic_mode_fails_fast() {
    // Rather than silently falling back to a default, which would be the
    // difference between "I turned verification off" and "I typoed it".
    let cfg = Config::parse("[topics]\nmode = \"varify\"").unwrap();
    let err = cfg.topics().expect_err("an unknown mode must be rejected");
    assert!(err.to_string().contains("topics"), "{err}");
}

#[test]
fn an_unknown_topics_key_fails_fast() {
    let cfg = Config::parse("[topics]\nmode = \"verify\"\nmoed = \"create\"").unwrap();
    cfg.topics()
        .expect_err("an unknown key must be rejected rather than ignored");
}

#[test]
fn the_topic_prefix_defaults_to_empty_and_parses_when_set() {
    let cfg = Config::parse("[topics]").unwrap();
    assert_eq!(cfg.topics().unwrap().prefix, "");

    let cfg = Config::parse("[topics]\nprefix = \"dev-slot1.\"").unwrap();
    assert_eq!(cfg.topics().unwrap().prefix, "dev-slot1.");
}

#[test]
fn a_topic_prefix_is_accepted_only_in_its_documented_shape() {
    for ok in ["", "p.", "dev-slot1.", "dev-slot1-", "0a.b-"] {
        TopicsSection::validate_prefix(ok).unwrap_or_else(|err| panic!("`{ok}`: {err}"));
    }
    // No trailing separator, a leading separator, upper case, and characters
    // outside the set: each would make a topic name nobody can split back into
    // prefix and type, or one Kafka rejects.
    for bad in [
        "p",
        "dev-slot1",
        ".p.",
        "-p.",
        "Dev.",
        "dev_slot.",
        "dev slot.",
        "dév.",
    ] {
        TopicsSection::validate_prefix(bad).expect_err(bad);
    }
}
