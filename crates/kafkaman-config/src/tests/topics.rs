//! Unit tests for the `[topics]` section.

use kafkaman_core::TopicMode;

use crate::Config;

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
