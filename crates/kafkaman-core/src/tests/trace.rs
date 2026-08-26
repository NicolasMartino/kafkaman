//! The W3C grammar, and the two doors into [`TraceContext`].
//!
//! These are the tests for a parser whose failures are silent by construction:
//! a malformed value is dropped, message flow is unaffected, and the only thing
//! that changes is that a trace quietly stops — or, worse, that kafkaman
//! republishes something a stricter downstream will reject. Nothing else in the
//! suite would notice either outcome.

use crate::{OutboxRow, TraceContext};

/// The example from the W3C Recommendation.
const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

fn parse(traceparent: &str) -> Option<TraceContext> {
    TraceContext::from_parts(Some(traceparent.to_owned()), None)
}

/// The `tracestate` kafkaman would forward for `value`, asserting along the way
/// that a bad one never costs the `traceparent` beside it.
fn with_state(value: &str) -> Option<String> {
    TraceContext::from_parts(Some(VALID.to_owned()), Some(value.to_owned()))
        .expect("the traceparent is valid, so a bad tracestate must not lose it")
        .tracestate()
        .map(ToOwned::to_owned)
}

#[test]
fn a_well_formed_traceparent_round_trips() {
    let context = TraceContext::from_parts(Some(VALID.to_owned()), Some("vendor=1".to_owned()))
        .expect("the specification's own example should parse");
    assert_eq!(context.traceparent(), VALID);
    assert_eq!(context.tracestate(), Some("vendor=1"));
}

#[test]
fn absent_context_is_absent_rather_than_an_error() {
    assert_eq!(TraceContext::from_parts(None, None), None);
    assert_eq!(
        TraceContext::from_parts(None, Some("vendor=1".to_owned())),
        None,
        "tracestate without traceparent describes nothing"
    );
}

#[test]
fn malformed_traceparents_are_dropped() {
    for value in [
        "",
        "not a traceparent",
        // Too few fields.
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
        // Trace id one character short.
        "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
        // Non-hex in the span id.
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902bg-01",
        // The specification's invalid all-zero ids.
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        // `ff` is reserved as a forbidden version.
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        // Uppercase hex. The grammar is `HEXDIGLC`, and a system comparing
        // trace ids as bytes would treat this as a different trace.
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00F067AA0BA902B7-01",
        // Version 00 is a closed format: nothing follows the flags.
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-",
    ] {
        assert_eq!(
            parse(value),
            None,
            "{value:?} should not be accepted as trace context"
        );
    }
}

#[test]
fn a_future_version_may_append_fields_but_never_an_empty_one() {
    // Forward compatibility is a rule of the Recommendation, not a courtesy: a
    // later version appends fields rather than rearranging the first four, so
    // refusing the whole value would drop context from an upstream that is
    // simply newer than this parser. This is the one place trailing content is
    // legal — version 00 rejects it, above.
    for value in [
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-a-b",
    ] {
        assert!(
            parse(value).is_some(),
            "{value:?} comes from a newer upstream and its first four fields parse"
        );
    }

    // An empty field is not an appended field — it is a truncation or a bad
    // join, and every one of them has to be checked. `-aa-` passes any parser
    // that looks only at the first thing after the flags.
    for value in [
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-",
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-aa-",
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-aa--bb",
    ] {
        assert_eq!(
            parse(value),
            None,
            "{value:?} has an empty appended field and is not a traceparent"
        );
    }
}

#[test]
fn an_unbounded_field_count_is_not_a_traceparent() {
    // The header is stored in a column and rewritten onto the wire at every hop,
    // so an unbounded field count is an unbounded header travelling under
    // kafkaman's name.
    let value = format!("{VALID}{}", "-aa".repeat(64)).replacen("00-", "01-", 1);
    assert_eq!(parse(&value), None);
}

#[test]
fn a_tracestate_that_breaks_the_grammar_is_dropped_without_the_traceparent() {
    // The asymmetry that matters: the vendor list decorates, the trace id
    // correlates. Losing the first must never cost the second — that would turn
    // one vendor's malformed header into a hole in the trace.
    for value in [
        "novalue",
        "=1",
        "UPPER=1",
        "-leading=1",
        "vendor=has,comma",
        "a@b@c=1",
        "vendor@=1",
    ] {
        assert_eq!(
            with_state(value),
            None,
            "{value:?} is not a tracestate and must not be forwarded"
        );
    }
}

#[test]
fn a_tracestate_is_normalized_rather_than_echoed() {
    // Optional whitespace sits around the commas, not inside a member, so the
    // same logical value must produce the same stored bytes however an upstream
    // spaced it. Empty members are legal and carry nothing.
    assert_eq!(with_state("a=1, b=2").as_deref(), Some("a=1,b=2"));
    assert_eq!(
        with_state(" a=1 ,\tb=2 ").as_deref(),
        Some("a=1,b=2"),
        "a blank after a value is the whitespace around a comma, not part of it"
    );
    assert_eq!(with_state("a=1,,b=2").as_deref(), Some("a=1,b=2"));
    assert_eq!(with_state("  "), None, "nothing left to forward");
    assert_eq!(with_state(""), None, "an empty tracestate is no tracestate");
    assert_eq!(
        with_state("1@vendor=x,other/key*1=v-2").as_deref(),
        Some("1@vendor=x,other/key*1=v-2"),
        "the multi-tenant and punctuation forms are both legal keys"
    );
}

#[test]
fn a_tab_inside_a_value_is_not_the_whitespace_around_a_comma() {
    // The two look alike and are not. `chr = %x20-2B / ...` starts at space, so
    // the only place a tab may legally appear is the optional whitespace
    // *between* members — which the normalization test above shows is trimmed.
    // Inside a value it is a byte the grammar has no room for, and forwarding it
    // hands a stricter downstream a header kafkaman signed.
    assert_eq!(with_state("vendor=a\tb"), None);
    assert_eq!(
        with_state("vendor=\tab"),
        None,
        "still inside the value once the member's own edges are trimmed"
    );
    assert_eq!(
        with_state("vendor=ab\t").as_deref(),
        Some("vendor=ab"),
        "and a tab at the member's edge really is the whitespace around a comma"
    );
}

#[test]
fn a_key_names_at_most_one_list_member() {
    assert_eq!(with_state("a=1,a=2"), None);
    assert_eq!(
        with_state("a=1,b=2,a=3"),
        None,
        "the repeat need not be adjacent"
    );

    // Past the 32-member ceiling, where the duplicate would be truncated away
    // before anything looked at it. Truncation is what a valid list gets for
    // being too long; a list with a repeated key was never valid, so the
    // ceiling must not be able to launder one into looking well-formed.
    let mut members: Vec<String> = (0..40).map(|index| format!("v{index}=x")).collect();
    members.push("v0=y".to_owned());
    assert_eq!(with_state(&members.join(",")), None);
}

#[test]
fn a_tracestate_too_long_to_be_worth_parsing_is_dropped() {
    // The member ceiling bounds what is stored; this bounds what is read. Every
    // member costs a uniqueness check against every key before it, so an
    // unbounded input is unbounded work on a value that could never have been
    // stored whole anyway.
    let huge: Vec<String> = (0..600).map(|index| format!("v{index}=x")).collect();
    let huge = huge.join(",");
    assert!(huge.len() > 2048, "the fixture has to exceed the ceiling");
    assert_eq!(with_state(&huge), None);
}

#[test]
fn a_tracestate_longer_than_the_ceiling_is_truncated_from_the_right() {
    // The Recommendation truncates rather than rejects, and the difference is
    // real: rejecting would throw away 32 usable vendor entries because a
    // thirty-third arrived.
    let members: Vec<String> = (0..40).map(|index| format!("v{index}=x")).collect();
    let state = with_state(&members.join(",")).expect("a long list is still a list");
    assert_eq!(state.split(',').count(), 32);
    assert!(
        state.starts_with("v0=x,v1=x"),
        "the left-most members survive"
    );
    assert!(!state.contains("v32="), "the right-most ones do not");
}

#[test]
fn deserializing_a_context_validates_instead_of_assigning_fields() {
    // Without this, `serde` is a second constructor that skips the only one
    // there is — and the value it builds goes onto the wire as a standard header
    // that the next service will parse without asking kafkaman's permission.
    let valid: TraceContext = serde_json::from_str(&format!(
        r#"{{"traceparent":"{VALID}","tracestate":"vendor=1"}}"#
    ))
    .expect("a well-formed context should deserialize");
    assert_eq!(valid.traceparent(), VALID);
    assert_eq!(valid.tracestate(), Some("vendor=1"));

    assert!(
        serde_json::from_str::<TraceContext>(r#"{"traceparent":"nonsense"}"#).is_err(),
        "a bare TraceContext asserts it holds one; a false assertion should say so"
    );
}

#[test]
fn a_row_drops_stored_context_that_no_longer_parses() {
    // The lenient half of the pair. A row serialized before the grammar was
    // tightened — uppercase hex was accepted once — must still load, minus a
    // trace link that no longer means anything. Failing the whole row would stop
    // a message over a field with no business meaning, which is the one thing
    // trace context is never allowed to do.
    let unreadable = "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01";
    let row: OutboxRow = serde_json::from_value(super::rows::outbox_row_json(unreadable))
        .expect("a row with unreadable context is still a row");
    assert!(row.trace.is_none());

    let row: OutboxRow = serde_json::from_value(super::rows::outbox_row_json(VALID))
        .expect("a row with readable context is still a row");
    assert_eq!(
        row.trace.as_ref().map(TraceContext::traceparent),
        Some(VALID)
    );
}
