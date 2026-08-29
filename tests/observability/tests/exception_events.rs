//! Whether a recorded failure reaches a backend as an *error*, not just a status.
//!
//! An APM backend groups errors by exception, and derives them from the
//! OpenTelemetry `exception` span event — not from a span's status. A span that
//! is merely red produces a failed transaction with nothing behind it to open.
//!
//! Everything asserted here is a property of the `tracing-opentelemetry` bridge
//! rather than of kafkaman, which is exactly why it is pinned: `record_exception`
//! satisfies three of the bridge's conditions at once (unnamed event, a field
//! called `error`, pre-truncated value), and none of the three is self-evident
//! from reading the call. A bridge upgrade that changes any of them would
//! otherwise turn every kafkaman error into a silently unreported one.
//!
//! # Why its own binary
//!
//! It installs a tracer subscriber, which is process-wide. Each observability
//! trace concern lives in one integration test target for that reason.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use kafkaman_core::{problem, record_error, record_exception, record_exception_as, InstrumentDb};
use observability_tests::{TestResult, TracePipeline};
use opentelemetry::Value;
use opentelemetry_sdk::trace::SpanData;

/// A span shaped like the `kafkaman.*` phase spans: both status fields declared,
/// because the bridge writes them through fields rather than through an API.
fn phase_span(name: &'static str) -> tracing::Span {
    tracing::info_span!(
        "phase",
        "otel.name" = name,
        "otel.status_code" = tracing::field::Empty,
        "otel.status_description" = tracing::field::Empty,
    )
}

fn exception_events(span: &SpanData) -> Vec<&opentelemetry::trace::Event> {
    span.events
        .iter()
        .filter(|event| event.name == "exception")
        .collect()
}

fn attr<'a>(event: &'a opentelemetry::trace::Event, key: &str) -> Option<&'a Value> {
    event
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
}

#[test]
fn a_recorded_exception_is_typed_bounded_and_named() -> TestResult {
    let pipeline = TracePipeline::install_at_default_filter();

    // A handler error, an application panic, and a boxed publish error: the three
    // shapes the durable path actually reports.
    let handler = kafkaman_sqlx::Error::Handler("boom".to_owned());
    let panicked = kafkaman_sqlx::Error::HandlerPanicked("unwound".to_owned());
    {
        let span = phase_span("typed");
        record_exception(&span, &handler);
    }
    {
        let span = phase_span("panicked");
        record_exception(&span, &panicked);
    }
    {
        let span = phase_span("boxed");
        record_exception_as(&span, problem::PUBLISH, &"broker refused the record");
    }
    // The status-only helper, which must stay status-only: one failure reaching a
    // backend as several errors is worse than it reaching it as none.
    {
        let span = phase_span("status-only");
        record_error(&span, &handler);
    }
    // An error message can quote the value that violated a constraint, so its
    // length is attacker-influenced wherever it lands.
    {
        let span = phase_span("oversized");
        record_exception(&span, &kafkaman_sqlx::Error::Handler("x".repeat(4096)));
    }

    // A `db.query` span reports its failure as a *status* and never as an event.
    // The rule this pins is one failure, one error document: a red query span
    // nested under a red phase span is one thing that went wrong, and reporting
    // it at both depths would double an operator's error count and their error
    // rate for every failing statement.
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        let failing: std::future::Ready<Result<(), kafkaman_sqlx::Error>> = std::future::ready(
            Err(kafkaman_sqlx::Error::Handler("query failed".to_owned())),
        );
        let _ = runtime.block_on(failing.instrument_db(kafkaman_core::db_span!(
            "SELECT",
            "kafkaman.outbox",
            "read outbox row",
        )));
    }

    let finished = pipeline.finished();
    let named = |name: &str| -> SpanData {
        finished
            .iter()
            .find(|span| span.name == name)
            .unwrap_or_else(|| {
                panic!(
                    "no {name} span among {:?}",
                    finished
                        .iter()
                        .map(|span| span.name.as_ref())
                        .collect::<Vec<_>>()
                )
            })
            .clone()
    };

    for (name, expected_type) in [
        ("typed", problem::HANDLER),
        ("panicked", problem::HANDLER_PANICKED),
        ("boxed", problem::PUBLISH),
    ] {
        let span = named(name);
        let events = exception_events(&span);
        assert_eq!(
            events.len(),
            1,
            "{name} should carry exactly one exception event, found {:?}",
            span.events
                .iter()
                .map(|e| e.name.as_ref())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            attr(events[0], "exception.type").map(Value::to_string),
            Some(expected_type.to_owned()),
            "{name} must name its problem type; the bridge never sets this itself"
        );
        assert!(
            attr(events[0], "exception.message").is_some(),
            "{name} must carry an exception.message, which is what the bridge \
             rewrites the `error` field into"
        );
    }

    // A panic and a returned error are the same `ReceivedFailureKind`, on purpose.
    // The whole reason to classify from the error type is that they need not be
    // the same here.
    assert_ne!(
        attr(exception_events(&named("typed"))[0], "exception.type").map(Value::to_string),
        attr(exception_events(&named("panicked"))[0], "exception.type").map(Value::to_string),
    );

    assert!(
        exception_events(&named("status-only")).is_empty(),
        "record_error reports a status and must not also report an error"
    );

    // The bridge copies the `error` field into the span status as well as into
    // the event, so truncating once before recording is what keeps the two equal
    // and keeps neither unbounded.
    let oversized = named("oversized");
    let events = exception_events(&oversized);
    let message = attr(events[0], "exception.message")
        .map(Value::to_string)
        .expect("exception.message");
    assert!(
        message.len() <= 256,
        "exception.message must be bounded, was {} bytes",
        message.len()
    );
    let opentelemetry::trace::Status::Error { description } = &oversized.status else {
        panic!("the span must be marked failed, was {:?}", oversized.status);
    };
    assert!(
        description.len() <= 256,
        "the status description must stay bounded, was {} bytes",
        description.len()
    );

    let query = named("db.query read outbox row");
    assert!(
        matches!(query.status, opentelemetry::trace::Status::Error { .. }),
        "a failing db.query span must be red; before `instrument_db` these \
         declared their status fields and nothing ever wrote them, so a failing \
         statement exported green under a red parent"
    );
    assert!(
        exception_events(&query).is_empty(),
        "a db.query span reports status only; the exception belongs to the phase \
         span that owns the failure"
    );

    Ok(())
}

/// The two spellings of one target, held equal.
///
/// `kafkaman-otel` excludes this target from OTLP log export, because an
/// exception event is message-less by construction and the log bridge exports it
/// as a record with an empty body — one blank ERROR line for every error already
/// reported as an error. It cannot import the constant: the crate deliberately
/// does not depend on kafkaman, which is what lets a host copy the file and own
/// it. So the drift is caught here, in the one suite that depends on both.
///
/// Drift would not fail anything. The filter would simply stop matching, the
/// blank records would come back, and nothing would say so.
#[test]
fn the_exception_target_matches_kafkaman_core() {
    assert_eq!(
        kafkaman_otel::EXCEPTION_TARGET,
        kafkaman_core::TELEMETRY_TARGET
    );
}
