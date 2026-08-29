//! That this crate's failures reach APM under a declared name.
//!
//! Two mechanisms cover this between them, and neither covers it alone. The
//! `match` in `problem_type` is exhaustive, so the compiler refuses a variant
//! nobody classified. What the compiler cannot see is a classification written
//! as a bare string instead of a `problem::*` constant: `"urn:kafkaman:problem:hadler"`
//! compiles, ships, and appears in Kibana as an error group of one that nothing
//! ever joins. That is what the source check below is for.

use kafkaman_core::problem::ALL_PROBLEM_TYPES;
use kafkaman_core::ProblemType;

/// The classification of every error in this crate comes from a declared
/// constant.
///
/// Reading the source is the point rather than a shortcut: the property is
/// "no literal URI exists here", and there is no value to inspect at runtime
/// that would show one. `include_str!` resolves at compile time, so this needs
/// no filesystem at test time and moves with the file if it is renamed.
#[test]
fn no_classification_in_this_crate_invents_a_uri() {
    for (module, source) in [
        ("error.rs", include_str!("../error.rs")),
        ("dispatch.rs", include_str!("../dispatch.rs")),
    ] {
        assert!(
            !source.contains("\"urn:kafkaman:problem:"),
            "{module} contains a literal problem URI. Classifications must go \
             through a `kafkaman_core::problem::*` constant so that they stay in \
             the declared vocabulary and a typo cannot mint a new APM error group."
        );
    }
}

/// A sample across every URI this crate can return, checked against the set.
///
/// Not exhaustive over variants — the compiler already is. This is the check
/// that the constants themselves are declared, which matters because
/// `ALL_PROBLEM_TYPES` is hand-listed and a new constant can be added without
/// being added to it.
#[test]
fn every_uri_this_crate_returns_is_declared() {
    let samples: Vec<crate::Error> = vec![
        crate::Error::Handler("boom".to_owned()),
        crate::Error::HandlerPanicked("unwound".to_owned()),
        crate::Error::MissingHandler("product_snapshot".to_owned()),
        crate::Error::MissingIdempotencyKey,
        crate::Error::MigrationLockBusy,
        crate::Error::ReservedHeader("kafkaman-message-id".to_owned()),
        crate::Error::InvalidReceivedFilter("nope".to_owned()),
        crate::Error::InvalidIngestFailureKind("nope".to_owned()),
        crate::Error::UnknownMessageType("nope".to_owned()),
        crate::Error::ApplicationPanicked {
            message_type: "product_snapshot".to_owned(),
            operation: "enqueue",
            message: "unwound".to_owned(),
        },
        crate::Error::MissingEntityKey {
            message_id: uuid::Uuid::nil(),
            message_type: "product_snapshot".to_owned(),
        },
        // The delegating arm. A wrapped core error must keep its own
        // classification rather than being relabelled by the wrapper, which is
        // the whole reason `Core` is not collapsed to one URI.
        crate::Error::Core(kafkaman_core::Error::InvalidOutboxStatus("nope".to_owned())),
    ];

    for error in &samples {
        let uri = error.problem_type();
        assert!(
            ALL_PROBLEM_TYPES.contains(&uri),
            "{error} classified as {uri}, which is not in ALL_PROBLEM_TYPES"
        );
    }

    assert_eq!(
        crate::Error::Core(kafkaman_core::Error::InvalidOutboxStatus("nope".to_owned()))
            .problem_type(),
        kafkaman_core::problem::SCHEMA,
        "a transparently wrapped core error keeps its own class; `#[error(transparent)]` \
         already means the caller sees only the inner message, and the classification \
         should travel the same way"
    );
}

/// A database error returned by a handler is infrastructure in both places.
///
/// The regression test for the defect the taxonomy/blame decision exists for.
/// Measured before the fix:
///
/// ```text
/// Error::Sqlx(PoolClosed)   APM  exception.type    = urn:kafkaman:problem:infrastructure
///                           DLQ  latest_error.type = urn:kafkaman:problem:handler
/// ```
///
/// One failure, one URI namespace, two values — so an operator filtering the
/// dead-letter queue for infrastructure failures found none of the four hundred
/// rows a pool exhaustion had just parked there.
#[test]
fn a_database_error_is_infrastructure_whichever_frame_returned_it() {
    use crate::dispatch_failure::failure_disposition;
    use kafkaman_core::{FailureStage, ReceivedFailureKind};

    let error = crate::Error::Sqlx(sqlx::Error::PoolClosed);

    for stage in FailureStage::ALL {
        let disposition = failure_disposition(&error, stage);
        assert_eq!(
            disposition.kind,
            ReceivedFailureKind::Infrastructure,
            "the frame a database error surfaced in must not change what kind \
             of failure it is; {stage:?} said otherwise"
        );
        assert_eq!(
            disposition.kind.problem_type(),
            error.problem_type(),
            "the row and the span must name one class"
        );
        assert_eq!(
            disposition.stage, stage,
            "the frame is still recorded — it moved to its own field rather \
             than being dropped"
        );
    }
}

/// What the handler frame still changes, and what it no longer does.
#[test]
fn the_stage_records_blame_without_rewriting_the_class() {
    use crate::dispatch_failure::failure_disposition;
    use kafkaman_core::{FailureStage, ReceivedFailureKind};

    // A handler's own error is still a handler failure. Nothing about that was
    // wrong; the catch-all around it was.
    let own = failure_disposition(
        &crate::Error::Handler("boom".to_owned()),
        FailureStage::Handler,
    );
    assert_eq!(own.kind, ReceivedFailureKind::Handler);

    // A panic is too, and dead-letters the same way, while keeping its own APM
    // group through the finer vocabulary.
    let panicked = failure_disposition(
        &crate::Error::HandlerPanicked("unwound".to_owned()),
        FailureStage::Handler,
    );
    assert_eq!(panicked.kind, ReceivedFailureKind::Handler);
    assert_eq!(
        crate::Error::HandlerPanicked("unwound".to_owned()).problem_type(),
        kafkaman_core::problem::HANDLER_PANICKED
    );

    // And an undecodable payload keeps the class it always kept — this was the
    // one case the old catch-all had already been patched by hand to preserve.
    let serde_error = crate::Error::Serde(serde_json::from_str::<u8>("[]").unwrap_err());
    assert_eq!(
        failure_disposition(&serde_error, FailureStage::Handler).kind,
        ReceivedFailureKind::InvalidPayload
    );
}

/// A database error is classified by what the database refused.
///
/// Driven through a stub `DatabaseError` rather than a live Postgres, because
/// what is under test is the SQLSTATE mapping and the fallbacks around it — and
/// several of these classes are hard to provoke on demand (a serialization
/// failure needs two concurrent transactions racing). The real ones are covered
/// end to end by `tests/durable-send`.
mod sql_classification {
    use kafkaman_core::{problem, ProblemType as _};

    /// The smallest thing sqlx will accept as a database error.
    #[derive(Debug)]
    struct Sqlstate(&'static str);

    impl std::fmt::Display for Sqlstate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "SQLSTATE {}", self.0)
        }
    }

    impl std::error::Error for Sqlstate {}

    impl sqlx::error::DatabaseError for Sqlstate {
        fn message(&self) -> &str {
            "stub"
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(std::borrow::Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        /// `Other` for every code, so these cases exercise the SQLSTATE path
        /// rather than the `kind()` shortcut. The shortcut is what a real
        /// `PgDatabaseError` takes for the four constraint kinds, and it is
        /// covered by the live constraint-violation test in `tests/durable-send`.
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn classify(sqlstate: &'static str) -> &'static str {
        crate::Error::Sqlx(sqlx::Error::Database(Box::new(Sqlstate(sqlstate)))).problem_type()
    }

    #[test]
    fn a_refused_write_is_not_an_environment_failure() {
        // 23505 unique_violation, 23503 foreign_key_violation, 23P01 exclusion.
        // Nothing is broken; the write was refused.
        for code in ["23505", "23503", "23502", "23P01"] {
            assert_eq!(classify(code), problem::CONSTRAINT, "{code}");
        }
    }

    #[test]
    fn contention_groups_apart_from_an_outage() {
        // 40001 serialization_failure, 40P01 deadlock_detected. Expected under
        // load, and the retry is the correct response — which is the opposite
        // reading from a connection failure charted beside it.
        for code in ["40001", "40P01"] {
            assert_eq!(classify(code), problem::CONTENTION, "{code}");
        }
    }

    #[test]
    fn a_statement_that_cannot_run_is_its_own_class() {
        // 42P01 undefined_table, 42703 undefined_column, 42501
        // insufficient_privilege, 22003 numeric_value_out_of_range.
        for code in ["42P01", "42703", "42501", "22003", "22P02"] {
            assert_eq!(classify(code), problem::STATEMENT, "{code}");
        }
    }

    #[test]
    fn the_environment_keeps_the_rest() {
        // 08006 connection_failure, 53300 too_many_connections, 57014
        // query_canceled, 58030 io_error — and anything this version does not
        // recognise, which must not be attributed to the statement.
        for code in ["08006", "53300", "57014", "58030", "XX000", "1"] {
            assert_eq!(classify(code), problem::INFRASTRUCTURE, "{code}");
        }
    }

    #[test]
    fn a_failure_with_no_statement_behind_it_is_infrastructure() {
        // These never reached the server, so there is no SQLSTATE and nothing
        // to classify but the connection.
        for error in [sqlx::Error::PoolClosed, sqlx::Error::PoolTimedOut] {
            assert_eq!(
                crate::Error::Sqlx(error).problem_type(),
                problem::INFRASTRUCTURE
            );
        }
    }

    /// All three still store as `Infrastructure`.
    ///
    /// The refinement is a telemetry one. Adding a fifth stored kind is a
    /// stored-vocabulary change, and none of the existing four fits a
    /// constraint violation better than `Infrastructure` does.
    #[test]
    fn the_refinement_does_not_reach_the_stored_class() {
        use kafkaman_core::ReceivedFailureKind;
        for code in ["23505", "40001", "42P01", "08006"] {
            assert_eq!(
                ReceivedFailureKind::coarsening(classify(code)),
                ReceivedFailureKind::Infrastructure,
                "{code}"
            );
        }
    }
}
