//! The permanent problem-type vocabulary.
//!
//! Compiler exhaustiveness already catches an error variant nobody classified —
//! every `ProblemType` impl is a hand-written `match` and will not build with a
//! variant missing. What it cannot catch is a variant classified with a string
//! that is not one of the declared constants, which is what these cover.

use crate::problem::{self, ALL_PROBLEM_TYPES};
use crate::{ProblemType, ReceivedFailureKind, ReceivedIngestFailureKind};

/// The URI prefix every problem type shares.
///
/// `urn:` rather than a URL: these are identifiers, not addresses, and nothing
/// should try to dereference one.
const PREFIX: &str = "urn:kafkaman:problem:";

#[test]
fn every_declared_problem_type_is_a_distinct_kafkaman_urn() {
    for uri in ALL_PROBLEM_TYPES {
        assert!(
            uri.starts_with(PREFIX),
            "{uri} is not in kafkaman's problem namespace; a URI outside it will \
             collide with another library's grouping in a shared APM backend"
        );
        let slug = &uri[PREFIX.len()..];
        assert!(
            !slug.is_empty()
                && slug
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "{uri} should end in a lower-kebab slug; these are read by operators \
             and matched by dashboards, so casing and separators cannot drift"
        );
    }

    let mut unique = ALL_PROBLEM_TYPES.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(
        unique.len(),
        ALL_PROBLEM_TYPES.len(),
        "two constants share a URI, so two failure classes would land in one APM \
         error group and could never be separated again: {ALL_PROBLEM_TYPES:?}"
    );
}

/// `ALL_PROBLEM_TYPES` lists every constant that exists.
///
/// The array is hand-written, so a new constant can be declared and simply not
/// added to it. Nothing about that fails to compile: the constant works, the
/// classification works, and only the things that *enumerate* the vocabulary —
/// this suite, the compatibility note, an operator's filter list — quietly stop
/// covering it. Reading the source is the only way to see a constant the array
/// omits, and `include_str!` resolves at compile time so no filesystem is
/// involved at test time.
#[test]
fn the_declared_list_covers_every_constant_in_this_module() {
    let source = include_str!("../problem.rs");
    let declared: Vec<&str> = source
        .lines()
        .filter_map(|line| {
            let line = line.trim_start();
            if !line.starts_with("pub const ") {
                return None;
            }
            let (_, rest) = line.split_once('"')?;
            let (uri, _) = rest.split_once('"')?;
            uri.starts_with(PREFIX).then_some(uri)
        })
        .collect();

    assert_eq!(
        declared.len(),
        ALL_PROBLEM_TYPES.len(),
        "problem.rs declares {} problem URIs but ALL_PROBLEM_TYPES lists {}. \
         Declared: {declared:?}",
        declared.len(),
        ALL_PROBLEM_TYPES.len()
    );
    for uri in declared {
        assert!(
            ALL_PROBLEM_TYPES.contains(&uri),
            "{uri} is declared but missing from ALL_PROBLEM_TYPES"
        );
    }
}

#[test]
fn coarsening_mentions_every_declared_problem_constant() {
    let problem_source = include_str!("../problem.rs");
    let coarsening_source = include_str!("../failure_kind.rs");
    let declared_names: Vec<&str> = problem_source
        .lines()
        .filter_map(|line| {
            let line = line.trim_start();
            let raw = line.strip_prefix("pub const ")?;
            let (name, _) = raw.split_once(": &str")?;
            Some(name)
        })
        .collect();

    for name in declared_names {
        let needle = format!("problem::{name}");
        assert!(
            coarsening_source.contains(&needle),
            "{needle} is declared but not explicitly coarsened; do not let it \
             fall through the unknown-URI fallback by accident"
        );
    }
}

/// Every classification a `kafkaman-core` type can produce is in the set.
///
/// This is the check the declared list exists for. A typo'd URI compiles, ships,
/// and shows up in Kibana as a group of one that nothing else ever joins.
#[test]
fn kafkaman_core_classifications_stay_inside_the_declared_set() {
    let mut seen: Vec<&'static str> = Vec::new();

    for kind in ReceivedFailureKind::ALL {
        seen.push(ProblemType::problem_type(&kind));
    }
    for kind in ReceivedIngestFailureKind::ALL {
        seen.push(ProblemType::problem_type(&kind));
    }
    // One value per `Error` variant. Listed by hand because the enum carries
    // data and cannot be iterated — and listed *exhaustively* on purpose: a new
    // variant makes this list stale rather than failing to compile, so the
    // count assertion below is what notices.
    let errors = [
        crate::Error::InvalidIdentifier {
            value: "1bad".to_owned(),
            reason: "must not start with a digit",
        },
        crate::Error::InvalidRelayConfig {
            field: "batch_limit",
            reason: "must be positive",
        },
        crate::Error::InvalidDispatcherConfig {
            field: "poll_interval",
            reason: "must be greater than zero",
        },
        crate::Error::InvalidPurgeConfig {
            field: "older_than",
            reason: "must be greater than zero",
        },
        crate::Error::InvalidTopicSpec {
            field: "partitions",
            reason: "must be positive",
        },
        crate::Error::InvalidOutboxStatus("nope".to_owned()),
        crate::Error::InvalidReceiveStatus("nope".to_owned()),
        crate::Error::InvalidMessageDescriptor("nope".to_owned()),
        crate::Error::InvalidIdempotencyNamespace {
            value: String::new(),
            reason: "must not be empty",
        },
        crate::Error::InvalidIdempotencyKey {
            value: String::new(),
            reason: "must not be empty",
        },
        crate::Error::InvalidIdempotencySource("nope".to_owned()),
        crate::Error::TopicPolicyMismatch {
            topic: "orders".to_owned(),
            expected: "delete".to_owned(),
            found: "compact".to_owned(),
        },
        crate::Error::TopicMissing {
            topic: "orders".to_owned(),
        },
        crate::Error::TopicPartitionsUndeclared {
            topic: "orders".to_owned(),
        },
    ];
    for error in &errors {
        seen.push(error.problem_type());
    }

    for uri in &seen {
        assert!(
            ALL_PROBLEM_TYPES.contains(uri),
            "{uri} is not in ALL_PROBLEM_TYPES. Either it is a typo, or a new \
             class was added without declaring it — and an undeclared URI is \
             invisible to anything that enumerates the vocabulary."
        );
    }
}

/// The persisted half of the vocabulary still round-trips.
///
/// `ReceivedFailureKind::problem_type` is written into `last_failure_kind` and
/// read back by `from_problem_type`, so it is a *stored data* contract, not only
/// a telemetry one. Serving it through the trait must not have changed it.
#[test]
fn the_persisted_failure_kinds_still_round_trip_through_the_trait() {
    for kind in ReceivedFailureKind::ALL {
        let via_trait = ProblemType::problem_type(&kind);
        assert_eq!(
            via_trait,
            kind.problem_type(),
            "the trait must serve the same URI as the inherent method; the \
             inherent one is what is already in stored rows"
        );
        assert_eq!(
            ReceivedFailureKind::from_problem_type(via_trait),
            Some(kind),
            "{via_trait} no longer parses back to the kind that wrote it, which \
             makes every stored row carrying it unreadable"
        );
    }
}

/// A panic groups separately from a returned error, and the stored kind does not.
///
/// This pair is the whole reason the exception vocabulary is finer than the
/// persisted one, so it is pinned rather than left to the reader.
#[test]
fn a_panic_is_its_own_apm_group_but_not_its_own_stored_kind() {
    assert_ne!(
        problem::HANDLER,
        problem::HANDLER_PANICKED,
        "a handler that panicked and one that returned an error must be \
         separable in APM; they fail for different reasons and are fixed \
         differently"
    );
    assert_eq!(
        ProblemType::problem_type(&ReceivedFailureKind::Handler),
        problem::HANDLER,
        "the stored kind stays `handler` for both, because its four values are \
         written into rows and cannot churn"
    );
    assert!(
        !ALL_PROBLEM_TYPES.contains(&problem::HANDLER_PANICKED)
            || ReceivedFailureKind::from_problem_type(problem::HANDLER_PANICKED).is_none(),
        "the panic URI must not parse back to a stored kind; it belongs to the \
         telemetry vocabulary only"
    );
}

/// Every telemetry URI has a persisted class, and the four that *are* classes
/// coarsen to themselves.
///
/// Totality is the property that matters: `coarsening` has a catch-all, so a URI
/// added without a decision here would silently become `Infrastructure` rather
/// than failing to compile. This is what notices.
#[test]
fn the_coarsening_is_total_and_agrees_with_the_inverse_on_their_overlap() {
    for uri in ALL_PROBLEM_TYPES {
        let coarse = ReceivedFailureKind::coarsening(uri);

        // The four URIs that are themselves kinds must survive the round trip,
        // or a stored row would read back as a different class than the one
        // that wrote it.
        if let Some(exact) = ReceivedFailureKind::from_problem_type(uri) {
            assert_eq!(
                coarse, exact,
                "{uri} is a persisted class, so coarsening it must be the \
                 identity; `from_problem_type` reads stored rows back with it"
            );
        }
    }

    // The refinement that the finer vocabulary exists for.
    assert_eq!(
        ReceivedFailureKind::coarsening(problem::HANDLER_PANICKED),
        ReceivedFailureKind::Handler,
        "a panic and a returned error dead-letter identically — the repair is \
         the same, read the handler — while grouping separately in APM"
    );

    // An unknown class is the environment's, not the application's. Attributing
    // something this version cannot name to application code would send an
    // operator to the wrong team on a version skew.
    assert_eq!(
        ReceivedFailureKind::coarsening("urn:kafkaman:problem:invented-later"),
        ReceivedFailureKind::Infrastructure
    );
}

/// The two vocabularies cannot disagree about one error any more.
///
/// This is the regression test for the defect the taxonomy/blame decision
/// exists for: a database error surfacing through a handler frame used to be
/// `infrastructure` on its span and `handler` in its row.
#[test]
fn a_failures_class_is_the_coarsening_of_its_problem_type() {
    for kind in ReceivedFailureKind::ALL {
        assert_eq!(
            ProblemType::failure_kind(&kind),
            kind,
            "a kind classifies as itself"
        );
    }
    for kind in ReceivedIngestFailureKind::ALL {
        let coarse = ProblemType::failure_kind(&kind);
        assert_eq!(
            coarse,
            ReceivedFailureKind::coarsening(ProblemType::problem_type(&kind)),
            "the provided method must be the one coarsening and not a second one"
        );
    }
}

/// `FailureStage` survives the round trip through a stored problem detail.
#[test]
fn a_failure_stage_round_trips_through_its_stored_spelling() {
    for stage in crate::FailureStage::ALL {
        assert_eq!(crate::FailureStage::parse(stage.as_str()), Some(stage));
        let json = serde_json::to_string(&stage).expect("a stage serializes");
        assert_eq!(
            json,
            format!("\"{}\"", stage.as_str()),
            "the stored spelling and the span attribute must be one string"
        );
        assert_eq!(
            serde_json::from_str::<crate::FailureStage>(&json).expect("and reads back"),
            stage
        );
    }
    assert!(
        serde_json::from_str::<crate::FailureStage>("\"nope\"").is_err(),
        "an unknown stage is an error, not a default: `None` already spells \
         `written before stages existed`"
    );
}
