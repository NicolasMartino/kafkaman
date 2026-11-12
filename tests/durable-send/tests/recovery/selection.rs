//! Choosing what a run does, before any I/O (T12).
use super::*;

use kafkaman::recovery::{Failure, Operation};

/// `extra` goes first, so a top-level key stays top-level.
fn file(extra: &str) -> Config {
    Config::parse(&format!("{extra}[database]\nschema = \"kafkaman\"\n")).unwrap()
}

fn refused(recovery: &Recovery, config: &Config, inputs: RecoveryInputs) -> RecoveryError {
    recovery.request_from(config, inputs).unwrap_err()
}

fn redrive(source: Option<&str>, max_rows: Option<&str>) -> RecoveryInputs {
    RecoveryInputs {
        operation: Some("redrive".to_owned()),
        source: source.map(str::to_owned),
        max_rows: max_rows.map(str::to_owned),
        ..RecoveryInputs::default()
    }
}

/// A pool that fails on first use: any I/O before a refusal shows up as a
/// connection error instead of the refusal.
fn sentinel_pool() -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://nobody@127.0.0.1:1/none")
        .unwrap()
}

#[test]
fn the_declared_list_is_checked_when_it_is_built() {
    assert!(matches!(
        Recovery::new(Vec::new()),
        Err(RecoveryError::NoSources)
    ));
    assert!(matches!(
        Recovery::new(vec![product_source("  ")]),
        Err(RecoveryError::BlankSourceName)
    ));
    assert!(matches!(
        Recovery::new(vec![product_source("a"), regional_source("a")]),
        Err(RecoveryError::DuplicateSource(name)) if name == "a"
    ));
    // One received table, two names: the second claims a handler already
    // declared, which the role registry refuses.
    assert!(matches!(
        Recovery::new(vec![
            Source::received::<ProductSnapshot>("handled"),
            Source::received::<ProductSnapshot>("handled-again"),
        ]),
        Err(RecoveryError::InvalidSource { source_name, .. }) if source_name == "handled-again"
    ));
    // A resync and a redrive of the same type are a publisher that also
    // handles it, which is allowed.
    assert!(Recovery::new(vec![
        product_source("snapshots"),
        Source::received::<ProductSnapshot>("handled"),
    ])
    .is_ok());
}

#[test]
fn an_unset_source_resyncs_every_resync_source_in_order() {
    let recovery = mixed();
    let request = recovery
        .request_from(&file(""), RecoveryInputs::default())
        .unwrap();
    assert_eq!(request.operation(), Operation::Resync);
    assert_eq!(request.start_after(), None);

    let named = recovery
        .request_from(&file(""), resync_inputs(Some("regional"), None))
        .unwrap();
    assert_eq!(named.operation(), Operation::Resync);
}

#[test]
fn a_cursor_needs_exactly_one_named_source() {
    // DIX 2: one cursor cannot resume a list, even a list of one, so a resume
    // always says which source it continues.
    let recovery = mixed();
    assert!(matches!(
        refused(&recovery, &file(""), resync_inputs(None, Some("p-02"))),
        RecoveryError::StartAfterNeedsSource
    ));
    let single = Recovery::new(vec![product_source("products")]).unwrap();
    assert!(matches!(
        refused(&single, &file(""), resync_inputs(None, Some("p-02"))),
        RecoveryError::StartAfterNeedsSource
    ));

    let request = recovery
        .request_from(&file(""), resync_inputs(Some("products"), Some("p-02")))
        .unwrap();
    assert_eq!(request.start_after(), Some("p-02"));

    // A blank cursor is no cursor, so it needs no source.
    let request = recovery
        .request_from(&file(""), resync_inputs(None, Some("  ")))
        .unwrap();
    assert_eq!(request.start_after(), None);
}

#[tokio::test]
async fn a_cursor_without_a_source_is_refused_before_any_database_work() {
    let mut recovery = mixed();
    let pool = sentinel_pool();
    let error = match recovery.request_from(&file(""), resync_inputs(None, Some("p-02"))) {
        Ok(request) => recovery
            .run(
                &pool,
                &file(""),
                Some("127.0.0.1:1"),
                &MigrationContext::default(),
                request,
            )
            .await
            .unwrap_err(),
        Err(error) => error,
    };
    assert!(
        matches!(error, RecoveryError::StartAfterNeedsSource),
        "{error}"
    );
}

#[test]
fn names_operations_and_kinds_are_exact() {
    let recovery = mixed();
    let config = file("");
    let unknown = RecoveryInputs {
        operation: Some("Resync".to_owned()),
        ..RecoveryInputs::default()
    };
    assert!(matches!(
        refused(&recovery, &config, unknown),
        RecoveryError::UnknownOperation(op) if op == "Resync"
    ));
    assert!(matches!(
        refused(&recovery, &config, resync_inputs(Some(" "), None)),
        RecoveryError::BlankSource
    ));
    match refused(&recovery, &config, resync_inputs(Some("product"), None)) {
        RecoveryError::UnknownSource { name, valid } => {
            assert_eq!(name, "product");
            assert_eq!(valid, ["products", "keyless", "regional"]);
        }
        other => panic!("{other}"),
    }
    assert!(matches!(
        refused(&recovery, &config, resync_inputs(Some("keyless"), None)),
        RecoveryError::IncompatibleSource {
            operation: Operation::Resync,
            ..
        }
    ));
    assert!(matches!(
        refused(&recovery, &config, redrive(Some("products"), Some("5"))),
        RecoveryError::IncompatibleSource {
            operation: Operation::Redrive,
            ..
        }
    ));
    let received_only =
        Recovery::new(vec![Source::received::<ProductSnapshot>("handled")]).unwrap();
    assert!(matches!(
        refused(&received_only, &config, RecoveryInputs::default()),
        RecoveryError::NoResyncSources
    ));
}

#[test]
fn a_redrive_names_its_source_and_a_bounded_row_count() {
    let recovery = mixed();
    let config = file("");
    assert!(matches!(
        refused(&recovery, &config, redrive(None, Some("5"))),
        RecoveryError::RedriveNeedsSource
    ));
    assert!(matches!(
        refused(&recovery, &config, redrive(Some("keyless"), None)),
        RecoveryError::MissingMaxRows
    ));
    assert!(matches!(
        refused(&recovery, &config, redrive(Some("keyless"), Some(" "))),
        RecoveryError::MissingMaxRows
    ));
    for bad in ["0", "-1", "10001", "1.5", "lots", "99999999999999999999"] {
        assert!(
            matches!(
                refused(&recovery, &config, redrive(Some("keyless"), Some(bad))),
                RecoveryError::InvalidMaxRows {
                    ceiling: 10_000,
                    ..
                }
            ),
            "{bad}"
        );
    }
    for (raw, rows) in [("1", 1), (" 7 ", 7), ("10000", 10_000)] {
        let request = recovery
            .request_from(&config, redrive(Some("keyless"), Some(raw)))
            .unwrap();
        assert_eq!(request.operation(), Operation::Redrive);
        assert_eq!(request.max_rows(), rows);
    }

    // A lower ceiling in the file bounds the run too.
    let lower = file("[recovery]\nmax_redrive_rows = 50\n");
    assert!(matches!(
        refused(&recovery, &lower, redrive(Some("keyless"), Some("51"))),
        RecoveryError::InvalidMaxRows { ceiling: 50, .. }
    ));
    assert_eq!(
        recovery
            .request_from(&lower, redrive(Some("keyless"), Some("50")))
            .unwrap()
            .max_rows(),
        50
    );

    // A row count means nothing to a resync, and a cursor inherited from a
    // resync environment means nothing to a redrive.
    let with_rows = RecoveryInputs {
        max_rows: Some("5".to_owned()),
        ..RecoveryInputs::default()
    };
    assert!(matches!(
        refused(&recovery, &config, with_rows),
        RecoveryError::MaxRowsOnResync
    ));
    let inherited = RecoveryInputs {
        start_after: Some("p-02".to_owned()),
        ..redrive(Some("keyless"), Some("5"))
    };
    assert_eq!(
        recovery
            .request_from(&config, inherited)
            .unwrap()
            .start_after(),
        None
    );
}

#[test]
fn pacing_comes_from_the_file() {
    for (batch_size, pause_ms) in [("7", "900"), ("0", "-1"), ("lots", "soon")] {
        let _legacy = LegacyPacing::set(batch_size, pause_ms);
        file_pacing_is_used();
    }
}

fn file_pacing_is_used() {
    let recovery = mixed();
    let defaults = recovery
        .request_from(&file(""), RecoveryInputs::default())
        .unwrap();
    assert_eq!(defaults.batch_size(), 100);
    assert_eq!(defaults.pause(), std::time::Duration::ZERO);

    let partial = recovery
        .request_from(
            &file("[recovery]\npause_ms = 100\n"),
            RecoveryInputs::default(),
        )
        .unwrap();
    assert_eq!(partial.batch_size(), 100);
    assert_eq!(partial.pause(), std::time::Duration::from_millis(100));

    let explicit = recovery
        .request_from(
            &file("[recovery]\nbatch_size = 25\npause_ms = 200\n"),
            RecoveryInputs::default(),
        )
        .unwrap();
    assert_eq!(explicit.batch_size(), 25);
    assert_eq!(explicit.pause(), std::time::Duration::from_millis(200));
}

#[test]
fn an_invalid_recovery_section_refuses_every_run() {
    let recovery = mixed();
    for section in [
        "[recovery]\nbatch_size = 0\n",
        "[recovery]\npause_ms = -1\n",
        "[recovery]\nmax_redrive_rows = 0\n",
        "[recovery]\nmax_redrive_rows = 10001\n",
        "[recovery]\nbatch_size = \"100\"\n",
        "[recovery]\nbatchsize = 100\n",
        "recovery = 3\n",
    ] {
        assert!(
            matches!(
                refused(&recovery, &file(section), RecoveryInputs::default()),
                RecoveryError::Config(_)
            ),
            "{section}"
        );
        assert!(
            matches!(
                refused(
                    &recovery,
                    &file(section),
                    redrive(Some("keyless"), Some("1"))
                ),
                RecoveryError::Config(_)
            ),
            "{section}"
        );
    }
}

#[tokio::test]
async fn a_resync_without_brokers_is_refused_before_any_database_work() {
    let mut recovery = mixed();
    let pool = sentinel_pool();
    for brokers in [None, Some(""), Some("  ")] {
        let request = recovery
            .request_from(&file(""), RecoveryInputs::default())
            .unwrap();
        let error = recovery
            .run(
                &pool,
                &file(""),
                brokers,
                &MigrationContext::default(),
                request,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, RecoveryError::MissingBrokers), "{error}");
    }
}

/// A request names its sources, so run on another list it runs what those
/// names mean there, or is refused before any I/O.
#[tokio::test]
async fn a_request_runs_the_sources_it_names_on_the_list_it_is_given() {
    let pool = sentinel_pool();
    let config = file("");
    let declared = Recovery::new(vec![
        Source::received::<ProductSnapshot>("a"),
        Source::received::<RegionalProduct>("b"),
    ])
    .unwrap();
    let request = declared
        .request_from(&config, redrive(Some("b"), Some("5")))
        .unwrap();
    let run = |mut recovery: Recovery| {
        let (pool, request) = (pool.clone(), request.clone());
        async move {
            let config = file(RELAY);
            recovery
                .run(&pool, &config, None, &MigrationContext::default(), request)
                .await
                .unwrap_err()
        }
    };

    // Reordered: `b` still runs, and reaches the pool.
    let reordered = Recovery::new(vec![
        Source::received::<RegionalProduct>("b"),
        Source::received::<ProductSnapshot>("a"),
    ])
    .unwrap();
    match run(reordered).await {
        RecoveryError::Failed {
            source_name, cause, ..
        } => {
            assert_eq!(source_name, "b");
            assert!(matches!(*cause, Failure::Redrive(_)), "{cause}");
        }
        other => panic!("{other}"),
    }

    let shorter = Recovery::new(vec![Source::received::<ProductSnapshot>("a")]).unwrap();
    match run(shorter).await {
        RecoveryError::UnknownSource { name, valid } => {
            assert_eq!(name, "b");
            assert_eq!(valid, ["a"]);
        }
        other => panic!("{other}"),
    }

    let resync_only = Recovery::new(vec![product_source("a"), regional_source("b")]).unwrap();
    assert!(matches!(
        run(resync_only).await,
        RecoveryError::IncompatibleSource {
            source_name,
            operation: Operation::Redrive,
        } if source_name == "b"
    ));
}

/// A file override for a type no source declares is still refused, naming
/// the first selected source, before any I/O.
#[tokio::test]
async fn an_override_for_an_undeclared_type_is_refused() {
    let mut recovery = mixed();
    let pool = sentinel_pool();
    let config = file(&format!(
        "{RETRY_DEFAULTS}[retry.messages.order_snapshot]\nmax_attempts = 3\n{RELAY}"
    ));
    let request = recovery
        .request_from(&config, RecoveryInputs::default())
        .unwrap();
    let error = recovery
        .run(
            &pool,
            &config,
            Some("127.0.0.1:1"),
            &MigrationContext::default(),
            request,
        )
        .await
        .unwrap_err();
    match error {
        RecoveryError::Failed {
            source_name, cause, ..
        } => {
            assert_eq!(source_name, "products");
            assert!(matches!(*cause, Failure::Resolve(_)), "{cause}");
            assert!(cause.to_string().contains("order_snapshot"), "{cause}");
        }
        other => panic!("{other}"),
    }
}
