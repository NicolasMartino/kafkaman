//! The example boot files must not name kafkaman internals.
//!
//! This is the developer-UX claim stated as a test rather than as prose. The
//! runtime builder's whole purpose is that a service author declares roles and
//! never learns that "publishing" means an outbox table plus a relay loop, or
//! that "caching" means a received table *and* a cache table *and* two loops.
//! That property is invisible to the compiler and to every behavioural test —
//! the services work identically whether the wiring is declared or hand-rolled
//! — so nothing but a source-level check can stop it regressing.
//!
//! Deliberately scoped to the two blessed boot files. `service_manual.rs` is
//! the hand-wired escape hatch and is *required* to name these symbols; it is
//! proven equivalent by running the end-to-end suite against it, not by this
//! test.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};

/// Symbols a service author should never have to write.
///
/// Each entry is something the builder derives from a declared role. They are
/// matched as plain substrings, so `OutboxTable` also catches
/// `CreateOutboxTable`; the redundancy is deliberate, because a failure that
/// names the specific symbol is easier to act on.
const FORBIDDEN: &[&str] = &[
    // Changelog construction — derived from roles.
    "CreateOutboxTable",
    "CreateReceivedTable",
    "CreateCacheTable",
    "InitSchema",
    // Table handles — derived from roles.
    "OutboxTable",
    "ReceivedTable",
    // Topic convergence — run by `build()` in the host-configured mode.
    "TopicAdmin",
    "converge_topics",
    // Transport construction — owned by the runtime.
    "RdkafkaConsumer",
    "RdkafkaPublisher",
    // Loop spawning and supervision — owned by the runtime.
    "worker::run",
    "JoinSet",
    // Migration — run by `build()`.
    "migrate(",
];

/// The boot files the builder is supposed to make declarative.
const BLESSED_BOOT_FILES: &[&str] = &["order/src/service.rs", "product/src/service.rs"];

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

fn find_forbidden(source: &str) -> Vec<(&'static str, usize)> {
    source
        .lines()
        .enumerate()
        .flat_map(|(index, line)| {
            FORBIDDEN
                .iter()
                .filter(move |symbol| line.contains(*symbol))
                .map(move |symbol| (*symbol, index + 1))
        })
        .collect()
}

#[test]
fn the_blessed_boot_files_name_no_kafkaman_internals() {
    let mut failures = Vec::new();

    for relative in BLESSED_BOOT_FILES {
        let path = examples_dir().join(relative);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));

        for (symbol, line) in find_forbidden(&source) {
            failures.push(format!("examples/{relative}:{line} names `{symbol}`"));
        }
    }

    assert!(
        failures.is_empty(),
        "the builder is supposed to derive these from declared roles:\n  {}",
        failures.join("\n  ")
    );
}

#[test]
fn the_worker_entry_point_is_no_http_and_explicit_about_subsystems() {
    let relative = "product/src/bin/worker.rs";
    let path = examples_dir().join(relative);
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading {}: {error}", path.display()));

    assert!(
        source.contains("RuntimeBuilder::new"),
        "the worker should use the facade builder rather than low-level loops"
    );
    assert!(
        source.contains(".subsystems(") && source.contains("Subsystems::PIPELINE"),
        "the worker should make its runtime topology explicit"
    );

    for forbidden in [
        "kafkaman::axum",
        "build_router",
        "TcpListener",
        "SocketAddr",
        "BIND_ADDR",
    ] {
        assert!(
            !source.contains(forbidden),
            "examples/{relative} should not bind or assemble HTTP, but it names `{forbidden}`"
        );
    }

    let internals = find_forbidden(&source);
    assert!(
        internals.is_empty(),
        "the worker should stay on the facade surface:\n  {}",
        internals
            .into_iter()
            .map(|(symbol, line)| format!("examples/{relative}:{line} names `{symbol}`"))
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// The escape hatch has to stay exercised, and it cannot be exercised by a file
/// that does not exist.
#[test]
fn the_hand_wired_boot_module_still_exists() {
    let path = examples_dir().join("product/src/service_manual.rs");
    assert!(
        Path::new(&path).exists(),
        "examples/product/src/service_manual.rs is the low-level boot path the \
         end-to-end suite proves equivalent; deleting it would leave the \
         escape hatch documented but untested"
    );
}

#[test]
fn the_forbidden_list_still_matches_the_hand_wired_boot_module() {
    // Guards the test above. A rename that made every pattern stale would leave
    // it passing vacuously, reporting a clean boot surface for a file that could
    // name anything at all.
    //
    // `service_manual.rs` is the right oracle precisely because it is *required*
    // to name these symbols: it is the low-level path, and if it stopped naming
    // them it would no longer be the low-level path.
    let source = std::fs::read_to_string(examples_dir().join("product/src/service_manual.rs"))
        .expect("reading examples/product/src/service_manual.rs");

    let matched: Vec<&str> = find_forbidden(&source)
        .into_iter()
        .map(|(symbol, _)| symbol)
        .collect();

    for expected in [
        "CreateOutboxTable",
        "TopicAdmin",
        "converge_topics",
        "RdkafkaConsumer",
        "worker::run",
        "JoinSet",
        "migrate(",
    ] {
        assert!(
            matched.contains(&expected),
            "`{expected}` no longer matches the hand-wired boot module, so the \
             pattern is stale and the absence test proves less than it claims"
        );
    }
}
