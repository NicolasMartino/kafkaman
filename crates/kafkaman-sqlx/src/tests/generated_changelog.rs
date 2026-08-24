use kafkaman_core::MessageDescriptor;

use crate::generated_changelog::{build_changelog, build_changelog_with_forced_band};
use crate::{
    band, band_of, changeset_version, Changeset, Error, TableKind, BAND_WIDTH, RESERVED_CEILING,
};

fn entity(message_type: &str, topic: &str) -> MessageDescriptor {
    MessageDescriptor::new(message_type, topic).unwrap()
}

fn order() -> MessageDescriptor {
    entity("order_snapshot", "orders")
}

fn product() -> MessageDescriptor {
    entity("product_snapshot", "products")
}

fn names(changesets: &[Box<dyn Changeset>]) -> Vec<&str> {
    changesets.iter().map(|c| c.name()).collect()
}

fn versions(changesets: &[Box<dyn Changeset>]) -> Vec<i64> {
    changesets.iter().map(|c| c.version()).collect()
}

/// The order service's tables: it publishes orders and consumes products.
fn order_service_tables() -> Vec<(TableKind, MessageDescriptor)> {
    vec![
        (TableKind::Outbox, order()),
        (TableKind::Received, product()),
        (TableKind::Cache, product()),
    ]
}

// ---------------------------------------------------------------------------
// Band allocation
// ---------------------------------------------------------------------------

/// The numbers become primary keys in a user's `changelog_history`, so they are
/// pinned against an independent SHA-256 computation rather than against
/// whatever the implementation happens to return.
///
/// A failure here means generated changesets have been renumbered. That is a
/// breaking migration change, not a test to update.
#[test]
fn generated_versions_are_pinned_to_a_reproducible_digest() {
    assert_eq!(
        changeset_version(TableKind::Outbox, "order_snapshot", 0),
        292_728_960_218_088
    );
    assert_eq!(
        changeset_version(TableKind::Received, "order_snapshot", 0),
        1_047_848_963_948_520
    );
    assert_eq!(
        changeset_version(TableKind::Cache, "order_snapshot", 0),
        444_374_315_277_288
    );
    assert_eq!(
        changeset_version(TableKind::Outbox, "product_snapshot", 0),
        501_720_859_833_320
    );
    assert_eq!(
        changeset_version(TableKind::Received, "product_snapshot", 0),
        1_038_071_105_556_456
    );
    assert_eq!(
        changeset_version(TableKind::Cache, "product_snapshot", 0),
        734_279_107_563_496
    );
}

/// `InitSchema` is hardcoded to version 1 and has to keep sorting first.
#[test]
fn no_generated_version_reaches_the_reserved_range() {
    for kind in TableKind::ALL {
        for message_type in ["a", "order_snapshot", "z_very_long_message_type_name"] {
            let version = changeset_version(kind, message_type, 0);
            assert!(
                version >= RESERVED_CEILING,
                "{kind:?}/{message_type} landed at {version}, inside the reserved range"
            );
        }
    }
}

/// The widest version a band can produce must still fit the `BIGINT` primary key
/// with room to spare.
#[test]
fn the_widest_band_stays_far_below_the_bigint_ceiling() {
    let widest = RESERVED_CEILING + ((1_i64 << 40) - 1) * BAND_WIDTH + (BAND_WIDTH - 1);
    assert!(widest < (1_i64 << 51), "{widest} is too wide");
    assert!(widest < i64::MAX / 4);
}

/// Three tables for one message type must not share a band, or the second would
/// overwrite the first's slot space.
#[test]
fn the_three_table_kinds_of_one_message_type_occupy_distinct_bands() {
    let bands: Vec<i64> = TableKind::ALL
        .iter()
        .map(|kind| band(*kind, "order_snapshot"))
        .collect();
    let mut unique = bands.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(bands.len(), unique.len(), "bands collided: {bands:?}");
}

#[test]
fn band_of_inverts_the_version_computation() {
    for kind in TableKind::ALL {
        for slot in [0, 1, 7, BAND_WIDTH - 1] {
            let version = changeset_version(kind, "order_snapshot", slot);
            assert_eq!(band_of(version), band(kind, "order_snapshot"));
        }
    }
}

// ---------------------------------------------------------------------------
// Template slots
// ---------------------------------------------------------------------------

/// Every shipped template is at slot 0. This is not decoration: the pinned
/// versions above are slot-0 versions, and `upgrades()` returns nothing, so a
/// bump without a matching upgrade changeset would silently generate a table's
/// create at a version no database has ever seen.
#[test]
fn every_shipped_template_is_still_at_slot_zero() {
    for kind in TableKind::ALL {
        assert_eq!(
            kind.template_version(),
            0,
            "{kind:?} was bumped; add its upgrade changeset to \
             `generated_changelog::upgrades` and extend these tests"
        );
    }
}

/// A bump has to sort after that table's create and leave every other identity
/// untouched. Asserted arithmetically because that is where the property lives —
/// the slot is a summand inside one band, so it cannot reach another band.
#[test]
fn a_template_bump_sorts_after_its_create_and_disturbs_nothing_else() {
    let create = changeset_version(TableKind::Outbox, "order_snapshot", 0);
    let upgrade = changeset_version(TableKind::Outbox, "order_snapshot", 1);
    assert!(upgrade > create);
    assert_eq!(band_of(upgrade), band_of(create));

    // Every other generated identity is byte-identical after the bump.
    for kind in TableKind::ALL {
        for message_type in ["product_snapshot", "other_snapshot"] {
            if (kind, message_type) == (TableKind::Outbox, "order_snapshot") {
                continue;
            }
            assert_ne!(band_of(upgrade), band(kind, message_type));
        }
    }

    // And a bump can never overrun into the next band.
    let last_slot = changeset_version(TableKind::Outbox, "order_snapshot", BAND_WIDTH - 1);
    assert_eq!(band_of(last_slot), band_of(create));
}

// ---------------------------------------------------------------------------
// Changelog generation
// ---------------------------------------------------------------------------

/// The semantic set the hand-written `examples/order/src/changelog.rs` declares:
/// `InitSchema`, the order outbox, the product received table, the product cache
/// table. The generated changelog must contain exactly that, differing only in
/// the version integers nobody should have been choosing by hand.
#[test]
fn the_generated_order_changelog_matches_the_hand_written_semantic_set() {
    let changelog = build_changelog(order_service_tables()).unwrap();
    assert_eq!(
        names(&changelog),
        vec![
            "init_schema",
            "create_outbox_table",
            "create_cache_table",
            "create_received_table",
        ],
        "inter-table order is by version and is deliberately arbitrary; only the \
         set and the intra-table order are contractual"
    );
}

/// The mirror image, for the product service.
#[test]
fn the_generated_product_changelog_matches_the_hand_written_semantic_set() {
    let changelog = build_changelog(vec![
        (TableKind::Outbox, product()),
        (TableKind::Received, order()),
        (TableKind::Cache, order()),
    ])
    .unwrap();

    let mut sorted = names(&changelog);
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        vec![
            "create_cache_table",
            "create_outbox_table",
            "create_received_table",
            "init_schema",
        ]
    );
}

/// The property the whole scheme exists for. Registration order is not identity,
/// so shuffling the declarations must produce a byte-identical changelog —
/// versions *and* checksums.
#[test]
fn declaration_order_changes_neither_versions_nor_checksums() {
    let forward = build_changelog(order_service_tables()).unwrap();

    let mut reversed_input = order_service_tables();
    reversed_input.reverse();
    let reversed = build_changelog(reversed_input).unwrap();

    assert_eq!(versions(&forward), versions(&reversed));
    assert_eq!(
        forward.iter().map(|c| c.checksum()).collect::<Vec<_>>(),
        reversed.iter().map(|c| c.checksum()).collect::<Vec<_>>(),
    );
}

#[test]
fn the_generated_changelog_is_sorted_and_starts_at_init_schema() {
    let changelog = build_changelog(order_service_tables()).unwrap();
    let versions = versions(&changelog);

    assert_eq!(versions[0], 1, "InitSchema must sort first");
    assert!(
        versions.windows(2).all(|pair| pair[0] < pair[1]),
        "not ascending: {versions:?}"
    );
}

/// `build_changelog` sorts before `assert_changelog_order` sees the slice.
/// Without that, a perfectly valid set of versions arriving in declaration order
/// would fail as a *disorder* error — which would be a lie about what went
/// wrong.
#[test]
fn an_unsorted_declaration_is_sorted_rather_than_rejected() {
    let mut shuffled = order_service_tables();
    shuffled.swap(0, 2);
    assert!(build_changelog(shuffled).is_ok());
}

#[test]
fn declaring_the_same_table_twice_is_deduplicated() {
    let mut duplicated = order_service_tables();
    duplicated.extend(order_service_tables());

    let changelog = build_changelog(duplicated).unwrap();
    assert_eq!(changelog.len(), 4);
}

/// A collision is not reported as a duplicate version, because that error reads
/// as a library bug to the one user who ever hits it and says nothing about
/// which two message types are involved.
///
/// Forced through the real code path with the band allocator stubbed to a
/// constant. The 40-bit digest cannot be made to collide on demand, and a test
/// that constructed the error value by hand would assert the wording while
/// proving nothing about whether generation ever reaches it.
#[test]
fn a_forced_band_collision_names_both_claimants() {
    // `Vec<Box<dyn Changeset>>` is not `Debug`, so `expect_err` is unavailable.
    let Err(error) = build_changelog_with_forced_band(order_service_tables(), 42) else {
        panic!("two tables sharing one band must not generate a changelog");
    };

    let Error::ChangesetBandCollision {
        band,
        first,
        second,
    } = &error
    else {
        panic!("expected a band collision, got {error:?}");
    };

    // The first two entries of `order_service_tables()`, because generation
    // fails at the first clash rather than collecting every one of them.
    assert_eq!(*band, 42);
    assert_eq!(first, "outbox/order_snapshot");
    assert_eq!(second, "received/product_snapshot");

    let rendered = error.to_string();
    assert!(rendered.contains("outbox/order_snapshot"), "{rendered}");
    assert!(rendered.contains("received/product_snapshot"), "{rendered}");
    assert!(
        rendered.contains("not a mistake in your roles"),
        "the message must not read as user error: {rendered}"
    );
}

/// The negative half: a real collision must not reach `assert_changelog_order`
/// and surface as a duplicate version.
#[test]
fn a_forced_collision_is_not_reported_as_a_duplicate_version() {
    let Err(error) = build_changelog_with_forced_band(order_service_tables(), 7) else {
        panic!("a forced collision must fail");
    };
    assert!(
        !matches!(error, Error::DuplicateChangesetVersion(_)),
        "collision detection must run before the ordering assertion"
    );
}
