use kafkaman_core::MessageDescriptor;

use crate::{Error, Role, RoleRegistry};

fn order() -> MessageDescriptor {
    MessageDescriptor::new("order_snapshot", "orders").unwrap()
}

fn product() -> MessageDescriptor {
    MessageDescriptor::new("product_snapshot", "products").unwrap()
}

fn types(descriptors: Vec<&MessageDescriptor>) -> Vec<&str> {
    descriptors
        .into_iter()
        .map(|d| d.message_type.as_str())
        .collect()
}

// ---------------------------------------------------------------------------
// Deduplication
// ---------------------------------------------------------------------------

#[test]
fn a_role_declared_twice_is_the_same_as_declared_once() {
    let mut roles = RoleRegistry::new();
    roles.declare(order(), Role::Publish).unwrap();
    roles.declare(order(), Role::Publish).unwrap();
    roles.declare(product(), Role::Cache).unwrap();
    roles.declare(product(), Role::Cache).unwrap();

    assert_eq!(roles.descriptors().len(), 2);
    assert_eq!(types(roles.published().collect()), vec!["order_snapshot"]);
    assert_eq!(types(roles.consumed().collect()), vec!["product_snapshot"]);
}

/// Owning a topic and keeping a cache of it are independent facts. A service is
/// allowed to do both — it just has to say so, which is why this is two calls
/// rather than an inferred default.
#[test]
fn a_service_may_publish_and_consume_the_same_message_type() {
    let mut roles = RoleRegistry::new();
    roles.declare(order(), Role::Publish).unwrap();
    roles.declare(order(), Role::Handle).unwrap();

    assert_eq!(types(roles.published().collect()), vec!["order_snapshot"]);
    assert_eq!(types(roles.consumed().collect()), vec!["order_snapshot"]);
    assert_eq!(
        roles.changelog().unwrap().len(),
        4,
        "init_schema plus outbox, received and cache for the one type"
    );
}

// ---------------------------------------------------------------------------
// The one legal pair, and every conflict
// ---------------------------------------------------------------------------

/// Two hooks at two positions, which the dispatch path runs at most once each.
#[test]
fn handle_before_and_handle_on_one_type_is_the_only_legal_pair() {
    let mut roles = RoleRegistry::new();
    roles.declare(order(), Role::HandleBefore).unwrap();
    roles.declare(order(), Role::Handle).unwrap();

    assert!(roles.has_application_handler("order_snapshot"));
    assert_eq!(types(roles.consumed().collect()), vec!["order_snapshot"]);
}

#[test]
fn two_handlers_at_the_same_position_are_rejected() {
    for role in [Role::Handle, Role::HandleBefore] {
        let mut roles = RoleRegistry::new();
        roles.declare(order(), role).unwrap();

        let error = roles
            .declare(order(), role)
            .expect_err("two handlers at one position have no defensible resolution");
        assert!(matches!(error, Error::ConflictingRole { .. }), "{error:?}");
    }
}

/// `cache::<T>()` installs kafkaman's own no-op handler, so pairing it with an
/// application handler would leave two claims on one position.
#[test]
fn cache_cannot_be_combined_with_a_handler() {
    for role in [Role::Handle, Role::HandleBefore] {
        let mut roles = RoleRegistry::new();
        roles.declare(product(), Role::Cache).unwrap();
        assert!(roles.declare(product(), role).is_err());

        // And in the other order, which is a different branch.
        let mut reversed = RoleRegistry::new();
        reversed.declare(product(), role).unwrap();
        assert!(reversed.declare(product(), Role::Cache).is_err());
    }
}

/// The error has to name the type and both roles, because the fix is to delete
/// one of two lines the author wrote and nothing else identifies which.
#[test]
fn a_role_conflict_names_the_message_type_and_both_roles() {
    let mut roles = RoleRegistry::new();
    roles.declare(product(), Role::Cache).unwrap();
    let rendered = roles
        .declare(product(), Role::Handle)
        .unwrap_err()
        .to_string();

    assert!(rendered.contains("product_snapshot"), "{rendered}");
    assert!(rendered.contains("cache"), "{rendered}");
    assert!(rendered.contains("handle"), "{rendered}");
    assert!(
        rendered.contains("handle_before"),
        "the message must point at the one legal pair: {rendered}"
    );
}

#[test]
fn one_message_type_cannot_be_declared_on_two_topics() {
    let mut roles = RoleRegistry::new();
    roles.declare(order(), Role::Publish).unwrap();

    let elsewhere = MessageDescriptor::new("order_snapshot", "orders_v2").unwrap();
    let error = roles.declare(elsewhere, Role::Cache).unwrap_err();
    assert!(
        matches!(error, Error::ConflictingMessageType { .. }),
        "{error:?}"
    );
}

// ---------------------------------------------------------------------------
// Validation happens before any I/O
// ---------------------------------------------------------------------------

/// Every rejection above is reachable from a registry alone — no pool, no
/// broker, no config. That is the property that lets `build()` fail fast.
#[test]
fn every_conflict_is_detectable_without_a_pool_or_a_broker() {
    let mut roles = RoleRegistry::new();
    assert!(roles.is_empty());

    roles.declare(product(), Role::Cache).unwrap();
    assert!(!roles.is_empty());
    assert!(roles.declare(product(), Role::Handle).is_err());
}

// ---------------------------------------------------------------------------
// Roles to schema
// ---------------------------------------------------------------------------

/// The whole point: a service author says "I publish orders and cache products",
/// and the four changesets they used to hand-number fall out of it.
#[test]
fn the_order_services_roles_generate_its_four_changesets() {
    let mut roles = RoleRegistry::new();
    roles.declare(order(), Role::Publish).unwrap();
    roles.declare(product(), Role::Cache).unwrap();

    let changelog = roles.changelog().unwrap();
    let mut sorted: Vec<&str> = changelog.iter().map(|changeset| changeset.name()).collect();
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

/// All three consuming roles are the same schema fact, so all three must produce
/// identical changelogs. Without this, swapping `cache::<T>()` for `handle::<T>`
/// would present as a new changeset against an already-created table.
#[test]
fn every_consuming_role_generates_the_same_schema() {
    let versions_for = |role| {
        let mut roles = RoleRegistry::new();
        roles.declare(product(), role).unwrap();
        roles
            .changelog()
            .unwrap()
            .iter()
            .map(|changeset| (changeset.version(), changeset.checksum()))
            .collect::<Vec<_>>()
    };

    let cached = versions_for(Role::Cache);
    assert_eq!(cached, versions_for(Role::Handle));
    assert_eq!(cached, versions_for(Role::HandleBefore));
}

/// Declaration order is not identity.
#[test]
fn declaration_order_does_not_change_the_generated_changelog() {
    // One service — publishes orders, caches products — declared both ways
    // round. Anything that reassigned a role to the other descriptor would be
    // comparing two different services and would prove nothing.
    let build = |publish_first: bool| {
        let mut roles = RoleRegistry::new();
        if publish_first {
            roles.declare(order(), Role::Publish).unwrap();
            roles.declare(product(), Role::Cache).unwrap();
        } else {
            roles.declare(product(), Role::Cache).unwrap();
            roles.declare(order(), Role::Publish).unwrap();
        }
        let changelog = roles.changelog().unwrap();
        changelog
            .iter()
            .map(|changeset| (changeset.version(), changeset.checksum()))
            .collect::<Vec<_>>()
    };

    assert_eq!(build(true), build(false));
}

#[test]
fn a_cache_only_type_has_no_application_handler() {
    let mut roles = RoleRegistry::new();
    roles.declare(product(), Role::Cache).unwrap();

    assert!(!roles.has_application_handler("product_snapshot"));
    assert!(!roles.has_application_handler("never_declared"));
}
