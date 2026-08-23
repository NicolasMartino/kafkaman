use crate::tests::OrderCreated;
use crate::{
    assert_changelog_order, Changeset, Error, InitSchema, MigrationAction, MigrationContext,
    MigrationStepReport, Replay,
};

#[test]
fn rejects_duplicate_changeset_versions() {
    let changesets: Vec<Box<dyn Changeset>> = vec![Box::new(InitSchema), Box::new(InitSchema)];
    assert!(matches!(
        assert_changelog_order(&changesets),
        Err(Error::DuplicateChangesetVersion(1))
    ));
}

#[test]
fn rejects_a_changelog_declared_out_of_order() {
    // Applying versions in one order on one replica and another order on the
    // next produces two different schemas from one source.
    let changesets: Vec<Box<dyn Changeset>> = vec![
        Box::new(crate::CreateOutboxTable::new(
            5,
            crate::tests::descriptor("order_created"),
        )),
        Box::new(InitSchema),
    ];
    assert!(matches!(
        assert_changelog_order(&changesets),
        Err(Error::DisorderedChangesetVersion {
            previous: 5,
            next: 1
        })
    ));
}

#[test]
fn migration_context_selects_by_declared_context() {
    let ctx = MigrationContext::default()
        .with_context("staging")
        .with_applied_by("deploy-bot");
    assert_eq!(ctx.applied_by(), "deploy-bot");

    // A changeset naming no context always runs; that is the common case, and
    // the one a changelog gets by default.
    assert!(MigrationStepReport::context_skip(&InitSchema, &ctx).is_none());

    // A changeset naming a context this run is in runs too.
    let staged = Replay::received::<OrderCreated>(5)
        .unwrap()
        .contexts(&["staging"]);
    assert!(MigrationStepReport::context_skip(&staged, &ctx).is_none());

    // One naming a context this run is *not* in is skipped, and the report says
    // which context would have run it — without that, an operator sees a
    // changeset silently not applied and has nothing to go on.
    let prod_only = Replay::received::<OrderCreated>(5)
        .unwrap()
        .contexts(&["prod"]);
    let skipped = MigrationStepReport::context_skip(&prod_only, &ctx)
        .expect("a context this run is not in must be skipped");
    assert!(matches!(skipped.action, MigrationAction::SkippedContext));
    assert!(skipped
        .preview
        .expect("a skip states its reason")
        .contains("prod"));
}

#[test]
fn changeset_checksums_change_with_their_material() {
    // The checksum is what detects a changeset edited after it was applied, so
    // it must move when anything that changes the emitted statements moves.
    let a = crate::CreateOutboxTable::new(2, crate::tests::descriptor("order_created"));
    let b = crate::CreateOutboxTable::new(3, crate::tests::descriptor("order_created"));
    let c = crate::CreateOutboxTable::new(2, crate::tests::descriptor("invoice_created"));

    assert_ne!(a.checksum(), b.checksum(), "version is material");
    assert_ne!(a.checksum(), c.checksum(), "message type is material");
    assert!(a.checksum().starts_with("sha256:"));
    assert_eq!(a.checksum().len(), "sha256:".len() + 64);
}
