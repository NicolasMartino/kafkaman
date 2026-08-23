use std::collections::BTreeSet;

use crate::changeset::Changeset;
use crate::{Error, Result};

/// Reject a changelog whose versions are not unique and ascending.
///
/// Order is what makes a changelog reproducible: applying versions out of order
/// on one replica and in order on another produces two different schemas from
/// one source. Duplicates are worse — the second is silently skipped, because
/// the first already wrote its history row.
pub fn assert_changelog_order(changesets: &[Box<dyn Changeset>]) -> Result<()> {
    let mut versions = BTreeSet::new();
    let mut previous = None;
    for changeset in changesets {
        let version = changeset.version();
        if !versions.insert(version) {
            return Err(Error::DuplicateChangesetVersion(version));
        }
        if let Some(previous) = previous {
            if version <= previous {
                return Err(Error::DisorderedChangesetVersion {
                    previous,
                    next: version,
                });
            }
        }
        previous = Some(version);
    }
    Ok(())
}

/// Build a `Vec<Box<dyn Changeset>>` and assert ascending, unique versions at
/// construction. Panics with the ordering error when the changelog is invalid;
/// use [`try_changelog!`](crate::try_changelog) when you need to inspect the
/// [`Error::DuplicateChangesetVersion`] / [`Error::DisorderedChangesetVersion`]
/// detail instead of a panic.
#[macro_export]
macro_rules! changelog {
    ($($changeset:expr),* $(,)?) => {{
        match $crate::try_changelog![$($changeset),*] {
            Ok(changesets) => changesets,
            Err(err) => panic!("invalid kafkaman changelog ordering: {err}"),
        }
    }};
}

/// Fallible sibling of [`changelog!`]: returns `Result<Vec<Box<dyn Changeset>>>`
/// so callers keep the structured ordering error rather than a panic.
#[macro_export]
macro_rules! try_changelog {
    ($($changeset:expr),* $(,)?) => {{
        let changesets: Vec<Box<dyn $crate::Changeset>> = vec![$(Box::new($changeset)),*];
        $crate::assert_changelog_order(&changesets).map(|()| changesets)
    }};
}
