//! Machinery for declaring an enum together with the lists derived from it.
//!
//! Every accessor these macros generate could be written by hand as a
//! `match self`, and the compiler would check it for exhaustiveness. `ALL` is
//! the one that it would not: a hand-written array with a hardcoded length
//! compiles perfectly well while missing a variant, and both the SQL vocabulary
//! and the stored-value recovery below are built by iterating it. So a variant
//! added to the enum and missed in `ALL` produces no diagnostic at all — just a
//! CHECK constraint that rejects a value the Rust type says is legal, or a
//! stored row that no longer reads back.
//!
//! Generating the array from the variant list is what makes that impossible.

/// Count identifiers at compile time, so the macros below can size an `ALL`
/// array from the variant list they were handed rather than from a number
/// someone has to remember to update.
macro_rules! count_idents {
    () => { 0usize };
    ($head:ident $($tail:ident)*) => { 1usize + $crate::enum_macros::count_idents!($($tail)*) };
}
pub(crate) use count_idents;

/// Declare a status enum together with the SQL vocabulary the schema derives
/// from it.
///
/// `ALL`, `as_str`, `sql_literal`, `sql_literal_list`, `Display` and `FromStr`
/// come from a single variant list. Generated DDL builds its CHECK constraints
/// and `IN (...)` lists from `ALL`, and row decoding goes through `FromStr`,
/// which searches it.
///
/// Variant names are the persisted representation. They appear in every stored
/// row and in generated CHECK constraints, so renaming one is a data migration,
/// not a refactor.
macro_rules! sql_enum {
    (
        $(#[$enum_meta:meta])*
        pub enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident),+ $(,)?
        }
        invalid = $invalid:path;
    ) => {
        $(#[$enum_meta])*
        #[derive(
            Clone, Copy, Debug, Eq, PartialEq, ::serde::Serialize, ::serde::Deserialize,
        )]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: [$name; $crate::enum_macros::count_idents!($($variant)+)] =
                [$($name::$variant),+];

            /// The canonical name, as persisted.
            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => stringify!($variant)),+
                }
            }

            /// The name as a single-quoted SQL string literal, e.g. `'Pending'`.
            ///
            /// Safe to interpolate directly into generated SQL without escaping:
            /// variant names are fixed ASCII identifiers chosen at compile time,
            /// never caller input.
            pub fn sql_literal(self) -> String {
                format!("'{}'", self.as_str())
            }

            /// Comma-separated SQL literal list of every variant, for `IN (...)`
            /// expressions and CHECK constraints.
            pub fn sql_literal_list() -> String {
                Self::ALL
                    .iter()
                    .map(|value| value.sql_literal())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $crate::Error;

            fn from_str(value: &str) -> $crate::Result<Self> {
                Self::ALL
                    .into_iter()
                    .find(|candidate| candidate.as_str() == value)
                    .ok_or_else(|| $invalid(value.to_owned()))
            }
        }
    };
}
pub(crate) use sql_enum;

/// Declare an enum whose variant names are the discriminants persisted for it.
///
/// Generates `ALL` and `discriminant`, and nothing else: anything carrying data
/// beyond the variant's own name — an RFC 9457 URI, a human-readable title —
/// stays a hand-written `match`, where the compiler checks exhaustiveness and
/// the values are legible next to the variants they belong to.
///
/// `discriminant` is generated even though a hand-written `match` would also be
/// checked, because what that check cannot catch is a copy-pasted arm mapping
/// one variant to another's string. These strings are persisted in
/// `last_failure_kind` and in the ingest failure table, and they feed changeset
/// checksums, so a wrong one is a data problem rather than a typo.
macro_rules! discriminant_enum {
    (
        $(#[$enum_meta:meta])*
        pub enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(
            Clone, Copy, Debug, Eq, PartialEq, ::serde::Serialize, ::serde::Deserialize,
        )]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: [$name; $crate::enum_macros::count_idents!($($variant)+)] =
                [$($name::$variant),+];

            /// The canonical discriminant, as persisted.
            pub const fn discriminant(self) -> &'static str {
                match self {
                    $($name::$variant => stringify!($variant)),+
                }
            }
        }
    };
}
pub(crate) use discriminant_enum;
