//! What a service declares it does with a message type.
//!
//! A role is the whole of the developer-facing vocabulary: publish it, cache it,
//! handle it, or handle it before the cache advances. Everything kafkaman needs
//! — which tables to create, which loops to run, which topics to converge,
//! whether a handler exists — is derivable from that, and none of it should be
//! something a service author restates by hand.
//!
//! This registry is the derivation, and it deliberately holds no handler
//! closures. Handlers are typed, live in the caller's crate, and end up in a
//! [`MessageRouter`](crate::MessageRouter); what has to be validated *before any
//! I/O* is only the shape of the declaration. Keeping the two apart means the
//! conflict rules are testable without constructing a single future.

use std::collections::BTreeMap;

use kafkaman_core::MessageDescriptor;

use crate::changeset::Changeset;
use crate::generated_changelog::{build_changelog, TableKind};
use crate::{Error, Result};

/// One thing a service declares about one message type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    /// This service produces the type. Needs an outbox table and a relay.
    Publish,
    /// This service consumes the type for its cache and nothing more.
    Cache,
    /// This service consumes the type and derives from it *after* the cache
    /// upsert.
    Handle,
    /// This service consumes the type and needs the entity's previous version,
    /// so it runs *before* the cache upsert.
    HandleBefore,
}

impl Role {
    /// The spelling a service author wrote, for error messages.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Cache => "cache",
            Self::Handle => "handle",
            Self::HandleBefore => "handle_before",
        }
    }

    /// Whether this role puts the type on the receive side.
    #[must_use]
    pub const fn consumes(self) -> bool {
        matches!(self, Self::Cache | Self::Handle | Self::HandleBefore)
    }
}

/// Every role declared for one message type.
#[derive(Clone, Debug)]
struct RoleEntry {
    descriptor: MessageDescriptor,
    publish: bool,
    cache: bool,
    handle: bool,
    handle_before: bool,
}

impl RoleEntry {
    fn new(descriptor: MessageDescriptor) -> Self {
        Self {
            descriptor,
            publish: false,
            cache: false,
            handle: false,
            handle_before: false,
        }
    }

    fn consumes(&self) -> bool {
        self.cache || self.handle || self.handle_before
    }

    /// The consuming role already declared, if any. Used to explain conflicts in
    /// the words the author used.
    fn consuming_role(&self) -> Option<Role> {
        if self.cache {
            Some(Role::Cache)
        } else if self.handle {
            Some(Role::Handle)
        } else if self.handle_before {
            Some(Role::HandleBefore)
        } else {
            None
        }
    }
}

/// The set of roles a service has declared.
///
/// Repeatable and deduplicated: declaring `publish::<T>()` twice is the same as
/// declaring it once. What is rejected is *ambiguity* — two ways to handle one
/// message type at one position, which has no defensible resolution.
#[derive(Clone, Debug, Default)]
pub struct RoleRegistry {
    entries: BTreeMap<String, RoleEntry>,
}

impl RoleRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether nothing has been declared. A runtime with no roles would start no
    /// loops and create no tables, which is never what the caller meant.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Record one role for one message type.
    ///
    /// Legal repeats are silently deduplicated. The one legal *pair* is
    /// `handle_before` plus `handle` on the same type — two hooks at two
    /// positions, which the dispatch path runs at most once each. Every other
    /// combination that would leave two handlers competing for one position is
    /// rejected here, before any database or broker is touched.
    pub fn declare(&mut self, descriptor: MessageDescriptor, role: Role) -> Result<()> {
        let message_type = descriptor.message_type.as_str().to_owned();

        let entry = match self.entries.get_mut(&message_type) {
            Some(entry) => {
                if entry.descriptor.topic != descriptor.topic {
                    return Err(Error::ConflictingMessageType {
                        message_type,
                        registered: entry.descriptor.topic.clone(),
                        conflicting: descriptor.topic,
                    });
                }
                entry
            }
            None => self
                .entries
                .entry(message_type.clone())
                .or_insert_with(|| RoleEntry::new(descriptor)),
        };

        match role {
            // Publishing is orthogonal to consuming: a service is allowed to own
            // a topic and also keep a cache of it, as long as it said so.
            Role::Publish => entry.publish = true,
            Role::Cache => {
                if let Some(existing) = entry.consuming_role() {
                    if existing != Role::Cache {
                        return Err(conflict(&message_type, existing, Role::Cache));
                    }
                }
                entry.cache = true;
            }
            Role::Handle => {
                if entry.cache {
                    return Err(conflict(&message_type, Role::Cache, Role::Handle));
                }
                if entry.handle {
                    return Err(conflict(&message_type, Role::Handle, Role::Handle));
                }
                entry.handle = true;
            }
            Role::HandleBefore => {
                if entry.cache {
                    return Err(conflict(&message_type, Role::Cache, Role::HandleBefore));
                }
                if entry.handle_before {
                    return Err(conflict(
                        &message_type,
                        Role::HandleBefore,
                        Role::HandleBefore,
                    ));
                }
                entry.handle_before = true;
            }
        }

        Ok(())
    }

    /// Every declared descriptor, for config resolution and topic convergence.
    #[must_use]
    pub fn descriptors(&self) -> Vec<MessageDescriptor> {
        self.entries
            .values()
            .map(|entry| entry.descriptor.clone())
            .collect()
    }

    /// Types this service produces, each needing an outbox table and a relay.
    pub fn published(&self) -> impl Iterator<Item = &MessageDescriptor> {
        self.entries
            .values()
            .filter(|entry| entry.publish)
            .map(|entry| &entry.descriptor)
    }

    /// Types this service consumes, each needing a received table, a cache
    /// table, an ingester, and a dispatcher.
    pub fn consumed(&self) -> impl Iterator<Item = &MessageDescriptor> {
        self.entries
            .values()
            .filter(|entry| entry.consumes())
            .map(|entry| &entry.descriptor)
    }

    /// Whether a consumed type was declared with an application handler, as
    /// opposed to `cache::<T>()` alone.
    #[must_use]
    pub fn has_application_handler(&self, message_type: &str) -> bool {
        self.entries
            .get(message_type)
            .is_some_and(|entry| entry.handle || entry.handle_before)
    }

    /// The tables these roles imply, as `(kind, descriptor)` pairs.
    fn tables(&self) -> Vec<(TableKind, MessageDescriptor)> {
        let mut tables = Vec::new();
        for entry in self.entries.values() {
            if entry.publish {
                tables.push((TableKind::Outbox, entry.descriptor.clone()));
            }
            if entry.consumes() {
                for kind in TableKind::CONSUMED {
                    tables.push((kind, entry.descriptor.clone()));
                }
            }
        }
        tables
    }

    /// The kafkaman-owned changelog these roles imply.
    ///
    /// Order-independent by construction: identity comes from
    /// `(table kind, message type, template version)`, and the result is sorted
    /// by version rather than by declaration.
    pub fn changelog(&self) -> Result<Vec<Box<dyn Changeset>>> {
        build_changelog(self.tables())
    }
}

fn conflict(message_type: &str, existing: Role, added: Role) -> Error {
    Error::ConflictingRole {
        message_type: message_type.to_owned(),
        existing: existing.as_str(),
        added: added.as_str(),
    }
}
