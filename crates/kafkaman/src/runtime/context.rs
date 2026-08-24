//! What a handler and a host can reach once the runtime is assembled.

use std::sync::Arc;

use kafkaman_core::{Envelope, KafkaMessage, ReceivedMeta};
use kafkaman_sqlx::{
    enqueue_on_connection, CacheTable, OutboxTable, ReceivedTable, ResolvedConfig, Result,
};
use serde::Serialize;
use sqlx::{PgConnection, PgPool};

/// The pool and resolved config the runtime was built from.
///
/// Handed to the host so an HTTP layer can share the runtime's pool and read the
/// same resolved table names the loops use, rather than rebuilding either and
/// risking disagreement.
#[derive(Clone, Debug)]
pub struct RuntimeContext {
    pool: PgPool,
    config: Arc<ResolvedConfig>,
}

impl RuntimeContext {
    pub(crate) fn new(pool: PgPool, config: Arc<ResolvedConfig>) -> Self {
        Self { pool, config }
    }

    /// The pool the runtime's loops use.
    ///
    /// The same pool, deliberately: making it visible that HTTP and the loops
    /// share one is the point of the host supplying it. A host that wants them
    /// separate can pass a different pool to its own layer.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The fully resolved `kafkaman.toml`, including every registered
    /// descriptor.
    #[must_use]
    pub fn config(&self) -> &Arc<ResolvedConfig> {
        &self.config
    }

    /// The cache table for a consumed message type.
    ///
    /// Fails for a type this runtime never declared, which is what makes the
    /// name trustworthy rather than merely well-formed.
    pub fn cache_table<P: KafkaMessage>(&self) -> Result<CacheTable> {
        CacheTable::for_message::<P>(&self.config)
    }

    /// The outbox table for a published message type.
    pub fn outbox_table<P: KafkaMessage>(&self) -> Result<OutboxTable> {
        OutboxTable::for_message::<P>(&self.config)
    }

    /// The received table for a consumed message type.
    pub fn received_table<P: KafkaMessage>(&self) -> Result<ReceivedTable> {
        ReceivedTable::for_message::<P>(&self.config)
    }
}

/// What a dispatch handler is given alongside its payload.
///
/// Holds the dispatch transaction's connection, so everything reached through it
/// — reads, writes, and [`enqueue`](HandlerCtx::enqueue) — commits or rolls back
/// with the cache upsert and the received row's status.
#[derive(Debug)]
pub struct HandlerCtx<'a> {
    conn: &'a mut PgConnection,
    config: Arc<ResolvedConfig>,
    meta: ReceivedMeta,
}

impl<'a> HandlerCtx<'a> {
    pub(crate) fn new(
        conn: &'a mut PgConnection,
        config: Arc<ResolvedConfig>,
        meta: ReceivedMeta,
    ) -> Self {
        Self { conn, config, meta }
    }

    /// The dispatch transaction's connection.
    #[must_use]
    pub fn conn(&mut self) -> &mut PgConnection {
        self.conn
    }

    /// Identity, routing, and provenance for this delivery.
    ///
    /// Also where a future tombstone arrives — see
    /// [`ReceivedMeta::is_deleted`](kafkaman_core::ReceivedMeta::is_deleted).
    #[must_use]
    pub fn meta(&self) -> &ReceivedMeta {
        &self.meta
    }

    /// The fully resolved `kafkaman.toml`.
    ///
    /// Handed back as the `Arc` rather than a plain reference, matching
    /// [`RuntimeContext::config`], so a handler can clone it and keep it past a
    /// borrow of the connection.
    #[must_use]
    pub fn config(&self) -> &Arc<ResolvedConfig> {
        &self.config
    }

    /// The cache table for any type this runtime consumes.
    pub fn cache_table<Q: KafkaMessage>(&self) -> Result<CacheTable> {
        CacheTable::for_message::<Q>(&self.config)
    }

    /// Enqueue a message in the dispatch transaction.
    ///
    /// Consume-then-produce, atomically: the state this handler derived and the
    /// announcement of it commit together or neither does. Nothing has to be
    /// reconciled afterwards, because there is no window in which one landed
    /// without the other.
    pub async fn enqueue<Q>(&mut self, envelope: &Envelope<Q>) -> Result<()>
    where
        Q: KafkaMessage + Serialize,
    {
        enqueue_on_connection(self.conn, &self.config, envelope).await
    }

    /// Take the connection, for a handler that would rather borrow it for the
    /// whole of its body than reach through the context repeatedly.
    #[must_use]
    pub fn into_conn(self) -> &'a mut PgConnection {
        self.conn
    }
}
