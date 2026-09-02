//! What the operator routes read from.
//!
//! Its own module because both routers take it: [`admin`](crate::admin) reads
//! through it and [`redrive`](crate::redrive) writes through it, and a type two
//! sibling modules share belongs to neither.

use std::sync::Arc;

use kafkaman_sqlx::ResolvedConfig;
use sqlx::PgPool;

/// Pool and resolved config the admin routes read from.
#[derive(Clone, Debug)]
pub struct AdminState {
    pub pool: PgPool,
    pub cfg: Arc<ResolvedConfig>,
}

impl AdminState {
    pub fn new(pool: PgPool, cfg: Arc<ResolvedConfig>) -> Self {
        Self { pool, cfg }
    }
}
