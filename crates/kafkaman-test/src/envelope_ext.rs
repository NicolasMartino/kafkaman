use kafkaman_core::{Envelope, IntoIdempotencyIdentity};

/// Test-only ergonomics for building envelopes.
///
/// The library deliberately has no infallible `with_idempotency_key`: deriving
/// an identity can fail, and a builder that panics on bad input has no place in
/// a library API. Tests pass literals they control, so panicking there is both
/// safe and the clearest way to fail.
pub trait EnvelopeTestExt: Sized {
    /// Derive a legacy-string idempotency identity and attach it.
    ///
    /// # Panics
    /// If the value is empty or whitespace.
    fn with_idempotency_key(self, idempotency_key: impl IntoIdempotencyIdentity) -> Self;
}

impl<P> EnvelopeTestExt for Envelope<P> {
    fn with_idempotency_key(self, idempotency_key: impl IntoIdempotencyIdentity) -> Self {
        let identity = idempotency_key
            .into_idempotency_identity()
            .expect("test idempotency source must be valid");
        self.with_idempotency_identity(identity)
    }
}
