//! The wire contract shared by the `product` and `order` example services.
//!
//! Both snapshots live here rather than being declared twice, because a
//! duplicated [`KafkaMessage`] impl drifts silently: a changed `TOPIC`,
//! `MESSAGE_TYPE`, or `entity_key` is a runtime mismatch with no compile error,
//! and the symptom is a cache that simply never converges.
//!
//! Nothing else belongs in this crate. Handlers, HTTP surface, and storage are
//! each service's own business; the contract is only the shape on the wire.

use kafkaman::KafkaMessage;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Where a product is in its lifecycle.
///
/// `product` owns this. `order` reads it out of its cache and refuses to accept
/// an order for anything that is not [`ProductStatus::Available`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
#[non_exhaustive]
pub enum ProductStatus {
    Draft,
    Available,
    Discontinued,
    /// A status this build does not know about.
    ///
    /// Required rather than stylistic. Snapshot enums travel on the wire, so a
    /// producer that gains a variant hands it to consumers that have not been
    /// redeployed. Without a catch-all those records fail to deserialize, and
    /// kafkaman's ingest treats a deserialization failure as a deterministic
    /// poison skip — enough of them in a row trips the consecutive-skip breaker
    /// and stops the topic for every entity, not just the new one.
    ///
    /// Both services treat it as "not usable", which is the safe reading: an
    /// unknown product status is not orderable.
    Unrecognized(String),
}

/// Where an order is in its lifecycle.
///
/// `order` owns this. `product` counts only [`OrderStatus::Fulfilled`] rows when
/// it recomputes availability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
#[non_exhaustive]
pub enum OrderStatus {
    Placed,
    Fulfilled,
    Cancelled,
    /// A status this build does not know about. See [`ProductStatus::Unrecognized`].
    Unrecognized(String),
}

/// A compact snapshot of one product, owned and published by `product`.
///
/// Full state, not a delta: the topic is compacted, so a late consumer must be
/// able to rebuild the entity from the single record compaction kept for its
/// key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProductSnapshot {
    pub product_id: Uuid,
    pub name: String,
    pub price_cents: i64,
    pub status: ProductStatus,
    /// Units a consumer may still order.
    ///
    /// `available`, not `on_hand`. `order` holds no cache of orders and so
    /// cannot subtract fulfilled ones itself; publishing the derived number is
    /// what lets `product` keep `on_hand` private while still telling consumers
    /// something they can act on.
    pub available: i64,
}

impl KafkaMessage for ProductSnapshot {
    const MESSAGE_TYPE: &'static str = "product_snapshot";
    const TOPIC: &'static str = "products";

    fn partition_key(&self) -> Option<String> {
        Some(self.product_id.to_string())
    }

    fn entity_key(&self) -> String {
        self.product_id.to_string()
    }
}

/// A compact snapshot of one order, owned and published by `order`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub order_id: Uuid,
    pub product_id: Uuid,
    pub quantity: i64,
    pub status: OrderStatus,
}

impl KafkaMessage for OrderSnapshot {
    const MESSAGE_TYPE: &'static str = "order_snapshot";
    const TOPIC: &'static str = "orders";

    fn partition_key(&self) -> Option<String> {
        Some(self.order_id.to_string())
    }

    fn entity_key(&self) -> String {
        self.order_id.to_string()
    }
}

/// The wire spelling of [`OrderStatus::Fulfilled`].
///
/// `product` filters its order cache with SQL against the stored JSONB payload,
/// so the literal in that query and the serialized variant name have to agree.
/// Naming it once here means a rename cannot leave the query silently matching
/// nothing — which would look exactly like "no orders are fulfilled".
pub const ORDER_STATUS_FULFILLED_WIRE: &str = "Fulfilled";

// Serde's `#[serde(other)]` catch-all is only available to internally- and
// adjacently-tagged enums, and these are plain strings on the wire. Round
// tripping through `String` is the supported way to get the same behaviour, and
// it also preserves the unknown spelling instead of discarding it, so an
// operator reading a cached payload can see what actually arrived.

impl From<String> for ProductStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Draft" => Self::Draft,
            "Available" => Self::Available,
            "Discontinued" => Self::Discontinued,
            _ => Self::Unrecognized(value),
        }
    }
}

impl From<ProductStatus> for String {
    fn from(value: ProductStatus) -> Self {
        match value {
            ProductStatus::Draft => "Draft".to_owned(),
            ProductStatus::Available => "Available".to_owned(),
            ProductStatus::Discontinued => "Discontinued".to_owned(),
            ProductStatus::Unrecognized(raw) => raw,
        }
    }
}

impl From<String> for OrderStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "Placed" => Self::Placed,
            ORDER_STATUS_FULFILLED_WIRE => Self::Fulfilled,
            "Cancelled" => Self::Cancelled,
            _ => Self::Unrecognized(value),
        }
    }
}

impl From<OrderStatus> for String {
    fn from(value: OrderStatus) -> Self {
        match value {
            OrderStatus::Placed => "Placed".to_owned(),
            OrderStatus::Fulfilled => ORDER_STATUS_FULFILLED_WIRE.to_owned(),
            OrderStatus::Cancelled => "Cancelled".to_owned(),
            OrderStatus::Unrecognized(raw) => raw,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn statuses_round_trip_through_their_wire_spelling() {
        let json = serde_json::to_string(&OrderStatus::Fulfilled).unwrap();
        assert_eq!(json, "\"Fulfilled\"");
        assert_eq!(
            serde_json::from_str::<OrderStatus>(&json).unwrap(),
            OrderStatus::Fulfilled
        );
    }

    #[test]
    fn an_unknown_status_deserializes_instead_of_poisoning_ingest() {
        // The failure this prevents is topic-wide, not per-record: a producer
        // that adds a variant would otherwise make every record carrying it a
        // deterministic ingest skip at an un-redeployed consumer.
        let status: ProductStatus = serde_json::from_str("\"Withdrawn\"").unwrap();
        assert_eq!(status, ProductStatus::Unrecognized("Withdrawn".to_owned()));
        assert_ne!(status, ProductStatus::Available);
        // And it survives a re-serialization with its original spelling.
        assert_eq!(serde_json::to_string(&status).unwrap(), "\"Withdrawn\"");
    }

    #[test]
    fn the_fulfilled_literal_matches_the_serialized_variant() {
        // `product`'s availability query compares against this constant. If the
        // two ever diverge the query matches nothing and availability silently
        // stops decreasing, which no availability assertion would attribute to
        // a rename.
        assert_eq!(
            serde_json::to_string(&OrderStatus::Fulfilled).unwrap(),
            format!("\"{ORDER_STATUS_FULFILLED_WIRE}\"")
        );
    }
}
