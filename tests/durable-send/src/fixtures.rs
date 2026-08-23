//! The entity types the suite publishes, one per partitioning shape.
//!
//! Three types rather than one because the shapes behave differently and the
//! differences are exactly what the cache convergence guard depends on.

use kafkaman_core::{Envelope, KafkaMessage};
use serde::{Deserialize, Serialize};

/// The canonical entity-snapshot fixture: a compact full-state snapshot keyed by
/// its own entity id, which is the only message shape kafkaman supports.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProductSnapshot {
    pub product_id: String,
    pub name: String,
}

impl KafkaMessage for ProductSnapshot {
    const MESSAGE_TYPE: &'static str = "product_snapshot";
    const TOPIC: &'static str = "products";

    fn partition_key(&self) -> Option<String> {
        Some(self.product_id.clone())
    }

    fn entity_key(&self) -> String {
        self.product_id.clone()
    }
}

impl ProductSnapshot {
    pub fn envelope(product_id: &str, name: &str) -> Envelope<Self> {
        Envelope::new(Self {
            product_id: product_id.to_owned(),
            name: name.to_owned(),
        })
    }
}

/// An entity type whose Kafka partition key is deliberately *not* its entity
/// key, so the cache must recover the entity key from somewhere other than the
/// record key. This is the shape that used to lose its entity identity across a
/// broker round trip.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegionalProduct {
    pub product_id: String,
    pub region: String,
    pub name: String,
}

impl KafkaMessage for RegionalProduct {
    const MESSAGE_TYPE: &'static str = "regional_product";
    const TOPIC: &'static str = "regional_products";

    fn partition_key(&self) -> Option<String> {
        Some(self.region.clone())
    }

    fn entity_key(&self) -> String {
        self.product_id.clone()
    }
}

impl RegionalProduct {
    pub fn envelope(product_id: &str, region: &str, name: &str) -> Envelope<Self> {
        Envelope::new(Self {
            product_id: product_id.to_owned(),
            region: region.to_owned(),
            name: name.to_owned(),
        })
    }
}

/// An entity type that declares no partition key at all. Kafka would route these
/// round-robin unless the publisher falls back to the entity key, which would
/// scatter one entity's snapshots across partitions and make the cache
/// convergence guard unable to compare them.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KeylessProduct {
    pub product_id: String,
    pub name: String,
}

impl KafkaMessage for KeylessProduct {
    const MESSAGE_TYPE: &'static str = "keyless_product";
    const TOPIC: &'static str = "keyless_products";

    fn entity_key(&self) -> String {
        self.product_id.clone()
    }
}

impl KeylessProduct {
    pub fn envelope(product_id: &str, name: &str) -> Envelope<Self> {
        Envelope::new(Self {
            product_id: product_id.to_owned(),
            name: name.to_owned(),
        })
    }
}
