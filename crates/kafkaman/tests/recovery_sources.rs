#![cfg(feature = "rdkafka")]

use kafkaman::recovery::{Recovery, Resync};
use kafkaman::sqlx::ResyncFuture;
use kafkaman::{Envelope, KafkaMessage};
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(serde::Serialize)]
struct Snapshot;

impl KafkaMessage for Snapshot {
    const MESSAGE_TYPE: &'static str = "snapshot";
    const TOPIC: &'static str = "snapshots";

    fn entity_key(&self) -> String {
        "entity".to_owned()
    }
}

struct First;
struct Second;

macro_rules! source {
    ($type:ident, $name:literal) => {
        impl Resync for $type {
            type Message = Snapshot;
            const NAME: &'static str = $name;

            fn keys<'a>(
                &'a self,
                _: &'a mut PgConnection,
                _: Option<&'a str>,
                _: usize,
            ) -> ResyncFuture<'a, Vec<String>> {
                Box::pin(async { Ok(vec![]) })
            }

            fn load<'a>(
                &'a self,
                _: &'a mut PgConnection,
                _: &'a str,
                _: Uuid,
            ) -> ResyncFuture<'a, Option<Envelope<Self::Message>>> {
                Box::pin(async { Ok(None) })
            }
        }
    };
}

source!(First, "first");
source!(Second, "second");

#[test]
fn typed_sources_keep_the_declared_order() -> Result<(), Box<dyn std::error::Error>> {
    let recovery = Recovery::of((First, Second))?;
    assert_eq!(recovery.source_names(), vec!["first", "second"]);
    Ok(())
}
