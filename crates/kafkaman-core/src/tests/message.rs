use std::collections::BTreeMap;

use crate::{reserved_header, MessageDescriptor};

#[test]
fn detects_reserved_header_namespace() {
    let mut headers = BTreeMap::new();
    headers.insert("x-trace-id".to_owned(), "abc".to_owned());
    assert!(reserved_header(&headers).is_none());

    headers.insert("Kafkaman-Message-Id".to_owned(), "spoof".to_owned());
    assert_eq!(reserved_header(&headers), Some("Kafkaman-Message-Id"));
}

#[test]
fn descriptor_rejects_an_empty_topic_and_an_invalid_type() {
    assert!(MessageDescriptor::new("order_created", "orders").is_ok());
    assert!(MessageDescriptor::new("order_created", "   ").is_err());
    assert!(MessageDescriptor::new("OrderCreated", "orders").is_err());
}
