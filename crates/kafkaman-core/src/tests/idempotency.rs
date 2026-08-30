use crate::{IdempotencyIdentity, IdempotencyKey, IdempotencySource};

#[test]
fn idempotency_identity_is_deterministic_and_retains_source() {
    let first = IdempotencyIdentity::derive(
        "OrderCreated:v1",
        serde_json::json!({ "order_id": "order-1" }),
    )
    .unwrap();
    let second = IdempotencyIdentity::derive(
        "OrderCreated:v1",
        serde_json::json!({ "order_id": "order-1" }),
    )
    .unwrap();
    let different = IdempotencyIdentity::derive(
        "OrderCreated:v1",
        serde_json::json!({ "order_id": "order-2" }),
    )
    .unwrap();

    assert_eq!(first.key, second.key);
    assert_ne!(first.key, different.key);
    assert_eq!(first.key.to_string().len(), IdempotencyKey::HEX_LEN);
    assert_eq!(
        first.source.as_ref().map(IdempotencySource::value),
        Some(&serde_json::json!({ "order_id": "order-1" }))
    );
}

#[test]
fn idempotency_digest_is_pinned_to_canonical_key_order() {
    // The derived digest is a durable dedupe key. Canonical JSON ordering is
    // explicit here rather than inherited from serde_json's map type, so an
    // adopter enabling serde_json's `preserve_order` feature cannot renumber
    // stored idempotency keys through Cargo feature unification.
    const PINNED: &str = "f0de9a117fcad54e5c8f444426129868ddb43d45517230f7fb2be43356b160e3";

    let identity =
        IdempotencyIdentity::derive("kafkaman:test:v1", serde_json::json!({"a": 1, "b": 2}))
            .unwrap();
    assert_eq!(identity.key.to_hex(), PINNED);

    // Same content, reversed insertion order, must produce the same digest.
    let reversed =
        IdempotencyIdentity::derive("kafkaman:test:v1", serde_json::json!({"b": 2, "a": 1}))
            .unwrap();
    assert_eq!(reversed.key.to_hex(), PINNED);
}

#[test]
fn nested_object_key_order_is_canonicalized() {
    let first = IdempotencyIdentity::derive(
        "kafkaman:test:nested:v1",
        serde_json::json!({"outer": {"a": 1, "b": 2}, "list": [{"x": 1, "y": 2}]}),
    )
    .unwrap();
    let reversed = IdempotencyIdentity::derive(
        "kafkaman:test:nested:v1",
        serde_json::json!({"list": [{"y": 2, "x": 1}], "outer": {"b": 2, "a": 1}}),
    )
    .unwrap();

    assert_eq!(first.key, reversed.key);
}

#[test]
fn idempotency_namespace_separates_identical_sources() {
    // The namespace is hashed with a NUL separator, so two namespaces cannot
    // be concatenation-confused into producing the same digest for different
    // (namespace, source) pairs.
    let source = serde_json::json!({ "id": "1" });
    let a = IdempotencyIdentity::derive("ns:a", &source).unwrap();
    let b = IdempotencyIdentity::derive("ns:b", &source).unwrap();
    assert_ne!(a.key, b.key);

    // `"ab" + source` must not collide with `"a" + "b" + source`.
    let joined = IdempotencyIdentity::derive("ns:ab", &source).unwrap();
    assert_ne!(joined.key, a.key);
    assert_ne!(joined.key, b.key);
}

#[test]
fn idempotency_namespace_must_not_be_blank() {
    assert!(IdempotencyIdentity::derive("", serde_json::json!({})).is_err());
    assert!(IdempotencyIdentity::derive("   ", serde_json::json!({})).is_err());
}

#[test]
fn string_source_derivation_rejects_a_blank_source() {
    assert!(IdempotencyIdentity::derive_from_string("").is_err());
    assert!(IdempotencyIdentity::derive_from_string("  \t ").is_err());
    assert!(IdempotencyIdentity::derive_from_string("idem-1").is_ok());
}

#[test]
fn idempotency_key_hex_round_trips() {
    let original = IdempotencyIdentity::derive("ns", serde_json::json!({"k": "v"}))
        .unwrap()
        .key;
    assert_eq!(
        IdempotencyKey::from_hex(&original.to_hex()).unwrap(),
        original
    );
    // Hex parsing is case-insensitive on input but canonical (lowercase) out.
    let upper = original.to_hex().to_uppercase();
    assert_eq!(IdempotencyKey::from_hex(&upper).unwrap(), original);
    assert_eq!(original.to_hex(), original.to_hex().to_lowercase());
}

#[test]
fn idempotency_key_rejects_invalid_hex() {
    assert!(IdempotencyKey::from_hex("").is_err());
    assert!(IdempotencyKey::from_hex("not-a-digest").is_err());
    assert!(IdempotencyKey::from_hex(
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
    )
    .is_err());
}
