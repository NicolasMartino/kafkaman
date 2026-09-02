//! Correlation ids, and the bounded route template a span reports.

use super::*;

/// A matched request reports the template, not the URL it was reached by.
///
/// This also pins that `MatchedPath` is visible to a layer added with
/// `Router::layer`, which is the only reason the template is available at
/// all — the layer wraps each route, so routing has already happened.
#[tokio::test]
async fn a_matched_request_reports_its_route_template() {
    let (route, name) = recorded_route("/products/6f9619ff-8b86-d011-b42d-00cf4fc964ff").await;
    assert_eq!(route, "/products/{product_id}");
    assert_eq!(name, "GET /products/{product_id}");
}

/// An unmatched request reports a constant.
///
/// `http.route` and the exported span name are what a backend groups
/// transactions by. Falling back to the raw path would mint one transaction
/// group per URL a scanner invents, which is unbounded cardinality from
/// unauthenticated input. The path itself is still recorded, as `url.path`,
/// which nothing groups by.
#[tokio::test]
async fn an_unmatched_request_reports_a_bounded_route() {
    for uri in ["/nope", "/totally/made/up/12345", "/products"] {
        let (route, name) = recorded_route(uri).await;
        assert_eq!(route, UNMATCHED_ROUTE, "{uri} should not become a route");
        assert_eq!(name, "GET <unmatched>", "{uri} should not become a group");
    }
}

#[tokio::test]
async fn correlation_layer_preserves_existing_header_and_sets_extension() {
    let request = Request::builder()
        .uri("/")
        .header(CORRELATION_ID_HEADER, "request-123")
        .body(Body::empty())
        .unwrap();

    let (header, body) = correlation_roundtrip(request).await;
    assert_eq!(header, "request-123");
    assert_eq!(
        body, "request-123",
        "the extension the handler sees must match the header echoed back"
    );
}

#[tokio::test]
async fn correlation_layer_generates_an_id_when_the_header_is_absent() {
    let request = Request::builder().uri("/").body(Body::empty()).unwrap();

    let (header, body) = correlation_roundtrip(request).await;
    assert_eq!(header, body);
    assert!(
        Uuid::parse_str(&header).is_ok(),
        "a generated correlation id should be a UUID, got {header:?}"
    );
}

#[tokio::test]
async fn correlation_layer_replaces_blank_and_oversized_values() {
    // Whitespace-only carries no information, and an oversized value is
    // attacker-controlled log volume. Both are replaced, not propagated.
    for supplied in ["   ", "\t"] {
        let request = Request::builder()
            .uri("/")
            .header(CORRELATION_ID_HEADER, supplied)
            .body(Body::empty())
            .unwrap();
        let (header, _) = correlation_roundtrip(request).await;
        assert!(
            Uuid::parse_str(&header).is_ok(),
            "blank value {supplied:?} should be replaced, got {header:?}"
        );
    }

    let oversized = "a".repeat(MAX_CORRELATION_ID_LEN + 1);
    let request = Request::builder()
        .uri("/")
        .header(CORRELATION_ID_HEADER, &oversized)
        .body(Body::empty())
        .unwrap();
    let (header, _) = correlation_roundtrip(request).await;
    assert_ne!(header, oversized);
    assert!(Uuid::parse_str(&header).is_ok());

    // Exactly at the bound is still accepted.
    let at_bound = "a".repeat(MAX_CORRELATION_ID_LEN);
    let request = Request::builder()
        .uri("/")
        .header(CORRELATION_ID_HEADER, &at_bound)
        .body(Body::empty())
        .unwrap();
    let (header, _) = correlation_roundtrip(request).await;
    assert_eq!(header, at_bound);
}

#[test]
fn correlation_id_rejects_unprintable_and_spaced_values() {
    assert!(CorrelationId::is_acceptable("abc-123"));
    assert!(!CorrelationId::is_acceptable(""));
    assert!(!CorrelationId::is_acceptable("has space"));
    assert!(!CorrelationId::is_acceptable("emoji-\u{1F600}"));
    assert!(!CorrelationId::is_acceptable(
        &"a".repeat(MAX_CORRELATION_ID_LEN + 1)
    ));
}
