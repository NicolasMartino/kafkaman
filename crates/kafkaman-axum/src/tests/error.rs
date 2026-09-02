//! Which status each admin failure earns, and what its body may say.

use super::*;

#[test]
fn admin_error_maps_to_the_right_status_and_hides_sql_detail() {
    let bad =
        AdminError::BadRequest("max_rows must be between 1 and 10000".to_owned()).into_response();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);

    let missing = AdminError::UnknownMessageType("nope".to_owned()).into_response();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let schema =
        AdminError::UnusableServiceTables(vec!["order_snapshot".to_owned()]).into_response();
    assert_eq!(schema.status(), StatusCode::SERVICE_UNAVAILABLE);

    let internal = AdminError::Sqlx(kafkaman_sqlx::Error::Handler(
        "schema kafkaman_x".to_owned(),
    ))
    .into_response();
    assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn a_refused_redrive_separates_the_wrong_service_from_a_broken_one() {
    let not_redrivable = |access| {
        AdminError::NotRedrivable {
            message_type: "order_snapshot".to_owned(),
            access,
        }
        .into_response()
        .status()
    };

    // No received table here at all: this service publishes the type and
    // something else consumes it. The caller is at the wrong address, and
    // no amount of waiting changes that.
    assert_eq!(not_redrivable(TableAccess::Missing), StatusCode::NOT_FOUND);

    // The table is there. A 404 would tell an operator the queue does not
    // exist while it sits full behind a missing grant, and would send them
    // to the caller's address instead of to the deployment.
    assert_eq!(
        not_redrivable(TableAccess::NoPrivilege),
        StatusCode::SERVICE_UNAVAILABLE,
        "a role without UPDATE is this deployment's fault, not the caller's"
    );
    assert_eq!(
        not_redrivable(TableAccess::NotATable),
        StatusCode::SERVICE_UNAVAILABLE,
        "a relation kafkaman did not create is a broken schema, not a 404"
    );

    // The body has to agree with the status. A 503 that says the queue does
    // not exist is worse than either half alone: it tells an operator to
    // wait *and* that there is nothing to wait for.
    let described = |access| {
        AdminError::NotRedrivable {
            message_type: "order_snapshot".to_owned(),
            access,
        }
        .to_string()
    };
    assert!(
        described(TableAccess::Missing).contains("has no dead-letter queue"),
        "the 404 case must say the queue is absent: {}",
        described(TableAccess::Missing)
    );
    assert!(
        described(TableAccess::NoPrivilege).contains("cannot be read"),
        "the 503 case must say the queue exists and is unreachable, not that \
         it is absent: {}",
        described(TableAccess::NoPrivilege)
    );

    // Whichever status, the body names the repair: the code alone cannot
    // distinguish the three, and only one of them is the caller's to fix.
    for access in [
        TableAccess::Missing,
        TableAccess::NoPrivilege,
        TableAccess::NotATable,
    ] {
        assert!(
            !access.repair().is_empty(),
            "{access:?} must tell an operator what to do about it"
        );
    }
}

#[tokio::test]
async fn admin_error_body_does_not_leak_storage_detail() {
    let response = AdminError::Sqlx(kafkaman_sqlx::Error::Handler(
        "secret schema name".to_owned(),
    ))
    .into_response();
    let body = http_body_util_collect(response.into_body()).await;
    assert!(
        !body.contains("secret schema name"),
        "an unauthenticated caller must not see storage internals, got {body}"
    );
}

#[test]
fn invalid_replay_is_a_client_error_not_a_server_error() {
    // `max_rows` is caller input, so a rejected replay is a 400.
    let err = AdminError::from(kafkaman_sqlx::Error::InvalidReplay {
        version: 0,
        message: "max_rows is required".to_owned(),
    });
    assert!(matches!(err, AdminError::BadRequest(_)));
    assert_eq!(err.into_response().status(), StatusCode::BAD_REQUEST);
}
