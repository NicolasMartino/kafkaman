use super::*;

#[tokio::test]
async fn config_validation_fails_before_database_work() -> TestResult {
    let config = kafkaman_test::kafkaman_config::Config::parse(
        r#"
        [database]
        schema = "kafkaman"

        [relay]
        worker_id = "worker-a"
        batch_limit = 10
        lease_for = "not-a-duration"
        retry_after = "1s"
        "#,
    )?;

    let err = match Harness::connect_with_config(
        "postgres://postgres:postgres@127.0.0.1:1/should_not_connect",
        config,
    )
    .await
    {
        Ok(_) => panic!("config validation must fail before connecting to Postgres"),
        Err(err) => err,
    };
    let rendered = err.to_string();
    assert!(rendered.contains("relay.lease_for"), "{rendered}");
    assert!(!rendered.contains("connection refused"), "{rendered}");

    Ok(())
}

#[tokio::test]
async fn invalid_retry_config_fails_before_database_work() -> TestResult {
    // A bad retry/DLQ policy must be rejected by the boot resolver, before any
    // pool connect or migration. The Postgres URL points at a dead port to prove
    // resolution never reaches the database.
    let config = kafkaman_test::kafkaman_config::Config::parse(
        r#"
        [database]
        schema = "kafkaman"

        [relay]
        worker_id = "worker-a"
        batch_limit = 10
        lease_for = "30s"
        retry_after = "1s"
        poll_interval = "250ms"

        [retry.defaults]
        max_attempts = 0
        initial_backoff = "100ms"
        max_backoff = "30s"
        multiplier = 2.0
        errors_limit = 16
        dlq = "table"
        "#,
    )?;

    let err = match Harness::connect_with_config(
        "postgres://postgres:postgres@127.0.0.1:1/should_not_connect",
        config,
    )
    .await
    {
        Ok(_) => panic!("retry validation must fail before connecting to Postgres"),
        Err(err) => err,
    };
    let rendered = err.to_string();
    assert!(
        rendered.contains("retry.defaults.max_attempts"),
        "{rendered}"
    );
    assert!(!rendered.contains("connection refused"), "{rendered}");

    Ok(())
}
