use std::time::Duration;

use crate::{Config, ConfigSchema};

#[test]
fn validate_reports_all_required_key_problems() {
    // All of them, not the first: an operator fixing a config should see the
    // whole list rather than rediscovering one more mistake per deploy.
    let cfg = Config::parse(
        r#"
            [relay]
            retry_after = 3
            "#,
    )
    .unwrap();
    let schema = ConfigSchema::new()
        .require::<String>("database.schema")
        .require::<Duration>("relay.retry_after");

    let err = cfg.validate(&schema).unwrap_err();

    assert_eq!(err.issues().len(), 2);
    assert!(err.to_string().contains("database.schema"));
    assert!(err.to_string().contains("relay.retry_after"));
}

#[test]
fn an_empty_schema_accepts_anything() {
    let cfg = Config::parse("[relay]\nworker_id = \"a\"\n").unwrap();
    assert!(cfg.validate(&ConfigSchema::new()).is_ok());
}
