use std::time::Duration;

use crate::tests::VALID;
use crate::Config;

/// A temp file that removes itself, so a failing assertion cannot leak it into
/// the next run — which, with a path keyed on the process id, is a recycled PID
/// away from reading a previous run's contents.
struct TempConfig {
    path: std::path::PathBuf,
}

impl TempConfig {
    fn write(contents: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "kafkaman-config-test-{}-{:?}.toml",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&path, contents).expect("write temp config");
        Self { path }
    }
}

impl Drop for TempConfig {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[test]
fn dotted_access_is_typed() {
    let cfg = Config::parse(VALID).unwrap();

    assert_eq!(
        cfg.get::<Duration>("relay.retry_after").unwrap(),
        Duration::from_millis(500)
    );
    assert_eq!(cfg.get::<i64>("relay.batch_limit").unwrap(), 25);
    assert!(cfg.get::<i64>("relay.retry_after").is_err());
    assert!(cfg.get_opt::<String>("missing.key").unwrap().is_none());
}

#[test]
fn path_loading_and_common_scalar_access_work() {
    let file = TempConfig::write(&format!(
        "{VALID}\n[feature]\nenabled = true\nratio = 1.25\nlabel = \"alpha\"\n"
    ));

    let cfg = Config::from_path(&file.path).unwrap();
    assert!(cfg.get::<bool>("feature.enabled").unwrap());
    assert_eq!(cfg.get::<f64>("feature.ratio").unwrap(), 1.25);
    assert_eq!(cfg.get::<String>("feature.label").unwrap(), "alpha");
    assert_eq!(cfg.relay().unwrap().worker_id, "worker-a");
    cfg.relay().unwrap().into_relay_config().unwrap();
    assert!(cfg
        .get::<String>("feature.missing")
        .unwrap_err()
        .to_string()
        .contains("feature.missing"));
}

#[test]
fn parses_through_the_from_str_trait_too() {
    // The inherent `parse` and the trait must agree; they did not use to, because
    // the inherent method was itself named `from_str`.
    let cfg: Config = VALID.parse().unwrap();
    assert_eq!(cfg.get::<String>("database.schema").unwrap(), "kafkaman");
}

#[test]
fn typed_relay_section_denies_unknown_fields() {
    // A typo in a knob name must fail the deploy, not be silently ignored.
    let cfg = Config::parse(
        r#"
            [relay]
            worker_id = "worker-a"
            batch_limit = 25
            lease_for = "30s"
            retry_after = "500ms"
            poll_interval = "250ms"
            surprise = true
            "#,
    )
    .unwrap();

    assert!(cfg.relay().unwrap_err().to_string().contains("surprise"));
}

#[test]
fn an_integer_too_large_for_exact_float_is_rejected() {
    // Silently rounding a multiplier to a different number than the file states
    // is worse than refusing to start.
    let cfg = Config::parse("[feature]\nratio = 9007199254740993\n").unwrap();
    let err = cfg.get::<f64>("feature.ratio").unwrap_err().to_string();
    assert!(err.contains("exactly"), "{err}");

    // The largest exactly-representable integer still works.
    let cfg = Config::parse("[feature]\nratio = 9007199254740992\n").unwrap();
    assert_eq!(cfg.get::<f64>("feature.ratio").unwrap(), 9007199254740992.0);
}
