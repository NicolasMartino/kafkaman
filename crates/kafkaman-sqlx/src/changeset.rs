use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};

use crate::{ResolvedConfig, Result};

/// The statements one changeset contributes, in order.
#[derive(Debug)]
pub struct ChangeBuilder {
    statements: Vec<String>,
}

impl ChangeBuilder {
    pub fn new() -> Self {
        Self {
            statements: Vec::new(),
        }
    }

    pub fn push(&mut self, sql: impl Into<String>) {
        self.statements.push(sql.into());
    }

    pub(crate) fn into_statements(self) -> Vec<String> {
        self.statements
    }
}

impl Default for ChangeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Which changesets this run is allowed to apply, and who is applying them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationContext {
    contexts: BTreeSet<String>,
    applied_by: String,
}

impl MigrationContext {
    pub fn new() -> Self {
        Self {
            contexts: BTreeSet::new(),
            applied_by: "unknown".to_owned(),
        }
    }

    /// Read the context from `KAFKAMAN_CONTEXTS` and `KAFKAMAN_APPLIED_BY`.
    pub fn from_env() -> Self {
        let mut ctx = Self::new();
        if let Ok(contexts) = std::env::var("KAFKAMAN_CONTEXTS") {
            ctx.contexts = contexts
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect();
        }
        if let Ok(applied_by) = std::env::var("KAFKAMAN_APPLIED_BY") {
            let applied_by = applied_by.trim();
            if !applied_by.is_empty() {
                ctx.applied_by = applied_by.to_owned();
            }
        }
        ctx
    }

    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.contexts.insert(context.into());
        self
    }

    pub fn with_applied_by(mut self, applied_by: impl Into<String>) -> Self {
        self.applied_by = applied_by.into();
        self
    }

    pub fn applied_by(&self) -> &str {
        &self.applied_by
    }

    /// Whether this context runs a changeset requiring one of `required`.
    ///
    /// An empty requirement means "every context", so a changeset that names no
    /// context always runs — the common case, and the one a changelog gets by
    /// default.
    pub(crate) fn matches(&self, required: &[String]) -> bool {
        required.is_empty()
            || required
                .iter()
                .any(|required| self.contexts.contains(required))
    }
}

impl Default for MigrationContext {
    fn default() -> Self {
        Self::new()
    }
}

/// What a migration run did, step by step.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MigrationReport {
    steps: Vec<MigrationStepReport>,
}

impl MigrationReport {
    pub fn push(&mut self, step: MigrationStepReport) {
        self.steps.push(step);
    }

    pub fn steps(&self) -> &[MigrationStepReport] {
        &self.steps
    }

    pub fn applied_count(&self) -> usize {
        self.steps
            .iter()
            .filter(|step| matches!(step.action, MigrationAction::Applied))
            .count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationStepReport {
    pub version: i64,
    pub name: String,
    pub action: MigrationAction,
    pub preview: Option<String>,
}

impl MigrationStepReport {
    pub(crate) fn new(changeset: &dyn Changeset, action: MigrationAction) -> Self {
        Self {
            version: changeset.version(),
            name: changeset.name().to_owned(),
            action,
            preview: None,
        }
    }

    pub(crate) fn with_preview(mut self, preview: String) -> Self {
        self.preview = Some(preview);
        self
    }

    /// The report entry for a changeset this migration context excludes, or
    /// `None` when the context does run it.
    ///
    /// Returning the entry rather than a bare `bool` keeps the skip reason —
    /// which contexts would have run it — attached to the decision that produced
    /// it, so the apply and dry-run paths cannot describe the same skip
    /// differently.
    pub(crate) fn context_skip(changeset: &dyn Changeset, ctx: &MigrationContext) -> Option<Self> {
        if ctx.matches(changeset.contexts()) {
            return None;
        }

        Some(
            Self::new(changeset, MigrationAction::SkippedContext).with_preview(format!(
                "requires one of contexts: {}",
                changeset.contexts().join(",")
            )),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationAction {
    Applied,
    SkippedAlreadyApplied,
    SkippedContext,
    WouldApply,
    ChecksumMismatch { stored: String, current: String },
}

/// One versioned, checksummed unit of schema change.
///
/// Implementations supply a version, a name, and the statements to run;
/// everything else has a default. [`checksum_material`](Self::checksum_material)
/// is the one worth overriding deliberately: it is what detects a changeset
/// edited after it was applied, so it must include every input that changes the
/// statements this changeset emits.
pub trait Changeset: Send + Sync {
    fn version(&self) -> i64;
    fn name(&self) -> &str;
    fn build(&self, cfg: &ResolvedConfig, builder: &mut ChangeBuilder) -> Result<()>;

    fn checksum_material(&self) -> String {
        format!("{}:{}", self.version(), self.name())
    }

    fn checksum(&self) -> String {
        stable_checksum(&self.checksum_material())
    }

    /// Contexts this changeset requires, or empty to always run.
    fn contexts(&self) -> &[String] {
        &[]
    }

    fn dry_run_preview(&self, cfg: &ResolvedConfig) -> Result<String> {
        let mut builder = ChangeBuilder::new();
        self.build(cfg, &mut builder)?;
        Ok(builder.into_statements().join("\n"))
    }

    /// How many rows this changeset would touch, for a dry run that wants to
    /// report scale before an operator commits to it.
    ///
    /// Hand-written `Pin<Box<dyn Future>>` rather than `async fn`, because the
    /// trait is used as `dyn Changeset` and must stay object-safe.
    fn estimate_replay_count<'a>(
        &'a self,
        _cfg: &'a ResolvedConfig,
        _tx: &'a mut Transaction<'_, Postgres>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<i64>>> + Send + 'a>> {
        Box::pin(async { Ok(None) })
    }
}

/// Stable SHA-256 (hex) checksum of a changeset's source-declared material,
/// stored as `sha256:<64 hex chars>`. The `changeset:` domain prefix keeps these
/// digests from ever colliding with hashes computed for another purpose.
pub(crate) fn stable_checksum(material: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut hasher = Sha256::new();
    hasher.update(b"changeset:");
    hasher.update(material.as_bytes());
    let digest = hasher.finalize();

    let mut checksum = String::with_capacity("sha256:".len() + digest.len() * 2);
    checksum.push_str("sha256:");
    for byte in digest {
        checksum.push(char::from(HEX[usize::from(byte >> 4)]));
        checksum.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    checksum
}
