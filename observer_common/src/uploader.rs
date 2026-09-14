//! Centralized publishing of generated artifacts to an S3-compatible bucket
//! (Garage, in production).
//!
//! Two producers use this today: the archiver's Parquet exports and the
//! controller's gossip graph dumps. Both write a finished file to a local
//! staging directory first; this module streams that file to the bucket and,
//! optionally, removes the staging copy afterwards.
//!
//! Design notes worth knowing before changing anything here:
//!
//! * **Bulk IO is sync `std::fs` inside a single `spawn_blocking`.** `tokio::fs`
//!   in a copy loop dispatches one blocking task per 8 KiB and blew up the
//!   tokio-console aggregator once already (see `.claude/rules/rust.md`).
//! * **Memory ceiling.** Per in-flight upload, peak resident bytes are roughly
//!   `part_size * (2 queued + 1 in-transit + max_concurrency)` — with the
//!   defaults (8 MiB parts, concurrency 4) about 56 MiB, and that figure does
//!   not grow with file size. A multi-GB Parquet file and a 112 MB graph dump
//!   cost the same. Process-wide the ceiling is `workers` times that; the
//!   archiver awaits [`ArtifactUploader::publish`] inline so its ceiling is
//!   one upload.
//! * **Nothing deletes a local file without an [`UploadReceipt`]**, which only
//!   [`ArtifactUploader::publish`] can build, and only after the object's
//!   size on the remote has been read back and compared. Existence is not
//!   validity — the same rule the Parquet export recovery path follows.

use std::collections::HashMap;
use std::fmt;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use futures_util::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::{
    BackoffConfig, ClientOptions, ObjectStore, ObjectStoreExt, PutPayload, RetryConfig,
    WriteMultipart,
};

/// An object key. Re-exported so callers can name one without depending on
/// `object_store` themselves.
pub use object_store::path::Path as ObjectKey;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// S3 requires every non-final multipart part to be at least 5 MiB.
const MIN_PART_MIB: u32 = 5;
/// Bytes queued between the blocking reader and the async multipart writer.
/// Small on purpose — this is the backpressure that bounds memory.
const READ_QUEUE_PARTS: usize = 2;

/// Which `object_store` backend to build.
///
/// `Local` is for tests and `*_MODE=local` runs: it exercises the identical
/// upload / verify / delete path with no network and no fake S3 server. It is
/// *not* the production "don't upload" switch — that is
/// [`UploadConfig::enabled`], which leaves the callers' plain `std::fs` paths
/// untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UploadBackend {
    #[default]
    S3,
    Local,
}

/// Publishing artifacts that were written to disk before uploads were enabled,
/// or whose upload failed, a bounded slice at a time.
///
/// **There is deliberately no deletion setting here.** A backfill sweep never
/// removes a local file, whatever [`UploadConfig::delete_local`] says -- see
/// [`ArtifactUploader::without_local_deletion`], which is how callers enforce
/// it. An earlier design had its own flag, which meant a file whose *live*
/// upload failed would be retried by the sweep and then deleted under a
/// different policy than its siblings, depending on nothing more than which
/// path happened to publish it. One unconditional rule is worth more than the
/// flexibility: the asymmetry it leaves (a retried file lingers on disk when
/// `delete_local` is on) errs toward keeping data, and a stale local file is
/// harmless.
///
/// Nested under `[upload]` so one feature reads as one TOML section and
/// [`UploadConfig::defaults_source`] carries it to every crate for free.
/// Environment override: `CONTROLLER_UPLOAD__BACKFILL__ENABLED`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BackfillConfig {
    /// Master switch. Same posture as [`UploadConfig::enabled`]: merging must
    /// change nothing on a deployed host.
    pub enabled: bool,
    /// Seconds between sweeps. The loop sleeps *after* a pass rather than
    /// ticking, so an overrunning pass delays the next one instead of queueing.
    pub interval_secs: u32,
    /// Jitter on the first sweep, so a restart loop cannot drive a scan and
    /// sibling instances on one host don't sweep in lockstep. Same reasoning as
    /// `log_rotation.startup_delay_max_secs`.
    pub startup_delay_max_secs: u32,
    /// Upload attempts per pass. Caps request rate.
    pub max_files_per_pass: u32,
    /// Bytes per pass, in MiB. Caps bandwidth and pass wall-clock, and is what
    /// lets one default serve crates whose artifacts differ ~60x in size: a
    /// file count alone would authorize wildly different transfers.
    pub max_mib_per_pass: u32,
    /// Also sweep the live output directory, for files older than this many
    /// seconds. `0` disables it.
    ///
    /// The live path publishes inline and has no other retry, so without this a
    /// failed upload leaves a file unpublished forever. The age gate is what
    /// keeps the sweep from racing a publish still in flight: an hour is orders
    /// of magnitude wider than any write-then-publish window, so the two can
    /// never contend for the same file. Filesystem-only: the sweep's other
    /// directories are ones the live path never writes to, and need no gate.
    pub output_dir_min_age_secs: u32,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: 600,
            startup_delay_max_secs: 300,
            max_files_per_pass: 250,
            max_mib_per_pass: 1024,
            output_dir_min_age_secs: 3600,
        }
    }
}

/// Where generated artifacts are published, and whether the local staging copy
/// is removed afterwards.
///
/// Embedded by each service's config the same way `logging::LogRotationConfig`
/// is. Nested keys are settable from the environment (e.g.
/// `ARCHIVER_UPLOAD__BUCKET`) because every crate pins both `Environment`
/// separators.
#[derive(Clone, Deserialize, Serialize)]
pub struct UploadConfig {
    /// Master switch. False means no client is built and no network call is
    /// ever made; artifacts stay on local disk exactly as before.
    pub enabled: bool,
    /// `s3` (Garage) or `local`.
    #[serde(default)]
    pub backend: UploadBackend,
    /// Root directory for the `local` backend. Ignored when backend is `s3`.
    #[serde(default)]
    pub local_root: String,

    /// Garage S3 endpoint, as a full URL: `http://host:port` or
    /// `https://host`. The scheme is the single source of truth for whether
    /// plain HTTP is allowed — there is no separate `use_ssl` flag to
    /// contradict it. Checked by [`UploadConfig::validate`].
    pub endpoint: String,
    /// Bucket name. No default: there is no sensible shared one, and a typo'd
    /// or missing bucket should be a startup error rather than a surprise
    /// destination.
    pub bucket: String,
    /// S3 region string configured in Garage (a label, not a real AWS region).
    pub region: String,
    /// A single path segment prepended to every key this instance writes, e.g.
    /// `"staging"`, so instances sharing a bucket don't collide. Empty means
    /// keys start at the artifact root. Must not contain `/`: the rest of the
    /// key is owned by the caller's layout function, and letting config splice
    /// in extra path structure is how a layout quietly forks.
    pub key_prefix: String,

    /// Read+write S3 credentials, templated from vault into config.toml.
    /// Either being absent disables uploads (warn and degrade).
    ///
    /// Never serialized: this type is serialized to seed config defaults, and
    /// a credential must not be able to ride along into a dump or a log.
    #[serde(default, skip_serializing)]
    pub access_key_id: Option<String>,
    #[serde(default, skip_serializing)]
    pub secret_access_key: Option<String>,

    /// Delete the local file after a *verified* successful upload. Has no
    /// effect unless both the upload and the size read-back succeeded.
    pub delete_local: bool,

    /// Multipart part size in MiB. Files at or below this go via a single PUT.
    /// Clamped up to 5 MiB, the S3 minimum for non-final parts.
    pub multipart_mib: u32,
    /// Concurrent in-flight parts within one file.
    pub max_concurrency: u32,
    /// Per-request timeout handed to `ClientOptions::with_timeout`.
    pub request_timeout_secs: u32,
    pub connect_timeout_secs: u32,
    /// `object_store::RetryConfig::max_retries`.
    pub max_retries: u32,
    /// When true, a failed startup probe is a fatal startup error rather than
    /// a logged degradation. Default false: a transiently unreachable bucket
    /// must not boot-loop a data-collection daemon.
    pub required_on_startup: bool,

    /// Publishing the backlog already on disk. See [`BackfillConfig`].
    #[serde(default)]
    pub backfill: BackfillConfig,
}

impl Default for UploadConfig {
    fn default() -> Self {
        Self {
            // Off, and staging-copy-preserving, so merging the uploader changes
            // nothing on a deployed host until config says otherwise.
            enabled: false,
            backend: UploadBackend::S3,
            local_root: String::new(),
            // No default endpoint or bucket: both are deployment facts, and a
            // wrong guess would publish somewhere unintended.
            endpoint: String::new(),
            bucket: String::new(),
            region: "garage".to_string(),
            key_prefix: String::new(),
            access_key_id: None,
            secret_access_key: None,
            delete_local: false,
            multipart_mib: 8,
            max_concurrency: 4,
            // Generous: one part of a large Parquet file over tailscale.
            request_timeout_secs: 300,
            connect_timeout_secs: 15,
            max_retries: 3,
            required_on_startup: false,
            backfill: BackfillConfig::default(),
        }
    }
}

impl UploadConfig {
    /// The `[upload]` defaults as a config source, so each service's
    /// `config.rs` doesn't carry its own copy of them (they drifted the moment
    /// there were two).
    ///
    /// Add it *before* the TOML file and the environment so both still
    /// override it:
    ///
    /// ```ignore
    /// Config::builder()
    ///     .add_source(UploadConfig::defaults_source()?)
    ///     .add_source(File::with_name(&cfg_path).required(false))
    ///     .add_source(Environment::with_prefix("ARCHIVER")...)
    /// ```
    ///
    /// Credentials are `skip_serializing`, so this never contributes one.
    pub fn defaults_source() -> anyhow::Result<config::Config> {
        #[derive(Serialize)]
        struct Defaults {
            upload: UploadConfig,
        }
        config::Config::try_from(&Defaults {
            upload: Self::default(),
        })
        .context("building [upload] config defaults")
    }

    /// Reject a configuration that would misbehave later, once, at startup.
    ///
    /// Everything checked here is a deployment mistake rather than a runtime
    /// condition, so it is worth failing loudly instead of discovering it on
    /// the first export tick six hours in. Only called when uploads are
    /// enabled — a disabled config is allowed to be blank.
    pub fn validate(&self) -> anyhow::Result<()> {
        // A single segment, so config can namespace an instance but cannot
        // splice extra path structure into a code-owned key layout.
        if self.key_prefix.contains('/') {
            bail!(
                "upload.key_prefix must be a single path segment with no '/': got {:?}",
                self.key_prefix
            );
        }
        if !self.key_prefix.is_empty() {
            // This parse will remove leading or trailing '/' without failing.
            // Important in case the check above becomes less strict.
            ObjectKey::parse(&self.key_prefix)
                .with_context(|| format!("invalid upload.key_prefix {:?}", self.key_prefix))?;
        }

        match self.backend {
            UploadBackend::Local => {
                if self.local_root.is_empty() {
                    bail!("upload.backend is \"local\" but upload.local_root is empty");
                }
            }
            UploadBackend::S3 => {
                if self.bucket.is_empty() {
                    bail!("upload.enabled but upload.bucket is empty");
                }
                // The scheme decides whether plain HTTP is allowed, so it has
                // to be present and unambiguous rather than inferred.
                if !(self.endpoint.starts_with("http://") || self.endpoint.starts_with("https://"))
                {
                    bail!(
                        "upload.endpoint must start with http:// or https://, got {:?}",
                        self.endpoint
                    );
                }
            }
        }
        // Budgets are checked whenever uploads are on, NOT only when backfill
        // is enabled: the archiver's pending-export sweep runs off these fields
        // without consulting `backfill.enabled`, so gating the check there let
        // a zero budget through and then disabled both guards at runtime.
        if self.backfill.interval_secs == 0 {
            bail!("upload.backfill.interval_secs must be greater than 0");
        }
        // Zero deliberately does not mean "unlimited". An unbounded sweep has
        // no use case here and is exactly the footgun above.
        if self.backfill.max_files_per_pass == 0 {
            bail!("upload.backfill.max_files_per_pass must be greater than 0");
        }
        if self.backfill.max_mib_per_pass == 0 {
            bail!("upload.backfill.max_mib_per_pass must be greater than 0");
        }
        Ok(())
    }

    /// Whether the endpoint speaks plain HTTP. Only meaningful after
    /// [`Self::validate`] has confirmed the scheme is one of the two.
    fn allow_http(&self) -> bool {
        self.endpoint.starts_with("http://")
    }
}

// Hand-written so the secret never reaches a log file. The archiver logs its
// whole config via Debug at startup, and those logs are compressed and kept for
// 30 days. `access_key_id` is shown as present/absent but not by value: it is
// itself a Garage identifier, and "creds missing" has to stay diagnosable.
impl fmt::Debug for UploadConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadConfig")
            .field("enabled", &self.enabled)
            .field("backend", &self.backend)
            .field("local_root", &self.local_root)
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("key_prefix", &self.key_prefix)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "<set>"),
            )
            .field("secret_access_key", &"<redacted>")
            .field("delete_local", &self.delete_local)
            .field("multipart_mib", &self.multipart_mib)
            .field("max_concurrency", &self.max_concurrency)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("max_retries", &self.max_retries)
            .field("required_on_startup", &self.required_on_startup)
            .field("backfill", &self.backfill)
            .finish()
    }
}

/// What a caller already knows about an object's state on the remote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Presence {
    /// Nothing known -- [`ArtifactUploader::publish`] issues one HEAD first.
    #[default]
    Unknown,
    /// Known not to be there. Skips the HEAD; the right choice for a file that
    /// was just written under a unique name.
    Absent,
    /// Known remote size, e.g. from a bulk [`ArtifactUploader::list_keys`]. A
    /// match short-circuits the transfer at the cost of no requests at all.
    Size(u64),
}

/// Per-publish policy.
///
/// `delete_local` lives here rather than on [`ArtifactUploader`] on purpose: it
/// is a property of one publish, not of a connection to a bucket.
#[derive(Debug, Clone, Copy, Default)]
pub struct PublishOptions {
    pub delete_local: bool,
    pub presence: Presence,
}

impl PublishOptions {
    /// A file just written locally: certainly not on the remote yet.
    pub fn fresh(delete_local: bool) -> Self {
        Self {
            delete_local,
            presence: Presence::Absent,
        }
    }
}

/// The outcome of a publish, distinguishing work done from work skipped.
///
/// Callers budget on this: a short-circuit costs nothing, which is what keeps a
/// catch-up sweep cheap once everything is already published.
#[derive(Debug, Clone)]
pub enum Published {
    /// Already on the remote at the right size; no bytes moved.
    AlreadyPresent(UploadReceipt),
    Transferred(UploadReceipt),
}

impl Published {
    pub fn receipt(&self) -> &UploadReceipt {
        match self {
            Self::AlreadyPresent(r) | Self::Transferred(r) => r,
        }
    }

    /// Bytes actually put on the wire -- zero for a short-circuit.
    pub fn transferred_bytes(&self) -> u64 {
        match self {
            Self::AlreadyPresent(_) => 0,
            Self::Transferred(r) => r.bytes,
        }
    }

    pub fn uri(&self) -> &str {
        &self.receipt().uri
    }
}

/// How much a single sweep may move.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub max_files: u32,
    pub max_bytes: u64,
}

impl Budget {
    pub fn from_config(cfg: &BackfillConfig) -> Self {
        Self {
            max_files: cfg.max_files_per_pass,
            max_bytes: u64::from(cfg.max_mib_per_pass) * 1024 * 1024,
        }
    }
}

/// What one sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchTally {
    pub transferred: u32,
    pub transferred_bytes: u64,
    pub already_present: u32,
    pub failed: u32,
}

/// Budget accounting for a bounded publish sweep.
///
/// * a transfer costs one file and its bytes,
/// * an already-present object costs **nothing** -- this is what makes a
///   drained pass free, and what stops a catch-up tick re-charging itself for
///   work it did not do,
/// * a failure costs one file and no bytes, so a permanently broken file
///   consumes one slot per pass instead of stalling the sweep forever.
///
/// The budget is checked *before* each item, so a pass overshoots by at most
/// one file.
#[derive(Debug)]
pub struct BudgetTracker {
    budget: Budget,
    tally: BatchTally,
}

impl BudgetTracker {
    pub fn new(budget: Budget) -> Self {
        Self {
            budget,
            tally: BatchTally::default(),
        }
    }

    /// Whether there is budget left to attempt another item.
    pub fn may_attempt(&self) -> bool {
        let files = self.tally.transferred + self.tally.failed;
        files < self.budget.max_files && self.tally.transferred_bytes < self.budget.max_bytes
    }

    /// Charge one item's outcome.
    pub fn record(&mut self, outcome: &anyhow::Result<Published>) {
        match outcome {
            Ok(Published::Transferred(r)) => {
                self.tally.transferred += 1;
                self.tally.transferred_bytes += r.bytes;
            }
            Ok(Published::AlreadyPresent(_)) => self.tally.already_present += 1,
            Err(_) => self.tally.failed += 1,
        }
    }

    pub fn tally(&self) -> BatchTally {
        self.tally
    }
}

/// Proof that an object exists on the remote at the expected size.
///
/// Only [`ArtifactUploader::publish`] constructs one, and only after the
/// read-back check. Callers record `uri` and may treat the artifact as
/// published once they hold this.
#[derive(Debug, Clone)]
pub struct UploadReceipt {
    /// Full object key, including `key_prefix`.
    pub key: String,
    /// `s3://{bucket}/{key}` — the form recorded in the DB and consumed
    /// directly by DuckDB httpfs.
    pub uri: String,
    pub bytes: u64,
    pub deleted_local: bool,
}

/// A configured, connection-tested handle to one bucket. Cheap to clone (`Arc`
/// inside); build one per process and share it.
#[derive(Clone)]
pub struct ArtifactUploader {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    /// Parsed once in [`ArtifactUploader::new`]; `None` means keys start at the
    /// artifact root. Holding the parsed form is what lets `key_for`, `probe`
    /// and `publish` stop re-parsing and re-validating a config string.
    key_prefix: Option<ObjectKey>,
    part_size: usize,
    max_concurrency: usize,
}

impl fmt::Debug for ArtifactUploader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArtifactUploader")
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_prefix.as_ref().map(|p| p.as_ref()))
            .field("part_size", &self.part_size)
            .field("max_concurrency", &self.max_concurrency)
            .finish()
    }
}

impl ArtifactUploader {
    /// Build a client from config.
    ///
    /// Returns `Ok(None)` — logged, never an error — when uploads are disabled
    /// or credentials are absent, matching how the archiver's retention pass
    /// degrades when its read-only credentials are unset. Performs no IO; call
    /// [`Self::probe`] for that.
    pub fn new(cfg: &UploadConfig) -> anyhow::Result<Option<Self>> {
        if !cfg.enabled {
            return Ok(None);
        }

        // Everything structural is checked once, here, so the accessors below
        // can be infallible and no method has to re-derive whether the config
        // made sense.
        cfg.validate()?;

        let store: Arc<dyn ObjectStore> = match cfg.backend {
            UploadBackend::Local => {
                std::fs::create_dir_all(&cfg.local_root)
                    .with_context(|| format!("creating upload.local_root {}", cfg.local_root))?;
                Arc::new(
                    LocalFileSystem::new_with_prefix(&cfg.local_root)
                        .context("building local object store")?,
                )
            }
            UploadBackend::S3 => {
                // Missing credentials are normally an operational state, not
                // a malformed config, so the default is to degrade rather than
                // refuse to start. But `required_on_startup` is a statement
                // that uploads must work, and silently publishing nothing is
                // the failure it exists to prevent -- so honour it here too,
                // not only for an unreachable bucket.
                let (Some(key_id), Some(secret)) = (&cfg.access_key_id, &cfg.secret_access_key)
                else {
                    if cfg.required_on_startup {
                        bail!(
                            "upload.required_on_startup is set but S3 credentials are not \
                             configured for bucket {}",
                            cfg.bucket
                        );
                    }
                    warn!(
                        bucket = cfg.bucket,
                        "upload.enabled but S3 credentials are not set; \
                         artifacts will stay on local disk"
                    );
                    return Ok(None);
                };

                // Explicit rather than defaulted so the total retry envelope is
                // a known quantity relative to each caller's cadence (the
                // archiver ticks every export.interval_secs, the controller on
                // a cron). Crate defaults are max_retries 10 / retry_timeout
                // 3 min / max_backoff 15s.
                let retry = RetryConfig {
                    backoff: BackoffConfig {
                        init_backoff: Duration::from_millis(200),
                        max_backoff: Duration::from_secs(15),
                        base: 2.0,
                    },
                    max_retries: cfg.max_retries as usize,
                    retry_timeout: Duration::from_secs(cfg.request_timeout_secs.into()),
                };

                Arc::new(
                    AmazonS3Builder::new()
                        .with_endpoint(&cfg.endpoint)
                        // Garage speaks path-style; virtual-hosted addressing
                        // would resolve bucket.host and fail.
                        .with_virtual_hosted_style_request(false)
                        .with_region(&cfg.region)
                        .with_bucket_name(&cfg.bucket)
                        .with_access_key_id(key_id)
                        .with_secret_access_key(secret)
                        // allow_http MUST be set on the ClientOptions passed
                        // here, not via the builder's `with_allow_http`:
                        // `with_client_options` replaces the whole
                        // ClientOptions ("overriding any already set").
                        .with_client_options(
                            ClientOptions::new()
                                .with_allow_http(cfg.allow_http())
                                .with_timeout(Duration::from_secs(cfg.request_timeout_secs.into()))
                                .with_connect_timeout(Duration::from_secs(
                                    cfg.connect_timeout_secs.into(),
                                )),
                        )
                        .with_retry(retry)
                        .build()
                        .context("building S3 object store")?,
                )
            }
        };

        Ok(Some(Self::from_store(store, cfg)?))
    }

    /// Wrap an already-built store. Used by [`Self::new`] and by tests, which
    /// drive an in-memory backend through the same code path.
    fn from_store(store: Arc<dyn ObjectStore>, cfg: &UploadConfig) -> anyhow::Result<Self> {
        let part_mib = cfg.multipart_mib.max(MIN_PART_MIB) as usize;
        let key_prefix = if cfg.key_prefix.is_empty() {
            None
        } else {
            Some(
                ObjectKey::parse(&cfg.key_prefix)
                    .with_context(|| format!("invalid upload.key_prefix {:?}", cfg.key_prefix))?,
            )
        };
        Ok(Self {
            store,
            bucket: cfg.bucket.clone(),
            key_prefix,
            part_size: part_mib * 1024 * 1024,
            max_concurrency: (cfg.max_concurrency.max(1)) as usize,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Verify the bucket is reachable and our credentials are accepted.
    ///
    /// This must be a *signed* request: Caddy fronts Garage and returns 404 to
    /// anything without an `Authorization` header, so an unsigned probe would
    /// "succeed" as a 404 and prove nothing. `list_with_delimiter` is signed
    /// and reads one page.
    pub async fn probe(&self) -> anyhow::Result<()> {
        debug!(bucket = self.bucket, "Probing artifact bucket");
        self.store
            .list_with_delimiter(self.key_prefix.as_ref())
            .await
            .with_context(|| format!("listing bucket {}", self.bucket))?;
        Ok(())
    }

    /// The object key for a caller-built relative path: the configured prefix,
    /// then `rel`.
    ///
    /// The only fallible step is `rel` itself, since the prefix was validated
    /// and parsed at construction. `ObjectKey::parse` rejects `..` and
    /// normalizes duplicate slashes, which is the validation a caller-built
    /// path wants.
    pub fn key_for(&self, rel: &FsPath) -> anyhow::Result<ObjectKey> {
        let rel = rel
            .to_str()
            .ok_or_else(|| anyhow!("artifact path is not valid UTF-8: {}", rel.display()))?;
        let rel =
            ObjectKey::parse(rel).with_context(|| format!("invalid artifact path {rel:?}"))?;
        Ok(match &self.key_prefix {
            Some(prefix) => prefix.parts().chain(rel.parts()).collect(),
            None => rel,
        })
    }

    /// `s3://{bucket}/{key}` — the form recorded in the DB and consumed
    /// directly by DuckDB httpfs.
    pub fn uri_for(&self, key: &ObjectKey) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }

    /// Size of an existing object, or `None` if it is absent.
    pub async fn head_size(&self, key: &ObjectKey) -> anyhow::Result<Option<u64>> {
        match self.store.head(key).await {
            Ok(meta) => Ok(Some(meta.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(anyhow::Error::new(e).context(format!("HEAD {key}"))),
        }
    }

    /// Every object under `rel_prefix`, as full keys mapped to their size in
    /// bytes. `None` lists everything under the configured `key_prefix`.
    ///
    /// The bulk form of [`Self::head_size`], and the reason a backfill sweep is
    /// affordable: the S3 client paginates internally at 1000 keys a request,
    /// so tens of thousands of objects cost tens of signed GETs rather than
    /// tens of thousands of HEADs.
    ///
    /// Three properties worth keeping:
    ///
    /// * **Full keys, not stripped relative ones.** [`Self::key_for`] is the
    ///   one place `key_prefix` is applied, so a caller diffs with
    ///   `listing.get(&self.key_for(rel)?)` and the prefix stays applied in one
    ///   direction by one function. Stripping instead would add a failure
    ///   branch that cannot happen and is therefore never tested.
    /// * **Sizes, not just presence.** A truncated multipart leaves an object
    ///   that exists at the wrong length; a presence-only check would adopt it
    ///   forever. Same "existence is not validity" rule the rest of this module
    ///   follows.
    /// * **All-or-nothing.** A failed page aborts the call rather than
    ///   returning a partial map: a partial listing is indistinguishable from
    ///   "those objects do not exist", and the only caller answers that by
    ///   re-uploading. Retrying next pass is cheap; re-sending the backlog is
    ///   not.
    ///
    /// Like [`Self::probe`] this is a signed request, so the Caddy-404s-
    /// unsigned-requests trap does not apply -- don't "optimize" it into one.
    pub async fn list_keys(
        &self,
        rel_prefix: Option<&FsPath>,
    ) -> anyhow::Result<HashMap<ObjectKey, u64>> {
        let prefix = match rel_prefix {
            Some(rel) => Some(self.key_for(rel)?),
            None => self.key_prefix.clone(),
        };
        debug!(bucket = self.bucket, ?prefix, "Listing published objects");

        let mut out = HashMap::new();
        let mut stream = self.store.list(prefix.as_ref());
        while let Some(meta) = stream.next().await {
            let meta = meta.with_context(|| match &prefix {
                Some(p) => format!("listing {} under {p}", self.bucket),
                None => format!("listing {}", self.bucket),
            })?;
            out.insert(meta.location, meta.size);
        }
        Ok(out)
    }

    /// Publish `local` to the key derived from `rel_key`.
    ///
    /// The one upload primitive. Both the live path (publish what was just
    /// written) and a backfill sweep (publish a backlog) go through here; they
    /// differ only in the options they pass.
    ///
    /// Sequence: derive the key, stat the local file, decide from
    /// `opts.presence` whether a transfer is needed at all, transfer if so,
    /// read the size back, then optionally delete.
    ///
    /// The read-back is not optional and never skipped on a transfer: existence
    /// is not validity -- a truncated multipart is still an object -- and it is
    /// what earns the right to delete the local copy. Deletion runs for an
    /// already-present object too, so a caller that short-circuits gets exactly
    /// the same end state as one that transferred.
    pub async fn publish(
        &self,
        local: &FsPath,
        rel_key: &FsPath,
        opts: &PublishOptions,
    ) -> anyhow::Result<Published> {
        let key = self.key_for(rel_key)?;
        let local_len = tokio::fs::metadata(local)
            .await
            .with_context(|| format!("stat {}", local.display()))?
            .len();

        // Does anything actually need to move?
        let remote_len = match opts.presence {
            Presence::Absent => None,
            Presence::Size(len) => Some(len),
            Presence::Unknown => self.head_size(&key).await?,
        };
        let already_present = remote_len == Some(local_len);

        if !already_present {
            if local_len <= self.part_size as u64 {
                self.put_whole(local, &key).await?;
            } else {
                self.put_multipart(local, &key).await?;
            }
            let written = self
                .head_size(&key)
                .await?
                .ok_or_else(|| anyhow!("object {key} missing immediately after upload"))?;
            if written != local_len {
                bail!(
                    "size mismatch for {key}: local {local_len} B, remote {written} B; \
                     leaving local file in place"
                );
            }
        }

        let mut deleted_local = false;
        if opts.delete_local {
            match tokio::fs::remove_file(local).await {
                Ok(()) => deleted_local = true,
                // A stale local file is harmless -- the object is safely up, so
                // this is not worth failing the receipt over.
                Err(e) => warn!(
                    path = %local.display(),
                    error = %e,
                    "Published but could not remove the local file"
                ),
            }
        }

        let receipt = UploadReceipt {
            uri: self.uri_for(&key),
            key: key.as_ref().to_string(),
            bytes: local_len,
            deleted_local,
        };
        if already_present {
            debug!(%key, bytes = local_len, deleted_local, "Artifact already published");
            Ok(Published::AlreadyPresent(receipt))
        } else {
            info!(%key, bytes = local_len, deleted_local, "Published artifact");
            Ok(Published::Transferred(receipt))
        }
    }

    /// Small file: one blocking read, one PUT.
    async fn put_whole(&self, local: &FsPath, path: &ObjectKey) -> anyhow::Result<()> {
        let owned = local.to_owned();
        let body = tokio::task::spawn_blocking(move || std::fs::read(&owned))
            .await
            .context("read task panicked")?
            .with_context(|| format!("reading {}", local.display()))?;
        self.store
            .put(path, PutPayload::from(body))
            .await
            .with_context(|| format!("PUT {path}"))?;
        Ok(())
    }

    /// Large file: one blocking reader feeding an async multipart writer.
    ///
    /// The reader is a *single* `spawn_blocking` using sync `std::fs`, and
    /// `blocking_send` on a 2-slot channel is what supplies backpressure — the
    /// read rate can never outrun the upload rate, so nothing accumulates.
    async fn put_multipart(&self, local: &FsPath, path: &ObjectKey) -> anyhow::Result<()> {
        use std::io::Read;

        let part_size = self.part_size;
        let owned = local.to_owned();
        let (tx, mut rx) = mpsc::channel::<Bytes>(READ_QUEUE_PARTS);

        let reader = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut file = std::fs::File::open(&owned)?;
            loop {
                let mut buf = vec![0u8; part_size];
                let mut filled = 0;
                // Fill a whole part before handing it over; `read` is free to
                // return short and every non-final part must be exactly
                // part_size for S3 to accept it.
                while filled < part_size {
                    match file.read(&mut buf[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }
                if filled == 0 {
                    return Ok(());
                }
                buf.truncate(filled);
                // Receiver gone means the upload already failed; stop quietly
                // and let that error be the one reported.
                if tx.blocking_send(Bytes::from(buf)).is_err() {
                    return Ok(());
                }
            }
        });

        let upload = self
            .store
            .put_multipart(path)
            .await
            .with_context(|| format!("initiating multipart upload for {path}"))?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, part_size);

        let mut write_err = None;
        while let Some(chunk) = rx.recv().await {
            if let Err(e) = writer.wait_for_capacity(self.max_concurrency).await {
                write_err =
                    Some(anyhow::Error::new(e).context(format!("uploading part of {path}")));
                break;
            }
            writer.put(chunk);
        }
        drop(rx);

        // Surface a read failure ahead of a write failure: a truncated source
        // is the more actionable diagnosis.
        let read_result = reader.await.context("read task panicked")?;

        if let Some(e) = write_err {
            abort_quietly(writer, path).await;
            return Err(e);
        }
        if let Err(e) = read_result {
            abort_quietly(writer, path).await;
            return Err(anyhow::Error::new(e).context(format!("reading {}", local.display())));
        }

        writer
            .finish()
            .await
            .with_context(|| format!("completing multipart upload for {path}"))?;
        Ok(())
    }
}

/// Abort a multipart upload so Garage does not retain orphan parts forever —
/// there is no lifecycle expiry configured on the cluster. Best effort; the
/// caller already has a more interesting error to report.
async fn abort_quietly(writer: WriteMultipart, path: &ObjectKey) {
    if let Err(e) = writer.abort().await {
        warn!(%path, error = %e, "Failed to abort multipart upload; parts may be orphaned");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::io::Write;
    use std::path::PathBuf;

    /// The S3 minimum part size, which is also the floor `from_store` clamps
    /// to — the smallest value that still exercises the real multipart path.
    const TEST_PART_MIB: u32 = MIN_PART_MIB;
    const TEST_PART: usize = TEST_PART_MIB as usize * 1024 * 1024;

    fn cfg(key_prefix: &str, delete_local: bool) -> UploadConfig {
        UploadConfig {
            enabled: true,
            backend: UploadBackend::Local,
            local_root: String::new(),
            endpoint: String::new(),
            bucket: "test-bucket".to_string(),
            region: "garage".to_string(),
            key_prefix: key_prefix.to_string(),
            access_key_id: None,
            secret_access_key: None,
            delete_local,
            multipart_mib: TEST_PART_MIB,
            max_concurrency: 2,
            request_timeout_secs: 30,
            connect_timeout_secs: 5,
            max_retries: 0,
            required_on_startup: false,
            backfill: BackfillConfig::default(),
        }
    }

    fn uploader(key_prefix: &str) -> (ArtifactUploader, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let up = ArtifactUploader::from_store(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &cfg(key_prefix, false),
        )
        .expect("build uploader");
        (up, store)
    }

    /// `publish` with the old `upload_file` shape: transfer unconditionally,
    /// don't delete. Keeps the pre-existing round-trip tests expressing what
    /// they always did.
    async fn upload(up: &ArtifactUploader, local: &FsPath, rel: &FsPath) -> UploadReceipt {
        up.publish(local, rel, &PublishOptions::fresh(false))
            .await
            .expect("publish")
            .receipt()
            .clone()
    }

    fn write_file(dir: &tempfile::TempDir, name: &str, len: usize) -> (PathBuf, Vec<u8>) {
        let path = dir.path().join(name);
        // Non-uniform bytes so a truncated or reordered upload is detectable.
        let body: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let mut f = std::fs::File::create(&path).expect("create temp file");
        f.write_all(&body).expect("write temp file");
        (path, body)
    }

    #[tokio::test]
    async fn small_file_round_trips_via_single_put() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (local, body) = write_file(&dir, "small.parquet", 4096);
        let (up, store) = uploader("");

        let receipt = upload(
            &up,
            &local,
            FsPath::new("timings/2026/03/30/timings.parquet"),
        )
        .await;

        assert_eq!(receipt.key, "timings/2026/03/30/timings.parquet");
        assert_eq!(
            receipt.uri,
            "s3://test-bucket/timings/2026/03/30/timings.parquet"
        );
        assert_eq!(receipt.bytes, body.len() as u64);
        assert!(!receipt.deleted_local);
        assert!(local.exists(), "delete_local is off");

        let got = store
            .get(&ObjectKey::parse(&receipt.key).expect("key"))
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(got.as_ref(), body.as_slice());
    }

    #[tokio::test]
    async fn large_file_round_trips_via_multipart_including_short_final_part() {
        let dir = tempfile::tempdir().expect("tempdir");
        // 2.5 parts: exercises full parts plus a short final one.
        let (local, body) = write_file(&dir, "big.zst", TEST_PART * 2 + TEST_PART / 2);
        let (up, store) = uploader("");

        let receipt = upload(&up, &local, FsPath::new("controller_graph/g.zst")).await;
        assert_eq!(receipt.bytes, body.len() as u64);

        let got = store
            .get(&ObjectKey::parse(&receipt.key).expect("key"))
            .await
            .expect("get")
            .bytes()
            .await
            .expect("bytes");
        assert_eq!(got.len(), body.len());
        assert_eq!(got.as_ref(), body.as_slice(), "bytes must survive intact");
    }

    #[tokio::test]
    async fn delete_local_removes_the_staging_file_only_after_success() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (local, _) = write_file(&dir, "small.parquet", 1024);
        let (up, _store) = uploader("");

        let published = up
            .publish(
                &local,
                FsPath::new("timings/t.parquet"),
                &PublishOptions::fresh(true),
            )
            .await
            .expect("publish");
        assert!(published.receipt().deleted_local);
        assert!(!local.exists(), "staging file should be gone");
    }

    /// Deletion is a property of the publish, not of the uploader: the same
    /// handle keeps or removes the file purely on what the caller asks for.
    /// This is what replaced `without_local_deletion`.
    #[tokio::test]
    async fn deletion_is_per_call_not_per_uploader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, _store) = uploader("");

        let (keep, _) = write_file(&dir, "keep.zst", 64);
        up.publish(
            &keep,
            FsPath::new("g/keep.zst"),
            &PublishOptions::fresh(false),
        )
        .await
        .expect("publish");
        assert!(keep.exists(), "delete_local was false for this call");

        let (drop, _) = write_file(&dir, "drop.zst", 64);
        up.publish(
            &drop,
            FsPath::new("g/drop.zst"),
            &PublishOptions::fresh(true),
        )
        .await
        .expect("publish");
        assert!(!drop.exists(), "delete_local was true for this call");
    }

    /// A short-circuit must reach the same end state as a transfer, or a caller
    /// that skips the upload would silently keep a file the transferring path
    /// would have removed.
    #[tokio::test]
    async fn an_already_present_object_still_honours_delete_local() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, _store) = uploader("");
        let rel = FsPath::new("timings/t.parquet");
        let (local, body) = write_file(&dir, "t.parquet", 512);

        up.publish(&local, rel, &PublishOptions::fresh(false))
            .await
            .expect("first publish");
        assert!(local.exists());

        let published = up
            .publish(
                &local,
                rel,
                &PublishOptions {
                    delete_local: true,
                    presence: Presence::Size(body.len() as u64),
                },
            )
            .await
            .expect("second publish");
        assert!(
            matches!(published, Published::AlreadyPresent(_)),
            "a matching known size must short-circuit"
        );
        assert_eq!(published.transferred_bytes(), 0);
        assert!(
            !local.exists(),
            "short-circuit must still delete when asked"
        );
    }

    /// A stale `Presence::Size` must not be trusted into skipping a needed
    /// upload -- a mismatch falls through to a real transfer.
    #[tokio::test]
    async fn a_mismatched_known_size_still_transfers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, _store) = uploader("");
        let (local, body) = write_file(&dir, "t.parquet", 512);

        let published = up
            .publish(
                &local,
                FsPath::new("timings/t.parquet"),
                &PublishOptions {
                    delete_local: false,
                    presence: Presence::Size(9999),
                },
            )
            .await
            .expect("publish");
        assert!(matches!(published, Published::Transferred(_)));
        assert_eq!(published.transferred_bytes(), body.len() as u64);
    }

    #[tokio::test]
    async fn failed_upload_leaves_the_local_file_in_place() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (local, _) = write_file(&dir, "small.parquet", 1024);

        // A read-only LocalFileSystem root makes every write fail.
        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(LocalFileSystem::new_with_prefix(root.path()).expect("local store"));
        let mut perms = std::fs::metadata(root.path()).expect("meta").permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o500);
        }
        std::fs::set_permissions(root.path(), perms).expect("chmod");

        let up = ArtifactUploader::from_store(store as Arc<dyn ObjectStore>, &cfg("", true))
            .expect("build uploader");
        let err = up
            .publish(
                &local,
                FsPath::new("timings/t.parquet"),
                &PublishOptions::fresh(true),
            )
            .await
            .expect_err("write to a read-only root must fail");

        assert!(
            local.exists(),
            "local file must survive a failed upload: {err:?}"
        );
    }

    #[tokio::test]
    async fn key_prefix_is_applied() {
        let rel = FsPath::new("timings/2026/03/30/timings_2026-03-30.parquet");

        let (bare, _) = uploader("");
        assert_eq!(
            bare.key_for(rel).expect("key").as_ref(),
            "timings/2026/03/30/timings_2026-03-30.parquet"
        );

        let (prefixed, _) = uploader("staging");
        assert_eq!(
            prefixed.key_for(rel).expect("key").as_ref(),
            "staging/timings/2026/03/30/timings_2026-03-30.parquet"
        );
    }

    #[tokio::test]
    async fn key_for_rejects_parent_traversal() {
        let (up, _) = uploader("staging");
        assert!(up.key_for(FsPath::new("../escape.parquet")).is_err());
    }

    /// A prefix carrying its own path structure is rejected up front rather
    /// than silently extending a code-owned key layout.
    #[test]
    fn validate_rejects_a_multi_segment_key_prefix() {
        for bad in ["staging/", "/staging", "a/b"] {
            let mut c = cfg(bad, false);
            c.backend = UploadBackend::Local;
            c.local_root = "/tmp".to_string();
            let err = c
                .validate()
                .expect_err("multi-segment prefix must be rejected");
            assert!(err.to_string().contains("key_prefix"), "{err}");
        }
    }

    /// The endpoint scheme is the only thing deciding plain HTTP vs TLS, so a
    /// scheme-less endpoint is a hard error rather than an assumed default.
    #[test]
    fn validate_requires_an_endpoint_scheme() {
        let mut c = cfg("", false);
        c.backend = UploadBackend::S3;
        c.endpoint = "do-medium:3939".to_string();
        let err = c.validate().expect_err("bare host:port must be rejected");
        assert!(err.to_string().contains("http://"), "{err}");

        c.endpoint = "http://do-medium:3939".to_string();
        c.validate().expect("plain http endpoint is valid");
        assert!(c.allow_http());

        c.endpoint = "https://data.example.org".to_string();
        c.validate().expect("https endpoint is valid");
        assert!(!c.allow_http());
    }

    #[test]
    fn validate_requires_a_bucket_for_s3() {
        let mut c = cfg("", false);
        c.backend = UploadBackend::S3;
        c.endpoint = "http://do-medium:3939".to_string();
        c.bucket = String::new();
        assert!(c.validate().is_err());
    }

    /// The shared defaults must deserialize back into exactly `Default`, and
    /// must never carry a credential.
    #[test]
    fn defaults_source_round_trips_and_omits_credentials() {
        let source = UploadConfig::defaults_source().expect("defaults source");
        let built: UploadConfig = config::Config::builder()
            .add_source(source)
            .build()
            .expect("build")
            .get("upload")
            .expect("deserialize [upload]");

        let want = UploadConfig::default();
        assert!(!built.enabled);
        assert!(!built.delete_local);
        assert_eq!(built.multipart_mib, want.multipart_mib);
        assert_eq!(built.max_concurrency, want.max_concurrency);
        assert_eq!(built.region, want.region);
        assert!(built.access_key_id.is_none());
        assert!(built.secret_access_key.is_none());
    }

    /// The nested section must survive the defaults round-trip too -- it is
    /// serialized through the same path and would silently vanish if the
    /// `#[serde(default)]` or the Serialize derive were wrong.
    #[test]
    fn defaults_source_carries_the_backfill_section() {
        let built: UploadConfig = config::Config::builder()
            .add_source(UploadConfig::defaults_source().expect("defaults"))
            .build()
            .expect("build")
            .get("upload")
            .expect("deserialize [upload]");

        let want = BackfillConfig::default();
        assert!(!built.backfill.enabled);
        assert_eq!(built.backfill.interval_secs, want.interval_secs);
        assert_eq!(built.backfill.max_files_per_pass, want.max_files_per_pass);
        assert_eq!(built.backfill.max_mib_per_pass, want.max_mib_per_pass);
    }

    /// Three-level env nesting (`PREFIX_UPLOAD__BACKFILL__ENABLED`).
    ///
    /// Worth an explicit test: nested env keys were silently ignored on two of
    /// these crates until the separators were pinned, and the failure mode is
    /// that the field quietly falls back to its default. This adds a level
    /// nobody had exercised.
    #[test]
    fn three_level_env_nesting_reaches_the_backfill_section() {
        let source = config::Environment::with_prefix("UPTEST")
            .prefix_separator("_")
            .separator("__")
            .source(Some(
                [
                    (
                        "UPTEST_UPLOAD__BACKFILL__ENABLED".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "UPTEST_UPLOAD__BACKFILL__MAX_FILES_PER_PASS".to_string(),
                        "7".to_string(),
                    ),
                ]
                .into_iter()
                .collect(),
            ));

        let built: UploadConfig = config::Config::builder()
            .add_source(UploadConfig::defaults_source().expect("defaults"))
            .add_source(source)
            .build()
            .expect("build")
            .get("upload")
            .expect("deserialize [upload]");

        assert!(built.backfill.enabled, "three-level env key was ignored");
        assert_eq!(built.backfill.max_files_per_pass, 7);
        // Untouched neighbours still come from the defaults.
        assert_eq!(
            built.backfill.interval_secs,
            BackfillConfig::default().interval_secs
        );
    }

    /// A zero budget is a config mistake, not a way to pause -- and it is
    /// checked whether or not `backfill.enabled` is set, because the archiver's
    /// sweep reads these fields without consulting that flag. Gating the check
    /// on it is what let a zero budget through and then disabled both guards.
    #[test]
    fn validate_rejects_a_budget_that_can_never_progress() {
        let mut c = cfg("", false);
        c.backend = UploadBackend::Local;
        c.local_root = "/tmp".to_string();

        for enabled in [false, true] {
            c.backfill = BackfillConfig {
                enabled,
                max_files_per_pass: 0,
                ..BackfillConfig::default()
            };
            let err = c
                .validate()
                .expect_err("a zero file budget must be rejected regardless of `enabled`");
            assert!(err.to_string().contains("max_files_per_pass"), "{err}");

            c.backfill.max_files_per_pass = 1;
            c.backfill.max_mib_per_pass = 0;
            let err = c
                .validate()
                .expect_err("a zero byte budget must be rejected");
            assert!(err.to_string().contains("max_mib_per_pass"), "{err}");

            c.backfill.max_mib_per_pass = 1;
            c.validate().expect("both budgets positive is valid");

            c.backfill.interval_secs = 0;
            assert!(c.validate().is_err(), "a zero interval would spin");
        }
    }

    /// The redacting Debug is hand-written, so a new field is invisible in the
    /// startup config dump unless someone remembers to add it.
    #[test]
    fn debug_output_includes_backfill_and_still_hides_the_secret() {
        let mut c = cfg("", false);
        c.access_key_id = Some("GKsomething".to_string());
        c.secret_access_key = Some("super-secret".to_string());
        c.backfill.enabled = true;
        let rendered = format!("{c:?}");
        assert!(
            rendered.contains("backfill"),
            "backfill missing from Debug: {rendered}"
        );
        assert!(
            !rendered.contains("super-secret"),
            "secret leaked: {rendered}"
        );
        assert!(
            !rendered.contains("GKsomething"),
            "key id leaked: {rendered}"
        );
    }

    /// A config file must be able to override the defaults source.
    #[test]
    fn config_file_overrides_the_defaults_source() {
        let built: UploadConfig = config::Config::builder()
            .add_source(UploadConfig::defaults_source().expect("defaults"))
            .add_source(config::File::from_str(
                "[upload]\nenabled = true\nbucket = \"b\"\nmax_concurrency = 7\n",
                config::FileFormat::Toml,
            ))
            .build()
            .expect("build")
            .get("upload")
            .expect("deserialize [upload]");

        assert!(built.enabled);
        assert_eq!(built.bucket, "b");
        assert_eq!(built.max_concurrency, 7);
        // Untouched keys still come from the defaults.
        assert_eq!(built.multipart_mib, UploadConfig::default().multipart_mib);
    }

    #[tokio::test]
    async fn head_size_reports_absence_and_presence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (local, body) = write_file(&dir, "small.parquet", 777);
        let (up, _store) = uploader("prod");

        let absent = up.key_for(FsPath::new("nope.parquet")).expect("key");
        assert_eq!(up.head_size(&absent).await.expect("head"), None);
        let receipt = upload(&up, &local, FsPath::new("timings/t.parquet")).await;
        let present = ObjectKey::parse(&receipt.key).expect("key");
        assert_eq!(
            up.head_size(&present).await.expect("head"),
            Some(body.len() as u64)
        );
    }

    #[tokio::test]
    async fn list_keys_returns_every_object_with_its_size() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, _store) = uploader("");

        let mut want = Vec::new();
        for (i, len) in [(0, 100usize), (1, 2048), (2, 64)] {
            let (local, _) = write_file(&dir, &format!("g{i}.zst"), len);
            let rel = PathBuf::from(format!("controller_graph/2026/03/30/g{i}.zst"));
            upload(&up, &local, &rel).await;
            want.push((up.key_for(&rel).expect("key"), len as u64));
        }

        let listing = up
            .list_keys(Some(FsPath::new("controller_graph")))
            .await
            .expect("list");
        assert_eq!(listing.len(), want.len());
        for (key, len) in want {
            assert_eq!(listing.get(&key), Some(&len), "wrong size for {key}");
        }
    }

    /// The pin for "return full keys, not stripped relative ones": a caller
    /// looks up exactly what `key_for` produces. Breaks the moment someone
    /// "helpfully" strips the prefix back off.
    #[tokio::test]
    async fn list_keys_returns_keys_that_match_key_for() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, _store) = uploader("staging");
        let rel = PathBuf::from("controller_graph/2026/03/30/g.zst");
        let (local, body) = write_file(&dir, "g.zst", 512);
        upload(&up, &local, &rel).await;

        let listing = up
            .list_keys(Some(FsPath::new("controller_graph")))
            .await
            .expect("list");
        let key = up.key_for(&rel).expect("key");
        assert_eq!(key.as_ref(), "staging/controller_graph/2026/03/30/g.zst");
        assert_eq!(listing.get(&key), Some(&(body.len() as u64)));
    }

    /// `collector` must not pick up `collector_diffs`.
    ///
    /// object_store documents prefix matching as segment-aware, and these two
    /// artifact families are exactly that edge case. A leak here would inflate
    /// the published count a backfill reports progress from.
    #[tokio::test]
    async fn list_keys_does_not_leak_across_sibling_prefixes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, _store) = uploader("");
        let (local, _) = write_file(&dir, "x.zst", 64);

        let collector = PathBuf::from("collector/2026/03/30/collector-abc-2026-03-30-14-05.zst");
        let diffs =
            PathBuf::from("collector_diffs/2026/03/30/collector_diffs-2026-03-30-14-05.zst");
        upload(&up, &local, &collector).await;
        upload(&up, &local, &diffs).await;

        let listing = up
            .list_keys(Some(FsPath::new("collector")))
            .await
            .expect("list");
        assert_eq!(
            listing.len(),
            1,
            "collector_diffs leaked into collector: {listing:?}"
        );
        assert!(listing.contains_key(&up.key_for(&collector).expect("key")));
    }

    /// A `None` prefix must still stay inside the configured `key_prefix`.
    #[tokio::test]
    async fn list_keys_confines_itself_to_the_key_prefix() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(InMemory::new());
        let up = ArtifactUploader::from_store(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &cfg("staging", false),
        )
        .expect("build uploader");

        let (local, _) = write_file(&dir, "g.zst", 32);
        upload(&up, &local, FsPath::new("controller_graph/g.zst")).await;
        // Straight into the store, outside our prefix.
        store
            .put(
                &ObjectKey::parse("other/z.zst").expect("key"),
                PutPayload::from(vec![0u8; 8]),
            )
            .await
            .expect("put");

        let listing = up.list_keys(None).await.expect("list");
        assert_eq!(listing.len(), 1, "listing escaped key_prefix: {listing:?}");
    }

    #[tokio::test]
    async fn list_keys_on_an_empty_prefix_is_empty_not_an_error() {
        let (up, _store) = uploader("");
        let listing = up
            .list_keys(Some(FsPath::new("controller_graph")))
            .await
            .expect("an empty prefix is not an error");
        assert!(listing.is_empty());
    }

    /// Ties the bulk path to the single path so the two cannot drift.
    #[tokio::test]
    async fn list_keys_agrees_with_head_size() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, _store) = uploader("prod");
        for i in 0..3 {
            let (local, _) = write_file(&dir, &format!("g{i}.zst"), 128 * (i + 1));
            upload(
                &up,
                &local,
                FsPath::new(&format!("collector/2026/03/30/g{i}.zst")),
            )
            .await;
        }

        let listing = up
            .list_keys(Some(FsPath::new("collector")))
            .await
            .expect("list");
        assert_eq!(listing.len(), 3);
        for (key, size) in &listing {
            assert_eq!(up.head_size(key).await.expect("head"), Some(*size));
        }
    }

    fn receipt(bytes: u64) -> UploadReceipt {
        UploadReceipt {
            key: "k".to_string(),
            uri: "s3://b/k".to_string(),
            bytes,
            deleted_local: false,
        }
    }

    /// The charging policy, which is the whole reason this is shared: two
    /// sweeps hand-rolled it, drifted, and one ended up able to disable both
    /// its guards.
    #[test]
    fn budget_charges_transfers_and_failures_but_not_short_circuits() {
        let mut t = BudgetTracker::new(Budget {
            max_files: 2,
            max_bytes: 1024,
        });
        assert!(t.may_attempt());

        // A short circuit is free: neither budget moves.
        t.record(&Ok(Published::AlreadyPresent(receipt(500))));
        assert!(
            t.may_attempt(),
            "an already-published file must not cost budget"
        );
        assert_eq!(t.tally().already_present, 1);
        assert_eq!(t.tally().transferred_bytes, 0);

        // A transfer costs a file and its bytes.
        t.record(&Ok(Published::Transferred(receipt(100))));
        assert_eq!(t.tally().transferred, 1);
        assert_eq!(t.tally().transferred_bytes, 100);
        assert!(t.may_attempt());

        // A failure costs a slot but no bandwidth, so one broken file cannot
        // stall a sweep forever.
        t.record(&Err(anyhow!("boom")));
        assert_eq!(t.tally().failed, 1);
        assert_eq!(t.tally().transferred_bytes, 100);
        assert!(!t.may_attempt(), "two files spent against a budget of two");
    }

    #[test]
    fn budget_stops_on_bytes_as_well_as_files() {
        let mut t = BudgetTracker::new(Budget {
            max_files: 100,
            max_bytes: 1000,
        });
        t.record(&Ok(Published::Transferred(receipt(999))));
        assert!(t.may_attempt(), "under the byte budget");
        t.record(&Ok(Published::Transferred(receipt(2))));
        assert!(!t.may_attempt(), "over the byte budget");
        // Checked before each item, so a pass overshoots by at most one file.
        assert_eq!(t.tally().transferred_bytes, 1001);
    }

    #[test]
    fn budget_from_config_converts_mib() {
        let b = Budget::from_config(&BackfillConfig {
            max_files_per_pass: 7,
            max_mib_per_pass: 3,
            ..BackfillConfig::default()
        });
        assert_eq!(b.max_files, 7);
        assert_eq!(b.max_bytes, 3 * 1024 * 1024);
    }

    #[tokio::test]
    async fn probe_succeeds_against_a_reachable_store() {
        let (up, _store) = uploader("prod");
        up.probe().await.expect("probe");
    }

    #[tokio::test]
    async fn disabled_config_builds_no_client() {
        let mut c = cfg("", false);
        c.enabled = false;
        assert!(ArtifactUploader::new(&c).expect("new").is_none());
    }

    #[tokio::test]
    async fn s3_backend_without_credentials_degrades_instead_of_failing() {
        let mut c = cfg("", false);
        c.backend = UploadBackend::S3;
        c.endpoint = "http://do-medium:3939".to_string();
        assert!(ArtifactUploader::new(&c).expect("new").is_none());
    }

    /// `required_on_startup` means uploads must work, so missing credentials
    /// are fatal rather than a silent degradation -- publishing nothing is
    /// exactly the failure the flag exists to prevent.
    #[tokio::test]
    async fn required_on_startup_makes_missing_credentials_fatal() {
        let mut c = cfg("", false);
        c.backend = UploadBackend::S3;
        c.endpoint = "http://do-medium:3939".to_string();
        c.required_on_startup = true;
        let err = ArtifactUploader::new(&c).expect_err("missing creds must be fatal");
        assert!(err.to_string().contains("credentials"), "{err}");
    }

    /// A plain-HTTP endpoint must actually reach the network layer.
    ///
    /// Regression test for a builder-ordering trap: `with_client_options`
    /// replaces the entire `ClientOptions`, so calling `with_allow_http` before
    /// it silently reset allow_http to false and every request failed with
    /// "URL scheme is not allowed" long before any socket was opened. Nothing
    /// listens on this port, so a *connection* failure is the passing result --
    /// what must not appear is a scheme complaint.
    #[tokio::test]
    async fn plain_http_endpoints_are_permitted() {
        let mut c = cfg("", false);
        c.backend = UploadBackend::S3;
        // Port 1 is privileged and never has a listener in test environments.
        c.endpoint = "http://127.0.0.1:1".to_string();
        c.access_key_id = Some("key".to_string());
        c.secret_access_key = Some("secret".to_string());
        c.max_retries = 0;
        c.connect_timeout_secs = 2;
        c.request_timeout_secs = 2;

        let up = ArtifactUploader::new(&c)
            .expect("build")
            .expect("creds present, so an uploader is built");
        let err = format!("{:#}", up.probe().await.expect_err("nothing is listening"));
        assert!(
            !err.to_lowercase().contains("scheme"),
            "plain HTTP was rejected before connecting: {err}"
        );
    }

    /// A structurally broken config is an error, not a silent degradation --
    /// that is the difference between "no creds yet" and "wrong endpoint".
    #[tokio::test]
    async fn s3_backend_with_a_bad_endpoint_is_a_hard_error() {
        let mut c = cfg("", false);
        c.backend = UploadBackend::S3;
        c.endpoint = "do-medium:3939".to_string();
        c.access_key_id = Some("k".to_string());
        c.secret_access_key = Some("s".to_string());
        assert!(ArtifactUploader::new(&c).is_err());
    }
}
