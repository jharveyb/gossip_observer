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
//!   archiver awaits [`ArtifactUploader::upload_file`] inline so its ceiling is
//!   one upload.
//! * **Nothing deletes a local file without an [`UploadReceipt`]**, which only
//!   [`ArtifactUploader::upload_file`] can build, and only after the object's
//!   size on the remote has been read back and compared. Existence is not
//!   validity — the same rule the Parquet export recovery path follows.

use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
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
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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
    /// Concurrent uploads in [`upload_worker`], so one slow artifact does not
    /// head-of-line block a whole tick's worth of them.
    pub workers: u32,
    /// Per-request timeout handed to `ClientOptions::with_timeout`.
    pub request_timeout_secs: u32,
    pub connect_timeout_secs: u32,
    /// `object_store::RetryConfig::max_retries`.
    pub max_retries: u32,
    /// When true, a failed startup probe is a fatal startup error rather than
    /// a logged degradation. Default false: a transiently unreachable bucket
    /// must not boot-loop a data-collection daemon.
    pub required_on_startup: bool,
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
            workers: 2,
            // Generous: one part of a large Parquet file over tailscale.
            request_timeout_secs: 300,
            connect_timeout_secs: 15,
            max_retries: 3,
            required_on_startup: false,
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
            .field("workers", &self.workers)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("max_retries", &self.max_retries)
            .field("required_on_startup", &self.required_on_startup)
            .finish()
    }
}

/// Proof that an object exists on the remote at the expected size.
///
/// Only [`ArtifactUploader::upload_file`] constructs one, and only after the
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
    /// and `upload_file` stop re-parsing and re-validating a config string.
    key_prefix: Option<ObjectKey>,
    part_size: usize,
    max_concurrency: usize,
    delete_local: bool,
}

impl fmt::Debug for ArtifactUploader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArtifactUploader")
            .field("bucket", &self.bucket)
            .field("key_prefix", &self.key_prefix.as_ref().map(|p| p.as_ref()))
            .field("part_size", &self.part_size)
            .field("max_concurrency", &self.max_concurrency)
            .field("delete_local", &self.delete_local)
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
            delete_local: cfg.delete_local,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn delete_local(&self) -> bool {
        self.delete_local
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

    /// Stream `local` to the key derived from `rel_key`.
    ///
    /// Single PUT when the file fits in one part, multipart otherwise. On
    /// success the object's size is read back and compared against the local
    /// file; only then, and only if `delete_local` is set, is the local file
    /// removed. On any failure the multipart upload is aborted and the local
    /// file is left untouched.
    pub async fn upload_file(
        &self,
        local: &FsPath,
        rel_key: &FsPath,
    ) -> anyhow::Result<UploadReceipt> {
        let key = self.key_for(rel_key)?;

        let local_len = tokio::fs::metadata(local)
            .await
            .with_context(|| format!("stat {}", local.display()))?
            .len();

        if local_len <= self.part_size as u64 {
            self.put_whole(local, &key).await?;
        } else {
            self.put_multipart(local, &key).await?;
        }

        // Existence is not validity: a truncated multipart can still be
        // accepted. Compare sizes before anyone treats this as published.
        let remote_len = self
            .head_size(&key)
            .await?
            .ok_or_else(|| anyhow!("object {key} missing immediately after upload"))?;
        if remote_len != local_len {
            bail!(
                "size mismatch for {key}: local {local_len} B, remote {remote_len} B; \
                 leaving local file in place"
            );
        }

        let mut deleted_local = false;
        if self.delete_local {
            match tokio::fs::remove_file(local).await {
                Ok(()) => deleted_local = true,
                // A stale local file is harmless — the object is safely up, so
                // this is not worth failing the receipt over.
                Err(e) => warn!(
                    path = %local.display(),
                    error = %e,
                    "Uploaded but could not remove local staging file"
                ),
            }
        }

        info!(%key, bytes = local_len, deleted_local, "Published artifact");
        Ok(UploadReceipt {
            uri: self.uri_for(&key),
            key: key.as_ref().to_string(),
            bytes: local_len,
            deleted_local,
        })
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

/// One artifact to publish.
///
/// `done` is optional so a caller that needs the receipt can await it while one
/// that does not (the controller's graph writer) can fire and forget.
pub struct UploadRequest {
    pub local: PathBuf,
    pub rel_key: PathBuf,
    pub done: Option<oneshot::Sender<anyhow::Result<UploadReceipt>>>,
}

impl UploadRequest {
    pub fn new(local: PathBuf, rel_key: PathBuf) -> Self {
        Self {
            local,
            rel_key,
            done: None,
        }
    }
}

/// Upload artifacts off the producer's critical path, up to `workers` at a
/// time.
///
/// A failed upload is logged and the loop continues — one bad artifact must not
/// kill the pipeline. A closed channel is a normal exit, not an error: task
/// teardown races the cancel signal.
pub async fn upload_worker(
    mut rx: mpsc::Receiver<UploadRequest>,
    uploader: ArtifactUploader,
    workers: usize,
    cancel: CancellationToken,
) {
    let permits = Arc::new(Semaphore::new(workers.max(1)));
    let mut tasks = JoinSet::new();

    loop {
        let req = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            maybe = rx.recv() => match maybe {
                Some(req) => req,
                None => break,
            },
        };

        // Bound concurrency here rather than in the channel so a slow artifact
        // does not head-of-line block the ones behind it.
        let permit = match Arc::clone(&permits).acquire_owned().await {
            Ok(p) => p,
            // The semaphore is never closed; this is unreachable in practice.
            Err(e) => {
                error!(error = %e, "Upload semaphore closed; worker exiting");
                break;
            }
        };

        let uploader = uploader.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let result = uploader.upload_file(&req.local, &req.rel_key).await;
            if let Err(e) = &result {
                warn!(
                    path = %req.local.display(),
                    error = ?e,
                    "Failed to publish artifact; it stays on local disk"
                );
            }
            if let Some(done) = req.done {
                // Receiver dropped just means the caller stopped caring.
                let _ = done.send(result);
            }
        });

        // Reap finished uploads so the JoinSet does not grow without bound.
        while let Some(Err(e)) = tasks.try_join_next() {
            error!(error = %e, "Upload task panicked");
        }
    }

    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            error!(error = %e, "Upload task panicked");
        }
    }
    debug!("Upload worker stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::io::Write;

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
            workers: 2,
            request_timeout_secs: 30,
            connect_timeout_secs: 5,
            max_retries: 0,
            required_on_startup: false,
        }
    }

    fn uploader(key_prefix: &str, delete_local: bool) -> (ArtifactUploader, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let up = ArtifactUploader::from_store(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            &cfg(key_prefix, delete_local),
        )
        .expect("build uploader");
        (up, store)
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
        let (up, store) = uploader("", false);

        let receipt = up
            .upload_file(&local, FsPath::new("timings/2026/03/30/timings.parquet"))
            .await
            .expect("upload");

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
        let (up, store) = uploader("", false);

        let receipt = up
            .upload_file(&local, FsPath::new("controller_graph/g.zst"))
            .await
            .expect("upload");
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
        let (up, _store) = uploader("", true);

        let receipt = up
            .upload_file(&local, FsPath::new("timings/t.parquet"))
            .await
            .expect("upload");
        assert!(receipt.deleted_local);
        assert!(!local.exists(), "staging file should be gone");
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
            .upload_file(&local, FsPath::new("timings/t.parquet"))
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

        let (bare, _) = uploader("", false);
        assert_eq!(
            bare.key_for(rel).expect("key").as_ref(),
            "timings/2026/03/30/timings_2026-03-30.parquet"
        );

        let (prefixed, _) = uploader("staging", false);
        assert_eq!(
            prefixed.key_for(rel).expect("key").as_ref(),
            "staging/timings/2026/03/30/timings_2026-03-30.parquet"
        );
    }

    #[tokio::test]
    async fn key_for_rejects_parent_traversal() {
        let (up, _) = uploader("staging", false);
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
        assert_eq!(built.workers, want.workers);
        assert_eq!(built.region, want.region);
        assert!(built.access_key_id.is_none());
        assert!(built.secret_access_key.is_none());
    }

    /// A config file must be able to override the defaults source.
    #[test]
    fn config_file_overrides_the_defaults_source() {
        let built: UploadConfig = config::Config::builder()
            .add_source(UploadConfig::defaults_source().expect("defaults"))
            .add_source(config::File::from_str(
                "[upload]\nenabled = true\nbucket = \"b\"\nworkers = 7\n",
                config::FileFormat::Toml,
            ))
            .build()
            .expect("build")
            .get("upload")
            .expect("deserialize [upload]");

        assert!(built.enabled);
        assert_eq!(built.bucket, "b");
        assert_eq!(built.workers, 7);
        // Untouched keys still come from the defaults.
        assert_eq!(built.multipart_mib, UploadConfig::default().multipart_mib);
    }

    #[tokio::test]
    async fn head_size_reports_absence_and_presence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (local, body) = write_file(&dir, "small.parquet", 777);
        let (up, _store) = uploader("prod", false);

        let absent = up.key_for(FsPath::new("nope.parquet")).expect("key");
        assert_eq!(up.head_size(&absent).await.expect("head"), None);
        let receipt = up
            .upload_file(&local, FsPath::new("timings/t.parquet"))
            .await
            .expect("upload");
        let present = ObjectKey::parse(&receipt.key).expect("key");
        assert_eq!(
            up.head_size(&present).await.expect("head"),
            Some(body.len() as u64)
        );
    }

    #[tokio::test]
    async fn probe_succeeds_against_a_reachable_store() {
        let (up, _store) = uploader("prod", false);
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

    #[tokio::test]
    async fn worker_uploads_queued_artifacts_and_reports_receipts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (up, store) = uploader("prod", false);
        let (tx, rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let worker = tokio::spawn(upload_worker(rx, up, 2, cancel.clone()));

        let mut waiters = Vec::new();
        for i in 0..3 {
            let (local, _) = write_file(&dir, &format!("a{i}.zst"), 2048);
            let (done, rx_done) = oneshot::channel();
            tx.send(UploadRequest {
                local,
                rel_key: PathBuf::from(format!("graphs/a{i}.zst")),
                done: Some(done),
            })
            .await
            .expect("queue upload");
            waiters.push(rx_done);
        }
        drop(tx);
        worker.await.expect("worker");

        for w in waiters {
            let receipt = w.await.expect("receipt delivered").expect("upload ok");
            assert!(receipt.key.starts_with("prod/graphs/"));
            store
                .head(&ObjectKey::parse(&receipt.key).expect("key"))
                .await
                .expect("object present");
        }
    }
}
