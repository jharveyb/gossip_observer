//! Publishing graph dumps that the live path did not.
//!
//! Two sources, deliberately kept apart:
//!
//! * **Directories the live path never writes to** -- the legacy `storage_dir`,
//!   where dumps landed before `graph_dir` was carved out, and where a restored
//!   archive is staged. These are swept unconditionally: nothing else is
//!   touching them, so there is no one to race.
//! * **The live output directory**, but only files older than
//!   `backfill.output_dir_min_age_secs`. This is the retry path for a publish
//!   that failed -- the live path publishes inline and has no other -- and the
//!   age gate is what keeps it from contending with a publish still in flight.
//!
//! That split is the whole design. An earlier version swept both directories
//! uniformly and needed canonicalize-dedupe, name-collision handling and a
//! race-tolerance story; none of that survives once the sweep's directories are
//! disjoint from the live path's by construction.
//!
//! * **It never deletes.** The caller passes `delete_local: false`, always. A
//!   sweep publishes; a human prunes once they have confirmed the bucket.
//! * **An already-published file costs nothing**: the diff is a lookup against
//!   one bulk listing, and [`ArtifactUploader::publish`] short-circuits on it
//!   without a request.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use observer_common::uploader::{
    ArtifactUploader, BackfillConfig, BatchTally, Budget, BudgetTracker, ObjectKey, Presence,
    PublishOptions,
};
use rand::Rng;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::collector_manager::{GraphKind, parse_dump_file_name};

const MIB: u64 = 1024 * 1024;

/// Where dumps may be found.
#[derive(Debug, Clone)]
pub struct Dirs {
    /// Where the live path writes. Swept only for files past the age gate.
    pub graph_dir: PathBuf,
    /// Legacy location, and where a restored archive is staged. Never written
    /// by the live path, so swept without an age gate.
    pub storage_dir: PathBuf,
}

/// One directory to sweep, and the minimum age a file there must have.
struct ScanTarget {
    dir: PathBuf,
    min_age: Duration,
}

impl Dirs {
    /// The directories to sweep.
    ///
    /// `storage_dir` is dropped when it *is* `graph_dir` -- on a host that
    /// never had the pre-split layout they are the same place, and sweeping it
    /// twice would just mean doing the age-gated pass and the ungated pass over
    /// one directory. Canonical paths, so a trailing slash or a symlink still
    /// collapses.
    fn targets(&self, output_dir_min_age: Duration) -> Vec<ScanTarget> {
        let mut targets = Vec::new();
        if !output_dir_min_age.is_zero() {
            targets.push(ScanTarget {
                dir: self.graph_dir.clone(),
                min_age: output_dir_min_age,
            });
        }
        if !self.same_dir() {
            targets.push(ScanTarget {
                dir: self.storage_dir.clone(),
                min_age: Duration::ZERO,
            });
        }
        targets
    }

    fn same_dir(&self) -> bool {
        match (
            std::fs::canonicalize(&self.graph_dir),
            std::fs::canonicalize(&self.storage_dir),
        ) {
            (Ok(a), Ok(b)) => a == b,
            _ => self.graph_dir == self.storage_dir,
        }
    }
}

/// One dump found on disk.
#[derive(Debug, Clone)]
struct Candidate {
    path: PathBuf,
    name: String,
    kind: GraphKind,
    stamp: DateTime<Utc>,
    rel_key: PathBuf,
    len: u64,
}

/// What one scan found, for logging.
#[derive(Debug, Default)]
struct ScanStats {
    entries: u64,
    skipped_non_artifact: u64,
    skipped_too_new: u64,
}

/// Walk the sweep's directories and return every dump old enough to consider.
///
/// One `spawn_blocking` with sync `std::fs` for the whole walk: tens of
/// thousands of dirents and as many `statx` calls is the bulk case the
/// workspace rule reserves sync IO for.
async fn scan(dirs: Dirs, output_dir_min_age: Duration) -> Result<(Vec<Candidate>, ScanStats)> {
    tokio::task::spawn_blocking(move || {
        let now = std::time::SystemTime::now();
        let mut found = Vec::new();
        let mut stats = ScanStats::default();

        for target in dirs.targets(output_dir_min_age) {
            let entries = match std::fs::read_dir(&target.dir) {
                Ok(entries) => entries,
                // Survivable: storage_dir always exists, graph_dir may not on a
                // host that has not written a dump yet, and neither is worth
                // failing a pass over.
                Err(e) => {
                    warn!(dir = %target.dir.display(), error = %e, "Graph backfill: cannot read directory");
                    continue;
                }
            };

            for entry in entries {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => {
                        warn!(dir = %target.dir.display(), error = %e, "Graph backfill: bad directory entry");
                        continue;
                    }
                };
                stats.entries += 1;

                let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                    stats.skipped_non_artifact += 1;
                    continue;
                };
                // Silent, not warned: a controller's storage dir holds thousands
                // of log files, and one warn per entry per pass would bury
                // everything else.
                let Some(parsed) = parse_dump_file_name(&name) else {
                    stats.skipped_non_artifact += 1;
                    continue;
                };
                // `d_type` on Linux, so no extra syscall.
                match entry.file_type() {
                    Ok(ft) if ft.is_file() => {}
                    Ok(_) => {
                        stats.skipped_non_artifact += 1;
                        continue;
                    }
                    Err(e) => {
                        warn!(name, error = %e, "Graph backfill: cannot stat entry");
                        continue;
                    }
                }
                let meta = match entry.metadata() {
                    Ok(meta) => meta,
                    Err(e) => {
                        warn!(name, error = %e, "Graph backfill: cannot read metadata");
                        continue;
                    }
                };

                // The age gate, for the live output directory only. mtime is
                // free here -- we already needed the length.
                if !target.min_age.is_zero() {
                    let age = meta
                        .modified()
                        .ok()
                        .and_then(|m| now.duration_since(m).ok())
                        .unwrap_or(Duration::ZERO);
                    if age < target.min_age {
                        stats.skipped_too_new += 1;
                        continue;
                    }
                }

                found.push(Candidate {
                    path: entry.path(),
                    name,
                    kind: parsed.kind,
                    stamp: parsed.stamp,
                    rel_key: parsed.rel_key,
                    len: meta.len(),
                });
            }
        }

        Ok((found, stats))
    })
    .await
    .context("graph backfill scan task panicked")?
}

/// Build the set of already-published keys, one bulk listing per family that
/// actually has candidates.
///
/// Separate calls per family rather than one listing of everything: the bucket
/// prefix may be shared with other producers, and per-family counts are what
/// make progress readable in the log.
async fn published_keys(
    uploader: &ArtifactUploader,
    candidates: &[Candidate],
) -> Result<HashMap<ObjectKey, u64>> {
    let mut published = HashMap::new();
    for kind in GraphKind::ALL {
        if !candidates.iter().any(|c| c.kind == kind) {
            continue;
        }
        let listing = uploader
            .list_keys(Some(Path::new(kind.dir())))
            .await
            .with_context(|| format!("listing published {} objects", kind.dir()))?;
        debug!(
            kind = kind.dir(),
            published = listing.len(),
            "Graph backfill: listed"
        );
        published.extend(listing);
    }
    Ok(published)
}

/// Outcome of one sweep.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PassOutcome {
    pub tally: BatchTally,
    /// Dumps still needing an upload after this pass -- deferred by the budget,
    /// or failed.
    pub remaining: u32,
    pub remaining_bytes: u64,
}

impl PassOutcome {
    fn drained(&self) -> bool {
        self.remaining == 0
    }

    /// Files this pass actually put on the wire.
    pub fn uploaded(&self) -> u32 {
        self.tally.transferred
    }

    /// Files already on the remote at the right size -- free, no request spent.
    pub fn skipped_published(&self) -> u32 {
        self.tally.already_present
    }

    pub fn failed(&self) -> u32 {
        self.tally.failed
    }
}

/// One sweep: scan, diff against the bucket, publish a bounded slice.
pub async fn pass(
    dirs: &Dirs,
    cfg: &BackfillConfig,
    uploader: &ArtifactUploader,
    cancel: &CancellationToken,
) -> Result<PassOutcome> {
    let min_age = Duration::from_secs(cfg.output_dir_min_age_secs.into());
    let (candidates, stats) = scan(dirs.clone(), min_age).await?;
    info!(
        entries = stats.entries,
        artifacts = candidates.len(),
        skipped = stats.skipped_non_artifact,
        too_new = stats.skipped_too_new,
        "Graph backfill: scanned"
    );
    if candidates.is_empty() {
        return Ok(PassOutcome::default());
    }

    let published = published_keys(uploader, &candidates).await?;

    // Oldest first, strictly chronological across families: that is what gives
    // a single contiguous "published through date W" frontier, which is what an
    // operator prunes against.
    let mut pending: Vec<Candidate> = candidates;
    pending.sort_by(|a, b| {
        a.stamp
            .cmp(&b.stamp)
            .then_with(|| a.kind.cmp(&b.kind))
            .then_with(|| a.name.cmp(&b.name))
    });

    // Never deletes, whatever the live path's policy is: a sweep publishes, a
    // human prunes once they have confirmed the bucket.
    let mut tracker = BudgetTracker::new(Budget::from_config(cfg));
    let mut remaining = 0u32;
    let mut remaining_bytes = 0u64;

    for c in &pending {
        // Between files, never around a publish: racing cancel against an
        // upload drops the future mid-multipart, and an abort only runs on the
        // error path -- Garage has no lifecycle expiry, so orphaned parts are
        // forever.
        if cancel.is_cancelled() || !tracker.may_attempt() {
            remaining += 1;
            remaining_bytes += c.len;
            continue;
        }

        let key = match uploader.key_for(&c.rel_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(name = c.name, error = %e, "Graph backfill: cannot build object key");
                remaining += 1;
                remaining_bytes += c.len;
                continue;
            }
        };
        // Feed the bulk listing straight in, so an already-published file costs
        // no request at all.
        let opts = PublishOptions {
            delete_local: false,
            presence: match published.get(&key) {
                Some(&len) => Presence::Size(len),
                None => Presence::Absent,
            },
        };

        let outcome = uploader.publish(&c.path, &c.rel_key, &opts).await;
        if let Err(e) = &outcome {
            // A prune racing the sweep lands here too: the file is simply gone
            // by the time we stat it. Not worth distinguishing.
            warn!(name = c.name, error = ?e, "Graph backfill: failed to publish");
            remaining += 1;
            remaining_bytes += c.len;
        }
        tracker.record(&outcome);
    }

    Ok(PassOutcome {
        tally: tracker.tally(),
        remaining,
        remaining_bytes,
    })
}

/// Sleep, sweep, repeat -- until cancelled.
///
/// Never exits on a failed pass, and never exits when drained: see the module
/// docs for why this stays running.
pub async fn run_graph_backfill(
    dirs: Dirs,
    cfg: BackfillConfig,
    uploader: ArtifactUploader,
    cancel: CancellationToken,
) {
    if !cfg.enabled {
        info!("Graph backfill is disabled, task exiting");
        return;
    }

    let jitter = if cfg.startup_delay_max_secs == 0 {
        0
    } else {
        rand::rng().random_range(0..=u64::from(cfg.startup_delay_max_secs))
    };
    info!(
        graph_dir = %dirs.graph_dir.display(),
        storage_dir = %dirs.storage_dir.display(),
        interval_secs = cfg.interval_secs,
        max_files_per_pass = cfg.max_files_per_pass,
        max_mib_per_pass = cfg.max_mib_per_pass,
        output_dir_min_age_secs = cfg.output_dir_min_age_secs,
        startup_delay_secs = jitter,
        "Graph backfill: starting"
    );

    // Sleep *after* a pass rather than ticking, so an overrunning pass delays
    // the next one instead of queueing ticks behind it.
    let mut delay = Duration::from_secs(jitter);
    let mut was_drained = false;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                info!("Graph backfill: shutting down");
                return;
            }
            () = tokio::time::sleep(delay) => {}
        }
        delay = Duration::from_secs(cfg.interval_secs.into());

        let started = Instant::now();
        match pass(&dirs, &cfg, &uploader, &cancel).await {
            Ok(out) => {
                let drained = out.drained();
                if drained && was_drained {
                    // Steady state: don't emit an info line every interval
                    // forever once there is nothing to do.
                    debug!(
                        already_present = out.tally.already_present,
                        "Graph backfill: nothing pending"
                    );
                } else {
                    info!(
                        published = out.tally.transferred,
                        mib = out.tally.transferred_bytes / MIB,
                        already_present = out.tally.already_present,
                        failed = out.tally.failed,
                        remaining = out.remaining,
                        remaining_mib = out.remaining_bytes / MIB,
                        elapsed_secs = started.elapsed().as_secs(),
                        "Graph backfill: pass complete"
                    );
                    if drained {
                        info!(
                            published = out.tally.already_present,
                            "Graph backfill: backlog drained; now acting as the retry sweep for live publishes"
                        );
                    }
                }
                was_drained = drained;
            }
            Err(e) => {
                warn!(error = ?e, "Graph backfill: pass failed, will retry next interval");
                was_drained = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use observer_common::uploader::{PublishOptions, UploadBackend, UploadConfig};
    use std::collections::HashSet;

    const INTERLOPERS: &[&str] = &[
        "controller.2026-09-09-22",
        "controller.2026-08-26-14.zst",
        "controller.2026-08-26-14.1.zst",
        "controller_graph-2026-08-26-16-15.zst.tmp",
        "collector-019c26b8-3191-7fff-895f-5ed752c53bdc-2026-06-09-16-15.zst.zst",
        "community_9854.csv",
        "nodes_normalized_initial.csv",
        "collector_mapping.toml",
        "diff_report_03_04.txt",
    ];

    struct Fixture {
        _root: tempfile::TempDir,
        dirs: Dirs,
        uploader: ArtifactUploader,
        bucket_root: PathBuf,
    }

    /// A `local`-backend uploader over a temp dir, built the same way the real
    /// binary builds one so the config path is exercised too.
    fn fixture(delete_local: bool) -> Fixture {
        let root = tempfile::tempdir().expect("tempdir");
        let graph_dir = root.path().join("graphs");
        let storage_dir = root.path().join("storage");
        let bucket_root = root.path().join("bucket");
        std::fs::create_dir_all(&graph_dir).expect("graphs");
        std::fs::create_dir_all(&storage_dir).expect("storage");
        std::fs::create_dir_all(&bucket_root).expect("bucket");

        let cfg = UploadConfig {
            enabled: true,
            backend: UploadBackend::Local,
            local_root: bucket_root.display().to_string(),
            bucket: "test".to_string(),
            delete_local,
            max_retries: 0,
            ..UploadConfig::default()
        };
        let uploader = ArtifactUploader::new(&cfg)
            .expect("build uploader")
            .expect("uploads are enabled");

        Fixture {
            _root: root,
            dirs: Dirs {
                graph_dir,
                storage_dir,
            },
            uploader,
            bucket_root,
        }
    }

    fn backfill_cfg() -> BackfillConfig {
        BackfillConfig {
            enabled: true,
            max_files_per_pass: 100,
            max_mib_per_pass: 1024,
            ..BackfillConfig::default()
        }
    }

    fn dump_name(kind: GraphKind, stamp: &str) -> String {
        match kind {
            GraphKind::ControllerGraph => format!("controller_graph-{stamp}.zst"),
            GraphKind::CollectorDiffs => format!("collector_diffs-{stamp}.zst"),
            GraphKind::Collector => {
                format!("collector-019bfdbc-10b8-794f-91cb-132c5f4cc390-{stamp}.zst")
            }
        }
    }

    fn write_dump(dir: &Path, name: &str, len: usize) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, vec![b'z'; len]).expect("write dump");
        path
    }

    /// Every object key currently in the stand-in bucket.
    fn bucket_keys(f: &Fixture) -> HashSet<String> {
        fn walk(dir: &Path, base: &Path, out: &mut HashSet<String>) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, base, out);
                } else if let Ok(rel) = path.strip_prefix(base) {
                    out.insert(rel.display().to_string());
                }
            }
        }
        let mut out = HashSet::new();
        walk(&f.bucket_root, &f.bucket_root, &mut out);
        out
    }

    #[tokio::test]
    async fn publishes_unpublished_dumps_and_skips_published_ones() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        for stamp in ["2026-03-30-10-00", "2026-03-30-11-00", "2026-03-30-12-00"] {
            write_dump(
                &f.dirs.storage_dir,
                &dump_name(GraphKind::ControllerGraph, stamp),
                64,
            );
        }

        let first = pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(first.uploaded(), 3);
        assert_eq!(first.skipped_published(), 0);
        assert_eq!(first.remaining, 0);
        assert_eq!(bucket_keys(&f).len(), 3);

        // A second pass must cost nothing: everything is already published at
        // the right size.
        let second = pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(second.uploaded(), 0);
        assert_eq!(second.skipped_published(), 3);
        assert_eq!(second.remaining, 0);
    }

    /// Existence is not validity: a truncated prior upload must be replaced.
    #[tokio::test]
    async fn reuploads_an_object_whose_remote_size_differs() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        let name = dump_name(GraphKind::ControllerGraph, "2026-03-30-10-00");
        write_dump(&f.dirs.storage_dir, &name, 64);

        pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("pass");
        // Grow the local file behind the uploader's back.
        write_dump(&f.dirs.storage_dir, &name, 128);

        let out = pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("pass");
        // No mismatch counter any more: `publish` compares the known remote
        // size itself and transfers when it disagrees.
        assert_eq!(out.uploaded(), 1);
        assert_eq!(out.skipped_published(), 0);

        let key = f
            .uploader
            .key_for(&parse_dump_file_name(&name).expect("parse").rel_key)
            .expect("key");
        assert_eq!(f.uploader.head_size(&key).await.expect("head"), Some(128));
    }

    /// The allowlist, end to end: nothing but a real dump may be published, and
    /// nothing that isn't may even be touched.
    #[tokio::test]
    async fn ignores_interlopers_and_leaves_them_on_disk() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        for name in INTERLOPERS {
            write_dump(&f.dirs.storage_dir, name, 32);
        }
        write_dump(
            &f.dirs.storage_dir,
            &dump_name(GraphKind::CollectorDiffs, "2026-03-30-10-00"),
            32,
        );

        let out = pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(out.uploaded(), 1, "only the real dump should publish");
        assert_eq!(bucket_keys(&f).len(), 1);

        for name in INTERLOPERS {
            assert!(
                f.dirs.storage_dir.join(name).exists(),
                "backfill disturbed an interloper: {name}"
            );
        }
    }

    #[tokio::test]
    async fn respects_the_file_budget_and_resumes_next_pass() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        for hour in 0..6 {
            write_dump(
                &f.dirs.storage_dir,
                &dump_name(
                    GraphKind::ControllerGraph,
                    &format!("2026-03-30-{hour:02}-00"),
                ),
                16,
            );
        }
        let cfg = BackfillConfig {
            max_files_per_pass: 2,
            ..backfill_cfg()
        };

        let first = pass(&f.dirs, &cfg, &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(first.uploaded(), 2);
        assert_eq!(first.remaining, 4);

        let second = pass(&f.dirs, &cfg, &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(second.uploaded(), 2);
        assert_eq!(
            second.skipped_published(),
            2,
            "the first pass's work must not repeat"
        );
        assert_eq!(second.remaining, 2);
    }

    #[tokio::test]
    async fn respects_the_byte_budget() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        for hour in 0..4 {
            write_dump(
                &f.dirs.storage_dir,
                &dump_name(
                    GraphKind::ControllerGraph,
                    &format!("2026-03-30-{hour:02}-00"),
                ),
                (MIB as usize) / 2,
            );
        }
        // 1 MiB of budget against half-MiB files: two fit, the third is
        // deferred because the budget is already spent when it is reached.
        // A file budget far above the file count, so the byte budget is
        // unambiguously the one that binds. (Zero is no longer "unlimited" --
        // it is rejected by validate, because it used to silently disable the
        // guard entirely.)
        let cfg = BackfillConfig {
            max_files_per_pass: 1000,
            max_mib_per_pass: 1,
            ..backfill_cfg()
        };

        let out = pass(&f.dirs, &cfg, &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(out.uploaded(), 2);
        assert_eq!(out.remaining, 2);
    }

    /// Oldest first, with families interleaved -- the contiguous date frontier
    /// an operator prunes against.
    #[tokio::test]
    async fn publishes_oldest_first_across_families() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        // Deliberately written newest-first so insertion order can't be what
        // produces the expected result.
        write_dump(
            &f.dirs.storage_dir,
            &dump_name(GraphKind::Collector, "2026-03-30-09-00"),
            16,
        );
        write_dump(
            &f.dirs.storage_dir,
            &dump_name(GraphKind::ControllerGraph, "2026-03-29-09-00"),
            16,
        );
        write_dump(
            &f.dirs.storage_dir,
            &dump_name(GraphKind::CollectorDiffs, "2026-03-28-09-00"),
            16,
        );

        let cfg = BackfillConfig {
            max_files_per_pass: 1,
            ..backfill_cfg()
        };
        let out = pass(&f.dirs, &cfg, &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(out.uploaded(), 1);
        let keys = bucket_keys(&f);
        assert!(
            keys.iter()
                .any(|k| k.contains("collector_diffs-2026-03-28")),
            "expected the oldest dump regardless of family, got {keys:?}"
        );
    }

    /// The sweep's directories are disjoint from the live path's: `storage_dir`
    /// unconditionally, and the output directory only past the age gate. That
    /// disjointness is what replaced the old canonicalize-dedupe and
    /// name-collision handling.
    #[tokio::test]
    async fn scans_storage_dir_and_only_aged_files_in_the_output_dir() {
        let f = fixture(false);
        let cancel = CancellationToken::new();

        // Legacy location: swept regardless of age.
        let legacy = dump_name(GraphKind::CollectorDiffs, "2026-03-30-11-00");
        write_dump(&f.dirs.storage_dir, &legacy, 16);

        // Live output directory: one fresh file, one backdated past the gate.
        let fresh = dump_name(GraphKind::ControllerGraph, "2026-03-30-12-00");
        write_dump(&f.dirs.graph_dir, &fresh, 16);
        let stale = dump_name(GraphKind::ControllerGraph, "2026-03-30-13-00");
        let stale_path = write_dump(&f.dirs.graph_dir, &stale, 16);
        let old = std::time::SystemTime::now() - Duration::from_secs(7200);
        filetime::set_file_mtime(&stale_path, filetime::FileTime::from_system_time(old))
            .expect("backdate");

        let cfg = BackfillConfig {
            output_dir_min_age_secs: 3600,
            ..backfill_cfg()
        };
        let out = pass(&f.dirs, &cfg, &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(out.uploaded(), 2, "the legacy dump and the aged one");

        let keys = bucket_keys(&f);
        assert!(
            keys.iter().any(|k| k.contains(&legacy)),
            "legacy dump missing"
        );
        assert!(keys.iter().any(|k| k.contains(&stale)), "aged dump missing");
        assert!(
            !keys.iter().any(|k| k.contains(&fresh)),
            "a fresh output-dir file must be left to the live path: {keys:?}"
        );
    }

    /// With the age gate off, the output directory is not swept at all.
    #[tokio::test]
    async fn a_zero_age_gate_leaves_the_output_directory_alone() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        write_dump(
            &f.dirs.graph_dir,
            &dump_name(GraphKind::ControllerGraph, "2026-03-30-12-00"),
            16,
        );
        let cfg = BackfillConfig {
            output_dir_min_age_secs: 0,
            ..backfill_cfg()
        };
        let out = pass(&f.dirs, &cfg, &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(out.uploaded(), 0);
    }

    /// The guarantee the whole design rests on: a sweep publishes, a human
    /// prunes. Structural now -- `pass` passes `delete_local: false` on every
    /// publish, so there is no setting and no wrong-clone to get wrong.
    #[tokio::test]
    async fn never_deletes_local_files_even_when_the_live_path_would() {
        // delete_local: true on the *live* config, which the sweep ignores.
        let f = fixture(true);
        let uploader = f.uploader.clone();
        let cancel = CancellationToken::new();

        let mut paths = Vec::new();
        for hour in 0..3 {
            paths.push(write_dump(
                &f.dirs.storage_dir,
                &dump_name(
                    GraphKind::ControllerGraph,
                    &format!("2026-03-30-{hour:02}-00"),
                ),
                32,
            ));
        }

        let out = pass(&f.dirs, &backfill_cfg(), &uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(out.uploaded(), 3);
        for path in paths {
            assert!(path.exists(), "backfill deleted {}", path.display());
        }
    }

    /// A file the *live* path already published is skipped outright, so the
    /// backfill's deletion policy never touches it.
    ///
    /// Belt and braces: the sweep is handed a non-deleting uploader anyway, but
    /// this pins the earlier layer of protection too -- a live-published file
    /// is skipped before deletion could even be reached, so the guarantee does
    /// not rest solely on the caller passing the right clone.
    #[tokio::test]
    async fn a_live_published_file_is_skipped_not_deleted() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        let name = dump_name(GraphKind::ControllerGraph, "2026-03-30-10-00");
        let path = write_dump(&f.dirs.storage_dir, &name, 64);

        // Stand in for the live path having published it and left it on disk.
        let rel = parse_dump_file_name(&name).expect("parse").rel_key;
        f.uploader
            .publish(&path, &rel, &PublishOptions::fresh(false))
            .await
            .expect("live upload");
        assert!(path.exists(), "live path has delete_local off");

        // A backfill that *would* delete anything it publishes.
        let deleting = ArtifactUploader::new(&UploadConfig {
            enabled: true,
            backend: UploadBackend::Local,
            local_root: f.bucket_root.display().to_string(),
            bucket: "test".to_string(),
            delete_local: true,
            max_retries: 0,
            ..UploadConfig::default()
        })
        .expect("build")
        .expect("enabled");

        let out = pass(&f.dirs, &backfill_cfg(), &deleting, &cancel)
            .await
            .expect("pass");
        assert_eq!(out.skipped_published(), 1);
        assert_eq!(out.uploaded(), 0);
        assert!(
            path.exists(),
            "backfill deleted a file the live path had published and kept"
        );
    }

    #[tokio::test]
    async fn survives_a_missing_directory() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        write_dump(
            &f.dirs.storage_dir,
            &dump_name(GraphKind::ControllerGraph, "2026-03-30-10-00"),
            16,
        );
        // graph_dir legitimately may not exist on a host that has not written a
        // dump yet; the sweep must still drain what it can find elsewhere.
        std::fs::remove_dir_all(&f.dirs.graph_dir).expect("remove graph dir");

        let out = pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("a missing directory must not fail the pass");
        assert_eq!(out.uploaded(), 1);
    }

    #[tokio::test]
    async fn stops_promptly_when_cancelled() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        for hour in 0..5 {
            write_dump(
                &f.dirs.storage_dir,
                &dump_name(
                    GraphKind::ControllerGraph,
                    &format!("2026-03-30-{hour:02}-00"),
                ),
                16,
            );
        }
        cancel.cancel();

        let out = pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(
            out.uploaded(),
            0,
            "a cancelled pass must not start an upload"
        );
        assert_eq!(out.remaining, 5);
    }

    /// An empty directory is a normal steady state, not an error.
    #[tokio::test]
    async fn an_empty_directory_is_a_no_op() {
        let f = fixture(false);
        let cancel = CancellationToken::new();
        let out = pass(&f.dirs, &backfill_cfg(), &f.uploader, &cancel)
            .await
            .expect("pass");
        assert_eq!(out, PassOutcome::default());
    }
}
