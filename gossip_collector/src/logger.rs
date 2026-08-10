use ldk_node::logger::{LogLevel, LogRecord, LogWriter};

use anyhow::Context;
use chrono::{DateTime, Utc};
use observer_common::logging::LogRotation;

use std::collections::HashSet;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Filename prefix for the LDK node's log files; the current file is
/// `{prefix}.YYYY-MM-DD-HH`, matching `tracing_appender`'s rolling scheme so
/// the log janitor can compress and expire them the same way.
pub(crate) const LDK_LOG_PREFIX: &str = "ldk_node";

/// A [`LogWriter`] that writes LDK node logs to periodically-rolled files,
/// with optional filtering of specific log call sites.
pub(crate) struct Writer {
    current: Mutex<CurrentFile>,
    log_dir: PathBuf,
    rotation: LogRotation,
    max_log_level: LogLevel,
    log_source_filter: HashSet<String>,
}

struct CurrentFile {
    file: File,
    // Start of the rotation period `file` covers.
    period: DateTime<Utc>,
    // Gates the stderr fallback so a persistent write failure (e.g. full
    // disk) logs once per outage instead of flooding journald. Plain bool:
    // only ever accessed under the mutex.
    write_failed: bool,
}

fn open_period_file(
    log_dir: &Path,
    rotation: LogRotation,
    period: DateTime<Utc>,
) -> std::io::Result<File> {
    let path = log_dir.join(format!("{LDK_LOG_PREFIX}.{}", rotation.file_suffix(period)));
    OpenOptions::new().create(true).append(true).open(path)
}

impl LogWriter for Writer {
    fn log(&self, record: LogRecord) {
        if record.level < self.max_log_level {
            return;
        }

        // Filter by module_path + line level, so we can omit frequent
        // but uninteresting logs without changing the overall log level.
        if self
            .log_source_filter
            .contains(&format!("{}:{}", record.module_path, record.line))
        {
            return;
        }

        let now = Utc::now();
        let log = format!(
            "{} {:<5} [{}:{}] {}\n",
            now.format("%Y-%m-%d %H:%M:%S"),
            record.level.to_string(),
            record.module_path,
            record.line,
            record.args
        );

        let Ok(mut current) = self.current.lock() else {
            return;
        };

        // Roll to a new file on the period boundary. If the open fails, keep
        // writing to the previous period's file rather than losing the line.
        let period = self.rotation.period_start(now);
        if period != current.period {
            match open_period_file(&self.log_dir, self.rotation, period) {
                Ok(file) => {
                    current.file = file;
                    current.period = period;
                }
                Err(e) => {
                    if !current.write_failed {
                        current.write_failed = true;
                        eprintln!("LDK logger: failed to open new log file: {e}");
                    }
                }
            }
        }

        // Best-effort: a failed write (e.g. full disk) must not panic inside
        // an LDK thread.
        match current.file.write_all(log.as_bytes()) {
            Ok(()) => current.write_failed = false,
            Err(e) => {
                if !current.write_failed {
                    current.write_failed = true;
                    eprintln!("LDK logger: failed to write to log file: {e}");
                }
            }
        }
    }
}

impl Writer {
    /// Creates a new logger writing periodically-rolled files named
    /// [`LDK_LOG_PREFIX`]`.<timestamp>` under `log_dir`.
    pub fn new_fs_writer(
        log_dir: impl Into<PathBuf>,
        rotation: LogRotation,
        max_log_level: LogLevel,
        log_source_filter: HashSet<String>,
    ) -> anyhow::Result<Writer> {
        let log_dir = log_dir.into();
        create_dir_all(&log_dir).context("Failed to create log directory")?;

        let period = rotation.period_start(Utc::now());
        let file = open_period_file(&log_dir, rotation, period)
            .with_context(|| format!("Failed to open log file in {}", log_dir.display()))?;

        Ok(Writer {
            current: Mutex::new(CurrentFile {
                file,
                period,
                write_failed: false,
            }),
            log_dir,
            rotation,
            max_log_level,
            log_source_filter,
        })
    }
}
