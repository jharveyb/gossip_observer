use ldk_node::logger::{LogLevel, LogRecord, LogWriter};

use anyhow::Context;
use chrono::Utc;

use std::collections::HashSet;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// A [`LogWriter`] that writes LDK node logs to a file, with optional
/// filtering of specific log call sites.
pub(crate) struct Writer {
    file: Arc<Mutex<File>>,
    max_log_level: LogLevel,
    log_source_filter: HashSet<String>,
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

        let log = format!(
            "{} {:<5} [{}:{}] {}\n",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            record.level.to_string(),
            record.module_path,
            record.line,
            record.args
        );

        if let Ok(mut file_guard) = self.file.lock() {
            file_guard
                .write_all(log.as_bytes())
                .expect("Failed to write to log file");
        }
    }
}

impl Writer {
    /// Creates a new logger with a filesystem writer. The parameters to this function
    /// are the path to the log file, and the log level.
    pub fn new_fs_writer(
        file_path: String,
        max_log_level: LogLevel,
        log_source_filter: HashSet<String>,
    ) -> anyhow::Result<Writer> {
        if let Some(parent_dir) = Path::new(&file_path).parent() {
            create_dir_all(parent_dir).context("Failed to create log parent directory")?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .with_context(|| format!("Failed to open log file {file_path}"))?;

        Ok(Writer {
            file: Arc::new(Mutex::new(file)),
            max_log_level,
            log_source_filter,
        })
    }
}
