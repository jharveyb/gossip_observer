use ldk_node::logger::{LogLevel, LogRecord, LogWriter};

use chrono::Utc;
use log::Level as LogFacadeLevel;
use log::Record as LogFacadeRecord;

use std::collections::HashSet;
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

// TODO: Do we need to have this duplicated from ldk-node at all?
/// Defines a writer for [`Logger`].
#[allow(clippy::enum_variant_names)]
pub(crate) enum Writer {
    /// Writes logs to the file system.
    FileWriter {
        file: Arc<Mutex<File>>,
        max_log_level: LogLevel,
        log_source_filter: HashSet<String>,
    },
    /// Forwards logs to the `log` facade.
    LogFacadeWriter,
    /// Forwards logs to a custom writer.
    CustomWriter(Arc<dyn LogWriter>),
}

impl LogWriter for Writer {
    fn log(&self, record: LogRecord) {
        match self {
            Writer::FileWriter {
                file,
                max_log_level,
                log_source_filter,
            } => {
                if record.level < *max_log_level {
                    return;
                }

                // Filter by module_path + line level, so we can omit frequent
                // but uninteresting logs without changing the overall log level.
                if log_source_filter.contains(&format!("{}:{}", record.module_path, record.line)) {
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

                if let Ok(mut file_guard) = file.lock() {
                    file_guard
                        .write_all(log.as_bytes())
                        .expect("Failed to write to log file");
                }
            }
            Writer::LogFacadeWriter => {
                let mut builder = LogFacadeRecord::builder();

                match record.level {
                    LogLevel::Gossip | LogLevel::Trace => builder.level(LogFacadeLevel::Trace),
                    LogLevel::Debug => builder.level(LogFacadeLevel::Debug),
                    LogLevel::Info => builder.level(LogFacadeLevel::Info),
                    LogLevel::Warn => builder.level(LogFacadeLevel::Warn),
                    LogLevel::Error => builder.level(LogFacadeLevel::Error),
                };

                log::logger().log(
                    &builder
                        .module_path(Some(record.module_path))
                        .line(Some(record.line))
                        .args(format_args!("{}", record.args))
                        .build(),
                );
            }
            Writer::CustomWriter(custom_logger) => custom_logger.log(record),
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
    ) -> Result<Writer, ()> {
        if let Some(parent_dir) = Path::new(&file_path).parent() {
            create_dir_all(parent_dir).map_err(|e| {
                // Use eprintln! here since tracing may not be initialized yet
                eprintln!("ERROR: Failed to create log parent directory: {e}");
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(|e| {
                // Use eprintln! here since tracing may not be initialized yet
                eprintln!("ERROR: Failed to open log file: {e}");
            })?;

        Ok(Writer::FileWriter {
            file: Arc::new(Mutex::new(file)),
            max_log_level,
            log_source_filter,
        })
    }
}

// OPTIONAL: Bridge LDK logs to tracing (currently commented out)
// Uncomment this code if you want LDK node logs to go through the tracing system
// instead of the existing file-based Writer implementation
//
// pub(crate) struct TracingLogWriter;
//
// impl LogWriter for TracingLogWriter {
//     fn log(&self, record: LogRecord) {
//         let level = match record.level {
//             LogLevel::Gossip => tracing::Level::TRACE,
//             LogLevel::Trace => tracing::Level::TRACE,
//             LogLevel::Debug => tracing::Level::DEBUG,
//             LogLevel::Info => tracing::Level::INFO,
//             LogLevel::Warn => tracing::Level::WARN,
//             LogLevel::Error => tracing::Level::ERROR,
//         };
//
//         tracing::event!(
//             level,
//             target = record.module_path,
//             file = record.file,
//             line = record.line,
//             "{}",
//             record.args
//         );
//     }
// }
//
// Usage in main.rs:
// let writer = Arc::new(crate::logger::TracingLogWriter);
// builder.set_custom_logger(writer);
