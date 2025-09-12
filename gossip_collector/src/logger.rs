use ldk_node::logger::{LogLevel, LogRecord, LogWriter};

use chrono::Utc;
use log::Level as LogFacadeLevel;
use log::Record as LogFacadeRecord;

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

// TODO: Do we need to have this duplicated from ldk-node at all?
/// Defines a writer for [`Logger`].
pub(crate) enum Writer {
    /// Writes logs to the file system.
    FileWriter {
        file_path: String,
        max_log_level: LogLevel,
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
                file_path,
                max_log_level,
            } => {
                if record.level < *max_log_level {
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

                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file_path)
                    .expect("Failed to open log file")
                    .write_all(log.as_bytes())
                    .expect("Failed to write to log file")
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
    pub fn new_fs_writer(file_path: String, max_log_level: LogLevel) -> Result<Writer, ()> {
        if let Some(parent_dir) = Path::new(&file_path).parent() {
            fs::create_dir_all(parent_dir)
                .map_err(|e| eprintln!("ERROR: Failed to create log parent directory: {e}"))?;

            // make sure the file exists.
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .map_err(|e| eprintln!("ERROR: Failed to open log file: {e}"))?;
        }

        Ok(Writer::FileWriter {
            file_path,
            max_log_level,
        })
    }
}
