// This file is Copyright its original authors, visible in version control history.
//
// This file is licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. You may not use this file except in
// accordance with one or both of these licenses.

//! Logging-related objects.

use bitcoin::hashes::Hash;
use bitcoin::secp256k1::PublicKey;
use lightning::util::logger::{Logger as LdkLogger, Record as LdkRecord};
use lightning::util::ser::Writeable;

use ldk_node::logger::LogLevel;
use ldk_node::logger::LogRecord;
use ldk_node::logger::LogWriter;

use chrono::{TimeZone, Utc};
use log::Level as LogFacadeLevel;
use log::Record as LogFacadeRecord;

use core::fmt;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

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

// add a second writer that only handles exports
// we can send export lines over a channel to the actual exporter
// to keep things async
pub(crate) struct Logger {
    /// Specifies the logger's writer.
    writer: Writer,
}

impl Logger {
    /// Creates a new logger with a filesystem writer. The parameters to this function
    /// are the path to the log file, and the log level.
    pub fn new_fs_writer(file_path: String, max_log_level: LogLevel) -> Result<Self, ()> {
        if let Some(parent_dir) = Path::new(&file_path).parent() {
            fs::create_dir_all(parent_dir)
                .map_err(|e| eprintln!("ERROR: Failed to create log parent directory: {}", e))?;

            // make sure the file exists.
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .map_err(|e| eprintln!("ERROR: Failed to open log file: {}", e))?;
        }

        Ok(Self {
            writer: Writer::FileWriter {
                file_path,
                max_log_level,
            },
        })
    }

    pub fn new_custom_writer(log_writer: Arc<dyn LogWriter>) -> Self {
        Self {
            writer: Writer::CustomWriter(log_writer),
        }
    }
}

impl LdkLogger for Logger {
    fn log(&self, record: LdkRecord) {
        match &self.writer {
            Writer::FileWriter {
                file_path: _,
                max_log_level,
            } => {
                if record.level < *max_log_level {
                    return;
                }
                self.writer.log(record.into());
            }
            Writer::LogFacadeWriter => {
                self.writer.log(record.into());
            }
            Writer::CustomWriter(_arc) => {
                self.writer.log(record.into());
            }
        }
    }

    fn export(&self, their_node_id: PublicKey, msg: lightning::ln::msgs::UnsignedGossipMessage) {
        let msg_type = match msg {
            lightning::ln::msgs::UnsignedGossipMessage::ChannelAnnouncement(_) => "ca",
            lightning::ln::msgs::UnsignedGossipMessage::ChannelUpdate(_) => "cu",
            lightning::ln::msgs::UnsignedGossipMessage::NodeAnnouncement(_) => "na",
        };
        if let Writer::FileWriter { ref file_path, .. } = self.writer {
            if let Some(parent_dir) = Path::new(&file_path).parent() {
                let now = chrono::Utc::now().timestamp_micros();
                let recv_peer = their_node_id.to_string();
                let size = msg.serialized_length();
                let smthn = msg.encode();
                let msg_hash = bitcoin::hashes::sha256::Hash::hash(&smthn);
                let export_line = format!("{now},{msg_hash},{recv_peer},{msg_type},{size}\n");
                let export_path = parent_dir.join("gossip_export.csv");

                // CSV header:
                // unix_micros,msg_hash,recv_peer,type,size
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&export_path)
                    .expect("Failed to open log file")
                    .write_all(export_line.as_bytes())
                    .expect("Failed to write to log file")
            }
        }
    }
}

// Needed to use Logger as a custom logger.
impl LogWriter for Logger {
    fn log(&self, record: LogRecord) {
        self.writer.log(record);
    }
}
