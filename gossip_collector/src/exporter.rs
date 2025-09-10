use crate::logger::Writer;
use ldk_node::logger::{LogRecord, LogWriter};
use std::path::PathBuf;
use std::sync::Arc;

// An LDK logger that includes the behavior of the filesystem logger, but
// also exports gossip messages to object storage.
/*
pub struct Exporter {
    config: ExporterConfig,
    // fs_logger: ldk_node::logger::Logger,
}
    */

pub struct ExporterConfig {
    // filesystem logger
    storage_dir_path: PathBuf,
    log_file_path: Option<PathBuf>,
    log_level: ldk_node::logger::LogLevel,
    // object storage
}

// new trait: exporter
// receives strings forwarded from a Logger, and does something async
// here, we'll accrue some # of lines, then push to S3
// bucket name: region_network_impl_nodekey
// keep line format as CSV

// async behavior has to be handled by the exporter internally
pub trait Exporter: Send + Sync {
    fn export(&self, msg: Vec<u8>);
}

pub struct MockExporter {}

impl Exporter for MockExporter {
    fn export(&self, _msg: Vec<u8>) {
        let now = chrono::Utc::now().timestamp_micros();
        println!("{now}: exported!");
    }
}

// TODO: use something that contains a normal logger, and an exporter
// the exporter can have some async export method, and an inner config I guess?
// we'll want a ring buffer or similar inside this wrapper

/// A LogWriter that forwards regular logs to a file but prints export messages to stdout
pub struct LogWriterExporter {
    // Receiver for log messages
    logger: Writer,

    // Receiver for exported gossip messages
    exporter: Arc<dyn Exporter>,
}

impl LogWriterExporter {
    pub fn new(logger: Writer, exporter: Arc<dyn Exporter>) -> Self {
        Self { logger, exporter }
    }
}

impl LogWriter for LogWriterExporter {
    fn log(&self, record: LogRecord) {
        match record.module_path {
            "custom::gossip_collector" => {
                let msg = record.args.to_string();
                println!("{msg}");
                self.exporter.export(msg.into());
            }
            _ => {
                self.logger.log(record);
            }
        }
    }
}
