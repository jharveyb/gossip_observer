use crate::logger::Writer;
use async_nats::jetstream;
use flume::Receiver;
use flume::Sender;
use ldk_node::logger::{LogRecord, LogWriter};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::config::NATSConfig;

static NATS_SUBJECT: OnceLock<String> = OnceLock::new();
static NODEKEY: OnceLock<String> = OnceLock::new();

// NATS subject name: observer.gossip.$NODEKEY
// Extra credit:
// Second subject for telemetry(?): observer.telemetry.$NODEKEY

pub trait Exporter: Send + Sync {
    // TODO: this should probably return a Result, though we should handle
    // errors here not in ldk-node
    fn export(&self, msg: String);

    // Provide extra data to be included alongside each exported message.
    fn set_export_metadata(&self, meta: String) -> Result<(), String>;
}

pub struct StdoutExporter {}

impl Exporter for StdoutExporter {
    fn export(&self, _msg: String) {
        let now = chrono::Utc::now().timestamp_micros();
        println!("{now}: exported!");
    }

    fn set_export_metadata(&self, _meta: String) -> Result<(), String> {
        Ok(())
    }
}

pub struct NATSExporter {
    cfg: NATSConfig,
    runtime: Arc<tokio::runtime::Runtime>,
    pub tasks: JoinSet<()>,
    export_tx: Sender<String>,
    export_rx: Receiver<String>,
    publish: Arc<Notify>,
    publish_ack: Arc<Notify>,
    nats_tx: Sender<String>,
    nats_rx: Receiver<String>,
    // What else?
}

impl Exporter for NATSExporter {
    fn export(&self, msg: String) {
        // Non-blocking send, so we don't hang the node.
        if let Err(e) = self.export_tx.send(msg) {
            // TODO: crash?
            println!("internal exporter error: export(): {e}");
        }
    }

    fn set_export_metadata(&self, meta: String) -> Result<(), String> {
        let stream_name = format!("{}.{}", self.cfg.stream, meta);
        NATS_SUBJECT.get_or_init(|| stream_name);
        NODEKEY.get_or_init(|| meta);
        Ok(())
    }
}

impl NATSExporter {
    pub fn new(cfg: NATSConfig, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        // Use flume channels to bridge sync -> async, and as a ring buffer.
        let (export_tx, export_rx) = flume::unbounded();
        // TODO: dynamic buffer size here; needs to stay below NATS msg_size limit
        // Max. export msg size is ~500B, we could go higher here
        let (nats_tx, nats_rx) = flume::bounded(256);
        // TODO: there's probably a better sync primitive for this
        let (publish, publish_ack) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let tasks = JoinSet::new();

        Self {
            cfg,
            runtime,
            tasks,
            export_tx,
            export_rx,
            publish,
            publish_ack,
            nats_tx,
            nats_rx,
        }
        // TODO: How do we handle NATS error? iunno, cancellation tokens I guess
    }

    // Build connections + spawn any long-running tasks we need for export.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        let export_rx = self.export_rx.clone();
        let nats_tx = self.nats_tx.clone();
        let publish = self.publish.clone();
        let publish_ack = self.publish_ack.clone();

        let nats_client = async_nats::connect(self.cfg.server_addr.clone()).await?;
        let stream_ctx = jetstream::new(nats_client);
        self.tasks.spawn_on(
            async move { Self::queue_exported_msg(export_rx, nats_tx, publish, publish_ack).await },
            self.runtime.clone().handle(),
        );

        let nats_rx = self.nats_rx.clone();
        let publish = self.publish.clone();
        let publish_ack = self.publish_ack.clone();
        self.tasks.spawn_on(
            async move { Self::publish_msgs(stream_ctx, nats_rx, publish, publish_ack).await },
            self.runtime.clone().handle(),
        );
        Ok(())
    }

    // Move msg from our unbounded buffer fed by ldk-node, to our ring buffer.
    // If the ring buffer is full, signal another task to drain it. This pauses
    // msg shuffling, but that's ok since our input buffer is unbounded.
    // TODO: track backpressure here?
    async fn queue_exported_msg(
        rx: Receiver<String>,
        tx: Sender<String>,
        publish: Arc<Notify>,
        publish_ack: Arc<Notify>,
    ) {
        while let Ok(msg) = rx.recv_async().await {
            if tx.is_full() {
                publish.notify_one();
                publish_ack.notified().await;
            }
            // Add our node ID to the message, after the timestamp.
            let msg_parts = msg.split_once(',').unwrap();
            let msg = format!("{},{},{}", msg_parts.0, NODEKEY.get().unwrap(), msg_parts.1);
            if let Err(e) = tx.send_async(msg).await {
                // TODO: crash?
                println!("internal exporter error: queue_exported_msg(): {e}");
            }
        }
    }

    // Drain our ring buffer of msgs by publishing to NATS.
    async fn publish_msgs(
        ctx: jetstream::Context,
        rx: Receiver<String>,
        publish: Arc<Notify>,
        publish_ack: Arc<Notify>,
    ) {
        loop {
            publish.notified().await;

            let msg_batch = rx.drain().collect::<Vec<_>>().join(";");
            // TODO: err handling here
            let msg_send_time = chrono::Utc::now().timestamp_micros();
            let ack = ctx
                .publish(NATS_SUBJECT.get().unwrap().as_str(), msg_batch.into())
                .await
                .unwrap();
            ack.await.unwrap();
            let msg_ack_time = chrono::Utc::now().timestamp_micros();
            println!("NATS upload time: {}us", msg_ack_time - msg_send_time);

            publish_ack.notify_one();
        }
    }
}

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
                // Format: format_args!("{now},{recv_peer},{msg_type},{msg_size},{msg}")
                // ldk-node/src/logger.rs#L248, LdkLogger.export()
                let msg = record.args.to_string();
                self.exporter.export(msg);
            }
            _ => {
                self.logger.log(record);
            }
        }
    }
}
