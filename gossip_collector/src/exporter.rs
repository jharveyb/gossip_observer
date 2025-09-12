use crate::logger::Writer;
use flume::Receiver;
use flume::Sender;
use ldk_node::logger::{LogRecord, LogWriter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::sleep;

use crate::config::NATSConfig;

// NATS subject name: observer.gossip.$NODEKEY
// Extra credit:
// Second subject for telemetry(?): observer.telemetry.$NODEKEY

pub trait Exporter: Send + Sync {
    // TODO: this should probably return a Result, though we should handle
    // errors here not in ldk-node
    fn export(&self, msg: String);
}

pub struct StdoutExporter {}

impl Exporter for StdoutExporter {
    fn export(&self, _msg: String) {
        let now = chrono::Utc::now().timestamp_micros();
        println!("{now}: exported!");
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
    // actual jetstream client
    // What else?
}

impl Exporter for NATSExporter {
    fn export(&self, msg: String) {
        // Non-blocking send, so we don't hang the node.
        // TODO: annotate the message with our pubkey!
        if let Err(e) = self.export_tx.send(msg) {
            // TODO: crash?
            println!("internal exporter error: export(): {e}");
        }
    }
}

impl NATSExporter {
    pub fn new(cfg: NATSConfig, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        // Use flume channels to bridge sync -> async, and as a ring buffer.
        let (export_tx, export_rx) = flume::unbounded();
        // TODO: dynamic buffer size here; needs to stay below NATS msg_size limit
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
    pub fn start(&mut self) -> anyhow::Result<()> {
        let export_rx = self.export_rx.clone();
        let nats_tx = self.nats_tx.clone();
        let publish = self.publish.clone();
        let publish_ack = self.publish_ack.clone();
        self.tasks.spawn_on(
            async move { Self::queue_exported_msg(export_rx, nats_tx, publish, publish_ack).await },
            self.runtime.clone().handle(),
        );

        let nats_rx = self.nats_rx.clone();
        let publish = self.publish.clone();
        let publish_ack = self.publish_ack.clone();
        self.tasks.spawn_on(
            async move { Self::publish_msgs(nats_rx, publish, publish_ack).await },
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
                println!("RX queue size: notifying: {}", rx.len());
                publish.notify_one();
                publish_ack.notified().await;
            }
            if let Err(e) = tx.send_async(msg).await {
                // TODO: crash?
                println!("internal exporter error: queue_exported_msg(): {e}");
            }
        }
    }

    // Drain our ring buffer of msgs by publishing to NATS.
    async fn publish_msgs(rx: Receiver<String>, publish: Arc<Notify>, publish_ack: Arc<Notify>) {
        loop {
            publish.notified().await;

            // TODO: add delimiter that's not a comma
            let msg_batch = rx.drain().collect::<Vec<_>>();
            // do NATS stuff
            // println!("NATS: {msg_batch:?}");
            println!("NATS publish!");

            // artificial delay to test queueing in the unbounded channel
            sleep(Duration::from_millis(500)).await;

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
