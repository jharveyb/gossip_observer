use crate::logger::Writer;
use async_nats::jetstream;
use flume::Receiver;
use flume::Sender;
use ldk_node::logger::{LogRecord, LogWriter};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::config::NATSConfig;

static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
pub(crate) static STATS_INTERVAL: Duration = Duration::from_secs(120);
static NATS_SUBJECT: OnceLock<String> = OnceLock::new();
static NODEKEY: OnceLock<String> = OnceLock::new();
static MSG_SUFFIX: OnceLock<String> = OnceLock::new();
static EXPORT_DELAY: OnceLock<u64> = OnceLock::new();

// NATS subject name: observer.gossip.$NODEKEY
// Extra credit:
// Second subject for telemetry(?): observer.telemetry.$NODEKEY

pub trait Exporter: Send + Sync {
    // TODO: this should probably return a Result, though we should handle
    // errors here not in ldk-node
    fn export(&self, msg: String);

    // Provide extra data to be included alongside each exported message.
    fn set_export_metadata(&self, meta: String) -> Result<(), String>;

    // Set the global delay before exporting any messages.
    fn set_export_delay(&self, delay: u64) -> anyhow::Result<()>;
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

    fn set_export_delay(&self, _delay: u64) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct NATSExporter {
    cfg: NATSConfig,
    runtime: Arc<tokio::runtime::Runtime>,
    // TODO: How do we join on this later?
    pub tasks: JoinSet<()>,
    export_tx: Sender<String>,
    export_rx: Receiver<String>,
    publish_ready_tx: Sender<bool>,
    publish_ready_rx: Receiver<bool>,
    publish_ack_tx: Sender<bool>,
    publish_ack_rx: Receiver<bool>,
    nats_tx: Sender<String>,
    nats_rx: Receiver<String>,
    stop_signal: CancellationToken,
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
        NODEKEY.get_or_init(|| meta.clone());
        MSG_SUFFIX.get_or_init(|| format!("{}{}", INTRA_MSG_DELIM, meta));
        Ok(())
    }

    fn set_export_delay(&self, delay: u64) -> anyhow::Result<()> {
        println!("NATS export delay: {} seconds", delay);
        EXPORT_DELAY.get_or_init(|| delay);
        Ok(())
    }
}

impl NATSExporter {
    pub fn new(
        cfg: NATSConfig,
        runtime: Arc<tokio::runtime::Runtime>,
        stop_signal: CancellationToken,
    ) -> Self {
        // Use flume channels to bridge sync -> async, and as a ring buffer.
        let (export_tx, export_rx) = flume::unbounded();
        // TODO: adjust buffer size here; needs to stay below NATS msg_size limit; default 1 MB
        // Max. export msg size is ~500B, we could go higher here
        let (nats_tx, nats_rx) = flume::bounded(1024);
        // TODO: there's probably a better sync primitive for this
        let (publish_ready_tx, publish_ready_rx) = flume::bounded(1);
        let (publish_ack_tx, publish_ack_rx) = flume::bounded(1);
        let tasks = JoinSet::new();

        Self {
            cfg,
            runtime,
            tasks,
            export_tx,
            export_rx,
            publish_ready_tx,
            publish_ready_rx,
            publish_ack_tx,
            publish_ack_rx,
            nats_tx,
            nats_rx,
            stop_signal,
        }
    }

    // Build connections + spawn any long-running tasks we need for export.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        println!("Starting NATS exporter");
        let export_rx = self.export_rx.clone();
        let nats_tx = self.nats_tx.clone();
        let publish_ready_tx = self.publish_ready_tx.clone();
        let publish_ack_tx = self.publish_ack_tx.clone();
        let publish_ready_rx = self.publish_ready_rx.clone();
        let publish_ack_rx = self.publish_ack_rx.clone();

        let nats_client = async_nats::connect(self.cfg.server_addr.clone()).await?;
        let stream_ctx = jetstream::new(nats_client);

        let queue_stop_signal = self.stop_signal.child_token();
        let export_stop_signal = self.stop_signal.child_token();
        self.tasks.spawn_on(
            async move {
                Self::queue_exported_msg(
                    export_rx,
                    nats_tx,
                    publish_ready_tx,
                    publish_ack_rx,
                    queue_stop_signal,
                )
                .await
            },
            self.runtime.clone().handle(),
        );

        let nats_rx = self.nats_rx.clone();
        self.tasks.spawn_on(
            async move {
                Self::publish_msgs(
                    stream_ctx,
                    nats_rx,
                    publish_ready_rx,
                    publish_ack_tx,
                    export_stop_signal,
                )
                .await
            },
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
        publish_ready_tx: Sender<bool>,
        publish_ack_rx: Receiver<bool>,
        stop_signal: CancellationToken,
    ) {
        let start_time = std::time::Instant::now();
        loop {
            tokio::select! {
                Ok(mut msg) = rx.recv_async() => {
                    // Drop any messages within the startup window.
                    // TODO: Triggger this for every new connection vs. globally
                    if start_time.elapsed().as_secs() < *EXPORT_DELAY.get().unwrap() {
                        continue;
                    }

                    // Drain msgs to NATS
                    if tx.is_full() {
                        publish_ready_tx.send_async(true).await.unwrap();
                        publish_ack_rx.recv_async().await.unwrap();
                    }

                    // Add our node ID to the message.
                    msg.push_str(MSG_SUFFIX.get().unwrap());
                    if let Err(e) = tx.send_async(msg).await {
                        // TODO: crash?
                        println!("internal exporter error: queue_exported_msg(): {e}");
                    }
                }
                _ = stop_signal.cancelled() => {
                    break;
                }
            };
        }
    }

    // Drain our ring buffer of msgs by publishing to NATS.
    async fn publish_msgs(
        ctx: jetstream::Context,
        rx: Receiver<String>,
        publish_ready_rx: Receiver<bool>,
        publish_ack_tx: Sender<bool>,
        stop_signal: CancellationToken,
    ) {
        let mut total_upload_time = 0;
        let mut msg_send_time;
        let mut msg_submit_interval;
        let mut upload_count = 0;
        let mut stats_waiter = time::interval(STATS_INTERVAL);
        loop {
            tokio::select! {
                _ = publish_ready_rx.recv_async() => {
                    let msg_batch = rx.drain().collect::<Vec<_>>().join(INTER_MSG_DELIM);
                    // TODO: err handling here
                    // TODO: Is this useful to measure?
                    msg_send_time = std::time::Instant::now();
                    let ack = ctx
                        .publish(NATS_SUBJECT.get().unwrap().as_str(), msg_batch.into())
                        .await
                        .unwrap();
                    ack.await.unwrap();
                    msg_submit_interval = msg_send_time.elapsed().as_micros();
                    total_upload_time += msg_submit_interval;
                    upload_count += 1;
                    publish_ack_tx.send_async(true).await.unwrap();
                }
                _ = stats_waiter.tick() => {
                    if upload_count > 0 {
                        println!(
                            "Avg. NATS upload time: {}us",
                            total_upload_time / upload_count
                        );
                    }
                }
                _ = stop_signal.cancelled() => {
                    break;
                }
            }
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
                let msg = record.args.to_string();
                self.exporter.export(msg);
            }
            _ => {
                self.logger.log(record);
            }
        }
    }
}
