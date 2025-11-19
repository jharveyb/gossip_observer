use crate::logger::Writer;
use async_nats::jetstream;
use ldk_node::logger::{LogRecord, LogWriter};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinSet;
use tokio::time;
use tokio_util::sync::CancellationToken;

use crate::config::NATSConfig;

static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
// Max. export msg size is ~500B, we could go higher here
// TODO: adjust buffer size here; needs to stay below NATS msg_size limit; default 1 MB
static NATS_BATCH_SIZE: usize = 1024;
pub(crate) static STATS_INTERVAL: Duration = Duration::from_secs(60);
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
    // TODO: How do we join on this later?
    pub tasks: JoinSet<()>,
    export_tx: UnboundedSender<String>,
    export_rx: Option<UnboundedReceiver<String>>,
    stop_signal: CancellationToken,
}

impl Exporter for NATSExporter {
    fn export(&self, msg: String) {
        // Non-blocking send, so we don't hang the node.
        // TODO: handle a closed channel (who would close it?)
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
    pub fn new(cfg: NATSConfig, stop_signal: CancellationToken) -> Self {
        // Bridge sync -> async, since a sync send won't block for an unbounded channel.
        let (export_tx, export_rx) = tokio::sync::mpsc::unbounded_channel();
        let tasks = JoinSet::new();

        Self {
            cfg,
            tasks,
            export_tx,
            export_rx: Some(export_rx),
            stop_signal,
        }
    }

    // Build connections + spawn any long-running tasks we need for export.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        println!("Starting NATS exporter");

        let export_rx = self.export_rx.take().unwrap();
        let nats_client = async_nats::connect(self.cfg.server_addr.clone()).await?;
        let stream_ctx = jetstream::new(nats_client);

        let (nats_tx, nats_rx) = tokio::sync::mpsc::channel(NATS_BATCH_SIZE);
        {
            let queue_stop_signal = self.stop_signal.child_token();
            self.tasks.spawn(async move {
                Self::queue_exported_msg(export_rx, nats_tx, queue_stop_signal).await
            });
        }
        {
            let export_stop_signal = self.stop_signal.child_token();
            self.tasks.spawn(async move {
                Self::publish_msgs(stream_ctx, nats_rx, export_stop_signal).await
            });
        }
        Ok(())
    }

    // Move msg from our unbounded buffer fed by ldk-node, to our fixed-size buffer.
    // If the ring buffer is full, wait for another task to drain it. This should
    // be fine, since our input buffer is unbounded.
    // TODO: track backpressure here?
    async fn queue_exported_msg(
        mut rx: UnboundedReceiver<String>,
        tx: Sender<String>,
        stop_signal: CancellationToken,
    ) {
        let start_time = std::time::Instant::now();
        loop {
            let msg = match tokio::select! {
                rx_msg = rx.recv() => rx_msg,
                _ = stop_signal.cancelled() => {
                    break;
                }
            } {
                Some(mut rx_msg) => {
                    // Drop any messages within the startup window.
                    // TODO: Triggger this for every new connection vs. globally
                    if start_time.elapsed().as_secs() < *EXPORT_DELAY.get().unwrap() {
                        continue;
                    }

                    // Add our node ID to the message.
                    rx_msg.push_str(MSG_SUFFIX.get().unwrap());
                    rx_msg
                }
                None => {
                    println!("internal: queue_exported_msg: rx chan closed");
                    break;
                }
            };

            tokio::select! {
                tx_permit = tx.reserve() => {
                    match tx_permit {
                        Ok(permit) => permit.send(msg),
                        Err(e) => {
                            println!("internal: queue_exported_msg: tx chan error: {e}");
                            break;
                        }
                    }
                }
                _ = stop_signal.cancelled() => {
                    break;
                }
            }
        }
    }

    // Drain our fixed-size buffer of msgs by publishing to NATS.
    async fn publish_msgs(
        ctx: jetstream::Context,
        mut rx: Receiver<String>,
        stop_signal: CancellationToken,
    ) {
        let mut total_upload_time = 0;
        let mut msg_send_time;
        let mut msg_submit_interval;
        let mut upload_count = 0;
        let mut stats_waiter = time::interval(STATS_INTERVAL);
        let mut msg_batch = Vec::with_capacity(NATS_BATCH_SIZE);
        loop {
            tokio::select! {
                // Do we want recv_many here?
                rx_msg = rx.recv() => {
                    match rx_msg {
                        Some(msg) => msg_batch.push(msg),
                        None => {
                            println!("internal: publish_msgs: rx stream closed");
                            break;
                        }
                    }
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

            if msg_batch.len() >= NATS_BATCH_SIZE {
                // Our vec should have the same capacity after being drained.
                // std intersperse is nightly-only for now: https://github.com/rust-lang/rust/issues/79524
                let batch_output =
                    itertools::Itertools::join(&mut msg_batch.drain(..), INTER_MSG_DELIM);
                msg_send_time = std::time::Instant::now();
                let ack = ctx
                    .publish(NATS_SUBJECT.get().unwrap().as_str(), batch_output.into())
                    .await
                    .unwrap();
                // Wait for explicit ACK from collector.
                tokio::select! {
                    // TODO: propagate errors here, add timeout, etc.
                    // The timeout (or something else) is important to be able
                    // to handle a collector restart.
                    ack_res = ack => {
                        ack_res.unwrap()
                    }
                    _ = stop_signal.cancelled() => {
                        break;
                    }
                };

                msg_submit_interval = msg_send_time.elapsed().as_micros();
                total_upload_time += msg_submit_interval;
                upload_count += 1;
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
