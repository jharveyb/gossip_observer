use crate::config::Nats;
use crate::logger::Writer;
use crate::peer_conn_manager::PeerConnManagerHandle;
use anyhow::bail;
use async_nats::jetstream;
use ldk_node::logger::{LogRecord, LogWriter};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Barrier, watch};
use tokio::task::JoinSet;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;

static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
pub(crate) static STATS_INTERVAL: Duration = Duration::from_secs(600);

// NATS subject name: observer.gossip.$NODEKEY
// Extra credit:
// Second subject for telemetry(?): observer.telemetry.$NODEKEY

pub trait Exporter: Send + Sync {
    // TODO: this should probably return a Result, though we should handle
    // errors here not in ldk-node
    fn export(&self, msg: String);
}

#[allow(dead_code)]
pub struct StdoutExporter {}

impl Exporter for StdoutExporter {
    fn export(&self, _msg: String) {
        let now = chrono::Utc::now().timestamp_micros();
        debug!(timestamp = now, "Exported message");
    }
}

#[derive(Clone)]
struct ExportMetadata {
    // The collector pubkey we add to our message.
    msg_suffix: String,
    // Needed to start our NATS exporter.
    nats_subject: String,
}

pub struct NATSExporter {
    cfg: Nats,
    conn_manager: PeerConnManagerHandle,
    export_tx: UnboundedSender<String>,
    export_rx: Option<UnboundedReceiver<String>>,
    stop_signal: CancellationToken,
    metadata_tx: watch::Sender<Option<ExportMetadata>>,
    startup_barrier: Arc<Barrier>,
}

impl Exporter for NATSExporter {
    fn export(&self, msg: String) {
        // Non-blocking send, so we don't hang the node.
        if let Err(e) = self.export_tx.send(msg) {
            error!(error = %e, "Internal exporter error in export()");
            self.stop_signal.cancel();
        }
    }
}

impl NATSExporter {
    pub fn new(
        cfg: Nats,
        conn_manager: PeerConnManagerHandle,
        stop_signal: CancellationToken,
    ) -> Self {
        // Bridge sync -> async, since a sync send won't block for an unbounded channel.
        let (export_tx, export_rx) = tokio::sync::mpsc::unbounded_channel();
        let (metadata_tx, _) = watch::channel(None);
        // 3 tasks: main thread that calls set_export_metadata(),
        // queue_exported_msg, and publish_msgs
        let startup_barrier = Arc::new(Barrier::new(3));

        Self {
            cfg,
            conn_manager,
            export_tx,
            export_rx: Some(export_rx),
            stop_signal,
            metadata_tx,
            startup_barrier,
        }
    }

    /// Set the NATS subject and message suffix derived from our node ID,
    /// then wait for both exporter tasks to acknowledge via the barrier.
    pub async fn set_export_metadata(&self, node_id: String) -> anyhow::Result<()> {
        let metadata = ExportMetadata {
            nats_subject: format!("{}.{}", self.cfg.stream, node_id),
            msg_suffix: format!("{}{}", INTRA_MSG_DELIM, node_id),
        };
        self.metadata_tx.send_replace(Some(metadata));
        tokio::select! {
            _ = self.startup_barrier.wait() => {}
            _ = self.stop_signal.cancelled() => {
                bail!("Exporter task failed during startup");
            }
        }
        Ok(())
    }

    // Build connections + spawn any long-running tasks we need for export.
    pub async fn start(
        &mut self,
        mut task_pool: JoinSet<anyhow::Result<()>>,
    ) -> anyhow::Result<JoinSet<anyhow::Result<()>>> {
        info!("Starting NATS exporter");

        let export_rx = self.export_rx.take().unwrap();
        let nats_client = async_nats::connect(self.cfg.server_addr.clone()).await?;
        let stream_ctx = jetstream::new(nats_client);
        // Allow longer timeout in case we get stalled. Also support reconnects.
        /*
        let nats_client_options = ConnectOptions::new()
            .connection_timeout(Duration::from_secs(120))
            .max_reconnects(None);

        let nats_client =
            async_nats::connect_with_options(self.cfg.server_addr.clone(), nats_client_options)
                .await?;
        // Extend the timeouts here as well.
        let stream_ctx = ContextBuilder::new()
            .timeout(Duration::from_secs(120))
            .ack_timeout(Duration::from_secs(60))
            .build(nats_client);
        */

        let (nats_tx, nats_rx) = tokio::sync::mpsc::channel(self.cfg.batch_size as usize);
        {
            let conn_manager = self.conn_manager.clone();
            let queue_stop_signal = self.stop_signal.clone();
            let barrier = self.startup_barrier.clone();
            let metadata_rx = self.metadata_tx.subscribe();
            task_pool.spawn(Self::queue_exported_msg(
                export_rx,
                nats_tx,
                conn_manager,
                queue_stop_signal,
                barrier,
                metadata_rx,
            ));
        }
        {
            let export_stop_signal = self.stop_signal.clone();
            let batch_size = self.cfg.batch_size as usize;
            let barrier = self.startup_barrier.clone();
            let metadata_rx = self.metadata_tx.subscribe();
            task_pool.spawn(Self::publish_msgs(
                stream_ctx,
                batch_size,
                nats_rx,
                export_stop_signal,
                barrier,
                metadata_rx,
            ));
        }

        Ok(task_pool)
    }

    // Move msg from our unbounded buffer fed by ldk-node, to our fixed-size buffer.
    // If the ring buffer is full, wait for another task to drain it. This should
    // be fine, since our input buffer is unbounded.
    async fn queue_exported_msg(
        mut rx: UnboundedReceiver<String>,
        tx: Sender<String>,
        mut conn_manager: PeerConnManagerHandle,
        stop_signal: CancellationToken,
        barrier: Arc<Barrier>,
        metadata_rx: watch::Receiver<Option<ExportMetadata>>,
    ) -> anyhow::Result<()> {
        // Wait for export metadata to be set before processing messages.
        // Any gossip messages emitted by LDK during startup will queue in the
        // unbounded channel and be processed after the barrier releases.
        tokio::select! {
            _ = barrier.wait() => {}
            _ = stop_signal.cancelled() => {
                return Ok(());
            }
        }

        let _drop_guard = stop_signal.drop_guard_ref();
        let msg_suffix = metadata_rx
            .borrow()
            .as_ref()
            .expect("metadata must be set before barrier releases")
            .msg_suffix
            .clone();

        // Local version of the peer filter set; mark the initial value as
        // changed before we start.
        let mut msg_filter_rules = HashSet::new();
        msg_filter_rules.clone_from(&conn_manager.pending_notifier.borrow_and_update());
        loop {
            let msg = match tokio::select! {
                // Prioritize receiving config updates over other events.
                biased;

                _ = stop_signal.cancelled() => {
                    break;
                }
                _ = conn_manager.pending_notifier.changed() => {
                    // After a changed() call, the value is already marked as seen, so borrow() could be used.
                    // borrow_and_update() is used to safely handle a potential race:
                    // https://docs.rs/tokio/latest/tokio/sync/watch/index.html#borrow_and_update-versus-borrow
                    msg_filter_rules.clone_from(&conn_manager.pending_notifier.borrow_and_update());

                    // Sort for readability.
                    if !msg_filter_rules.is_empty() {
                        let mut filter_fmt = msg_filter_rules.iter().collect::<Vec<_>>();
                        filter_fmt.sort_unstable();
                        debug!(filter = ?filter_fmt, "Exporter: New filter config details");
                        info!(filter_len = filter_fmt.len(), "Exporter: New filter config");
                    }

                    // We don't want to return anything, so just skip any following logic.
                    continue;
                }
                rx_msg = rx.recv() => rx_msg,
            } {
                Some(mut rx_msg) => {
                    // Add our node ID to the message.
                    rx_msg.push_str(&msg_suffix);
                    rx_msg
                }
                None => {
                    error!("Internal: queue_exported_msg: rx chan closed");
                    bail!("Internal: queue_exported_msg: rx chan closed");
                }
            };

            // Filter out messages from specific peers we've connected to recently.
            // Peer pubkey is field 2.
            match msg.split(INTRA_MSG_DELIM).nth(1) {
                Some(recv_peer) => {
                    if msg_filter_rules.contains(recv_peer) {
                        continue;
                    }
                }
                // Should not happen, this means msg formatting
                // is broken.
                None => {
                    error!("Internal: publish_msgs: rx msg format broken");
                    error!(msg = %msg, "Broken message");
                    bail!("Internal: publish_msgs: rx msg format broken");
                }
            }

            tokio::select! {
                tx_permit = tx.reserve() => {
                    match tx_permit {
                        Ok(permit) => permit.send(msg),
                        Err(e) => {
                            error!(error = %e, "Internal: queue_exported_msg: tx chan error");
                            bail!("Internal: queue_exported_msg: tx chan error");
                        }
                    }
                }
                _ = stop_signal.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }

    // Drain our fixed-size buffer of msgs by publishing to NATS.
    async fn publish_msgs(
        ctx: jetstream::Context,
        batch_size: usize,
        mut rx: Receiver<String>,
        stop_signal: CancellationToken,
        barrier: Arc<Barrier>,
        metadata_rx: watch::Receiver<Option<ExportMetadata>>,
    ) -> anyhow::Result<()> {
        // Wait for export metadata to be set before processing messages.
        tokio::select! {
            _ = barrier.wait() => {}
            _ = stop_signal.cancelled() => {
                return Ok(());
            }
        }

        let _drop_guard = stop_signal.drop_guard_ref();
        let nats_subject = metadata_rx
            .borrow()
            .as_ref()
            .expect("metadata must be set before barrier releases")
            .nats_subject
            .clone();

        let mut total_upload_time = 0;
        let mut msg_send_time;
        let mut msg_submit_interval;
        let mut upload_count = 0;
        let mut stats_waiter = time::interval(STATS_INTERVAL);
        let mut msg_batch = Vec::with_capacity(batch_size);

        loop {
            tokio::select! {
                rx_msg = rx.recv() => {
                    match rx_msg {
                        Some(msg) => {
                            msg_batch.push(msg);
                        },
                        None => {
                            error!("Internal: publish_msgs: rx stream closed");
                            bail!("Internal: publish_msgs: rx stream closed");
                        }
                    }
                }
                _ = stop_signal.cancelled() => {
                    break;
                }
                _ = stats_waiter.tick() => {
                    if upload_count > 0 {
                        info!(
                            avg_time_ms = total_upload_time / upload_count,
                            "Avg. NATS upload + ACK time"
                        );
                        upload_count = 0;
                    }
                }
            }

            if msg_batch.len() >= batch_size {
                // Our vec should have the same capacity after being drained.
                // std intersperse is nightly-only for now: https://github.com/rust-lang/rust/issues/79524
                let batch_output =
                    itertools::Itertools::join(&mut msg_batch.drain(..), INTER_MSG_DELIM);
                msg_send_time = std::time::Instant::now();
                let ack = ctx
                    .publish(nats_subject.clone(), batch_output.into())
                    .await
                    .unwrap();
                // Wait for explicit ACK from collector.
                tokio::select! {
                    // TODO: propagate errors here, add timeout, etc.
                    // The timeout (or something else) is important to be able
                    // to handle a collector restart.
                    // Update: maybe not
                    ack_res = ack => {
                        ack_res.unwrap()
                    }
                    _ = stop_signal.cancelled() => {
                        break;
                    }
                };

                msg_submit_interval = msg_send_time.elapsed().as_millis();
                total_upload_time += msg_submit_interval;
                upload_count += 1;
            }
        }

        Ok(())
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
