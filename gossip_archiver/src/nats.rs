use anyhow::bail;
use async_nats::Message;
use async_nats::jetstream;
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{self};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

static STATS_INTERVAL: Duration = Duration::from_secs(600);

// Wrapper that handles reconnection when the stream closes
// This shouldn't really be needed, let's see
pub async fn nats_reader_with_reconnect(
    nats_client: async_nats::Client,
    cfg: (String, String, String),
    raw_msg_tx: UnboundedSender<Message>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let (stream_name, consumer_name, subject_prefix) = cfg;
    let _drop_guard = cancel_token.drop_guard_ref();

    loop {
        info!("Setting up NATS JetStream consumer");

        if cancel_token.is_cancelled() {
            info!("NATS JetStream consumer reconnect cancelled");
            return Ok(());
        }

        let stream_ctx = jetstream::new(nats_client.clone());
        let stream = match upsert_stream(stream_ctx, &stream_name, &subject_prefix).await {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "Failed to create/get stream, retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let consumer = match upsert_consumer(stream, &consumer_name, &subject_prefix).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to create/get consumer, retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let msg_stream = match consumer.messages().await {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "Failed to create message stream, retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("NATS JetStream consumer ready");

        // Run the reader; if it returns (stream closed), we'll reconnect
        match nats_reader(msg_stream, raw_msg_tx.clone()).await {
            Ok(_) => {
                warn!("NATS reader exited normally, reconnecting in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Err(e) => {
                error!(error = %e, "NATS reader error");
                cancel_token.cancel();
                bail!(e);

                // tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// pull msgs from NATS, and ACK the sender / collector so they can continue
pub async fn nats_reader(
    mut stream: jetstream::consumer::pull::Stream,
    raw_msg_tx: UnboundedSender<Message>,
) -> anyhow::Result<()> {
    info!("Starting NATS reader");

    let mut msg_count = 0;
    let mut stats_waiter = time::interval(STATS_INTERVAL);
    loop {
        let mut msg_ack_pair = None;
        tokio::select! {
            nats_msg = stream.next() => {
                match nats_msg {
                    Some(msg) => {
                        // TODO: if we crash while holding an ACK handle, will
                        // that hang the collector? I think so, with the current
                        // stream policy
                        msg_ack_pair = Some(msg.map_err(anyhow::Error::msg)?.split());
                    }
                    // This may have been tripping before if the stream was set
                    // up incorrectly on reconnect.
                    None => {
                        warn!(
                            "NATS stream closed, will reconnect"
                        );
                        return Ok(());  // Return Ok to trigger reconnection
                    }
                }
            }
            _ = stats_waiter.tick() => {
                info!(
                    msg_per_min = msg_count,
                    "NATS reader stats"
                );
                msg_count = 0;
            }
        };

        // ACK before we actually write to DB; yeet
        if let Some((message, ack_handle)) = msg_ack_pair {
            raw_msg_tx.send(message)?;
            ack_handle.ack().await.map_err(anyhow::Error::msg)?;
            msg_count += 1;
        }
    }
}

// TODO: add flag to recreate?
pub async fn upsert_consumer(
    ctx: jetstream::stream::Stream,
    cons_name: &str,
    filter_subject: &str,
) -> anyhow::Result<jetstream::consumer::PullConsumer> {
    // TODO: review ack policy, behavior if collector is down for maintenance
    let consumer = ctx
        .get_or_create_consumer(
            cons_name,
            jetstream::consumer::pull::Config {
                durable_name: Some(cons_name.to_string()),
                name: Some(cons_name.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                filter_subject: filter_subject.to_string(),
                ..Default::default()
            },
        )
        .await?;
    Ok(consumer)
}

// TODO: add flag to recreate?
pub async fn upsert_stream(
    ctx: jetstream::Context,
    stream_name: &str,
    subjects: &str,
) -> anyhow::Result<jetstream::stream::Stream> {
    let stream = ctx
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name.to_string(),
            subjects: vec![subjects.to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await?;
    Ok(stream)
}
