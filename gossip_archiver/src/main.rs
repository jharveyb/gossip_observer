use std::sync::Arc;
use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use async_nats::jetstream;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::Stream;
use futures::StreamExt;
use tokio::time::sleep;

// TODO: move to some gossip_common module
static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";

#[derive(Debug, Clone)]
pub struct GossipMessage {
    pub recv_timestamp: DateTime<Utc>,
    // node pubkeys
    pub collector: String,
    pub recv_peer: String,
    pub msg_type: String,
    pub msg_size: u16,
    pub msg: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let nats_server = "100.68.143.113:4222";
    let nats_client = async_nats::connect(nats_server).await?;
    let stream_ctx = jetstream::new(nats_client);
    let stream = upsert_stream(stream_ctx).await?;
    let consumer = upsert_consumer(stream).await?;
    let msg_stream = consumer.messages().await?;

    let nats_handle = tokio::spawn(async move { nats_reader(msg_stream).await });
    let _ = tokio::try_join!(nats_handle)?;
    Ok(())
}

pub async fn nats_reader(mut stream: jetstream::consumer::pull::Stream) -> anyhow::Result<()> {
    while let Some(message) = stream.next().await {
        let message = message.map_err(anyhow::Error::msg)?;
        message.ack().await.map_err(anyhow::Error::msg)?;

        let mut contents = str::from_utf8(&message.payload)?
            .split(INTER_MSG_DELIM)
            .map(decode_msg)
            .collect::<Result<Vec<_>, _>>()?;

        // Actual message handling
        contents.sort_unstable_by_key(|m| m.recv_timestamp);

        // do smthn simple like print msg_types
        let mut freq_map = HashMap::with_capacity(contents.len());
        for msg in &contents {
            freq_map
                .entry(&msg.msg_type)
                .and_modify(|e| *e += 1)
                .or_insert(1);
        }
        println!(
            "Earliest: {:?}, Latest: {:?}",
            contents.first().unwrap().recv_timestamp,
            contents.last().unwrap().recv_timestamp,
        );
        println!("Message types: {:?}", freq_map);
    }
    Ok(())
}

// This should mirror whatever we're exporting to NATS
pub fn decode_msg(msg: &str) -> anyhow::Result<GossipMessage> {
    let parts = msg.split(INTRA_MSG_DELIM).collect::<Vec<&str>>();
    if parts.len() != 6 {
        anyhow::bail!("Invalid message from collector: {msg}")
    }

    Ok(GossipMessage {
        recv_timestamp: DateTime::from_timestamp_micros(parts[0].parse::<i64>()?).unwrap(),
        collector: parts[1].to_string(),
        recv_peer: parts[2].to_string(),
        msg_type: parts[3].to_string(),
        msg_size: parts[4].parse::<u16>()?,
        msg: parts[5].to_string(),
    })
}

pub async fn upsert_stream(ctx: jetstream::Context) -> anyhow::Result<jetstream::stream::Stream> {
    let stream_name = "main";
    let stream = ctx
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name.to_string(),
            subjects: vec!["observer.*".to_string()],
            retention: jetstream::stream::RetentionPolicy::WorkQueue,
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await?;
    Ok(stream)
}

pub async fn upsert_consumer(
    ctx: jetstream::stream::Stream,
) -> anyhow::Result<jetstream::consumer::PullConsumer> {
    let cons_name = "gossip_recv";
    let consumer = ctx
        .get_or_create_consumer(
            cons_name,
            jetstream::consumer::pull::Config {
                durable_name: Some(cons_name.to_string()),
                name: Some(cons_name.to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                filter_subject: "observer.*".to_string(),
                ..Default::default()
            },
        )
        .await?;
    Ok(consumer)
}
