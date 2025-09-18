use std::time::Duration;

use anyhow::anyhow;
use async_duckdb::Client as DuckClient;
use async_duckdb::ClientBuilder as DuckClientBuilder;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
// use duckdb::Connection;
use async_duckdb::duckdb::DropBehavior;
use async_duckdb::duckdb::params;
use async_duckdb::duckdb::types::Null as DuckNull;
use futures::Stream;
use futures::StreamExt;
use tokio::time::{self};

// TODO: move to some gossip_common module
static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
static TABLE_NAME: &str = "gossip_archive";

#[derive(Debug, Clone)]
pub struct GossipMessage {
    pub recv_timestamp: DateTime<Utc>,
    // node pubkeys
    pub collector: String,
    pub recv_peer: String,
    pub msg_type: u8,
    pub msg_size: u16,
    pub msg: String,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum MessageType {
    Unknown,
    ChannelAnnouncement,
    NodeAnnouncement,
    ChannelUpdate,
}

impl std::str::FromStr for MessageType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ca" => Ok(Self::ChannelAnnouncement),
            "na" => Ok(Self::NodeAnnouncement),
            "cu" => Ok(Self::ChannelUpdate),
            _ => Err(anyhow::Error::msg("Invalid message type")),
        }
    }
}

impl From<MessageType> for u8 {
    fn from(msg_type: MessageType) -> Self {
        msg_type as u8
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let nats_server = "100.68.143.113:4222";
    let nats_client = async_nats::connect(nats_server).await?;
    let stream_ctx = jetstream::new(nats_client);
    let stream = upsert_stream(stream_ctx).await?;
    let consumer = upsert_consumer(stream).await?;
    let msg_stream = consumer.messages().await?;
    println!("Set up NATS stream and consumer");

    // TODO: unbounded feels sketch but whatevs
    let (msg_tx, msg_rx) = flume::unbounded();

    let db_path = "./data/mainnet/gossip_archive.duckdb";
    let mut db_client = DuckClientBuilder::new().path(db_path).open().await?;
    init_db(&db_client).await?;
    println!("Initialized DuckDB connection");

    let nats_recv = msg_tx.clone();
    let nats_handle = tokio::spawn(async move { nats_reader(msg_stream, nats_recv).await });
    let duckdb_handle =
        tokio::spawn(async move { db_writer(&mut db_client, msg_rx.clone()).await });

    let stats_chan = msg_tx.clone();
    let stats_handle = tokio::spawn(async move {
        let mut stats_waiter = time::interval(Duration::from_secs(120));
        loop {
            tokio::select! {
                _ = stats_waiter.tick() => {
                    println!("NATS buffer size: {}", stats_chan.clone().len());
                }
            }
        }
    });

    let _ = tokio::try_join!(nats_handle, duckdb_handle, stats_handle)?;
    Ok(())
}

pub async fn db_writer(
    db: &mut DuckClient,
    msg_rx: flume::Receiver<GossipMessage>,
) -> anyhow::Result<()> {
    // Pick this based on observed ingest rate?
    let batch_size = 10_000;
    let flush_interval = Duration::from_secs(5);

    let mut flush_waiter = time::interval(flush_interval);
    let mut stats_waiter = time::interval(Duration::from_secs(120));

    let (db_buf_tx, db_buf_rx) = flume::bounded(batch_size);
    let (buf_full_tx, buf_full_rx) = flume::bounded(1);
    let (buf_ack_tx, buf_ack_rx) = flume::bounded(1);

    // TODO: join / return two join handles?
    let _buf_fill = tokio::spawn(async move {
        while let Ok(msg) = msg_rx.recv_async().await {
            if db_buf_tx.is_full() {
                // Block on DB write
                buf_full_tx.send_async(true).await.unwrap();
                buf_ack_rx.recv_async().await.unwrap();
            }

            if let Err(e) = db_buf_tx.send_async(msg).await {
                // TODO: crash?
                println!("internal archiver error: db_writer(): {e}");
            }
        }
    });

    let mut flush_counter = 0;
    let mut poll_counter = 0;
    println!("Starting DB writer");
    loop {
        tokio::select! {
            _ = flush_waiter.tick() => {
                let db_batch = db_buf_rx.clone().drain().collect::<Vec<_>>();
                if db_batch.is_empty() {
                    continue;
                }
                // let db_batch: Vec<GossipMessage> = msg_rx.stream().take(batch_size).collect().await;
                db_batch_write(db, db_batch).await?;
                flush_counter += 1;
            }
            _ = buf_full_rx.recv_async() => {
                    let db_batch = db_buf_rx.clone().drain().collect::<Vec<_>>();
                    // let db_batch: Vec<GossipMessage> = msg_rx.stream().take(batch_size).collect().await;
                    db_batch_write(db, db_batch).await?;
                    poll_counter += 1;
                    buf_ack_tx.send_async(true).await.unwrap();
            }
            _ = stats_waiter.tick() => {
                println!("Flush count: {flush_counter}, Poll count: {poll_counter}");
                flush_counter = 0;
                poll_counter = 0;
            }
        };
    }
}

pub async fn db_batch_write(db: &mut DuckClient, msgs: Vec<GossipMessage>) -> anyhow::Result<()> {
    db.conn_mut(|c| {
        let mut tx = c.transaction()?;
        tx.set_drop_behavior(DropBehavior::Commit);
        let mut append = tx.appender(TABLE_NAME)?;
        for msg in msgs {
            // We need a NULL placeholder here for the autoincrement column,
            // otherwise our number of columns won't match the table schema
            // and all appends will fail.
            append.append_row(params![
                DuckNull,
                msg.recv_timestamp,
                msg.collector,
                msg.recv_peer,
                msg.msg_type,
                msg.msg_size,
                msg.msg
            ])?;
        }
        append.flush()
    })
    .await?;
    Ok(())
}

pub async fn nats_reader(
    mut stream: jetstream::consumer::pull::Stream,
    msg_tx: flume::Sender<GossipMessage>,
) -> anyhow::Result<()> {
    println!("Starting NATS reader");

    let mut last_recv_time = std::time::Instant::now();
    let mut msg_count = 0;
    while let Some(message) = stream.next().await {
        let message = message.map_err(anyhow::Error::msg)?;
        let contents = str::from_utf8(&message.payload)?.split(INTER_MSG_DELIM);
        for raw_msg in contents {
            msg_tx.send_async(decode_msg(raw_msg)?).await?;
        }

        message.ack().await.map_err(anyhow::Error::msg)?;

        // TODO: use select & interval instead
        msg_count += 1;
        if last_recv_time.elapsed().as_secs() > 120 && msg_count >= 2 {
            println!("Avg. NATS msg/min: {}", msg_count / 2);
            last_recv_time = std::time::Instant::now();
            msg_count = 0;
        }
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
        msg_type: parts[3].parse::<MessageType>()?.into(),
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

pub async fn init_db(db: &DuckClient) -> anyhow::Result<()> {
    let create_autoinc_sql = "
        CREATE SEQUENCE IF NOT EXISTS id_seq START 1;
    ";
    let create_table_sql = "
    CREATE TABLE IF NOT EXISTS gossip_archive (
        msg_id BIGINT DEFAULT nextval('id_seq'),
        recv_ts TIMESTAMPTZ NOT NULL,
        collector VARCHAR NOT NULL,
        recv_peer VARCHAR NOT NULL,
        msg_type UINT8 NOT NULL,
        msg_size UINT16 NOT NULL,
        msg VARCHAR
    );";
    db.conn(|c| c.execute_batch(create_autoinc_sql)).await?;
    db.conn(|c| c.execute_batch(create_table_sql)).await?;
    Ok(())
}
