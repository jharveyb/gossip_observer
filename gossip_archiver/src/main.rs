use std::path::Path;
use std::time::Duration;

use async_duckdb::Client as DuckClient;
use async_duckdb::ClientBuilder as DuckClientBuilder;
use async_duckdb::duckdb::DropBehavior;
use async_duckdb::duckdb::Error as DuckError;
use async_duckdb::duckdb::params;
use async_nats::Message;
use async_nats::jetstream;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use std::fs;
use tokio::time::{self};

// TODO: move to some gossip_common module
static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
static TABLE_NAME: &str = "gossip_archive";
static STATS_INTERVAL: Duration = Duration::from_secs(120);
static DB_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

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
    // Enable tokio-console
    // console_subscriber::init();

    // TODO: proper INI cfg
    let nats_server = "100.68.143.113:4222";
    let nats_client = async_nats::connect(nats_server).await?;
    let stream_ctx = jetstream::new(nats_client);
    let stream = upsert_stream(stream_ctx).await?;
    let consumer = upsert_consumer(stream).await?;
    let msg_stream = consumer.messages().await?;
    println!("Set up NATS stream and consumer");

    // TODO: unbounded feels sketch but whatevs; backpressure?
    let (raw_msg_tx, raw_msg_rx) = flume::unbounded();
    let (msg_tx, msg_rx) = flume::unbounded();
    let now = chrono::Utc::now().format("%m%dT%H%M%S");

    // Adjust this based on observed ingest rate
    // Seeing 400-500 msg/min with 600-700 peers; 500*256 = 128k individual msgs, or ~2133 msg/sec.
    let batch_size = 25_000;
    let (db_buf_tx, db_buf_rx) = flume::bounded(batch_size);
    let (buf_tick_tx, buf_tick_rx) = flume::bounded(1);
    let (buf_ack_tx, buf_ack_rx) = flume::bounded(1);

    let db_path = format!("{}{}", "./data/mainnet/gossip_archive.duckdb", now);
    if let Some(parent_dir) = Path::new(&db_path).parent() {
        fs::create_dir_all(parent_dir)?;
    }
    let mut db_client = DuckClientBuilder::new().path(db_path).open().await?;
    init_db(&db_client).await?;
    println!("Initialized DuckDB connection");

    let nats_recv = raw_msg_tx.clone();
    let nats_handle = tokio::spawn(async move { nats_reader(msg_stream, nats_recv).await });
    let decode_handle = tokio::spawn(async move { msg_decoder(raw_msg_rx, msg_tx).await });
    let duckdb_handle = tokio::spawn(async move {
        db_write_handler(&mut db_client, db_buf_rx, buf_tick_rx, buf_ack_tx).await
    });
    let db_write_ticker_handle =
        tokio::spawn(
            async move { db_write_ticker(msg_rx, db_buf_tx, buf_tick_tx, buf_ack_rx).await },
        );

    let stats_chan = raw_msg_tx.clone();
    let stats_handle = tokio::spawn(async move {
        let mut stats_waiter = time::interval(STATS_INTERVAL);
        loop {
            tokio::select! {
                _ = stats_waiter.tick() => {
                    println!("NATS buffer size: {}", stats_chan.clone().len());
                }
            }
        }
    });

    let _ = tokio::try_join!(
        nats_handle,
        decode_handle,
        duckdb_handle,
        db_write_ticker_handle,
        stats_handle
    )?;
    Ok(())
}

pub async fn db_write_ticker(
    msg_rx: flume::Receiver<GossipMessage>,
    db_buf_tx: flume::Sender<GossipMessage>,
    buf_tick_tx: flume::Sender<()>,
    buf_ack_rx: flume::Receiver<()>,
) -> anyhow::Result<()> {
    let mut flush_waiter = time::interval(DB_FLUSH_INTERVAL);
    let mut stats_waiter = time::interval(STATS_INTERVAL);

    let mut poll_counter = 0;
    let mut full_counter = 0;

    println!("Starting DB write ticker");
    println!("Flush interval: {DB_FLUSH_INTERVAL:?}");
    println!("Stats interval: {STATS_INTERVAL:?}");
    loop {
        tokio::select! {
            _ = stats_waiter.tick() => {
                println!("Flush count: {poll_counter}, Full buffer count: {full_counter}");
                poll_counter = 0;
                full_counter = 0;
            }
            _ = flush_waiter.tick() => {
                buf_tick_tx.send_async(()).await.unwrap();
                buf_ack_rx.recv_async().await.unwrap();
                poll_counter += 1;
            }
            Ok(msg) = msg_rx.recv_async() => {
                if db_buf_tx.is_full() {
                    buf_tick_tx.send_async(()).await.unwrap();
                    buf_ack_rx.recv_async().await.unwrap();
                    full_counter += 1;
                }

                if let Err(e) = db_buf_tx.send_async(msg).await {
                    // TODO: crash?
                    println!("internal archiver error: db_writer(): {e}");
                }
            }
        };
    }
}

pub async fn db_write_handler(
    db: &mut DuckClient,
    db_buf_rx: flume::Receiver<GossipMessage>,
    buf_tick_rx: flume::Receiver<()>,
    buf_ack_tx: flume::Sender<()>,
) -> anyhow::Result<()> {
    println!("Starting DB writer");

    let mut row_idx = 0;
    loop {
        tokio::select! {
            _ = buf_tick_rx.recv_async() => {
                let db_batch = db_buf_rx.clone().drain().collect::<Vec<_>>();
                if db_batch.is_empty() {
                    buf_ack_tx.send_async(()).await.unwrap();
                    continue;
                } else {
                    row_idx = db_batch_write(db, db_batch, row_idx).await?;
                    buf_ack_tx.send_async(()).await.unwrap();
                }
            }
        };
    }
}

pub async fn db_batch_write(
    db: &mut DuckClient,
    msgs: Vec<GossipMessage>,
    row_idx: u64,
) -> anyhow::Result<u64> {
    let mut inner_idx = row_idx;
    let end_idx = db
        .conn_mut(move |c| -> Result<u64, DuckError> {
            let mut tx = c.transaction()?;
            tx.set_drop_behavior(DropBehavior::Commit);
            let mut append = tx.appender(TABLE_NAME)?;
            for msg in msgs {
                append.append_row(params![
                    inner_idx,
                    msg.recv_timestamp,
                    msg.collector,
                    msg.recv_peer,
                    msg.msg_type,
                    msg.msg_size,
                    msg.msg
                ])?;
                inner_idx += 1;
            }
            append.flush()?;
            Ok(inner_idx)
        })
        .await?;
    Ok(end_idx)
}

pub async fn nats_reader(
    mut stream: jetstream::consumer::pull::Stream,
    raw_msg_tx: flume::Sender<Message>,
) -> anyhow::Result<()> {
    println!("Starting NATS reader");

    let mut msg_count = 0;
    let mut stats_waiter = time::interval(STATS_INTERVAL);
    loop {
        tokio::select! {
            Some(message) = stream.next() => {
                // ACK before we actually write to DB; yeet
                let (message, ack_handle) = message.map_err(anyhow::Error::msg)?.split();
                raw_msg_tx.send_async(message).await?;
                ack_handle.ack().await.map_err(anyhow::Error::msg)?;
                msg_count += 1;
            }
            _ = stats_waiter.tick() => {
                if msg_count > 2 {
                    println!("Avg. NATS msg/min: {}", msg_count / 2);
                }
                msg_count = 0;
            }
        }
    }
}

pub async fn msg_decoder(
    raw_msg_rx: flume::Receiver<Message>,
    msg_tx: flume::Sender<GossipMessage>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            Ok(raw_msg) = raw_msg_rx.recv_async() => {
                let inner_msgs = str::from_utf8(&raw_msg.payload)?.split(INTER_MSG_DELIM);
                for raw_msg in inner_msgs {
                    msg_tx.send_async(decode_msg(raw_msg)?).await?;
                }
            }
        }
    }
}

// TODO: rkyv or smthn cool here? Jk this is fine on release builds
// This should mirror whatever we're exporting to NATS
pub fn decode_msg(msg: &str) -> anyhow::Result<GossipMessage> {
    let parts = msg.split(INTRA_MSG_DELIM).collect::<Vec<&str>>();
    if parts.len() != 6 {
        anyhow::bail!("Invalid message from collector: {msg}")
    }

    Ok(GossipMessage {
        recv_timestamp: DateTime::from_timestamp_micros(parts[0].parse::<i64>()?).unwrap(),
        collector: parts[5].to_owned(),
        recv_peer: parts[1].to_owned(),
        msg_type: parts[2].parse::<MessageType>()?.into(),
        msg_size: parts[3].parse::<u16>()?,
        msg: parts[4].to_owned(),
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
    let create_table_sql = "
    CREATE TABLE IF NOT EXISTS gossip_archive (
        msg_id UBIGINT NOT NULL,
        recv_ts TIMESTAMPTZ NOT NULL,
        collector VARCHAR NOT NULL,
        recv_peer VARCHAR NOT NULL,
        msg_type UINT8 NOT NULL,
        msg_size UINT16 NOT NULL,
        msg VARCHAR
    );";
    db.conn(|c| c.execute_batch(create_table_sql)).await?;
    Ok(())
}
