use std::path::Path;
use std::time::Duration;

use ahash::AHashMap;
use ahash::AHashSet;
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
use twox_hash::xxhash3_64::Hasher as XX3Hasher;

// TODO: move to some gossip_common module
static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
static MSG_TABLE: &str = "messages";
static TIMING_TABLE: &str = "timings";
static META_TABLE: &str = "metadata";
static STATS_INTERVAL: Duration = Duration::from_secs(120);
static DB_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
static MSG_DRAIN_INTERVAL: Duration = Duration::from_secs(2);

// Format: format_args!("{now},{recv_peer},{msg_type},{msg_size},{msg},{send_ts},{node_id},{scid},{collector_id}")
// ldk-node/src/logger.rs#L272, LdkLogger.export()
#[derive(Debug, Clone)]
pub struct ExportedGossip {
    // timestamps
    pub recv_timestamp: DateTime<Utc>,
    pub orig_timestamp: Option<DateTime<Utc>>,
    // node pubkeys
    pub collector: String,
    pub recv_peer: String,
    pub recv_peer_hash: u64,
    pub orig_node: Option<String>,
    // metadata
    pub msg_type: u8,
    pub msg_size: u16,
    pub scid: Option<u64>,
    // full message
    pub msg: String,
    pub msg_hash: u64,
}

pub struct RawMessage {
    pub msg_hash: u64,
    pub msg: String,
}

pub struct MessageNodeTimings {
    pub msg_hash: u64,
    pub collector: String,
    pub recv_peer: String,
    pub recv_peer_hash: u64,
    pub recv_timestamp: DateTime<Utc>,
    pub orig_timestamp: Option<DateTime<Utc>>,
}

pub struct MessageMetadata {
    pub msg_hash: u64,
    pub msg_type: u8,
    pub msg_size: u16,
    pub orig_node: Option<String>,
    pub scid: Option<u64>,
}

pub fn split_exported_gossip(
    msg: ExportedGossip,
) -> (RawMessage, MessageNodeTimings, MessageMetadata) {
    let raw = RawMessage {
        msg_hash: msg.msg_hash,
        msg: msg.msg,
    };
    let timings = MessageNodeTimings {
        msg_hash: msg.msg_hash,
        collector: msg.collector,
        recv_peer: msg.recv_peer,
        recv_peer_hash: msg.recv_peer_hash,
        recv_timestamp: msg.recv_timestamp,
        orig_timestamp: msg.orig_timestamp,
    };
    let metadata = MessageMetadata {
        msg_hash: msg.msg_hash,
        msg_type: msg.msg_type,
        msg_size: msg.msg_size,
        orig_node: msg.orig_node,
        scid: msg.scid,
    };
    (raw, timings, metadata)
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
    console_subscriber::init();

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
    let batch_size = 10_000;
    let (buf_raw_tx, buf_raw_rx) = flume::bounded(batch_size);
    let (buf_timings_tx, buf_timings_rx) = flume::bounded(batch_size);
    let (buf_meta_tx, buf_meta_rx) = flume::bounded(batch_size);
    let (buf_tick_tx, buf_tick_rx) = flume::bounded(1);
    let (buf_ack_tx, buf_ack_rx) = flume::bounded(1);

    let db_path = format!("{}{}{}", "./data/mainnet/gossip_archives/", now, ".duckdb");
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
        db_write_handler(
            &mut db_client,
            buf_raw_rx,
            buf_timings_rx,
            buf_meta_rx,
            buf_tick_rx,
            buf_ack_tx,
        )
        .await
    });
    let db_write_ticker_handle = tokio::spawn(async move {
        db_write_ticker(
            msg_rx,
            buf_raw_tx,
            buf_timings_tx,
            buf_meta_tx,
            buf_tick_tx,
            buf_ack_rx,
        )
        .await
    });

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

    match tokio::try_join!(
        nats_handle,
        decode_handle,
        duckdb_handle,
        db_write_ticker_handle,
        stats_handle
    ) {
        Ok(_) => Ok(()),
        Err(e) => {
            println!("Join error: {e}");
            Err(e.into())
        }
    }
}

pub async fn db_write_ticker(
    msg_rx: flume::Receiver<ExportedGossip>,
    buf_raw_tx: flume::Sender<RawMessage>,
    buf_timings_tx: flume::Sender<MessageNodeTimings>,
    buf_meta_tx: flume::Sender<MessageMetadata>,
    buf_tick_tx: flume::Sender<()>,
    buf_ack_rx: flume::Receiver<()>,
) -> anyhow::Result<()> {
    let mut msg_drain_waiter = time::interval(MSG_DRAIN_INTERVAL);
    let mut flush_waiter = time::interval(DB_FLUSH_INTERVAL);
    let mut stats_waiter = time::interval(STATS_INTERVAL);

    let mut poll_counter = 0;
    let mut full_counter = 0;

    // Concurrency issue mb?
    let mut dupe_tracker = AHashMap::new();

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

            // TODO: move this onto a timer + batched on a blocking task
            // Filter received messages by if they require a DB insertion.
            Ok(msg) = msg_rx.recv_async() => {
                if buf_timings_tx.is_full() {
                    buf_tick_tx.send_async(()).await.unwrap();
                    buf_ack_rx.recv_async().await.unwrap();
                    full_counter += 1;
                }

                // Move map management to a separate task?
                // Split our gossip message, and enforce that (msg_hash, hash(recv_peer)) is unique.
                let (msg_entry, timings_entry, meta_entry) = db_write_splitter(&mut dupe_tracker, msg);

                // TODO: crash on err?
                if let Some(msg) = msg_entry
                    && let Err(e) = buf_raw_tx.send_async(msg).await {
                        println!("internal archiver error: db_writer(): {e}");
                    }
                if let Some(timings) = timings_entry
                    && let Err(e) = buf_timings_tx.send_async(timings).await {
                        println!("internal archiver error: db_writer(): {e}");
                    }
                if let Some(metadata) = meta_entry
                    && let Err(e) = buf_meta_tx.send_async(metadata).await {
                        println!("internal archiver error: db_writer(): {e}");
                    }
        }
        };
    }
}

pub fn db_write_splitter(
    dupe_tracker: &mut AHashMap<u64, AHashSet<u64>>,
    msg: ExportedGossip,
) -> (
    Option<RawMessage>,
    Option<MessageNodeTimings>,
    Option<MessageMetadata>,
) {
    // Split our gossip message.
    let (raw_msg, timings, metadata) = split_exported_gossip(msg);

    // Enforce that (msg_hash, hash(recv_peer)) is unique.
    match dupe_tracker.get_mut(&raw_msg.msg_hash) {
        // New message, so we need an entry on all tables.
        None => {
            let mut seen_peers = AHashSet::new();
            seen_peers.insert(timings.recv_peer_hash);
            dupe_tracker.insert(timings.msg_hash, seen_peers);
            (Some(raw_msg), Some(timings), Some(metadata))
        }
        // Seen message, we may need to update our timestamp table.
        Some(seen_peers) => {
            match seen_peers.insert(timings.recv_peer_hash) {
                // Duplicate value, no DB update needed.
                false => (None, None, None),
                true => (None, Some(timings), None),
            }
        }
    }
}

pub async fn db_write_handler(
    db: &mut DuckClient,
    buf_raw_rx: flume::Receiver<RawMessage>,
    buf_timings_rx: flume::Receiver<MessageNodeTimings>,
    buf_meta_rx: flume::Receiver<MessageMetadata>,
    buf_tick_rx: flume::Receiver<()>,
    buf_ack_tx: flume::Sender<()>,
) -> anyhow::Result<()> {
    println!("Starting DB writer");

    loop {
        tokio::select! {
            _ = buf_tick_rx.recv_async() => {
                let raw_batch = buf_raw_rx.clone().drain().collect::<Vec<_>>();
                let mut timing_batch = buf_timings_rx.clone().drain().collect::<Vec<_>>();
                let meta_batch = buf_meta_rx.clone().drain().collect::<Vec<_>>();

                // We will always have more timing entries than any other table.
                if timing_batch.is_empty() {
                    buf_ack_tx.send_async(()).await.unwrap();
                    continue;
                } else {
                    // Sort our 'time-series' data to improve DB behavior.
                    // Sort could hang things?
                    timing_batch.sort_by_key(|x| x.recv_timestamp);
                    db_batch_write(db, raw_batch, timing_batch, meta_batch).await?;
                    buf_ack_tx.send_async(()).await.unwrap();
                }
            }
        };
    }
}

pub async fn db_batch_write(
    db: &mut DuckClient,
    raws: Vec<RawMessage>,
    timings: Vec<MessageNodeTimings>,
    metas: Vec<MessageMetadata>,
) -> anyhow::Result<()> {
    // Appenders are created per-table.
    db.conn_mut(move |c| -> Result<(), DuckError> {
        let mut raws_tx = c.transaction()?;
        raws_tx.set_drop_behavior(DropBehavior::Commit);
        let mut raw_append = raws_tx.appender(MSG_TABLE)?;
        for msg in raws {
            raw_append.append_row(params![msg.msg_hash, msg.msg])?;
        }
        raw_append.flush()?;
        Ok(())
    })
    .await?;
    db.conn_mut(move |c| -> Result<(), DuckError> {
        if !timings.is_empty() {
            let mut timings_tx = c.transaction()?;
            timings_tx.set_drop_behavior(DropBehavior::Commit);
            let mut timings_append = timings_tx.appender(TIMING_TABLE)?;
            for msg in timings {
                timings_append.append_row(params![
                    msg.msg_hash,
                    msg.collector,
                    msg.recv_peer,
                    msg.recv_peer_hash,
                    msg.recv_timestamp,
                    msg.orig_timestamp,
                ])?;
            }
            timings_append.flush()?;
        }
        Ok(())
    })
    .await?;
    db.conn_mut(move |c| -> Result<(), DuckError> {
        if !metas.is_empty() {
            let mut metas_tx = c.transaction()?;
            metas_tx.set_drop_behavior(DropBehavior::Commit);
            let mut meta_append = metas_tx.appender(META_TABLE)?;
            for msg in metas {
                meta_append.append_row(params![
                    msg.msg_hash,
                    msg.msg_type,
                    msg.msg_size,
                    msg.orig_node,
                    msg.scid,
                ])?;
            }
            meta_append.flush()?;
        }

        Ok(())
    })
    .await?;
    Ok(())
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
    msg_tx: flume::Sender<ExportedGossip>,
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

// TODO: add unit test
// TODO: rkyv or smthn cool here? Jk this is fine on release builds
// This should mirror whatever we're exporting to NATS
pub fn decode_msg(msg: &str) -> anyhow::Result<ExportedGossip> {
    let parts = msg.split(INTRA_MSG_DELIM).collect::<Vec<&str>>();
    // None values will be "", but we'll always have 9 fields.
    if parts.len() != 9 {
        anyhow::bail!("Invalid message from collector: {msg}")
    }

    // Format: format_args!("{now},{recv_peer},{msg_type},{msg_size},{msg},{send_ts},{node_id},{scid},{collector_id}")
    // ldk-node/src/logger.rs#L272, LdkLogger.export()
    let recv_timestamp = DateTime::from_timestamp_micros(parts[0].parse::<i64>()?).unwrap();
    let recv_peer = parts[1].to_owned();
    let recv_peer_hash = XX3Hasher::oneshot(recv_peer.as_bytes());
    let msg_type = parts[2].parse::<MessageType>()?.into();
    let msg_size = parts[3].parse::<u16>()?;
    let msg = parts[4].to_owned();
    let msg_hash = XX3Hasher::oneshot(msg.as_bytes());
    let orig_timestamp = (!parts[5].is_empty())
        .then(|| parts[5].parse::<i64>())
        .transpose()?
        .and_then(DateTime::from_timestamp_micros);
    let orig_node = (!parts[6].is_empty()).then(|| parts[6].to_owned());
    let scid = (!parts[7].is_empty())
        .then(|| parts[7].parse::<u64>())
        .transpose()?;
    let collector = parts[8].to_owned();

    Ok(ExportedGossip {
        recv_timestamp,
        orig_timestamp,
        collector,
        recv_peer,
        recv_peer_hash,
        orig_node,
        msg_type,
        msg_size,
        scid,
        msg,
        msg_hash,
    })
}

pub async fn upsert_stream(ctx: jetstream::Context) -> anyhow::Result<jetstream::stream::Stream> {
    let stream_name = "main";
    let _ = ctx.delete_stream(&stream_name).await;
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
    let _ = ctx.delete_consumer(cons_name).await;
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
    let create_raw_table = "
    CREATE TABLE IF NOT EXISTS messages (
        hash UBIGINT NOT NULL,
        raw VARCHAR NOT NULL
    );";
    let create_timings_table = "
    CREATE TABLE IF NOT EXISTS timings (
        hash UBIGINT NOT NULL,
        collector VARCHAR NOT NULL,
        recv_peer VARCHAR NOT NULL,
        recv_peer_hash UBIGINT NOT NULL,
        recv_timestamp TIMESTAMPTZ NOT NULL,
        orig_timestamp TIMESTAMPTZ,
    );";
    let create_metadata_table = "
    CREATE TABLE IF NOT EXISTS metadata (
        hash UBIGINT NOT NULL,
        type UINT8 NOT NULL,
        size UINT16 NOT NULL,
        orig_node VARCHAR,
        scid UBIGINT
    );";
    let relax_ordering = "
    SET preserve_insertion_order = false;
    ";
    // # of physical cores, not threads
    let threads = "
    SET threads = 8;
    ";
    let memory = "
    SET memory_limit = '16GB';
    ";
    let db_setup_sql = [
        create_raw_table,
        create_timings_table,
        create_metadata_table,
        relax_ordering,
        threads,
        memory,
    ];
    for sql in db_setup_sql {
        db.conn(|c| c.execute_batch(sql)).await?;
    }
    Ok(())
}
