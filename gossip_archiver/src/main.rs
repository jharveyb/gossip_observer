use std::time::Duration;

use ahash::AHashMap;
use ahash::AHashSet;
use async_nats::Message;
use async_nats::jetstream;
use futures::StreamExt;
use gossip_archiver::INTER_MSG_DELIM;
use gossip_archiver::{ExportedGossip, MessageMetadata, MessageNodeTimings, RawMessage};
use gossip_archiver::{decode_msg, split_exported_gossip};
use itertools::Itertools;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::time::{self};

// TODO: move to some gossip_common module
static STATS_INTERVAL: Duration = Duration::from_secs(60);
static DB_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO: decide how we want to load connection strings
    // for now, load from .env
    dotenvy::dotenv()?;

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

    // Adjust this based on observed ingest rate
    // Seeing 400-500 msg/min with 600-700 peers; 500*256 = 128k individual msgs, or ~2133 msg/sec.
    let batch_size = 10_000;
    let (buf_raw_tx, buf_raw_rx) = flume::bounded(batch_size);
    let (buf_timings_tx, buf_timings_rx) = flume::bounded(batch_size);
    let (buf_meta_tx, buf_meta_rx) = flume::bounded(batch_size);
    let (buf_tick_tx, buf_tick_rx) = flume::bounded(1);
    let (buf_ack_tx, buf_ack_rx) = flume::bounded(1);

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/gossip_observer".to_string());

    // Ensure database exists
    ensure_database_exists(&database_url).await?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    println!("Initialized TimescaleDB connection");

    let nats_recv = raw_msg_tx.clone();
    let nats_handle = tokio::spawn(async move { nats_reader(msg_stream, nats_recv).await });
    let decode_handle = tokio::spawn(async move { msg_decoder(raw_msg_rx, msg_tx).await });
    let db_handle = tokio::spawn(async move {
        db_write_handler(
            pool,
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
        db_handle,
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
    pool: PgPool,
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
                let raw_batch = buf_raw_rx.drain().collect::<Vec<_>>();
                let mut timing_batch = buf_timings_rx.drain().collect::<Vec<_>>();
                let meta_batch = buf_meta_rx.drain().collect::<Vec<_>>();

                // We will always have more timing entries than any other table.
                if timing_batch.is_empty() {
                    buf_ack_tx.send_async(()).await.unwrap();
                    continue;
                } else {
                    // Sort our 'time-series' data to improve DB behavior.
                    // Sort could hang things?
                    timing_batch.sort_by_key(|x| x.recv_timestamp);
                    db_batch_write(&pool, raw_batch, timing_batch, meta_batch).await?;
                    buf_ack_tx.send_async(()).await.unwrap();
                }
            }
        };
    }
}

pub async fn db_batch_write(
    pool: &PgPool,
    raws: Vec<RawMessage>,
    timings: Vec<MessageNodeTimings>,
    metas: Vec<MessageMetadata>,
) -> anyhow::Result<()> {
    // UNNEST is the recommended way to do batch insertions with query!:
    // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-bind-an-array-to-a-values-clause-how-can-i-do-bulk-inserts

    // Insert raw messages
    if !raws.is_empty() {
        let (hashes, raw_msgs): (Vec<_>, Vec<_>) =
            raws.into_iter().map(RawMessage::unroll).multiunzip();

        sqlx::query!(
            "INSERT INTO messages (hash, raw)
             SELECT * FROM UNNEST($1::bytea[], $2::text[])",
            &hashes,
            &raw_msgs
        )
        .execute(pool)
        .await?;
    }

    // Insert timings (time-series data)
    if !timings.is_empty() {
        let (hashes, collectors, recv_peers, recv_peer_hashes, recv_timestamps, orig_timestamps): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = timings
            .into_iter()
            .map(MessageNodeTimings::unroll)
            .multiunzip();

        sqlx::query!(
            "INSERT INTO timings (hash, collector, recv_peer, recv_peer_hash, recv_timestamp, orig_timestamp)
             SELECT * FROM UNNEST($1::bytea[], $2::text[], $3::text[], $4::bytea[], $5::timestamptz[], $6::timestamptz[])",
            &hashes,
            &collectors,
            &recv_peers,
            &recv_peer_hashes,
            &recv_timestamps,
            &orig_timestamps as &[Option<chrono::DateTime<chrono::Utc>>]
        )
        .execute(pool)
        .await?;
    }

    // Insert metadata
    if !metas.is_empty() {
        let (hashes, types, sizes, orig_nodes, scids): (Vec<_>, Vec<_>, Vec<_>, Vec<_>, Vec<_>) =
            metas.into_iter().map(MessageMetadata::unroll).multiunzip();

        sqlx::query!(
            "INSERT INTO metadata (hash, type, size, orig_node, scid)
             SELECT * FROM UNNEST($1::bytea[], $2::smallint[], $3::integer[], $4::text[], $5::bytea[])",
            &hashes,
            &types,
            &sizes,
            &orig_nodes as &[Option<String>],
            &scids as &[Option<Vec<u8>>]
        )
        .execute(pool)
        .await?;
    }

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

pub async fn ensure_database_exists(database_url: &str) -> anyhow::Result<()> {
    // Parse the database URL to extract database name and construct a URL to 'postgres' db
    // TODO: proper validation
    let db_name = database_url.rsplit('/').next().unwrap_or("gossip_observer");

    let postgres_url = database_url
        .rsplit_once('/')
        .map(|x| x.0)
        .unwrap_or("postgres://postgres:postgres@localhost");
    let postgres_url = format!("{}/postgres", postgres_url);

    // Connect to the default 'postgres' database
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&postgres_url)
        .await?;

    // Check if our database exists
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(db_name)
            .fetch_one(&pool)
            .await?;
    pool.close().await;

    if !exists {
        println!("Database '{}' does not exist", db_name);
        Err(anyhow::Error::msg("Database does not exist"))
    } else {
        println!("Database '{}' already exists", db_name);
        Ok(())
    }
}
