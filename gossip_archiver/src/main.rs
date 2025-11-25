use std::time::Duration;

use async_nats::Message;
use async_nats::jetstream;
use futures::StreamExt;
use gossip_archiver::INTER_MSG_DELIM;
use gossip_archiver::{ExportedGossip, MessageMetadata, MessageNodeTimings, RawMessage};
use gossip_archiver::{decode_msg, split_exported_gossip};
use itertools::Itertools;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::mpsc::channel as tokio_channel;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::{self};

// TODO: move to some gossip_common module
static STATS_INTERVAL: Duration = Duration::from_secs(60);
static DB_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
static DB_WRITE_BATCH_SIZE: usize = 10_000;

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
    let (raw_msg_tx, raw_msg_rx) = tokio::sync::mpsc::unbounded_channel();
    let (msg_tx, msg_rx) = tokio::sync::mpsc::unbounded_channel();

    // Adjust this based on observed ingest rate
    // Seeing 400-500 msg/min with 600-700 peers; 500*256 = 128k individual msgs, or ~2133 msg/sec.
    let (buf_raw_tx, buf_raw_rx) = tokio_channel(DB_WRITE_BATCH_SIZE);
    let (buf_timings_tx, buf_timings_rx) = tokio_channel(DB_WRITE_BATCH_SIZE);
    let (buf_meta_tx, buf_meta_rx) = tokio_channel(DB_WRITE_BATCH_SIZE);
    let (buf_tick_tx, buf_tick_rx) = tokio_channel(1);

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
        db_write_handler(pool, buf_raw_rx, buf_timings_rx, buf_meta_rx, buf_tick_rx).await
    });
    let db_write_ticker_handle = tokio::spawn(async move {
        db_write_ticker(msg_rx, buf_raw_tx, buf_timings_tx, buf_meta_tx, buf_tick_tx).await
    });

    match tokio::try_join!(
        nats_handle,
        decode_handle,
        db_handle,
        db_write_ticker_handle,
    ) {
        Ok(_) => Ok(()),
        Err(e) => {
            println!("Join error: {e}");
            Err(e.into())
        }
    }
}

pub async fn db_write_ticker(
    mut msg_rx: UnboundedReceiver<ExportedGossip>,
    buf_raw_tx: Sender<RawMessage>,
    buf_timings_tx: Sender<MessageNodeTimings>,
    buf_meta_tx: Sender<MessageMetadata>,
    buf_tick_tx: Sender<()>,
) -> anyhow::Result<()> {
    let mut flush_waiter = time::interval(DB_FLUSH_INTERVAL);
    let mut stats_waiter = time::interval(STATS_INTERVAL);

    let mut poll_counter = 0;
    let mut full_counter = 0;

    // Signal another task to write buffered values to the DB.
    let signal_flush = async || -> anyhow::Result<()> {
        let tick_permit = buf_tick_tx.reserve().await?;
        tick_permit.send(());
        Ok(())
    };

    println!("Starting DB write ticker");
    println!("Flush interval: {DB_FLUSH_INTERVAL:?}");
    println!("Stats interval: {STATS_INTERVAL:?}");
    loop {
        let mut gossip_msg = None;
        let mut flush_tick = false;

        // Receive a new message, or perform a scheduled flush of values to the
        // DB. We should never end up trying to do both in the same loop iteration.
        tokio::select! {
            // TODO: we could probably put this on a timer and use recv_many
            msg = msg_rx.recv() => {
                match msg {
                    Some(msg) => gossip_msg = Some(msg),
                    None => {
                        // TODO: crash?
                        println!("internal: db_write_ticker: decoded msg chan closed");
                        return Ok(());
                    }
                }
            }
            _ = stats_waiter.tick() => {
                println!("Flush count: {poll_counter}, Full buffer count: {full_counter}");
                poll_counter = 0;
                full_counter = 0;
            }
            _ = flush_waiter.tick() => {
                flush_tick = true;
            }
        };

        if flush_tick {
            signal_flush().await?;
            poll_counter += 1;
        }

        if let Some(msg) = gossip_msg {
            let (msg_entry, timings_entry, meta_entry) = split_exported_gossip(msg);

            // Reserve capcity to be able to submit a message to the DB write queue.
            // This will only complete if all DB write queues have capacity.
            // If a chan. capacity is 0 after we acquire a permit, then we know
            // a chan. has filled and we can signal the writer.
            let (msg_permit, timings_permit, meta_permit) = (
                buf_raw_tx.reserve(),
                buf_timings_tx.reserve(),
                buf_meta_tx.reserve(),
            );
            let (msg_permit, timings_permit, meta_permit) =
                tokio::try_join!(msg_permit, timings_permit, meta_permit)?;

            let buf_full = buf_raw_tx.capacity() == 0
                || buf_timings_tx.capacity() == 0
                || buf_meta_tx.capacity() == 0;
            if buf_full {
                signal_flush().await?;
                full_counter += 1;
            }

            msg_permit.send(msg_entry);
            timings_permit.send(timings_entry);
            meta_permit.send(meta_entry);
        }
    }
}

pub async fn db_write_handler(
    pool: PgPool,
    mut buf_raw_rx: Receiver<RawMessage>,
    mut buf_timings_rx: Receiver<MessageNodeTimings>,
    mut buf_meta_rx: Receiver<MessageMetadata>,
    mut buf_tick_rx: Receiver<()>,
) -> anyhow::Result<()> {
    println!("Starting DB writer");
    let mut raw_msgs = Vec::with_capacity(DB_WRITE_BATCH_SIZE);
    let mut timings = Vec::with_capacity(DB_WRITE_BATCH_SIZE);
    let mut metas = Vec::with_capacity(DB_WRITE_BATCH_SIZE);
    loop {
        let mut should_flush = false;
        // TODO: clean up our closed chan. handling
        tokio::select! {
            raw_msg = buf_raw_rx.recv() => {
                match raw_msg {
                    Some(msg) => raw_msgs.push(msg),
                    None => {
                        println!("internal: db_write_handler: raw msg chan closed");
                        return Ok(());
                    }
                }
            }
            timings_msg = buf_timings_rx.recv() => {
                match timings_msg {
                    Some(msg) => timings.push(msg),
                    None => {
                        println!("internal: db_write_handler: timings msg chan closed");
                        return Ok(());
                    }
                }
            }
            meta_msg = buf_meta_rx.recv() => {
                match meta_msg {
                    Some(msg) => metas.push(msg),
                    None => {
                        println!("internal: db_write_handler: meta msg chan closed");
                        return Ok(());
                    }
                }
            }
            _ = buf_tick_rx.recv() => {
                should_flush = true;
            }
        };

        // Another task will signal us to actually write to the DB.
        if should_flush {
            // Sometimes that signal is from a timer, and we haven't received
            // any messages.
            if timings.is_empty() {
                continue;
            }

            // Sort our 'time-series' data to improve DB behavior.
            // TODO: move sort to spawn_blocking? not sure if worth
            timings.sort_by_key(|x| x.recv_timestamp);
            let (db_raws, db_timings, db_metas) =
                (raw_msgs.clone(), timings.clone(), metas.clone());
            db_batch_write(&pool, db_raws, db_timings, db_metas).await?;

            raw_msgs.clear();
            timings.clear();
            metas.clear();
        }
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
             SELECT * FROM UNNEST($1::bytea[], $2::text[])
             ON CONFLICT DO NOTHING",
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
             SELECT * FROM UNNEST($1::bytea[], $2::smallint[], $3::integer[], $4::text[], $5::bytea[])
             ON CONFLICT DO NOTHING",
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

// pull msgs from NATS, and ACK the sender / collector so they can continue
pub async fn nats_reader(
    mut stream: jetstream::consumer::pull::Stream,
    raw_msg_tx: UnboundedSender<Message>,
) -> anyhow::Result<()> {
    println!("Starting NATS reader");

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
                    None => {
                        // TODO: crash?
                        println!("internal: nats_reader: NATS stream closed");
                        return Ok(());
                    }
                }
            }
            _ = stats_waiter.tick() => {
                println!("Avg. NATS msg/min: {}", msg_count);
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

// split a big NATS msg into multiple decoded msgs
pub async fn msg_decoder(
    mut raw_msg_rx: UnboundedReceiver<Message>,
    msg_tx: UnboundedSender<ExportedGossip>,
) -> anyhow::Result<()> {
    let mut stats_waiter = time::interval(STATS_INTERVAL);
    loop {
        // TODO: add graceful shutdown, some other branch?
        tokio::select! {
            rx_msg = raw_msg_rx.recv() => {
                match rx_msg {
                    Some(raw_msg) => {
                        let inner_msgs = str::from_utf8(&raw_msg.payload)?.split(INTER_MSG_DELIM);
                        for raw_msg in inner_msgs {
                            msg_tx.send(decode_msg(raw_msg)?)?;
                        }
                    }
                    None => {
                        println!("internal: msg_decoder: rx chan closed");
                        return Ok(());
                    }
                }
            }
            _ = stats_waiter.tick() => {
                println!("NATS pull queue size: {}", raw_msg_rx.len());
            }
        };
    }
}

pub async fn upsert_stream(ctx: jetstream::Context) -> anyhow::Result<jetstream::stream::Stream> {
    let stream_name = "main";
    // TODO: don't delete streams on start, that would drop msgs,
    // maybe break collectors
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
    // TODO: check if this is safe
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
