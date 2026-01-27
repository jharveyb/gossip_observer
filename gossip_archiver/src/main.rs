use std::time::Duration;

use async_nats::Message;
use async_nats::jetstream;
use futures::StreamExt;
use gossip_archiver::INTER_MSG_DELIM;
use gossip_archiver::MessageHashMapping;
use gossip_archiver::config::ArchiverConfig;
use gossip_archiver::{ExportedGossip, MessageMetadata, MessageNodeTimings, RawMessage};
use gossip_archiver::{decode_msg, split_exported_gossip};
use itertools::Itertools;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::mpsc::channel as tokio_channel;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::{self};

static STATS_INTERVAL: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv()?;
    let cfg = ArchiverConfig::new()?;

    // Enable tokio-console
    console_subscriber::init();

    println!("Gossip archiver: {}", cfg.uuid);
    let nats_client = async_nats::connect(cfg.nats.server_addr).await?;
    let stream_ctx = jetstream::new(nats_client);
    let stream = upsert_stream(stream_ctx, &cfg.nats.stream_name, &cfg.nats.subject_prefix).await?;
    let consumer =
        upsert_consumer(stream, &cfg.nats.consumer_name, &cfg.nats.subject_prefix).await?;
    let msg_stream = consumer.messages().await?;
    println!("Set up NATS stream and consumer");

    let (raw_msg_tx, raw_msg_rx) = unbounded_channel();
    let (msg_tx, msg_rx) = unbounded_channel();

    let (buf_raw_tx, buf_raw_rx) = unbounded_channel();
    let (buf_timings_tx, buf_timings_rx) = unbounded_channel();
    let (buf_meta_tx, buf_meta_rx) = unbounded_channel();
    let (buf_hash_mapping_tx, buf_hash_mapping_rx) = unbounded_channel();
    let (buf_tick_tx, buf_tick_rx) = tokio_channel(1);

    let database_url = cfg.db_url.to_owned();
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
            buf_hash_mapping_rx,
            buf_tick_rx,
            cfg.database.batch_size,
        )
        .await
    });
    let db_write_ticker_handle = tokio::spawn(async move {
        db_write_ticker(
            msg_rx,
            buf_raw_tx,
            buf_timings_tx,
            buf_meta_tx,
            buf_hash_mapping_tx,
            buf_tick_tx,
            cfg.database.flush_interval,
            cfg.database.batch_size,
        )
        .await
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
    buf_raw_tx: UnboundedSender<RawMessage>,
    buf_timings_tx: UnboundedSender<MessageNodeTimings>,
    buf_meta_tx: UnboundedSender<MessageMetadata>,
    buf_hash_mapping_tx: UnboundedSender<MessageHashMapping>,
    buf_tick_tx: Sender<()>,
    flush_interval: u32,
    batch_size: u32,
) -> anyhow::Result<()> {
    let mut flush_waiter = time::interval(Duration::from_secs(flush_interval.into()));
    let mut stats_waiter = time::interval(STATS_INTERVAL);

    let mut poll_counter = 0;
    let mut full_counter = 0;
    let mut msg_counter = 0;
    let batch_size = batch_size as usize;

    // Signal another task to write buffered values to the DB.
    let signal_flush = async || -> anyhow::Result<usize> {
        let tick_permit = buf_tick_tx.reserve().await?;
        tick_permit.send(());
        Ok(0)
    };

    println!("Starting DB write ticker");
    println!("Flush interval: {flush_interval:?}");
    println!("Batch size: {batch_size:?}");
    println!("Stats interval: {STATS_INTERVAL:?}");

    let mut gossip_msg = None;
    let mut flush_tick = false;
    loop {
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
            _ = flush_waiter.tick() => {
                flush_tick = true;
            }
            _ = stats_waiter.tick() => {
                println!("Flush count: {poll_counter}, Full buffer count: {full_counter}");
                poll_counter = 0;
                full_counter = 0;
            }
        };

        if flush_tick {
            flush_tick = false;
            msg_counter = signal_flush().await?;
            poll_counter += 1;
        }

        if let Some(msg) = gossip_msg {
            // Check if we've received enough messages to fill a batch since the
            // last flush.
            if msg_counter >= batch_size {
                msg_counter = signal_flush().await?;
                full_counter += 1;
            }

            msg_counter += 1;
            let (msg_entry, timings_entry, meta_entry, hash_mapping) = split_exported_gossip(msg);

            buf_raw_tx.send(msg_entry)?;
            buf_timings_tx.send(timings_entry)?;
            buf_meta_tx.send(meta_entry)?;
            buf_hash_mapping_tx.send(hash_mapping)?;
            gossip_msg = None;
        }
    }
}

pub async fn db_write_handler(
    pool: PgPool,
    mut buf_raw_rx: UnboundedReceiver<RawMessage>,
    mut buf_timings_rx: UnboundedReceiver<MessageNodeTimings>,
    mut buf_meta_rx: UnboundedReceiver<MessageMetadata>,
    mut buf_hash_mapping_rx: UnboundedReceiver<MessageHashMapping>,
    mut buf_tick_rx: Receiver<()>,
    batch_size: u32,
) -> anyhow::Result<()> {
    println!("Starting DB writer");
    println!("Batch size: {batch_size:?}");
    let batch_size = batch_size as usize;
    let mut raw_msgs = Vec::with_capacity(batch_size);
    let mut timings = Vec::with_capacity(batch_size);
    let mut metas = Vec::with_capacity(batch_size);
    let mut hash_mappings = Vec::with_capacity(batch_size);
    let mut should_flush = false;
    loop {
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
            hash_mapping_msg = buf_hash_mapping_rx.recv() => {
                match hash_mapping_msg {
                    Some(msg) => hash_mappings.push(msg),
                    None => {
                        println!("internal: db_write_handler: hash mapping msg chan closed");
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
            should_flush = false;
            if timings.is_empty() {
                continue;
            }

            // Sort our 'time-series' data to improve DB behavior.
            timings.sort_by_key(|x| x.net_timestamp);

            // Move our values into the batch writer instead of cloning.
            let db_raws = std::mem::replace(&mut raw_msgs, Vec::with_capacity(batch_size));
            let db_timings = std::mem::replace(&mut timings, Vec::with_capacity(batch_size));
            let db_metas = std::mem::replace(&mut metas, Vec::with_capacity(batch_size));
            let db_hash_mappings =
                std::mem::replace(&mut hash_mappings, Vec::with_capacity(batch_size));

            db_batch_write(&pool, db_raws, db_timings, db_metas, db_hash_mappings).await?;
        }
    }
}

pub async fn db_batch_write(
    pool: &PgPool,
    raws: Vec<RawMessage>,
    timings: Vec<MessageNodeTimings>,
    metas: Vec<MessageMetadata>,
    hash_mapppings: Vec<MessageHashMapping>,
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

    // Insert the outer->inner hash mappings
    if !hash_mapppings.is_empty() {
        let (outer_hashes, inner_hashes): (Vec<_>, Vec<_>) = hash_mapppings
            .into_iter()
            .map(MessageHashMapping::unroll)
            .multiunzip();

        sqlx::query!(
            "INSERT INTO message_hashes (outer_hash, inner_hash)
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[])
             ON CONFLICT DO NOTHING",
            &outer_hashes,
            &inner_hashes
        )
        .execute(pool)
        .await?;
    }

    // Insert timings (time-series data)
    if !timings.is_empty() {
        let (
            hashes,
            inner_hashes,
            collectors,
            peers,
            peer_hashes,
            dirs,
            net_timestamps,
            orig_timestamps,
        ): (
            Vec<_>,
            Vec<_>,
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
            "INSERT INTO timings (net_timestamp, hash, inner_hash, collector, peer, dir, peer_hash, orig_timestamp)
             SELECT * FROM UNNEST($1::timestamptz[], $2::bytea[], $3::bytea[], $4::text[], $5::text[], $6::smallint[], $7::bytea[], $8::timestamptz[])",
            &net_timestamps,
            &hashes,
            &inner_hashes,
            &collectors,
            &peers,
            &dirs,
            &peer_hashes,
            &orig_timestamps as &[Option<chrono::DateTime<chrono::Utc>>]
        )
        .execute(pool)
        .await?;
    }

    // Insert metadata
    if !metas.is_empty() {
        let (hashes, inner_hashes, types, sizes, orig_nodes, scids): (
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
            Vec<_>,
        ) = metas.into_iter().map(MessageMetadata::unroll).multiunzip();

        sqlx::query!(
            "INSERT INTO metadata (hash, inner_hash, type, size, orig_node, scid)
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::smallint[], $4::integer[], $5::text[], $6::bytea[])
             ON CONFLICT DO NOTHING",
            &hashes,
            &inner_hashes,
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
// TODO: add cancel token
pub async fn msg_decoder(
    mut raw_msg_rx: UnboundedReceiver<Message>,
    msg_tx: UnboundedSender<ExportedGossip>,
) -> anyhow::Result<()> {
    let mut stats_waiter = time::interval(STATS_INTERVAL);
    loop {
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
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await?;
    Ok(stream)
}

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

pub async fn ensure_database_exists(database_url: &str) -> anyhow::Result<()> {
    let (db_host, db_name) = database_url
        .rsplit_once('/')
        .ok_or_else(|| anyhow::anyhow!("DB connection string malformed"))?;
    let postgres_url = format!("{}/postgres", db_host);

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
