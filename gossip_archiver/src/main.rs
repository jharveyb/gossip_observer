use std::path::Path;
use std::time::Duration;

use anyhow::anyhow;
use anyhow::bail;
use async_nats::Message;
use gossip_archiver::INTER_MSG_DELIM;
use gossip_archiver::MessageHashMapping;
use gossip_archiver::config::ArchiverConfig;
use gossip_archiver::nats::nats_reader_with_reconnect;
use gossip_archiver::{ExportedGossip, MessageMetadata, MessageNodeTimings, RawMessage};
use gossip_archiver::{decode_msg, split_exported_gossip};
use itertools::Itertools;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::mpsc::channel as tokio_channel;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::time::{self};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

static STATS_INTERVAL: Duration = Duration::from_secs(600);

fn main() -> anyhow::Result<()> {
    // Load .env file if present (optional for production with systemd)
    let _ = dotenvy::dotenv();
    let cfg = ArchiverConfig::new()?;

    // Initialize structured logging with tokio-console support.
    // This must be called BEFORE the tokio runtime is created to avoid
    // conflicts with console-subscriber's internal tokio::spawn().
    let _logger_guard = observer_common::logging::init_logging(
        &cfg.log_level,
        Path::new(&cfg.storage_dir),
        "archiver",
        Some(cfg.console.clone()),
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()?;

    runtime.block_on(async_main(cfg))
}

async fn async_main(cfg: ArchiverConfig) -> anyhow::Result<()> {
    info!(uuid = %cfg.uuid, "Gossip archiver initialized");

    // Configure NATS client with extra retry; default ping interval is 60 seconds
    let nats_options = async_nats::ConnectOptions::new().retry_on_initial_connect(); // Enable reconnection attempts
    let nats_client = async_nats::connect_with_options(&cfg.nats.server_addr, nats_options).await?;
    info!("Connected to NATS server");

    let (raw_msg_tx, raw_msg_rx) = unbounded_channel();
    let (msg_tx, msg_rx) = unbounded_channel();

    let (buf_raw_tx, buf_raw_rx) = unbounded_channel();
    let (buf_timings_tx, buf_timings_rx) = unbounded_channel();
    let (buf_meta_tx, buf_meta_rx) = unbounded_channel();
    let (buf_hash_mapping_tx, buf_hash_mapping_rx) = unbounded_channel();
    let (buf_tick_tx, buf_tick_rx) = tokio_channel(1);

    let database_url = cfg.db_url.to_owned();
    ensure_database_exists(&database_url).await?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    info!("Initialized TimescaleDB connection");

    let root_cancel_token = CancellationToken::new();
    let nats_reader_cfg = (
        cfg.nats.stream_name.clone(),
        cfg.nats.consumer_name.clone(),
        cfg.nats.subject_prefix.clone(),
    );
    let nats_cancel_token = root_cancel_token.clone();
    let nats_handle = tokio::spawn(async move {
        nats_reader_with_reconnect(nats_client, nats_reader_cfg, raw_msg_tx, nats_cancel_token)
            .await
    });
    let decode_cancel_token = root_cancel_token.clone();
    let decode_handle =
        tokio::spawn(async move { msg_decoder(raw_msg_rx, msg_tx, decode_cancel_token).await });
    let db_cancel_token = root_cancel_token.clone();
    let db_handle = tokio::spawn(async move {
        db_write_handler(
            pool,
            buf_raw_rx,
            buf_timings_rx,
            buf_meta_rx,
            buf_hash_mapping_rx,
            buf_tick_rx,
            cfg.database.batch_size,
            db_cancel_token,
        )
        .await
    });
    let db_write_ticker_cancel_token = root_cancel_token.clone();
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
            db_write_ticker_cancel_token,
        )
        .await
    });

    match tokio::try_join!(
        nats_handle,
        decode_handle,
        db_handle,
        db_write_ticker_handle,
    ) {
        Ok((nats_res, decode_res, db_handle_res, db_write_res)) => {
            info!(nats = ?nats_res, decode = ?decode_res, db_handle = ?db_handle_res, db_write = ?db_write_res, "Final task output");
            let res = vec![nats_res, decode_res, db_handle_res, db_write_res];
            let mut final_err = false;
            for r in res {
                if let Err(e) = r {
                    error!(error = %e, "Task error");
                    final_err = true;
                }
            }
            if final_err {
                Err(anyhow!("Final task error"))
            } else {
                Ok(())
            }
        }
        Err(e) => {
            error!(error = %e, "Join error");
            Err(e.into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn db_write_ticker(
    mut msg_rx: UnboundedReceiver<ExportedGossip>,
    buf_raw_tx: UnboundedSender<RawMessage>,
    buf_timings_tx: UnboundedSender<MessageNodeTimings>,
    buf_meta_tx: UnboundedSender<MessageMetadata>,
    buf_hash_mapping_tx: UnboundedSender<MessageHashMapping>,
    buf_tick_tx: Sender<()>,
    flush_interval: u32,
    batch_size: u32,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    let mut flush_waiter = time::interval(Duration::from_secs(flush_interval.into()));
    let mut stats_waiter = time::interval(STATS_INTERVAL);

    let mut poll_counter = 0;
    let mut full_counter = 0;
    let mut msg_counter = 0;
    let batch_size = batch_size as usize;

    // Send cancel signal if this task exits. It should never exit under normal operation.
    let _drop_guard = cancel_token.clone().drop_guard();

    // Signal another task to write buffered values to the DB.
    let signal_flush = async || -> anyhow::Result<usize> {
        let tick_permit = buf_tick_tx.reserve().await?;
        tick_permit.send(());
        Ok(0)
    };

    let critical_error = |e: anyhow::Error, msg: &str| {
        error!(error = %e, msg = %msg, "Internal: db_write_ticker");
        cancel_token.cancel();
        e
    };

    info!(
        flush_interval_secs = flush_interval,
        batch_size,
        stats_interval_secs = STATS_INTERVAL.as_secs(),
        "Starting DB write ticker"
    );

    let mut gossip_msg = None;
    let mut flush_tick = false;
    loop {
        // Receive a new message, or perform a scheduled flush of values to the
        // DB. We should never end up trying to do both in the same loop iteration.
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                warn!("Internal: db_write_ticker: cancel signal received");
                return Ok(());
            }
            _ = flush_waiter.tick() => {
                flush_tick = true;
            }
            // TODO: we could probably put this on a timer and use recv_many
            msg = msg_rx.recv() => {
                match msg {
                    Some(msg) => gossip_msg = Some(msg),
                    None => {
                        // Critical error, shut down the archiver.
                        let errmsg = "Internal: db_write_ticker: msg chan closed";
                        error!(message = ?msg, errmsg);
                        cancel_token.cancel();
                        bail!(errmsg);
                    }
                }
            }
            _ = stats_waiter.tick() => {
                info!(flush_count = poll_counter, full_buffer_count = full_counter, "DB write ticker stats");
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

            if let Err(e) = buf_raw_tx.send(msg_entry) {
                bail!(critical_error(e.into(), "buf_raw_tx send error",));
            }
            if let Err(e) = buf_timings_tx.send(timings_entry) {
                bail!(critical_error(e.into(), "buf_timings_tx send error",));
            }
            if let Err(e) = buf_meta_tx.send(meta_entry) {
                bail!(critical_error(e.into(), "buf_meta_tx send error",));
            }
            if let Err(e) = buf_hash_mapping_tx.send(hash_mapping) {
                bail!(critical_error(e.into(), "buf_hash_mapping_tx send error",));
            }
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
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    info!(batch_size, "Starting DB writer");
    let batch_size = batch_size as usize;
    let mut raw_msgs = Vec::with_capacity(batch_size);
    let mut timings = Vec::with_capacity(batch_size);
    let mut metas = Vec::with_capacity(batch_size);
    let mut hash_mappings = Vec::with_capacity(batch_size);
    let mut should_flush = false;
    let _drop_guard = cancel_token.drop_guard_ref();

    // Log and send cancel signal before we exit.
    let critical_error = |msg: String| {
        error!(message = ?msg, "Internal: db_write_handler");
        cancel_token.cancel();
        msg + "Internal: db_write_handler"
    };

    loop {
        // TODO: clean up our closed chan. handling
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {
                warn!("Internal: db_write_handler: cancel signal received");
                return Ok(());
            }
            _ = buf_tick_rx.recv() => {
                should_flush = true;
            }
            raw_msg = buf_raw_rx.recv() => {
                match raw_msg {
                    Some(msg) => raw_msgs.push(msg),
                    None => {
                        let errmsg = "raw msg chan closed";
                        bail!(critical_error(errmsg.into()));
                    }
                }
            }
            timings_msg = buf_timings_rx.recv() => {
                match timings_msg {
                    Some(msg) => timings.push(msg),
                    None => {
                        let errmsg = "timings msg chan closed";
                        bail!(critical_error(errmsg.into()));
                    }
                }
            }
            meta_msg = buf_meta_rx.recv() => {
                match meta_msg {
                    Some(msg) => metas.push(msg),
                    None => {
                        let errmsg = "meta msg chan closed";
                        bail!(critical_error(errmsg.into()));
                    }
                }
            }
            hash_mapping_msg = buf_hash_mapping_rx.recv() => {
                match hash_mapping_msg {
                    Some(msg) => hash_mappings.push(msg),
                    None => {
                        let errmsg = "hash mapping msg chan closed";
                        bail!(critical_error(errmsg.into()));
                    }
                }
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

            if let Err(e) =
                db_batch_write(&pool, db_raws, db_timings, db_metas, db_hash_mappings).await
            {
                error!(error = ?e, "Internal: db_batch_write");
                cancel_token.cancel();
                bail!(e)
            }
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

    let critical_error = |e: sqlx::Error, msg: String| {
        error!(error = ?e, message = ?msg, "Internal: db_batch_write");
        msg + "Internal: db_batch_write"
    };

    // Insert raw messages
    if !raws.is_empty() {
        let (hashes, raw_msgs): (Vec<_>, Vec<_>) =
            raws.into_iter().map(RawMessage::unroll).multiunzip();

        if let Err(e) = sqlx::query!(
            "INSERT INTO messages (hash, raw)
             SELECT * FROM UNNEST($1::bytea[], $2::text[])
             ON CONFLICT DO NOTHING",
            &hashes,
            &raw_msgs
        )
        .execute(pool)
        .await
        {
            bail!(critical_error(e, "insert raw messages error".into()));
        }
    }

    // Insert the outer->inner hash mappings
    if !hash_mapppings.is_empty() {
        let (outer_hashes, inner_hashes): (Vec<_>, Vec<_>) = hash_mapppings
            .into_iter()
            .map(MessageHashMapping::unroll)
            .multiunzip();

        if let Err(e) = sqlx::query!(
            "INSERT INTO message_hashes (outer_hash, inner_hash)
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[])
             ON CONFLICT DO NOTHING",
            &outer_hashes,
            &inner_hashes
        )
        .execute(pool)
        .await
        {
            bail!(critical_error(e, "insert hash mappings error".into()));
        }
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

        if let Err(e) = sqlx::query!(
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
        .await {
            bail!(critical_error(e, "insert timings error".into()));
        }
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

        if let Err(e) = sqlx::query!(
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
        .await {
            bail!(critical_error(e, "insert metadata error".into()));
        }
    }

    Ok(())
}

// split a big NATS msg into multiple decoded msgs
// TODO: add cancel token
pub async fn msg_decoder(
    mut raw_msg_rx: UnboundedReceiver<Message>,
    msg_tx: UnboundedSender<ExportedGossip>,
    cancel_token: CancellationToken,
) -> anyhow::Result<()> {
    // TODO: remove explicit cancels since we have the drop guard
    let _drop_guard = cancel_token.drop_guard_ref();
    let critical_error = |e: anyhow::Error, msg: String| {
        error!(error = %e, message = ?msg, "Internal: msg_decoder");
        cancel_token.cancel();
        e
    };
    loop {
        let msg = tokio::select! {
            rx_msg = raw_msg_rx.recv() => rx_msg,
        };

        match msg {
            Some(raw_msg) => {
                let inner_msgs = match str::from_utf8(&raw_msg.payload) {
                    Ok(s) => s,
                    Err(e) => {
                        bail!(critical_error(
                            e.into(),
                            "Failed to decode NATS message as UTF-8".into()
                        ));
                    }
                };

                for raw_msg in inner_msgs.split(INTER_MSG_DELIM) {
                    match decode_msg(raw_msg) {
                        Ok(decoded) => {
                            if let Err(e) = msg_tx.send(decoded) {
                                bail!(critical_error(
                                    e.into(),
                                    "Failed to send decoded message to downstream".into()
                                ));
                            }
                        }
                        Err(e) => {
                            let errmsg = "Failed to decode gossip message";
                            error!(error = %e, raw_msg_preview = &raw_msg[..raw_msg.len().min(100)], errmsg);
                            cancel_token.cancel();
                            bail!(errmsg);
                        }
                    }
                }
            }
            None => {
                let errmsg = "Internal: msg_decoder: rx chan closed";
                error!(errmsg);
                cancel_token.cancel();
                bail!(errmsg);
            }
        }
    }
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
        let errmsg = "Database does not exist";
        error!(database = db_name, errmsg);
        bail!(errmsg);
    } else {
        let errmsg = "Database already exists";
        info!(database = db_name, errmsg);
        Ok(())
    }
}
