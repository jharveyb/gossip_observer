use anyhow::{anyhow, bail};
use bitcoin::Address;
use chrono::Utc;
use ldk_node::config::{BackgroundSyncConfig, ChannelConfig};
use ldk_node::{BalanceDetails, NodeError, NodeStatus, PeerDetails, UserChannelId};
use rand::prelude::*;
use std::ops::Mul;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;

use observer_common::types::OpenChannelCommand;
use observer_common::types::PeerSpecifier;

pub async fn node_peer_connect(
    node_copy: Arc<ldk_node::Node>,
    peer: &PeerSpecifier,
) -> anyhow::Result<()> {
    let peer_fmt = format!("{}@{}", peer.pubkey, peer.addr);
    let (pubkey, addr) = (peer.pubkey, peer.addr.clone());

    // Default 10 second timeout, deep in rust-lightning
    // lightning-net-tokio::lib::connect_outbound()
    // for ldk-node, this is async on the inside
    // Never persist peers, we'll maange that outside of LDK
    match spawn_blocking(move || node_copy.connect(pubkey, addr, false)).await {
        Ok(Ok(_)) => {
            info!(peer = %peer_fmt, "LDK: node connected");
            Ok(())
        }
        Ok(Err(e)) => match e {
            NodeError::ConnectionFailed => {
                let err_str = format!("Collector: LDK: connection failed: {}", peer_fmt);
                anyhow::bail!("{}", err_str);
            }
            _ => {
                error!(error = ?e, peer = %peer_fmt, "LDK: Unexpected error");
                Err(e.into())
            }
        },
        Err(e) => {
            error!(error = ?e, "Tokio: node_peer_connect error");
            Err(e.into())
        }
    }
}

pub async fn current_peers(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<Vec<PeerDetails>> {
    spawn_blocking(move || node_copy.list_peers())
        .await
        .map_err(anyhow::Error::msg)
}

pub async fn connected_peer_count(node_copy: Arc<ldk_node::Node>) -> usize {
    let peer_list = current_peers(node_copy).await.unwrap_or_default();
    peer_list
        .iter()
        .fold(0, |acc, i| if i.is_connected { acc + 1 } else { acc })
}

pub async fn status(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<NodeStatus> {
    // This shouldn't time out, even if the node is stopped.
    let default_status_wait = Duration::from_secs(10);
    let base_check = spawn_blocking(move || node_copy.status());
    match tokio::time::timeout(default_status_wait, base_check).await {
        Err(e) => {
            error!(error = ?e, "Collector: LDK: status call failed");
            Err(e.into())
        }
        Ok(Err(e)) => {
            error!(error = ?e, "Tokio: status error");
            Err(e.into())
        }
        Ok(Ok(status)) => Ok(status),
    }
}

pub async fn stop_node(node_copy: Arc<ldk_node::Node>, timeout: Duration) -> anyhow::Result<()> {
    let base_stop = spawn_blocking(move || node_copy.stop());
    match tokio::time::timeout(timeout, base_stop).await {
        Err(e) => {
            error!(error = ?e, "Collector: LDK: stop call failed");
            Err(e.into())
        }
        Ok(Err(e)) => {
            error!(error = ?e, "Tokio: stop error");
            Err(e.into())
        }
        Ok(Ok(stop)) => Ok(stop?),
    }
}

pub fn next_address(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<Address> {
    let addr = node_copy.onchain_payment().next_address()?;
    Ok(addr)
}

pub fn balances(node_copy: Arc<ldk_node::Node>) -> BalanceDetails {
    node_copy.list_balances()
}

pub async fn open_channel(
    node_copy: Arc<ldk_node::Node>,
    mut cmd: OpenChannelCommand,
    channel_cfg: Option<ChannelConfig>,
) -> anyhow::Result<UserChannelId> {
    let pubkey = cmd.peer.pubkey;
    let addr = cmd.peer.addrs.pop().ok_or(anyhow!("Missing peer addr"))?;
    match spawn_blocking(move || {
        node_copy.open_announced_channel(
            pubkey,
            addr,
            cmd.capacity_sats,
            cmd.push_amount_msat,
            channel_cfg,
        )
    })
    .await
    {
        Ok(Ok(id)) => Ok(id),
        Ok(Err(e)) => {
            error!(error = ?e, peer = ?pubkey, amount = %cmd.capacity_sats, "Collector: LDK: Unexpected error");
            Err(e.into())
        }
        Err(e) => {
            error!(error = ?e, "Tokio: open_channel error");
            Err(e.into())
        }
    }
}

pub fn random_channel_cfg() -> ChannelConfig {
    let mut cfg = ChannelConfig::default();
    let mut rng = StdRng::from_os_rng();
    let max_ppm = 500;
    let max_base_fee = 3000;
    cfg.forwarding_fee_proportional_millionths = rng.random_range(0..max_ppm);
    cfg.forwarding_fee_base_msat = rng.random_range(0..max_base_fee);
    cfg.cltv_expiry_delta = rng.random_range(72..144);
    cfg
}

/// Returns the name of the first chain sync that has gone stale, if any. Stale
/// is some multiple of the configured sync interval.
fn stale_sync(
    status: &NodeStatus,
    thresholds: &[(&'static str, i64); 3],
    now_secs: i64,
    elapsed_secs: i64,
) -> Option<&'static str> {
    let timestamps = [
        status.latest_onchain_wallet_sync_timestamp,
        status.latest_lightning_wallet_sync_timestamp,
        status.latest_fee_rate_cache_update_timestamp,
    ];
    let mut elapsed = *thresholds;
    for ((idx, &(name, threshold)), ts) in thresholds.iter().enumerate().zip(timestamps) {
        if elapsed_secs <= threshold {
            // Still within the startup grace for this sync. We may have read
            // an old sync time from an old shut-down collector.
            continue;
        }
        // The None case for a status timestamp should be caught on initial start.
        // Rather, by the time threshold seconds have passed after LDK startup,
        // at least one sync should have succeeded.
        match ts {
            // We failed to sync wihtin 'threshold' seconds from startup.
            None => return Some(name),
            Some(ts) => {
                let elapsed_sync = now_secs - (ts as i64);
                elapsed[idx].1 = elapsed_sync;
                if elapsed_sync > threshold {
                    info!("{elapsed:?}");
                    return Some(name);
                }
            }
        }
    }
    info!("LDK watchdog: sync check: passed");
    info!("{elapsed:?}");
    None
}

/// Watchdog task to detect if the LDK node is unhealthy. Defer the shutdown of
/// other tasks to the main shutdown handler.
///
/// Triggers if the node reports stopped, a status check fails, or if any
/// background chain sync has gone stale. LDK keeps running even if it loses
/// a chain backend, so we have to explicitly check the last sync times.
pub async fn ldk_watchdog(
    node: Arc<ldk_node::Node>,
    mut waiter: Interval,
    cancel: CancellationToken,
    sync_cfg: BackgroundSyncConfig,
    health_check_interval: u32,
    fail_limit: u16,
) -> anyhow::Result<()> {
    info!("Starting LDK watchdog");

    let fail_limit_secs = |interval: u64| -> i64 {
        // The interval duration we use should never be less than our health
        // check interval. Otherwise, we would be checking syncs faster than
        // we told LDK they should occur.
        let check_interval = interval.max(health_check_interval.into()) as i64;
        check_interval.mul(i64::from(fail_limit))
    };

    let thresholds = [
        (
            "onchain",
            fail_limit_secs(sync_cfg.onchain_wallet_sync_interval_secs),
        ),
        (
            "lightning",
            fail_limit_secs(sync_cfg.lightning_wallet_sync_interval_secs),
        ),
        (
            "fee_rate",
            fail_limit_secs(sync_cfg.fee_rate_cache_update_interval_secs),
        ),
    ];
    let start = Utc::now();

    info!("LDK watchdog: start time: {:?}", start);
    info!("LDK watchdog thresholds: {:?}", thresholds);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("LDK health watchdog: shutting down");
                return Ok(());
            }
            _ = waiter.tick() => {}
        }

        let status = match status(node.clone()).await {
            Ok(s) => s,
            Err(e) => {
                error!("LDK watchdog: node status check failed");
                cancel.cancel();
                return Err(e);
            }
        };

        // Node object / handle is live, but the actual node may be stopped.
        if !status.is_running {
            error!("LDK watchdog: node status is stopped; shutting down collector");
            cancel.cancel();
            return Ok(());
        }

        // Node is running; if it is failing to sync, we should restart.
        let now = Utc::now();
        let elapsed = now - start;
        if let Some(name) = stale_sync(&status, &thresholds, now.timestamp(), elapsed.num_seconds())
        {
            error!(
                sync = name,
                "LDK watchdog: chain sync stale, shutting down collector"
            );
            cancel.cancel();
            bail!("LDK chain sync '{name}' is stale");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Network;
    use lightning::chain::BestBlock;

    // fail_limit 3 applied to the default sync intervals (80 / 30 / 600).
    const THRESHOLDS: [(&str, i64); 3] = [("onchain", 240), ("lightning", 90), ("fee_rate", 1800)];
    const NOW: i64 = 110_000;

    fn make_offsets(base_time: i64, offsets: &[i64]) -> Vec<u64> {
        offsets.iter().map(|o| (base_time + o) as u64).collect()
    }

    fn make_status(
        onchain: Option<u64>,
        lightning: Option<u64>,
        fee_rate: Option<u64>,
    ) -> NodeStatus {
        NodeStatus {
            is_running: true,
            current_best_block: BestBlock::from_network(Network::Bitcoin),
            latest_lightning_wallet_sync_timestamp: lightning,
            latest_onchain_wallet_sync_timestamp: onchain,
            latest_fee_rate_cache_update_timestamp: fee_rate,
            latest_rgs_snapshot_timestamp: None,
            latest_pathfinding_scores_sync_timestamp: None,
            latest_node_announcement_broadcast_timestamp: None,
            latest_channel_monitor_archival_height: None,
        }
    }

    fn status_from_offsets(base_time: i64, offsets: &[i64]) -> NodeStatus {
        let offsets = make_offsets(base_time, offsets);
        make_status(Some(offsets[0]), Some(offsets[1]), Some(offsets[2]))
    }

    #[test]
    fn fresh_syncs_are_healthy() {
        let offsets = [-10, -5, -100];
        let status = status_from_offsets(NOW, &offsets);
        assert_eq!(stale_sync(&status, &THRESHOLDS, NOW, 100_000), None);
    }

    #[test]
    fn stale_onchain_trips() {
        // onchain age 1000 > 240, others fresh.
        let offsets = [-1000, -5, -100];
        let status = status_from_offsets(NOW, &offsets);
        assert_eq!(
            stale_sync(&status, &THRESHOLDS, NOW, 100_000),
            Some("onchain")
        );
    }

    #[test]
    fn stale_lightning_trips_when_onchain_fresh() {
        // onchain fresh, lightning age 1000 > 90.
        let offsets = [-10, -1000, -100];
        let status = status_from_offsets(NOW, &offsets);
        assert_eq!(
            stale_sync(&status, &THRESHOLDS, NOW, 100_000),
            Some("lightning")
        );
    }

    #[test]
    fn old_timestamps_within_grace_are_ignored() {
        // uptime below the smallest threshold (90) -> nothing is judged yet,
        // even with no successful syncs (the boot-with-persisted-timestamps case).
        let status = make_status(None, None, None);
        assert_eq!(stale_sync(&status, &THRESHOLDS, NOW, 50), None);

        let offsets = [-1000, -1000, -1000];
        let status = status_from_offsets(NOW, &offsets);
        assert_eq!(stale_sync(&status, &THRESHOLDS, NOW, 50), None);
    }

    #[test]
    fn never_synced_trips_after_grace() {
        let status = make_status(None, None, None);
        assert_eq!(
            stale_sync(&status, &THRESHOLDS, NOW, 100_000),
            Some("onchain")
        );
    }
}
