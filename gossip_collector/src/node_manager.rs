use anyhow::{anyhow, bail};
use bitcoin::{Address, BlockHash};
use chrono::{DateTime, Utc};
use ldk_node::config::ChannelConfig;
use ldk_node::{BalanceDetails, NodeError, NodeStatus, PeerDetails, UserChannelId};
use rand::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_blocking;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;

use observer_common::types::OpenChannelCommand;
use observer_common::types::PeerSpecifier;

/// Run a (potentially) blocking LDK node call off the async runtime, with an
/// optional timeout. Note that a timeout only abandons the join handle; the
/// blocking thread itself keeps running until the call returns.
async fn node_call<T, F>(op: &'static str, timeout: Option<Duration>, f: F) -> anyhow::Result<T>
where
    F: FnOnce() -> Result<T, NodeError> + Send + 'static,
    T: Send + 'static,
{
    let call = spawn_blocking(f);
    let joined = match timeout {
        Some(t) => match tokio::time::timeout(t, call).await {
            Ok(joined) => joined,
            Err(e) => {
                error!(
                    op,
                    timeout_secs = t.as_secs(),
                    "Collector: LDK: call timed out"
                );
                return Err(e.into());
            }
        },
        None => call.await,
    };
    match joined {
        Ok(Ok(val)) => Ok(val),
        Ok(Err(e)) => Err(e.into()),
        Err(e) => {
            error!(op, error = ?e, "Tokio: LDK call join error");
            Err(e.into())
        }
    }
}

pub async fn node_peer_connect(
    node_copy: Arc<ldk_node::Node>,
    peer: &PeerSpecifier,
) -> anyhow::Result<()> {
    let peer_fmt = format!("{}@{}", peer.pubkey, peer.addr);
    let (pubkey, addr) = (peer.pubkey, peer.addr.clone());

    // Default 10 second timeout, deep in rust-lightning
    // lightning-net-tokio::lib::connect_outbound()
    // for ldk-node, this is async on the inside
    // Never persist peers, we'll manage that outside of LDK
    match node_call("connect", None, move || {
        node_copy.connect(pubkey, addr, false)
    })
    .await
    {
        Ok(_) => {
            info!(peer = %peer_fmt, "LDK: node connected");
            Ok(())
        }
        // Connection failures are routine (offline peers), and NotRunning
        // means the node is being stopped under us (shutdown); don't log
        // either as unexpected, just report to the caller.
        Err(e)
            if matches!(
                e.downcast_ref::<NodeError>(),
                Some(NodeError::ConnectionFailed | NodeError::NotRunning)
            ) =>
        {
            anyhow::bail!("Collector: LDK: connection failed: {}", peer_fmt);
        }
        Err(e) => {
            error!(error = %e, peer = %peer_fmt, "LDK: Unexpected error");
            Err(e)
        }
    }
}

pub async fn current_peers(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<Vec<PeerDetails>> {
    node_call("list_peers", None, move || {
        Ok::<_, NodeError>(node_copy.list_peers())
    })
    .await
}

pub async fn connected_peer_count(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<usize> {
    let peer_list = current_peers(node_copy).await?;
    Ok(peer_list.iter().filter(|p| p.is_connected).count())
}

pub async fn status(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<NodeStatus> {
    // This shouldn't time out, even if the node is stopped.
    let default_status_wait = Duration::from_secs(10);
    node_call("status", Some(default_status_wait), move || {
        Ok::<_, NodeError>(node_copy.status())
    })
    .await
}

pub async fn stop_node(node_copy: Arc<ldk_node::Node>, timeout: Duration) -> anyhow::Result<()> {
    node_call("stop", Some(timeout), move || node_copy.stop()).await
}

pub async fn next_address(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<Address> {
    node_call("next_address", None, move || {
        node_copy.onchain_payment().next_address()
    })
    .await
}

pub async fn balances(node_copy: Arc<ldk_node::Node>) -> anyhow::Result<BalanceDetails> {
    node_call("list_balances", None, move || {
        Ok::<_, NodeError>(node_copy.list_balances())
    })
    .await
}

pub async fn open_channel(
    node_copy: Arc<ldk_node::Node>,
    mut cmd: OpenChannelCommand,
    channel_cfg: Option<ChannelConfig>,
) -> anyhow::Result<UserChannelId> {
    let pubkey = cmd.peer.pubkey;
    let addr = cmd.peer.addrs.pop().ok_or(anyhow!("Missing peer addr"))?;
    let capacity_sats = cmd.capacity_sats;
    node_call("open_announced_channel", None, move || {
        node_copy.open_announced_channel(
            pubkey,
            addr,
            cmd.capacity_sats,
            cmd.push_amount_msat,
            channel_cfg,
        )
    })
    .await
    .inspect_err(
        |e| error!(error = %e, peer = ?pubkey, amount = %capacity_sats, "LDK: channel open failed"),
    )
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

/// Tracks chain-tip progress for the watchdog. The node's best block must advance
/// at least once within `stale_secs`, or its chain backend has likely stalled and
/// we restart the collector. This is a uniform, robust liveness signal across every
/// chain source: they all advance `NodeStatus.current_best_block` as blocks arrive,
/// regardless of wallet-sync internals.
struct BestBlockMonitor {
    stale_secs: i64,
    last_block: BlockHash,
    last_change: DateTime<Utc>,
}

impl BestBlockMonitor {
    /// Seeds the monitor with the first observed best block. Anchoring
    /// `last_change` at `now` gives a full `stale_secs` startup grace before the
    /// tip is first required to advance.
    fn new(initial: BlockHash, now: DateTime<Utc>, stale_secs: u64) -> Self {
        Self {
            stale_secs: stale_secs as i64,
            last_block: initial,
            last_change: now,
        }
    }

    /// Records the node's current best block. Returns the seconds since the tip
    /// last advanced and whether that now exceeds `stale_secs`. A changed tip
    /// (new block or reorg — both change the hash) resets the timer.
    fn observe(&mut self, current: BlockHash, now: DateTime<Utc>) -> (i64, bool) {
        if self.last_block != current {
            self.last_block = current;
            self.last_change = now;
        }
        let elapsed = (now - self.last_change).num_seconds();
        (elapsed, elapsed > self.stale_secs)
    }
}

/// Watchdog task to detect if the LDK node is unhealthy. Defer the shutdown of
/// other tasks to the main shutdown handler.
///
/// Triggers if the node reports stopped, if `status_fail_limit` consecutive status
/// checks fail, or if the chain tip fails to advance within `bestblock_stale_secs`.
/// LDK keeps running even if it loses its chain backend, so we explicitly require
/// the best block to keep moving.
pub async fn ldk_watchdog(
    node: Arc<ldk_node::Node>,
    mut waiter: Interval,
    cancel: CancellationToken,
    bestblock_stale_secs: u64,
    status_fail_limit: u16,
) -> anyhow::Result<()> {
    info!(
        bestblock_stale_secs,
        status_fail_limit, "Starting LDK watchdog"
    );

    // Seeded from the first successful status read.
    let mut monitor: Option<BestBlockMonitor> = None;
    let mut consecutive_status_failures: u16 = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("LDK health watchdog: shutting down");
                return Ok(());
            }
            _ = waiter.tick() => {}
        }

        let status = match status(node.clone()).await {
            Ok(s) => {
                consecutive_status_failures = 0;
                s
            }
            Err(e) => {
                // A single status-call hiccup should not restart the collector;
                // only give up after repeated failures.
                consecutive_status_failures += 1;
                error!(
                    failures = consecutive_status_failures,
                    limit = status_fail_limit,
                    "LDK watchdog: node status check failed"
                );
                if consecutive_status_failures > status_fail_limit {
                    error!("LDK watchdog: status check failed too many times; shutting down");
                    cancel.cancel();
                    return Err(e);
                }
                continue;
            }
        };

        // Node object / handle is live, but the actual node may be stopped.
        if !status.is_running {
            // During shutdown the signal handler stops the node deliberately,
            // then cancels; give that cancellation a moment to propagate so we
            // don't report an intentional stop as a watchdog failure.
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("LDK health watchdog: shutting down");
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
            error!("LDK watchdog: node status is stopped; shutting down collector");
            cancel.cancel();
            return Ok(());
        }

        let tip = status.current_best_block.block_hash;
        let height = status.current_best_block.height;
        let now = Utc::now();

        let (elapsed, stale) = match monitor.as_mut() {
            Some(m) => m.observe(tip, now),
            None => {
                monitor = Some(BestBlockMonitor::new(tip, now, bestblock_stale_secs));
                (0, false)
            }
        };

        if stale {
            error!(
                height,
                elapsed, "LDK watchdog: best block stale, shutting down collector"
            );
            cancel.cancel();
            bail!("LDK best block stale ({elapsed}s at height {height})");
        }

        info!(height, elapsed, "LDK watchdog: best block check passed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    const STALE_SECS: u64 = 7200;

    fn ts(secs: u64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs as i64, 0).expect("valid timestamp")
    }

    fn hash(n: u8) -> BlockHash {
        BlockHash::from_byte_array([n; 32])
    }

    #[test]
    fn advancing_tip_is_healthy() {
        // The tip advances on every check, well within the deadline, across a long
        // span (longer than the deadline itself) -> never stale.
        let mut monitor = BestBlockMonitor::new(hash(1), ts(0), STALE_SECS);
        assert_eq!(monitor.observe(hash(2), ts(1_000)), (0, false));
        assert_eq!(monitor.observe(hash(3), ts(10_000)), (0, false));
        assert_eq!(monitor.observe(hash(4), ts(20_000)), (0, false));
    }

    #[test]
    fn stalled_tip_trips_after_deadline() {
        // The tip never advances. Exactly at the deadline is not yet stale; one
        // second past it trips.
        let mut monitor = BestBlockMonitor::new(hash(1), ts(0), STALE_SECS);
        assert_eq!(
            monitor.observe(hash(1), ts(STALE_SECS)),
            (STALE_SECS as i64, false)
        );
        assert_eq!(
            monitor.observe(hash(1), ts(STALE_SECS + 1)),
            ((STALE_SECS + 1) as i64, true)
        );
    }

    #[test]
    fn tip_change_resets_timer() {
        // Nearly stale, then the tip advances -> the timer resets and staleness is
        // measured from the new change time.
        let mut monitor = BestBlockMonitor::new(hash(1), ts(0), STALE_SECS);
        assert_eq!(monitor.observe(hash(1), ts(7_000)), (7_000, false));
        assert_eq!(monitor.observe(hash(2), ts(7_100)), (0, false));
        assert_eq!(monitor.observe(hash(2), ts(14_000)), (6_900, false));
        assert_eq!(monitor.observe(hash(2), ts(14_301)), (7_201, true));
    }
}
