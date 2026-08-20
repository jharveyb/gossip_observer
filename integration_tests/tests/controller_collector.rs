//! End-to-end control-plane test: spawns the real `observer_controller` and
//! `gossip_collector` binaries (plus regtest bitcoind and nats-server) with
//! generated configs, then verifies the full conversation:
//! register → eligible-peer delivery → target-count push → heartbeat,
//! followed by a clean SIGINT shutdown of both processes.
//!
//! External requirements (see `just test-integration`):
//! - the two workspace binaries built in `target/debug/`
//! - `bitcoind` via `BITCOIND_EXE` or PATH
//! - `nats-server` via `NATS_SERVER_EXE` or PATH

use std::fs::{self, File};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use corepc_node::{Conf, Node};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use observer_common::collectorrpc::NodeConfigRequest;
use observer_common::collectorrpc::collector_service_client::CollectorServiceClient;
use observer_common::controllerrpc::StatusRequest;
use observer_common::controllerrpc::controller_service_client::ControllerServiceClient;
use observer_common::types::LdkNodeConfig;
use tempfile::TempDir;

const TEST_UUID: &str = "019c0000-0000-7000-8000-000000000001";
// Standard BIP-39 test vector mnemonic; regtest-only throwaway wallet.
const TEST_MNEMONIC: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
const COMMUNITY_ID: u32 = 1;
const FIXTURE_PEERS: usize = 3;

/// Bind-then-drop to pick a free localhost port. Racy in principle, fine for
/// a single sequential test.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// The test executable lives at `target/debug/deps/<test>-<hash>`; the
/// workspace binaries live two levels up. `cargo test` does NOT build sibling
/// binaries, hence the pointer at the just recipe.
fn target_bin(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // deps/
    path.pop(); // debug/
    let bin = path.join(name);
    assert!(
        bin.exists(),
        "{} is not built; run `just test-integration` (or `cargo build -p {}` first)",
        bin.display(),
        name
    );
    bin
}

/// A spawned child process, killed on drop, with its output captured to a log
/// file we can dump on failure.
struct ChildGuard {
    name: &'static str,
    child: Child,
    log_path: PathBuf,
}

impl ChildGuard {
    fn spawn(
        name: &'static str,
        bin: &Path,
        cwd: &Path,
        envs: &[(&str, &str)],
    ) -> Result<Self> {
        let log_path = cwd.join(format!("{name}.log"));
        let stdout = File::create(&log_path)?;
        let stderr = stdout.try_clone()?;
        let child = Command::new(bin)
            .current_dir(cwd)
            .envs(envs.iter().copied())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .with_context(|| format!("spawning {name} from {}", bin.display()))?;
        println!("spawned {name} (pid {}), log: {}", child.id(), log_path.display());
        Ok(Self {
            name,
            child,
            log_path,
        })
    }

    /// Send a signal, then wait (bounded) for the process to exit.
    fn signal_and_wait(&mut self, signal: Signal, timeout: Duration) -> ExitStatus {
        kill(Pid::from_raw(self.child.id() as i32), signal).unwrap();
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "{} did not exit within {timeout:?} of {signal}\n{}",
                self.name,
                self.log_tail()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn log_tail(&self) -> String {
        let contents = fs::read_to_string(&self.log_path).unwrap_or_default();
        let lines: Vec<&str> = contents.lines().collect();
        let tail_start = lines.len().saturating_sub(40);
        format!(
            "--- last {} lines of {} ({}) ---\n{}",
            lines.len() - tail_start,
            self.name,
            self.log_path.display(),
            lines[tail_start..].join("\n")
        )
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Prints child log tails if the test panics before being disarmed.
struct DumpLogsOnPanic<'a> {
    guards: Vec<&'a ChildGuard>,
    armed: bool,
}

impl Drop for DumpLogsOnPanic<'_> {
    fn drop(&mut self) {
        if self.armed && std::thread::panicking() {
            for guard in &self.guards {
                eprintln!("{}", guard.log_tail());
            }
        }
    }
}

fn spawn_nats(port: u16, work_dir: &Path) -> Result<ChildGuard> {
    let exe = std::env::var("NATS_SERVER_EXE").unwrap_or_else(|_| "nats-server".to_string());
    let log_path = work_dir.join("nats-server.log");
    let stdout = File::create(&log_path)?;
    let stderr = stdout.try_clone()?;
    let child = Command::new(&exe)
        .args(["-a", "127.0.0.1", "-p", &port.to_string()])
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("spawning {exe}; set NATS_SERVER_EXE or add it to PATH"))?;
    let guard = ChildGuard {
        name: "nats-server",
        child,
        log_path,
    };

    // Wait until it accepts connections.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(guard);
        }
        assert!(
            Instant::now() < deadline,
            "nats-server did not come up on port {port}\n{}",
            guard.log_tail()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A throwaway regtest address to mine to — derived locally so the test never
/// needs bitcoind's wallet (which may not even be compiled in).
fn regtest_mining_address() -> bitcoin::Address {
    let secp = Secp256k1::new();
    let sk = SecretKey::from_slice(&[0xAA; 32]).unwrap();
    let pk = bitcoin::PublicKey::new(PublicKey::from_secret_key(&secp, &sk));
    let compressed = bitcoin::CompressedPublicKey::try_from(pk).unwrap();
    bitcoin::Address::p2wpkh(&compressed, bitcoin::Network::Regtest)
}

/// Deterministic valid pubkeys for the fixture peer records.
fn fixture_pubkeys() -> Vec<String> {
    let secp = Secp256k1::new();
    (1u8..=FIXTURE_PEERS as u8)
        .map(|i| {
            let sk = SecretKey::from_slice(&[i; 32]).unwrap();
            PublicKey::from_secret_key(&secp, &sk).to_string()
        })
        .collect()
}

struct ControllerFixture {
    dir: TempDir,
    grpc_port: u16,
}

/// Extra TOML appended to both daemon configs by the janitor test: minutely
/// rotation with second-scale compress/expire so a full cycle fits in minutes.
const JANITOR_TEST_TOML: &str = r#"
[log_rotation]
rotation = "minutely"
compress_after_secs = 60
retention_secs = 120
sweep_interval_secs = 5
startup_delay_max_secs = 1
max_compressions_per_pass = 20
"#;

fn setup_controller_dir(extra_toml: &str) -> Result<ControllerFixture> {
    let dir = TempDir::new()?;
    let root = dir.path();
    let storage = root.join("storage");
    fs::create_dir_all(&storage)?;

    let pubkeys = fixture_pubkeys();
    let mut nodes_csv = String::from("pubkey,net_type,sockets,alias\n");
    let mut communities_csv = String::from("pubkey,level_0,level_1,level_2\n");
    for (idx, pk) in pubkeys.iter().enumerate() {
        // Unroutable-but-parseable local sockets; the collector's connection
        // attempts to them simply fail fast.
        nodes_csv.push_str(&format!("{pk},ipv4,127.0.0.1:{},node-{idx}\n", 39000 + idx));
        communities_csv.push_str(&format!("{pk},{COMMUNITY_ID},1,1\n"));
    }
    fs::write(root.join("nodes.csv"), nodes_csv)?;
    fs::write(root.join("communities.csv"), communities_csv)?;
    fs::write(
        root.join("collector_mapping.toml"),
        format!("\"{TEST_UUID}\" = {COMMUNITY_ID}\n"),
    )?;

    let grpc_port = free_port();
    let config = format!(
        r#"
[grpc]
hostname = "127.0.0.1"
port = {grpc_port}

[controller]
storage_dir = "{storage}"
total_connections = 3
heartbeat_expiry = 30

[console]
listen_port = {console_port}
retention_secs = 30

[network_info]
node_clusterings = "{nodes}"
node_communities = "{communities}"
collector_mapping = "{mapping}"
"#,
        storage = storage.display(),
        console_port = free_port(),
        nodes = root.join("nodes.csv").display(),
        communities = root.join("communities.csv").display(),
        mapping = root.join("collector_mapping.toml").display(),
    );
    fs::write(root.join("controller_config.toml"), config + extra_toml)?;

    Ok(ControllerFixture { dir, grpc_port })
}

struct CollectorFixture {
    dir: TempDir,
    grpc_port: u16,
}

fn setup_collector_dir(
    bitcoind: &Node,
    nats_port: u16,
    controller_grpc_port: u16,
    extra_toml: &str,
) -> Result<CollectorFixture> {
    let dir = TempDir::new()?;
    let root = dir.path();
    let ldk_dir = root.join("ldk");
    let storage = root.join("storage");
    fs::create_dir_all(&ldk_dir)?;
    fs::create_dir_all(&storage)?;
    // No trailing newline: the collector parses the file contents verbatim.
    fs::write(ldk_dir.join("mnemonic.txt"), TEST_MNEMONIC)?;

    let cookie = bitcoind
        .params
        .get_cookie_values()?
        .context("bitcoind cookie file missing user/password")?;
    let rpc_socket = bitcoind.params.rpc_socket;

    let grpc_port = free_port();
    let config = format!(
        r#"
[ldk]
network = "regtest"
storage_dir = "{ldk_dir}"
log_level = "debug"
listen_addr = "127.0.0.1"
listen_port = {ldk_port}
enable_tor = false

[ldk.chain_source]
kind = "bitcoind"
rpc_host = "{rpc_host}"
rpc_port = {rpc_port}
rpc_user = "{rpc_user}"
rpc_password = "{rpc_password}"

[apiserver]
hostname = "127.0.0.1"
grpc_port = {grpc_port}

[nats]
server_addr = "127.0.0.1:{nats_port}"

[collector]
controller_addr = "http://127.0.0.1:{controller_grpc_port}"
heartbeat_interval_secs = 2
storage_dir = "{storage}"
shutdown_timeout_secs = 30

[console]
listen_port = {console_port}
retention_secs = 30
"#,
        ldk_dir = ldk_dir.display(),
        ldk_port = free_port(),
        rpc_host = rpc_socket.ip(),
        rpc_port = rpc_socket.port(),
        rpc_user = cookie.user,
        rpc_password = cookie.password,
        storage = storage.display(),
        console_port = free_port(),
    );
    fs::write(root.join("collector_config.toml"), config + extra_toml)?;

    Ok(CollectorFixture { dir, grpc_port })
}

/// Poll the controller's Status RPC until the collector shows up online with
/// the expected eligible-peer and target counts.
async fn wait_for_collector_online(controller_grpc_port: u16) -> Result<()> {
    let endpoint = format!("http://127.0.0.1:{controller_grpc_port}");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last_state = String::from("controller not reachable yet");
    loop {
        if let Ok(channel) = observer_common::connect_channel(&endpoint).await {
            let mut client = ControllerServiceClient::new(channel);
            match client.status(StatusRequest {}).await {
                Ok(resp) => {
                    let status = resp.into_inner();
                    if let Some(hb) = status.statuses.first() {
                        let info = hb.info.as_ref().unwrap();
                        // Display semantics: negative seconds since receipt,
                        // and recent (the collector heartbeats every 2s).
                        assert!(
                            (-30..=0).contains(&hb.heartbeat_age_secs),
                            "unexpected heartbeat age: {}",
                            hb.heartbeat_age_secs
                        );
                        last_state = format!(
                            "online={} uuid={} eligible_peers={} target_count={} hb_age={}",
                            status.online_collector_count,
                            info.uuid,
                            info.eligible_peers,
                            info.target_count,
                            hb.heartbeat_age_secs,
                        );
                        if status.online_collector_count == 1
                            && info.uuid == TEST_UUID
                            && info.eligible_peers == FIXTURE_PEERS as u32
                            && info.target_count >= 1
                        {
                            println!("collector online: {last_state}");
                            return Ok(());
                        }
                    } else {
                        last_state = format!(
                            "controller up, no heartbeats yet (online={})",
                            status.online_collector_count
                        );
                    }
                }
                Err(e) => last_state = format!("status RPC failed: {e}"),
            }
        }
        assert!(
            Instant::now() < deadline,
            "collector never reported online+provisioned; last state: {last_state}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns bitcoind/nats-server/workspace binaries; run via `just test-integration`"]
async fn controller_collector_pair() -> Result<()> {
    let collector_bin = target_bin("gossip_collector");
    let controller_bin = target_bin("observer_controller");

    // Regtest bitcoind (kill-on-drop, cookie auth); mine past genesis so the
    // collector's chain sync and fee estimation have something to look at.
    // No node wallet involved anywhere: `wallet: None` plus a locally derived
    // mining address keeps this working across Core versions and with
    // wallet-less bitcoind builds (corepc's typed wallet RPCs would also lag
    // new Core releases).
    let mut conf = Conf::default();
    conf.wallet = None;
    let bitcoind = Node::with_conf(
        corepc_node::exe_path().map_err(|e| {
            anyhow::anyhow!("bitcoind not found (set BITCOIND_EXE or add to PATH): {e}")
        })?,
        &conf,
    )?;
    let mining_addr = regtest_mining_address();
    bitcoind.client.call::<serde_json::Value>(
        "generatetoaddress",
        &[101.into(), mining_addr.to_string().into()],
    )?;

    let nats_port = free_port();
    let temp_root = TempDir::new()?;
    let nats = spawn_nats(nats_port, temp_root.path())?;

    let controller_fixture = setup_controller_dir("")?;
    let collector_fixture =
        setup_collector_dir(&bitcoind, nats_port, controller_fixture.grpc_port, "")?;

    let mut controller = ChildGuard::spawn(
        "observer_controller",
        &controller_bin,
        controller_fixture.dir.path(),
        &[
            ("CONTROLLER_MODE", "local"),
            ("CONTROLLER_UUID", "integration-test-controller"),
        ],
    )?;
    let mut collector = ChildGuard::spawn(
        "gossip_collector",
        &collector_bin,
        collector_fixture.dir.path(),
        &[("COLLECTOR_MODE", "local"), ("COLLECTOR_UUID", TEST_UUID)],
    )?;

    let mut dump = DumpLogsOnPanic {
        guards: vec![&nats, &controller, &collector],
        armed: true,
    };

    // The whole conversation, in one assertion: the collector registered, the
    // controller looked up its community, delivered the eligible peers, pushed
    // the target count, and a heartbeat carried the resulting state back.
    wait_for_collector_online(controller_fixture.grpc_port).await?;

    // Hit the collector's own gRPC server directly; converting the response
    // exercises the (bytes) pubkey conversion end-to-end.
    let channel = observer_common::connect_channel(&format!(
        "http://127.0.0.1:{}",
        collector_fixture.grpc_port
    ))
    .await?;
    let mut collector_client = CollectorServiceClient::new(channel);
    let node_config: LdkNodeConfig = collector_client
        .get_node_config(NodeConfigRequest {})
        .await?
        .into_inner()
        .try_into()?;
    println!("collector node id: {}", node_config.node_id);

    dump.armed = false;
    drop(dump);

    // Graceful shutdown must exit 0 on both sides (pins the clean-shutdown
    // behavior from the zombie-collector incident).
    let collector_status = collector.signal_and_wait(Signal::SIGINT, Duration::from_secs(60));
    assert!(
        collector_status.success(),
        "collector shutdown was unclean: {collector_status:?}\n{}",
        collector.log_tail()
    );

    let controller_status = controller.signal_and_wait(Signal::SIGINT, Duration::from_secs(30));
    assert!(
        controller_status.success(),
        "controller shutdown was unclean: {controller_status:?}\n{}",
        controller.log_tail()
    );

    Ok(())
}

/// A non-log file seeded next to the logs; the janitor must never touch it.
struct SeededFile {
    path: PathBuf,
    content: Vec<u8>,
}

impl SeededFile {
    fn create(path: PathBuf, content: &str) -> Result<Self> {
        fs::write(&path, content)?;
        Ok(Self {
            path,
            content: content.as_bytes().to_vec(),
        })
    }

    fn assert_untouched(&self) {
        let on_disk = fs::read(&self.path)
            .unwrap_or_else(|e| panic!("janitor removed/broke {}: {e}", self.path.display()));
        assert_eq!(
            on_disk,
            self.content,
            "janitor modified {}",
            self.path.display()
        );
        for suffix in [".zst", ".zst.tmp"] {
            let mut twin = self.path.as_os_str().to_owned();
            twin.push(suffix);
            assert!(
                !Path::new(&twin).exists(),
                "janitor compressed a non-log file: {}{suffix}",
                self.path.display()
            );
        }
    }
}

/// Names in `dir` starting with `{prefix}.`, split into (plain, compressed).
fn split_log_files(dir: &Path, prefix: &str) -> (Vec<String>, Vec<String>) {
    let match_prefix = format!("{prefix}.");
    let (mut plain, mut zst) = (Vec::new(), Vec::new());
    for entry in fs::read_dir(dir).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        if !name.starts_with(&match_prefix) {
            continue;
        }
        if name.ends_with(".zst") {
            zst.push(name);
        } else if !name.ends_with(".zst.tmp") {
            plain.push(name);
        }
    }
    plain.sort();
    zst.sort();
    (plain, zst)
}

/// Poll until `condition` returns true, re-asserting on every iteration that
/// the seeded non-log files are untouched.
fn wait_for(
    what: &str,
    deadline: Duration,
    seeded: &[SeededFile],
    mut condition: impl FnMut() -> bool,
) {
    let end = Instant::now() + deadline;
    loop {
        for file in seeded {
            file.assert_untouched();
        }
        if condition() {
            return;
        }
        assert!(Instant::now() < end, "timed out waiting for: {what}");
        std::thread::sleep(Duration::from_secs(1));
    }
}

/// End-to-end log janitor behavior against the real daemons: minutely
/// rotation with compress-after 60s / retention 120s, so within ~4 minutes we
/// observe closed files being zstd-compressed, the oldest archive being
/// expired, and rotation continuing — while seeded non-log files sharing the
/// directories are never modified.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns bitcoind/nats-server/workspace binaries and runs ~4 minutes; run via `just integration-test`"]
async fn log_janitor_rotation() -> Result<()> {
    let collector_bin = target_bin("gossip_collector");
    let controller_bin = target_bin("observer_controller");

    let mut conf = Conf::default();
    conf.wallet = None;
    let bitcoind = Node::with_conf(
        corepc_node::exe_path().map_err(|e| {
            anyhow::anyhow!("bitcoind not found (set BITCOIND_EXE or add to PATH): {e}")
        })?,
        &conf,
    )?;
    let mining_addr = regtest_mining_address();
    bitcoind.client.call::<serde_json::Value>(
        "generatetoaddress",
        &[101.into(), mining_addr.to_string().into()],
    )?;

    let nats_port = free_port();
    let temp_root = TempDir::new()?;
    let nats = spawn_nats(nats_port, temp_root.path())?;

    let controller_fixture = setup_controller_dir(JANITOR_TEST_TOML)?;
    let collector_fixture = setup_collector_dir(
        &bitcoind,
        nats_port,
        controller_fixture.grpc_port,
        JANITOR_TEST_TOML,
    )?;

    let controller_storage = controller_fixture.dir.path().join("storage");
    let collector_storage = collector_fixture.dir.path().join("storage");
    let ldk_dir = collector_fixture.dir.path().join("ldk");

    // Non-log files living in the janitored directories, including names that
    // share the log prefixes but lack the strict `{prefix}.` dot. The real
    // mnemonic is tracked too: the node reads it, nothing may rewrite it.
    let seeded = vec![
        SeededFile::create(
            controller_storage.join("nodes_normalized_initial.csv"),
            "pubkey,cluster\nabc,1\n",
        )?,
        SeededFile::create(
            controller_storage.join("controller_graph-2020-01-01-00-00.zst"),
            "not really zstd, must not be expired or recompressed",
        )?,
        SeededFile::create(
            collector_storage.join("collector-backup.2020-01-01-00-00.zst"),
            "prefix without the dot, must be ignored",
        )?,
        SeededFile::create(ldk_dir.join("ldk_node_backup.sqlite"), "fake sqlite")?,
        SeededFile {
            path: ldk_dir.join("mnemonic.txt"),
            content: TEST_MNEMONIC.as_bytes().to_vec(),
        },
    ];
    // NOTE: the poison-pill case (a source plus a leftover `.zst.tmp` from a
    // compression killed mid-write) is deliberately *not* asserted here. This
    // test runs with a 120 s retention, so any seeded source is legitimately
    // expired before it can demonstrate "skipped, not retried". It is covered
    // precisely in `observer_common::logging`'s unit tests, which pin `now`.

    let mut controller = ChildGuard::spawn(
        "observer_controller",
        &controller_bin,
        controller_fixture.dir.path(),
        &[
            ("CONTROLLER_MODE", "local"),
            ("CONTROLLER_UUID", "integration-test-controller"),
        ],
    )?;
    let mut collector = ChildGuard::spawn(
        "gossip_collector",
        &collector_bin,
        collector_fixture.dir.path(),
        &[("COLLECTOR_MODE", "local"), ("COLLECTOR_UUID", TEST_UUID)],
    )?;

    let mut dump = DumpLogsOnPanic {
        guards: vec![&nats, &controller, &collector],
        armed: true,
    };

    // Both daemons fully up and logging (heartbeats every 2s keep the
    // collector's and controller's current log files busy).
    wait_for_collector_online(controller_fixture.grpc_port).await?;

    // Phase A: a closed minute gets compressed while the current minute's
    // file stays plain. Files roll every minute and compress 60s after their
    // minute starts, so the first archive lands ~2 minutes in.
    wait_for(
        "first compressed collector log + a live uncompressed one",
        Duration::from_secs(180),
        &seeded,
        || {
            let (plain, zst) = split_log_files(&collector_storage, "collector");
            !zst.is_empty() && !plain.is_empty()
        },
    );
    let first_zst = split_log_files(&collector_storage, "collector").1.remove(0);
    println!("first compressed collector log: {first_zst}");

    // The controller and the LDK node log their startup burst in their first
    // minute, so each must eventually produce an archive of its own.
    wait_for(
        "compressed controller + ldk_node logs",
        Duration::from_secs(180),
        &seeded,
        || {
            !split_log_files(&controller_storage, "controller").1.is_empty()
                && !split_log_files(&ldk_dir, "ldk_node").1.is_empty()
        },
    );

    // Phase B: retention expires the oldest archive (120s after the minute it
    // covers) while rotation keeps a live file going.
    wait_for(
        &format!("expiry of {first_zst}"),
        Duration::from_secs(200),
        &seeded,
        || !collector_storage.join(&first_zst).exists(),
    );
    let (plain, _) = split_log_files(&collector_storage, "collector");
    assert!(
        !plain.is_empty(),
        "rotation should still be producing a live collector log file"
    );

    // The node's own database next to the LDK logs must have been left alone.
    assert!(ldk_dir.join("ldk_node_data.sqlite").exists());
    assert!(!ldk_dir.join("ldk_node_data.sqlite.zst").exists());
    for file in &seeded {
        file.assert_untouched();
    }

    dump.armed = false;
    drop(dump);

    let collector_status = collector.signal_and_wait(Signal::SIGINT, Duration::from_secs(60));
    assert!(
        collector_status.success(),
        "collector shutdown was unclean: {collector_status:?}\n{}",
        collector.log_tail()
    );
    let controller_status = controller.signal_and_wait(Signal::SIGINT, Duration::from_secs(30));
    assert!(
        controller_status.success(),
        "controller shutdown was unclean: {controller_status:?}\n{}",
        controller.log_tail()
    );

    Ok(())
}
