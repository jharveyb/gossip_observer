//! End-to-end control-plane test: spawns the real `observer_controller` and
//! `gossip_collector` binaries (plus regtest bitcoind and nats-server) with
//! generated configs, then verifies the full conversation:
//! register → eligible-peer delivery → target-count push → heartbeat,
//! followed by a clean SIGINT shutdown of both processes.
//!
//! Also covers the log janitor and artifact publishing to a real Garage S3
//! server.
//!
//! External requirements (see `just integration-test`):
//! - the two workspace binaries built in `target/debug/`
//! - `bitcoind` via `BITCOIND_EXE` or PATH
//! - `nats-server` via `NATS_SERVER_EXE` or PATH
//! - `garage` via `GARAGE_EXE` or PATH

use std::fs::{self, File};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bitcoin::secp256k1::{PublicKey, Secp256k1, SecretKey};
use chrono::{DateTime, Utc};
use corepc_node::{Conf, Node};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use observer_common::collectorrpc::NodeConfigRequest;
use observer_common::collectorrpc::collector_service_client::CollectorServiceClient;
use observer_common::controllerrpc::StatusRequest;
use observer_common::controllerrpc::controller_service_client::ControllerServiceClient;
use observer_common::types::LdkNodeConfig;
use observer_common::uploader::{ArtifactUploader, UploadBackend, UploadConfig};
use tempfile::TempDir;

const TEST_UUID: &str = "019c0000-0000-7000-8000-000000000001";
// Standard BIP-39 test vector mnemonic; regtest-only throwaway wallet.
const TEST_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
const COMMUNITY_ID: u32 = 1;
const FIXTURE_PEERS: usize = 3;

/// Bind-then-drop to pick a free localhost port. Racy in principle, fine for
/// a single sequential test.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// `N` *distinct* free localhost ports.
///
/// Not the same as calling `free_port` N times: that drops each listener
/// before binding the next, so the OS is free to hand out the same port twice.
/// Holding all N listeners open at once makes them distinct by construction.
/// Garage needs three (S3, RPC, admin) and a collision there is an unbootable
/// server with an error that points nowhere near the cause.
fn free_ports<const N: usize>() -> [u16; N] {
    let listeners: Vec<TcpListener> = (0..N)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let mut ports = [0u16; N];
    for (slot, listener) in ports.iter_mut().zip(&listeners) {
        *slot = listener.local_addr().unwrap().port();
    }
    ports
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
        "{} is not built; run `just integration-test` (or `cargo build -p {}` first)",
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
    fn spawn(name: &'static str, bin: &Path, cwd: &Path, envs: &[(&str, &str)]) -> Result<Self> {
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
        println!(
            "spawned {name} (pid {}), log: {}",
            child.id(),
            log_path.display()
        );
        Ok(Self {
            name,
            child,
            log_path,
        })
    }

    /// Send a signal, then wait (bounded) for the process to exit.
    fn signal_and_wait(&mut self, signal: Signal, timeout: Duration) -> ExitStatus {
        kill(Pid::from_raw(self.child.id() as i32), signal).unwrap();
        self.wait_for_exit(timeout, &format!("{signal}"))
    }

    /// Wait (bounded) for a process to exit *on its own*.
    ///
    /// Split out of `signal_and_wait` because that one signals first, and
    /// `kill` on an already-exited process fails with ESRCH and panics on the
    /// unwrap -- so there was no way to await a daemon expected to exit by
    /// itself, which is exactly what a fatal-startup-error test needs.
    fn wait_for_exit(&mut self, timeout: Duration, why: &str) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "{} did not exit within {timeout:?} of {why}\n{}",
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

/// Extra TOML for the artifact-upload test: dump graphs every 10 seconds into a
/// dedicated directory, and publish them through the real uploader against a
/// `local` object_store backend. That exercises the whole plumbing --- differ ->
/// writer -> upload queue -> upload_file -> size read-back -> local delete ---
/// with no extra external process.
///
/// Kept alongside `controller_publishes_graph_dumps_to_garage`, which covers
/// the same path over real HTTP. The two answer different questions and both
/// earn their place: this one is the fast plumbing check and needs no external
/// server, while the Garage one is the wire-protocol check --- `LocalFileSystem`
/// never opens a socket, which is exactly why the `allow_http` builder
/// regression got past this test and into production.
const UPLOAD_TEST_TOML: &str = r#"
[upload]
enabled = true
backend = "local"
key_prefix = "staging"
delete_local = true
local_root = "{bucket}"
"#;

/// Garage version this harness is written against. The CLI surface used below
/// (`-c`, `node id -q`, `layout assign/apply`, `key import`) was verified
/// against exactly this build; keep it in step with
/// `infra/ansible/inventory/group_vars/garage.yml`.
const GARAGE_EXPECTED_VERSION: &str = "v2.3.0";

/// Loopback-only test credentials. Deterministic constants rather than values
/// read back from the server: `garage key import` lets us *choose* the pair.
const GARAGE_KEY_ID: &str = "GK00112233445566778899aabb";
const GARAGE_SECRET: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
const GARAGE_RPC_SECRET: &str = "0011223344556677889900112233445566778899001122334455667788990011";
const GARAGE_BUCKET: &str = "test-graph-dumps";
const GARAGE_KEY_NAME: &str = "test-graph-dump-sync";

/// A running single-node Garage with one bucket and a read+write key on it.
struct GarageFixture {
    /// Declared before `dir` on purpose: struct fields drop in declaration
    /// order, so `ChildGuard::drop` reaps the server before the TempDir
    /// deletes the metadata and data directories out from under it.
    child: ChildGuard,
    dir: TempDir,
    endpoint: String,
}

impl GarageFixture {
    fn child(&self) -> &ChildGuard {
        &self.child
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// An `UploadConfig` pointing at this cluster, for driving
    /// `ArtifactUploader` directly as the test's own verifier.
    ///
    /// Short timeouts and no retries: every expected failure in these tests
    /// should be immediate.
    fn upload_config(&self, key_prefix: &str) -> UploadConfig {
        UploadConfig {
            enabled: true,
            backend: UploadBackend::S3,
            endpoint: self.endpoint.clone(),
            bucket: GARAGE_BUCKET.to_string(),
            region: "garage".to_string(),
            key_prefix: key_prefix.to_string(),
            access_key_id: Some(GARAGE_KEY_ID.to_string()),
            secret_access_key: Some(GARAGE_SECRET.to_string()),
            // The verifier only ever reads.
            delete_local: false,
            max_retries: 0,
            connect_timeout_secs: 5,
            request_timeout_secs: 15,
            ..UploadConfig::default()
        }
    }

    /// Keep the TempDir alive and readable for debugging; never dropped early.
    #[allow(dead_code)]
    fn dir(&self) -> &Path {
        self.dir.path()
    }
}

fn garage_exe() -> String {
    std::env::var("GARAGE_EXE").unwrap_or_else(|_| "garage".to_string())
}

/// Run one `garage` CLI command against the test cluster, returning stdout.
///
/// The config path goes through `GARAGE_CONFIG_FILE` rather than the `-c` flag:
/// both work in v2.3.0, but global flags must precede the subcommand, and the
/// env var keeps every call site immune to that ordering.
fn garage_cli(config: &Path, args: &[&str]) -> Result<String> {
    let exe = garage_exe();
    let out = Command::new(&exe)
        .env("GARAGE_CONFIG_FILE", config)
        .args(args)
        .output()
        .with_context(|| {
            format!(
                "running {exe} {args:?}; set GARAGE_EXE or add garage to PATH. Get the \
                 {GARAGE_EXPECTED_VERSION} static binary from \
                 https://garagehq.deuxfleurs.fr/_releases/{GARAGE_EXPECTED_VERSION}/x86_64-unknown-linux-musl/garage"
            )
        })?;
    if !out.status.success() {
        // Garage puts RPC/connection failures on stderr and command output on
        // stdout, so a useful diagnosis needs both.
        anyhow::bail!(
            "garage {args:?} failed ({}):\nstdout:\n{}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Poll a `garage` subcommand until it succeeds, or panic with the server log.
fn garage_wait_ok(config: &Path, args: &[&str], deadline: Duration, guard: &ChildGuard) {
    let end = Instant::now() + deadline;
    loop {
        let last = match garage_cli(config, args) {
            Ok(_) => return,
            Err(e) => format!("{e:#}"),
        };
        assert!(
            Instant::now() < end,
            "`garage {args:?}` never succeeded within {deadline:?}\nlast error: {last}\n{}",
            guard.log_tail()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Spawn a single-node Garage in a TempDir and bootstrap it to the point of
/// having `GARAGE_BUCKET` with `GARAGE_KEY_ID` granted read+write on it.
///
/// Hard-fails when the binary is missing, exactly like bitcoind and
/// nats-server. A silently skipped S3 test is how the `allow_http` regression
/// (see `observer_common::uploader`) reached production in the first place.
fn spawn_garage() -> Result<GarageFixture> {
    let dir = TempDir::new()?;
    let root = dir.path();
    fs::create_dir_all(root.join("meta"))?;
    fs::create_dir_all(root.join("data"))?;

    let [s3_port, rpc_port, admin_port] = free_ports::<3>();
    let config_path = root.join("garage.toml");

    // Mirrors infra/ansible/templates/garage.toml.j2, with three deliberate
    // test deltas: metadata_fsync off (the largest bootstrap cost with the
    // sqlite engine, and nothing here outlives the TempDir), no
    // metadata_auto_snapshot_interval, and a token capacity.
    //
    // replication_factor 1 means a quorum of 1 for both reads and writes, so
    // `consistent` costs nothing and gives the read-after-write that the
    // object-polling loop depends on. No root_domain, so path-style only --
    // matching production and `with_virtual_hosted_style_request(false)`.
    fs::write(
        &config_path,
        format!(
            r#"metadata_dir = "{meta}"
data_dir = "{data}"
db_engine = "sqlite"
metadata_fsync = false
replication_factor = 1
consistency_mode = "consistent"
rpc_bind_addr = "127.0.0.1:{rpc_port}"
rpc_public_addr = "127.0.0.1:{rpc_port}"
rpc_secret = "{GARAGE_RPC_SECRET}"
bootstrap_peers = []

[s3_api]
s3_region = "garage"
api_bind_addr = "127.0.0.1:{s3_port}"

[admin]
api_bind_addr = "127.0.0.1:{admin_port}"
admin_token = "integration-test-admin-token"
"#,
            meta = root.join("meta").display(),
            data = root.join("data").display(),
        ),
    )?;
    // Garage refuses to read a config holding secrets if it is group- or
    // world-readable (the alternative is --allow-world-readable-secrets).
    fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600))?;

    let exe = garage_exe();
    let log_path = root.join("garage.log");
    let stdout = File::create(&log_path)?;
    let stderr = stdout.try_clone()?;
    // Built literally rather than via ChildGuard::spawn, which takes no args
    // -- the same reason spawn_nats does it this way.
    let child = Command::new(&exe)
        .arg("server")
        .env("GARAGE_CONFIG_FILE", &config_path)
        .current_dir(root)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| {
            format!(
                "spawning {exe}; set GARAGE_EXE or add garage to PATH. Get the \
                 {GARAGE_EXPECTED_VERSION} static binary from \
                 https://garagehq.deuxfleurs.fr/_releases/{GARAGE_EXPECTED_VERSION}/x86_64-unknown-linux-musl/garage"
            )
        })?;
    println!(
        "spawned garage (pid {}), s3 port {s3_port}, log: {}",
        child.id(),
        log_path.display()
    );
    let guard = ChildGuard {
        name: "garage",
        child,
        log_path,
    };

    // Phase 1: the admin port accepts TCP. Slower than nats' 10s allowance
    // because sqlite metadata init happens first; Ansible allows 30s.
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", admin_port)).is_err() {
        assert!(
            Instant::now() < deadline,
            "garage admin port {admin_port} never opened\n{}",
            guard.log_tail()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Phase 2: the CLI can reach the daemon over RPC, which also proves
    // rpc_secret and rpc_public_addr are right. Needed before any of the
    // bootstrap commands below.
    garage_wait_ok(&config_path, &["status"], Duration::from_secs(20), &guard);

    // Bootstrap, from infra/ansible/tasks/garage_bootstrap.yml with the
    // idempotency machinery stripped -- this cluster is provably fresh, so
    // there is no existing layout version to read back and nothing to probe
    // for prior existence.
    //
    // Errors carry the server log on the error itself: a failure here happens
    // before any DumpLogsOnPanic exists, so otherwise garage.log is invisible.
    let bootstrap = || -> Result<()> {
        let node_raw = garage_cli(&config_path, &["node", "id", "-q"])?;
        let node_id = node_raw
            .trim()
            .split_once('@')
            .map(|(id, _)| id)
            .with_context(|| {
                format!("`garage node id -q` did not print <id>@<addr>: {node_raw:?}")
            })?;

        garage_cli(
            &config_path,
            &[
                "layout",
                "assign",
                node_id,
                "--zone",
                "dc1",
                "--capacity",
                "1G",
            ],
        )?;
        // Always 1: a fresh cluster starts at layout version 0.
        garage_cli(&config_path, &["layout", "apply", "--version", "1"])?;
        Ok(())
    };
    bootstrap().with_context(|| format!("garage bootstrap failed\n{}", guard.log_tail()))?;

    // `layout apply` returns before the new layout is necessarily live, and
    // bucket operations fail until it is. `bucket list` succeeding is the
    // precise signal that the next commands are safe -- note `status` is NOT,
    // it already returns 0 on a cluster with no layout at all.
    garage_wait_ok(
        &config_path,
        &["bucket", "list"],
        Duration::from_secs(20),
        &guard,
    );

    let provision = || -> Result<()> {
        garage_cli(&config_path, &["bucket", "create", GARAGE_BUCKET])?;
        garage_cli(
            &config_path,
            &[
                "key",
                "import",
                GARAGE_KEY_ID,
                GARAGE_SECRET,
                "--yes",
                "-n",
                GARAGE_KEY_NAME,
            ],
        )?;
        garage_cli(
            &config_path,
            &[
                "bucket",
                "allow",
                "--read",
                "--write",
                "--key",
                GARAGE_KEY_NAME,
                GARAGE_BUCKET,
            ],
        )?;
        Ok(())
    };
    provision().with_context(|| format!("garage provisioning failed\n{}", guard.log_tail()))?;

    Ok(GarageFixture {
        child: guard,
        dir,
        endpoint: format!("http://127.0.0.1:{s3_port}"),
    })
}

fn setup_controller_dir(extra_toml: &str) -> Result<ControllerFixture> {
    let dir = TempDir::new()?;
    let root = dir.path();
    let storage = root.join("storage");
    fs::create_dir_all(&storage)?;
    // Graph dumps go to their own directory, as in production, so the janitor
    // never even sees them.
    let graphs = root.join("graphs");
    fs::create_dir_all(&graphs)?;
    // Stands in for the bucket when the upload test selects the local backend.
    let bucket = root.join("bucket");
    fs::create_dir_all(&bucket)?;

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
graph_dir = "{graphs}"
total_connections = 3
heartbeat_expiry = 30
# Dump graphs every 10s so a test can observe one within its window. Harmless
# for the other tests: the dumps land in graph_dir, which nothing else asserts on.
graph_diff_cron = "*/10 * * * * *"

[console]
listen_port = {console_port}
retention_secs = 30

[network_info]
node_clusterings = "{nodes}"
node_communities = "{communities}"
collector_mapping = "{mapping}"
"#,
        storage = storage.display(),
        graphs = graphs.display(),
        console_port = free_port(),
        nodes = root.join("nodes.csv").display(),
        communities = root.join("communities.csv").display(),
        mapping = root.join("collector_mapping.toml").display(),
    );
    // `{bucket}` is the one value a caller's TOML can't hardcode: it is an
    // absolute path under a TempDir only known here. A no-op for TOML that
    // doesn't mention it.
    let extra_toml = extra_toml.replace("{bucket}", &bucket.display().to_string());
    fs::write(root.join("controller_config.toml"), config + &extra_toml)?;

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
#[ignore = "spawns bitcoind/nats-server/workspace binaries; run via `just integration-test`"]
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
            !split_log_files(&controller_storage, "controller")
                .1
                .is_empty()
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

/// End-to-end artifact publishing against the real controller binary: the
/// gossip differ writes a compressed graph dump, the writer hands the finished
/// file to the upload worker, and the uploader publishes it under the
/// configured key prefix and removes the local staging copy.
///
/// Runs the controller alone --- it needs neither bitcoind nor NATS --- and
/// uses the `local` object_store backend, so no extra process is involved. The
/// controller_graph dump is emitted every tick even with no collectors
/// registered, which is what keeps this fast.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns a workspace binary and runs ~1 minute; run via `just integration-test`"]
async fn artifact_upload_publishes_graph_dumps() -> Result<()> {
    let controller_bin = target_bin("observer_controller");

    let controller_fixture = setup_controller_dir(UPLOAD_TEST_TOML)?;
    let root = controller_fixture.dir.path().to_path_buf();
    let graphs = root.join("graphs");
    let bucket = root.join("bucket");

    let mut controller = ChildGuard::spawn(
        "observer_controller",
        &controller_bin,
        &root,
        &[
            ("CONTROLLER_MODE", "local"),
            ("CONTROLLER_UUID", "integration-test-controller"),
        ],
    )?;

    let mut dump = DumpLogsOnPanic {
        guards: vec![&controller],
        armed: true,
    };

    // `staging/` from UPLOAD_TEST_TOML, then the date-partitioned key the
    // controller builds: controller_graph/YYYY/MM/DD/controller_graph-*.zst.
    let published = bucket.join("staging").join("controller_graph");
    wait_for(
        "a controller graph dump to appear in the bucket",
        Duration::from_secs(90),
        &[],
        || find_zst(&published).is_some(),
    );

    let object = find_zst(&published).expect("just asserted present");
    let meta = fs::metadata(&object)?;
    assert!(meta.len() > 0, "published object must not be empty");

    // The key carries the local file name, so the staging copy it replaced is
    // identifiable --- and with delete_local it must be gone.
    let name = object
        .file_name()
        .and_then(|n| n.to_str())
        .context("published object has no file name")?;
    assert!(
        name.starts_with("controller_graph-") && name.ends_with(".zst"),
        "unexpected published object name: {name}"
    );
    assert!(
        !graphs.join(name).exists(),
        "delete_local is set, so the staging file {name} should be gone"
    );

    // Date partitioning: .../controller_graph/YYYY/MM/DD/<file>
    let day = object.parent().context("no day component")?;
    let month = day.parent().context("no month component")?;
    let year = month.parent().context("no year component")?;
    for (component, width) in [(day, 2), (month, 2), (year, 4)] {
        let part = component
            .file_name()
            .and_then(|n| n.to_str())
            .context("non-UTF-8 key component")?;
        assert_eq!(part.len(), width, "unexpected key component: {part}");
        assert!(
            part.chars().all(|c| c.is_ascii_digit()),
            "not a date: {part}"
        );
    }

    dump.armed = false;
    drop(dump);

    let status = controller.signal_and_wait(Signal::SIGINT, Duration::from_secs(30));
    assert!(
        status.success(),
        "controller exited with {status}\n{}",
        controller.log_tail()
    );
    Ok(())
}

/// First `.zst` anywhere under `dir`, or None if the tree does not exist yet.
fn find_zst(dir: &Path) -> Option<PathBuf> {
    let entries = fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_zst(&path) {
                return Some(found);
            }
        } else if path.extension().is_some_and(|e| e == "zst") {
            return Some(path);
        }
    }
    None
}

/// Failure modes that only a real S3 server produces: a rejected signature and
/// a missing bucket.
///
/// Driven through `ArtifactUploader` directly rather than a spawned
/// controller. Both are properties of the uploader/Garage pair, not of
/// `main.rs`; with `required_on_startup = false` a controller would only log an
/// `error!` and keep running, so the assertion would degrade to log-scraping.
///
/// All three cases share one Garage: sharing *across* test functions would need
/// a static with no deterministic teardown, and at `--test-threads 1` a
/// per-function instance costs only its few seconds of startup.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns a real garage server (~1s); run via `just integration-test`"]
async fn garage_rejects_bad_credentials_and_missing_buckets() -> Result<()> {
    let garage = spawn_garage()?;
    let mut dump = DumpLogsOnPanic {
        guards: vec![garage.child()],
        armed: true,
    };

    // Positive control first via bucket probe.
    let good = ArtifactUploader::new(&garage.upload_config("staging"))?
        .expect("credentials are set, so an uploader is built");
    good.probe()
        .await
        .context("positive control failed: the bootstrapped bucket should be reachable")?;

    // Right key id, wrong secret -> SigV4 rejection.
    let mut bad_secret = garage.upload_config("staging");
    bad_secret.secret_access_key = Some("f".repeat(64));
    let err = format!(
        "{:#}",
        ArtifactUploader::new(&bad_secret)?
            .expect("credentials are set")
            .probe()
            .await
            .expect_err("a wrong secret must be rejected")
    );
    // An allow_http regression must not be able to masquerade as an auth
    // failure: that would make this test pass for the wrong reason.
    assert!(
        !err.to_lowercase().contains("scheme"),
        "plain HTTP was rejected before connecting: {err}"
    );
    // Garage v2.3.0 returns `403 Forbidden` with `<Code>AccessDenied</Code>`
    // and `Forbidden: Invalid signature`. Asserting the signature message
    // rather than just the status keeps this from passing on any old 403.
    assert!(
        err.contains("403") && err.contains("Invalid signature"),
        "expected a rejected SigV4 signature, got: {err}"
    );

    // Good credentials, a bucket that was never created.
    let mut missing_bucket = garage.upload_config("staging");
    missing_bucket.bucket = "bucket-that-was-never-created".to_string();
    let err = format!(
        "{:#}",
        ArtifactUploader::new(&missing_bucket)?
            .expect("credentials are set")
            .probe()
            .await
            .expect_err("a missing bucket must be rejected")
    );
    assert!(
        !err.to_lowercase().contains("scheme"),
        "plain HTTP was rejected before connecting: {err}"
    );
    // `404 Not Found` with `<Code>NoSuchBucket</Code>`.
    assert!(
        err.contains("404") && err.contains("NoSuchBucket"),
        "expected a missing-bucket error, got: {err}"
    );

    dump.armed = false;
    drop(dump);
    Ok(())
}

/// The (file_name, rel_key) pair the controller would write at `t`.
///
/// Mirrors `GraphArtifact::file_name` / `rel_key` in
/// `observer_controller::collector_manager` -- keep the two in sync. Computing
/// the key rather than listing the bucket is deliberate: it makes this the
/// first *end-to-end* pin of that layout, which is otherwise only unit-tested
/// inside the controller crate.
///
/// Returns the current and previous minute. Filenames have minute granularity
/// while the cron fires every 10s, so a dump written just before a rollover is
/// still filed under the previous minute.
fn controller_graph_candidates(now: DateTime<Utc>) -> Vec<(String, PathBuf)> {
    [now, now - chrono::Duration::minutes(1)]
        .iter()
        .map(|t| {
            let file = t.format("controller_graph-%Y-%m-%d-%H-%M.zst").to_string();
            let rel = PathBuf::from("controller_graph")
                .join(t.format("%Y").to_string())
                .join(t.format("%m").to_string())
                .join(t.format("%d").to_string())
                .join(&file);
            (file, rel)
        })
        .collect()
}

/// The real controller publishing a graph dump to a real Garage over plain
/// HTTP: SigV4 signing accepted by an actual server, path-style addressing
/// resolving, and the post-upload size read-back gating the local delete.
///
/// This is the test the `local` backend could never be: `LocalFileSystem` never
/// opens a socket, which is why the `with_client_options`/`allow_http` builder
/// regression reached production. `required_on_startup = true` makes that
/// regression fail here in about a second at controller startup, instead of as
/// a silent two-minute timeout on a missing object.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns a real garage server + the controller binary, ~9s; run via `just integration-test`"]
async fn controller_publishes_graph_dumps_to_garage() -> Result<()> {
    let controller_bin = target_bin("observer_controller");
    let garage = spawn_garage()?;

    let upload_toml = format!(
        r#"
[upload]
enabled = true
backend = "s3"
endpoint = "{endpoint}"
bucket = "{bucket}"
region = "garage"
key_prefix = "staging"
access_key_id = "{key_id}"
secret_access_key = "{secret}"
delete_local = true
required_on_startup = true
max_retries = 2
connect_timeout_secs = 5
request_timeout_secs = 30
"#,
        endpoint = garage.endpoint(),
        bucket = GARAGE_BUCKET,
        key_id = GARAGE_KEY_ID,
        secret = GARAGE_SECRET,
    );
    let controller_fixture = setup_controller_dir(&upload_toml)?;
    let root = controller_fixture.dir.path().to_path_buf();
    let graphs = root.join("graphs");

    let mut controller = ChildGuard::spawn(
        "observer_controller",
        &controller_bin,
        &root,
        &[
            ("CONTROLLER_MODE", "local"),
            ("CONTROLLER_UUID", "integration-test-controller"),
        ],
    )?;
    let mut dump = DumpLogsOnPanic {
        guards: vec![garage.child(), &controller],
        armed: true,
    };

    // The test's own verifier is a second, independent real-HTTP SigV4 round
    // trip, so an allow_http regression fails here as well as at controller
    // startup.
    let verifier = ArtifactUploader::new(&garage.upload_config("staging"))?
        .expect("credentials are set, so an uploader is built");
    verifier
        .probe()
        .await
        .context("verifier could not reach the bucket")?;

    // Async loop rather than `wait_for`: that helper is synchronous, and
    // block_on-ing `head_size` from a #[tokio::test] worker thread panics.
    let deadline = Instant::now() + Duration::from_secs(120);
    let (file_name, bytes) = loop {
        let mut hit = None;
        for (name, rel) in controller_graph_candidates(Utc::now()) {
            let key = verifier.key_for(&rel)?;
            if let Some(size) = verifier.head_size(&key).await? {
                hit = Some((name, size));
                break;
            }
        }
        if let Some(hit) = hit {
            break hit;
        }
        assert!(
            Instant::now() < deadline,
            "no controller_graph object appeared in Garage bucket {GARAGE_BUCKET}\n{}\n{}",
            controller.log_tail(),
            garage.child().log_tail()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    assert!(bytes > 0, "published object {file_name} is empty");
    // With delete_local, the staging file being gone is transitively an
    // assertion that the remote-size read-back matched: nothing is deleted
    // without a verified UploadReceipt.
    assert!(
        !graphs.join(&file_name).exists(),
        "delete_local is set, so the staging file {file_name} should be gone"
    );
    // No separate layout assertion is needed: the key was *constructed* from
    // the expected layout, so finding the object is the layout assertion.

    // Released before shutdown: the guard borrows `controller` immutably and
    // `signal_and_wait` needs it mutably. Same ordering as the other tests --
    // the shutdown assert carries its own log_tail.
    dump.armed = false;
    drop(dump);

    let status = controller.signal_and_wait(Signal::SIGINT, Duration::from_secs(30));
    assert!(
        status.success(),
        "controller exited with {status}\n{}",
        controller.log_tail()
    );
    Ok(())
}

/// `upload.required_on_startup` must actually stop the daemon.
///
/// Goes through a spawned controller because "exits non-zero" is a property of
/// `main`, not of the uploader: `main` returns `anyhow::Result<()>`, so the
/// bail on a failed required probe becomes exit code 1 and the error text
/// reaches stderr, which ChildGuard captures.
///
/// Needs no Garage at all -- the endpoint points at a port nothing listens on.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "spawns the controller binary, <1s; run via `just integration-test`"]
async fn controller_fails_startup_when_the_required_bucket_is_unreachable() -> Result<()> {
    let controller_bin = target_bin("observer_controller");
    // Bound and dropped, so nothing is listening.
    let dead_port = free_port();

    // Credentials must be present-but-useless: with them absent,
    // `ArtifactUploader::new` bails on the credentials branch instead of the
    // probe, which is a different code path from the one under test.
    let upload_toml = format!(
        r#"
[upload]
enabled = true
backend = "s3"
endpoint = "http://127.0.0.1:{dead_port}"
bucket = "{GARAGE_BUCKET}"
region = "garage"
access_key_id = "{GARAGE_KEY_ID}"
secret_access_key = "{GARAGE_SECRET}"
required_on_startup = true
max_retries = 0
connect_timeout_secs = 2
request_timeout_secs = 5
"#
    );
    let controller_fixture = setup_controller_dir(&upload_toml)?;
    let mut controller = ChildGuard::spawn(
        "observer_controller",
        &controller_bin,
        controller_fixture.dir.path(),
        &[
            ("CONTROLLER_MODE", "local"),
            ("CONTROLLER_UUID", "integration-test-controller"),
        ],
    )?;

    let status = controller.wait_for_exit(
        Duration::from_secs(60),
        "an unreachable required upload bucket",
    );
    assert!(
        !status.success(),
        "controller should refuse to start when a required bucket is unreachable, got {status}\n{}",
        controller.log_tail()
    );
    assert!(
        controller.log_tail().contains("required_on_startup"),
        "expected the failure to name required_on_startup\n{}",
        controller.log_tail()
    );
    Ok(())
}
