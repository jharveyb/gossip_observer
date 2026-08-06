use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic_reflection::server::{v1, v1alpha};

pub mod logging;
pub mod token_bucket;
pub mod types;

// Prost-generated code
#[path = "gen/collectorrpc.rs"]
pub mod collectorrpc;
#[path = "gen/common.rs"]
pub mod common;
#[path = "gen/controllerrpc.rs"]
pub mod controllerrpc;

// Protobuf to/from logic + wrapped clients
pub mod collector_client;
pub mod controller_client;
pub mod util;

/// File descriptor set for gRPC reflection
/// // Not sure how this works if we built multiple servers?
pub const SERVER_FD_SET: &[u8] = include_bytes!("gen/file_descriptor_set.bin");

// Bump tonic max message size from 4 MB. The default for outbound messages
// is already unbounded.
pub const MAX_RECV_MSG_SIZE: usize = 1024 * 1024 * 32;

// Default settings for our gRPC clients. REQUEST_TIMEOUT bounds every RPC —
// generous enough for open_channel, which includes LDK's 10s peer-connect.
// HTTP/2 keepalives detect dead peers (e.g. an expired Tailscale route)
// instead of hanging on a black-holed TCP connection.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
pub const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// Connect a tonic channel with the shared client settings applied.
pub async fn connect_channel(endpoint: &str) -> Result<Channel, tonic::transport::Error> {
    Endpoint::new(endpoint.to_string())?
        .timeout(REQUEST_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .tcp_nodelay(true)
        .connect()
        .await
}

/// v1 reflection; stabilized, newer version
pub fn reflection_service_v1()
-> anyhow::Result<v1::ServerReflectionServer<impl v1::ServerReflection>> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(SERVER_FD_SET)
        .build_v1()
        .map_err(anyhow::Error::new)
}

/// v1alpha reflection, for backwards compat.
pub fn reflection_service_v1alpha()
-> anyhow::Result<v1alpha::ServerReflectionServer<impl v1alpha::ServerReflection>> {
    tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(SERVER_FD_SET)
        .build_v1alpha()
        .map_err(anyhow::Error::new)
}
