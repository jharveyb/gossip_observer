use std::pin::Pin;

use anyhow::anyhow;
use tokio::io::copy_bidirectional;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio_io_timeout::TimeoutStream;
use tokio_socks::tcp::Socks5Stream;

// Apply an idle timeout to some 'stream', like a TCP or UDP connection.
pub fn wrap_stream_with_timeout<S>(
    unused_stream: S,
    idle_timeout: Duration,
) -> Pin<Box<TimeoutStream<S>>>
where
    S: AsyncReadExt + AsyncWriteExt,
{
    let mut stream = TimeoutStream::new(unused_stream);
    stream.set_read_timeout(Some(idle_timeout));
    stream.set_write_timeout(Some(idle_timeout));
    Box::pin(stream)
}

// Connect two streams to each other; They should already have any internal
// timeouts configured.
pub async fn connect_streams<S1, S2>(
    mut local_stream: Pin<Box<S1>>,
    mut proxy_stream: Pin<Box<S2>>,
) -> anyhow::Result<()>
where
    S1: AsyncReadExt + AsyncWriteExt,
    S2: AsyncReadExt + AsyncWriteExt,
{
    match copy_bidirectional(&mut local_stream, &mut proxy_stream).await {
        Ok((up, down)) => {
            println!("Closed gracefully. Up: {}, Down: {}", up, down);
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
            println!("Connection closed due to inactivity / wrapper timeout.");
            Ok(())
        }
        Err(e) => {
            anyhow::bail!("Connection broke: {}", e);
        }
    }
}

pub async fn spawn_forwarder(
    local_listen_addr: &str,
    local_proxy_addr: &str,
    target_onion: &str,
    target_port: u16,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(local_listen_addr).await?;
    println!(
        "Listening on {} -> forwarding to {}:{}",
        listener.local_addr()?,
        target_onion,
        target_port
    );

    // 5 minutes is much longer than our application-level keepalive interval.
    // TODO: Support cancellation via cancel token?
    let idle_time = Duration::from_secs(5 * 60);
    let mut proxy_tasks = JoinSet::new();

    while let Ok((local_stream, _)) = listener.accept().await {
        let proxy_stream = Socks5Stream::connect(local_proxy_addr, (target_onion, target_port))
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to connect via Tor SOCKS proxy: {}, {}: {}",
                    local_proxy_addr,
                    target_onion,
                    e
                )
            })?;

        let local_stream = wrap_stream_with_timeout(local_stream, idle_time);
        let proxy_stream = wrap_stream_with_timeout(proxy_stream, idle_time);
        proxy_tasks.spawn(connect_streams(local_stream, proxy_stream));
    }

    Ok(())
}
