use std::path::PathBuf;

use async_compression::tokio::write::ZstdEncoder;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// A request to write pre-serialized JSON bytes to a zstd-compressed file.
pub struct WriteRequest {
    pub path: PathBuf,
    pub json_bytes: Vec<u8>,
}

/// Serialize a value to JSON bytes. Runs serialization on a blocking thread
/// to avoid stalling the async runtime for large payloads.
pub async fn serialize_to_json<T: serde::Serialize + Send + 'static>(
    value: T,
) -> anyhow::Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || serde_json::to_vec(&value).map_err(anyhow::Error::from))
        .await?
}

/// The writer task. Receives WriteRequests and writes them as zstd-compressed files.
pub async fn json_writer_task(
    mut rx: mpsc::Receiver<WriteRequest>,
    compression_level: i32,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!("JSON writer: shutting down");
                break;
            }
            msg = rx.recv() => {
                match msg {
                    Some(req) => {
                        let path_display = req.path.display().to_string();
                        let input_len = req.json_bytes.len();
                        if let Err(e) = write_compressed(&req.path, &req.json_bytes, compression_level).await {
                            error!(
                                error = %e,
                                path = %path_display,
                                "JSON writer: failed to write file"
                            );
                        } else {
                            info!(
                                path = %path_display,
                                input_bytes = input_len,
                                "JSON writer: wrote compressed file"
                            );
                        }
                    }
                    None => {
                        info!("JSON writer: channel closed, shutting down");
                        break;
                    }
                }
            }
        }
    }
}

async fn write_compressed(
    path: &PathBuf,
    data: &[u8],
    compression_level: i32,
) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file = tokio::fs::File::create(path).await?;
    let buf_writer = tokio::io::BufWriter::new(file);
    let mut encoder = ZstdEncoder::with_quality(
        buf_writer,
        async_compression::Level::Precise(compression_level),
    );
    encoder.write_all(data).await?;
    encoder.shutdown().await?;
    Ok(())
}
