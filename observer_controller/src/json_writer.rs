use std::io::Write;
use std::path::PathBuf;

use observer_common::uploader::UploadRequest;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Buffer between `serde_json` and the zstd encoder.
const JSON_BUF_SIZE: usize = 256 * 1024;

/// Staging suffix for the in-progress write.
const TMP_SUFFIX: &str = ".tmp";

/// Type-erased `serde_json::to_writer`, so payloads of different types can share
/// one channel. It runs inside the writer's blocking task, which is what keeps
/// the JSON from ever being materialized as a `Vec<u8>`.
type SerializeFn = Box<dyn FnOnce(&mut dyn Write) -> serde_json::Result<()> + Send>;

/// A request to serialize a value to JSON and write it as a zstd-compressed file.
pub struct WriteRequest {
    pub path: PathBuf,
    /// Object key suffix to publish the finished file under, if it should be
    /// published at all. `None` means local-only.
    pub rel_key: Option<PathBuf>,
    serialize: SerializeFn,
}

impl WriteRequest {
    pub fn new<T: serde::Serialize + Send + 'static>(path: PathBuf, value: T) -> Self {
        Self {
            path,
            rel_key: None,
            serialize: Box::new(move |w| serde_json::to_writer(w, &value)),
        }
    }

    /// As [`Self::new`], but the finished file is also queued for upload.
    pub fn new_with_key<T: serde::Serialize + Send + 'static>(
        path: PathBuf,
        rel_key: PathBuf,
        value: T,
    ) -> Self {
        Self {
            path,
            rel_key: Some(rel_key),
            serialize: Box::new(move |w| serde_json::to_writer(w, &value)),
        }
    }
}

/// The writer task. Receives WriteRequests and writes them as zstd-compressed files.
/// Compression stays serial here -- one file at a time is what bounds this
/// task's memory. Uploads are handed to a separate pooled worker rather than
/// awaited inline, so a slow bucket can't stall the next compression (and,
/// through the write channel, the differ's cron tick).
pub async fn json_writer_task(
    mut rx: mpsc::Receiver<WriteRequest>,
    compression_level: i32,
    uploads: Option<mpsc::Sender<UploadRequest>>,
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
                        let published = req.path.clone();
                        let rel_key = req.rel_key.clone();
                        match write_compressed(req, compression_level).await {
                            Ok(input_bytes) => {
                                info!(
                                    path = %path_display,
                                    input_bytes,
                                    "JSON writer: wrote compressed file"
                                );
                                if let (Some(tx), Some(rel_key)) = (uploads.as_ref(), rel_key) {
                                    let req = UploadRequest::new(published, rel_key);
                                    // select! on the cancel token so a full
                                    // upload queue can't deadlock shutdown.
                                    tokio::select! {
                                        biased;
                                        _ = cancel.cancelled() => break,
                                        res = tx.send(req) => {
                                            if res.is_err() {
                                                warn!(
                                                    path = %path_display,
                                                    "JSON writer: upload queue closed; file stays local"
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    error = %e,
                                    path = %path_display,
                                    "JSON writer: failed to write file"
                                );
                            }
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

/// Serialize, compress and write, all inside **one** `spawn_blocking`. Returns
/// the uncompressed JSON byte count.
///
/// See `.claude/rules/rust.md` and `observer_common::logging::compress_file`
/// for more context.
async fn write_compressed(req: WriteRequest, compression_level: i32) -> anyhow::Result<u64> {
    tokio::task::spawn_blocking(move || write_compressed_blocking(req, compression_level)).await?
}

fn write_compressed_blocking(req: WriteRequest, compression_level: i32) -> anyhow::Result<u64> {
    // rel_key is the writer task's business, not the blocking writer's.
    let WriteRequest {
        path, serialize, ..
    } = req;
    // Stage as `{path}.tmp` and only rename after a full write + fsync, so a
    // crash or a SIGKILL mid-write can't leave a truncated `.zst` that readers
    // (e.g. `electrum-cli filter_graph`) will choke on.
    let mut tmp_os = path.as_os_str().to_owned();
    tmp_os.push(TMP_SUFFIX);
    let tmp_path = PathBuf::from(tmp_os);

    let write_staged = || -> anyhow::Result<u64> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = std::fs::File::create(&tmp_path)?;
        let encoder = zstd::stream::write::Encoder::new(tmp, compression_level)?;
        // The encoder buffers its own output, so it needs no `BufWriter` beneath
        // it; the one above it is for `serde_json`'s per-token writes.
        let mut writer =
            std::io::BufWriter::with_capacity(JSON_BUF_SIZE, CountingWriter::new(encoder));
        serialize(&mut writer)?;
        let counter = writer.into_inner().map_err(|e| e.into_error())?;
        let input_bytes = counter.written;
        let tmp = counter.inner.finish()?;
        tmp.sync_all()?;
        Ok(input_bytes)
    };

    match write_staged() {
        Ok(input_bytes) => {
            std::fs::rename(&tmp_path, &path)?;
            Ok(input_bytes)
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            Err(e.context(format!("Failed to write {}", path.display())))
        }
    }
}

/// Tallies bytes on their way into the encoder, so the log line can report the
/// uncompressed size without materializing the JSON to measure it.
struct CountingWriter<W> {
    inner: W,
    written: u64,
}

impl<W: Write> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, written: 0 }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    // Cheap level; these tests care about framing and staging, not ratio.
    const TEST_LEVEL: i32 = 1;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Payload {
        name: String,
        values: Vec<u32>,
    }

    fn payload() -> Payload {
        Payload {
            name: "controller_graph".to_string(),
            values: (0..10_000).collect(),
        }
    }

    fn tmp_path(path: &std::path::Path) -> PathBuf {
        let mut os = path.as_os_str().to_owned();
        os.push(TMP_SUFFIX);
        PathBuf::from(os)
    }

    #[tokio::test]
    async fn round_trips_through_zstd_and_leaves_no_tmp() {
        let dir = tempfile::tempdir().unwrap();
        // A missing subdirectory exercises the `create_dir_all` branch.
        let path = dir.path().join("nested").join("controller_graph-x.zst");
        let value = payload();

        let input_bytes = write_compressed(WriteRequest::new(path.clone(), payload()), TEST_LEVEL)
            .await
            .unwrap();

        let compressed = std::fs::read(&path).unwrap();
        let raw = zstd::stream::decode_all(compressed.as_slice()).unwrap();
        let decoded: Payload = serde_json::from_slice(&raw).unwrap();

        assert_eq!(decoded, value);
        // The reported count is the JSON size, not the compressed size.
        assert_eq!(input_bytes, raw.len() as u64);
        assert!(
            compressed.len() < raw.len(),
            "zstd should shrink this payload"
        );
        assert!(
            !tmp_path(&path).exists(),
            "staging file should be renamed away"
        );
    }

    #[tokio::test]
    async fn failed_write_leaves_no_partial_output() {
        let dir = tempfile::tempdir().unwrap();
        // `create_dir_all` fails here: a regular file occupies the parent path.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let path = blocker.join("controller_graph-x.zst");

        let err = write_compressed(WriteRequest::new(path.clone(), payload()), TEST_LEVEL)
            .await
            .expect_err("write into a non-directory should fail");

        assert!(err.to_string().contains("controller_graph-x.zst"));
        assert!(!path.exists());
        assert!(!tmp_path(&path).exists());
    }

    #[tokio::test]
    async fn writer_task_survives_a_failing_write() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let good = dir.path().join("good.zst");

        let (tx, rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(json_writer_task(rx, TEST_LEVEL, None, cancel.clone()));

        // The bad request must not take the task down with it.
        tx.send(WriteRequest::new(blocker.join("nope.zst"), payload()))
            .await
            .unwrap();
        tx.send(WriteRequest::new(good.clone(), payload()))
            .await
            .unwrap();
        drop(tx);

        task.await.unwrap();

        let raw = zstd::stream::decode_all(std::fs::read(&good).unwrap().as_slice()).unwrap();
        let decoded: Payload = serde_json::from_slice(&raw).unwrap();
        assert_eq!(decoded, payload());
        assert!(!tmp_path(&good).exists());
    }

    /// A keyed request queues an upload of the *finished* file, and an unkeyed
    /// one stays local. The uploader must never see a half-written path.
    #[tokio::test]
    async fn keyed_writes_are_queued_for_upload_and_unkeyed_ones_are_not() {
        let dir = tempfile::tempdir().unwrap();
        let keyed = dir.path().join("controller_graph-x.zst");
        let plain = dir.path().join("local_only.zst");

        let (tx, rx) = mpsc::channel(4);
        let (up_tx, mut up_rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(json_writer_task(rx, TEST_LEVEL, Some(up_tx), cancel.clone()));

        tx.send(WriteRequest::new_with_key(
            keyed.clone(),
            PathBuf::from("controller_graph/2026/03/30/controller_graph-x.zst"),
            payload(),
        ))
        .await
        .unwrap();
        tx.send(WriteRequest::new(plain.clone(), payload()))
            .await
            .unwrap();
        drop(tx);
        task.await.unwrap();

        let req = up_rx.recv().await.expect("keyed write should be queued");
        assert_eq!(req.local, keyed);
        assert_eq!(
            req.rel_key,
            PathBuf::from("controller_graph/2026/03/30/controller_graph-x.zst")
        );
        // The file it points at is complete and readable, not the .tmp stage.
        let raw = zstd::stream::decode_all(std::fs::read(&req.local).unwrap().as_slice()).unwrap();
        assert_eq!(serde_json::from_slice::<Payload>(&raw).unwrap(), payload());
        assert!(!tmp_path(&keyed).exists());

        assert!(
            up_rx.recv().await.is_none(),
            "unkeyed write must not be queued for upload"
        );
    }

    /// A failed write must not queue an upload of a file that isn't there.
    #[tokio::test]
    async fn a_failed_write_queues_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();

        let (tx, rx) = mpsc::channel(4);
        let (up_tx, mut up_rx) = mpsc::channel(4);
        let cancel = CancellationToken::new();
        let task = tokio::spawn(json_writer_task(rx, TEST_LEVEL, Some(up_tx), cancel.clone()));

        tx.send(WriteRequest::new_with_key(
            blocker.join("nope.zst"),
            PathBuf::from("controller_graph/nope.zst"),
            payload(),
        ))
        .await
        .unwrap();
        drop(tx);
        task.await.unwrap();

        assert!(up_rx.recv().await.is_none());
    }
}
