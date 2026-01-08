// Copied from the tokio semaphore docs:
// https://docs.rs/tokio/1.49.0/tokio/sync/struct.Semaphore.html#rate-limiting-using-a-token-bucket
// With a normal sephamore used alongside the bucket.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::{Duration, interval};

// Rate and concurrency are adjustable at runtime.
pub struct TokenBucket {
    bucket: Arc<Semaphore>,
    bucket_size: Arc<AtomicUsize>,
    classic_sem: Arc<Semaphore>,
    jh: tokio::task::JoinHandle<()>,
}

impl TokenBucket {
    pub fn new(duration: Duration, bucket_size: usize, max_concurrent: usize) -> Self {
        let bucket_size = Arc::new(AtomicUsize::new(bucket_size));
        let bucket = Arc::new(Semaphore::new(bucket_size.load(SeqCst)));

        let classic_sem = Arc::new(Semaphore::new(max_concurrent));

        // refills the tokens at the end of each interval
        let jh = tokio::spawn({
            let bucket_size = bucket_size.clone();
            let bucket = bucket.clone();

            let mut interval = interval(duration);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            async move {
                loop {
                    interval.tick().await;

                    // Use the latest capacity value.
                    if bucket.available_permits() < bucket_size.load(SeqCst) {
                        bucket.add_permits(1);
                    }
                }
            }
        });

        Self {
            bucket,
            bucket_size,
            classic_sem,
            jh,
        }
    }

    pub async fn acquire(&self) -> SemaphorePermit<'_> {
        // This can return an error if the semaphore is closed, but we
        // never close it, so this error can never happen.
        let token = self.bucket.acquire().await.unwrap();

        // A 'classic' semaphore permit, to limit max concurrency / outstanding tokens.
        let permit = self.classic_sem.acquire().await.unwrap();

        // To avoid releasing the permit back to the semaphore, we use
        // the `SemaphorePermit::forget` method.
        token.forget();

        permit
    }

    pub fn set_rate(&self, capacity: usize) {
        self.bucket_size.store(capacity, SeqCst);
    }

    pub fn increase_rate(&self) {
        self.bucket_size.fetch_add(1, SeqCst);
    }

    pub fn decrease_rate(&self) {
        self.bucket_size.fetch_sub(1, SeqCst);
    }

    pub fn rate(&self) -> usize {
        self.bucket_size.load(SeqCst)
    }

    pub fn increase_concurrency(&self) {
        self.classic_sem.add_permits(1);
    }

    pub fn decrease_concurrency(&self) {
        self.classic_sem.forget_permits(1);
    }
}

impl Drop for TokenBucket {
    fn drop(&mut self) {
        // Kill the background task so it stops taking up resources when we
        // don't need it anymore.
        self.jh.abort();
    }
}
