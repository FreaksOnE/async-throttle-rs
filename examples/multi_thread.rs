//! Six workers on a multi-threaded runtime share one `TokenBucket` that only
//! allows two operations per 500ms, so tokens are handed out in pairs.
//!
//! Run with: `cargo run --example multi_thread`

use std::sync::Arc;
use std::time::Instant;

use async_throttle_rs::TokenBucket;
use tokio::time::Duration;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // At most 2 operations per 500ms, bursting up to 2.
    let bucket = Arc::new(TokenBucket::new(Duration::from_millis(500), 2));
    let start = Instant::now();

    let workers: Vec<_> = (0..6)
        .map(|id| {
            let bucket = Arc::clone(&bucket);
            tokio::spawn(async move {
                bucket.acquire().await;
                println!("[{:>7.1?}] worker {id} got a token", start.elapsed());
            })
        })
        .collect();

    let total = workers.len();
    for worker in workers {
        worker.await.unwrap();
    }

    println!("all {total} workers finished in {:.2?}", start.elapsed());
}
