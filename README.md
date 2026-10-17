# async-throttle-rs

Client-side rate limiting for async Rust (tokio).

`TokenBucket` is a small async token-bucket rate limiter: it allows a burst of up
to `capacity` operations, and each token is replenished exactly `duration` after
it was taken. When the bucket is empty, `acquire().await` suspends the task (it
never blocks the executor thread) until a token is returned.

## Usage

Wrap the bucket in an `Arc` and share it across tasks:

```rust
use std::sync::Arc;

use async_throttle_rs::TokenBucket;
use tokio::time::Duration;

#[tokio::main]
async fn main() {
    // At most 2 requests per 500ms, bursting up to 2.
    let bucket = Arc::new(TokenBucket::new(Duration::from_millis(500), 2));

    let mut tasks = Vec::new();
    for id in 0..5 {
        let bucket = Arc::clone(&bucket);
        tasks.push(tokio::spawn(async move {
            bucket.acquire().await;
            println!("request {id} sent");
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}
```

## Multi-threaded example

A complete example using a multi-threaded runtime lives in
[`examples/multi_thread.rs`](examples/multi_thread.rs). Run it with:

```sh
cargo run --example multi_thread
```

Six workers on four runtime threads race for a bucket that only allows two
operations per 500 ms, so the output shows requests going out in pairs roughly
every 500 ms:

```text
[115.3µs] worker 0 got a token
[118.0µs] worker 2 got a token
[501.8ms] worker 4 got a token
[502.0ms] worker 5 got a token
[   1.0s] worker 1 got a token
[   1.0s] worker 3 got a token
all 6 workers finished in 1.00s
```

## Semantics

- `TokenBucket::new(duration, capacity)` — a bucket holding `capacity` tokens,
  where every token is replenished `duration` after it is acquired.
- `acquire().await` — takes one token. Completes immediately while tokens are
  available; otherwise the task waits until the oldest token expires. Waiters
  are released in FIFO order.
- **Burst**: up to `capacity` calls can proceed back-to-back with no delay.
- **Sustained rate**: `capacity` operations per `duration`.
- Each token's timer starts when it is acquired, so permits acquired at
  different times are returned at different times.
- The returned future is cancellation-safe: dropping it while waiting (e.g. in
  `tokio::select!` or `tokio::time::timeout`) consumes no token.
- Dropping the `TokenBucket` aborts all outstanding replenishment timers.

## Testing

The test suite uses tokio's paused clock, so it runs instantly and is
deterministic:

```sh
cargo test
```

## License

MIT
