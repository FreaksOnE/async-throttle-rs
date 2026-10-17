use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::Duration;

pub struct TokenBucket {
    sem: Arc<Semaphore>,
    duration: Duration,
    /// Tasks that return acquired permits once they expire, so they can be
    /// aborted when the bucket is dropped.
    expiry_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl TokenBucket {
    pub fn new(duration: Duration, capacity: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(capacity)),
            duration,
            expiry_tasks: Mutex::new(Vec::new()),
        }
    }

    pub async fn acquire(&self) {
        let permit = self.sem.acquire().await.unwrap();
        // Consume the permit permanently; the expiry task below returns it.
        permit.forget();

        // The permit is returned exactly `duration` after it was acquired.
        // The deadline is computed eagerly because `sleep` only registers its
        // deadline on first poll, which may happen after time has moved on.
        let deadline = tokio::time::Instant::now() + self.duration;
        let sem = Arc::clone(&self.sem);
        let handle = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            sem.add_permits(1);
        });

        let mut tasks = self.expiry_tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }
}

impl Drop for TokenBucket {
    fn drop(&mut self) {
        for task in self.expiry_tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn token_bucket_is_shareable_across_threads() {
        // Compile-time check: `TokenBucket` can be moved to and shared between
        // threads, e.g. wrapped in an `Arc` and cloned into spawned tasks.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TokenBucket>();
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_consumes_a_token() {
        let bucket = TokenBucket::new(ms(100), 2);

        bucket.acquire().await;
        assert_eq!(bucket.sem.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_burst_up_to_capacity_is_allowed_without_delay() {
        let bucket = TokenBucket::new(ms(100), 5);

        let start = tokio::time::Instant::now();
        for _ in 0..5 {
            bucket.acquire().await;
        }
        // No simulated time had to pass: the initial capacity is available
        // immediately.
        assert_eq!(start, tokio::time::Instant::now());
    }

    #[tokio::test(start_paused = true)]
    async fn each_permit_is_freed_exactly_one_duration_after_it_was_acquired() {
        let bucket = TokenBucket::new(ms(1_000), 2);

        // First permit acquired at t = 0ms.
        bucket.acquire().await;
        assert_eq!(bucket.sem.available_permits(), 1);

        // Second permit acquired at t = 500ms.
        tokio::time::advance(ms(500)).await;
        bucket.acquire().await;
        assert_eq!(bucket.sem.available_permits(), 0);

        // At t = 900ms nothing has been freed yet.
        tokio::time::advance(ms(400)).await;
        tokio::task::yield_now().await;
        assert_eq!(bucket.sem.available_permits(), 0);

        // At t = 1000ms the first permit is freed.
        tokio::time::advance(ms(100)).await;
        tokio::task::yield_now().await;
        assert_eq!(bucket.sem.available_permits(), 1);

        // At t = 1500ms the second permit is freed too.
        tokio::time::advance(ms(500)).await;
        tokio::task::yield_now().await;
        assert_eq!(bucket.sem.available_permits(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_beyond_capacity_waits_until_a_permit_is_freed() {
        let bucket = Arc::new(TokenBucket::new(ms(100), 1));

        // Drain the only token.
        bucket.acquire().await;

        let waiter = tokio::spawn({
            let bucket = Arc::clone(&bucket);
            async move { bucket.acquire().await }
        });

        // Just before the permit is freed the request is still waiting.
        tokio::time::advance(ms(98)).await;
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        // At t = 100ms the permit is freed and the request goes through.
        tokio::time::advance(ms(2)).await;
        waiter.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn permits_acquired_together_are_freed_together() {
        let bucket = TokenBucket::new(ms(100), 3);

        for _ in 0..3 {
            bucket.acquire().await;
        }
        assert_eq!(bucket.sem.available_permits(), 0);

        tokio::time::advance(ms(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(bucket.sem.available_permits(), 0);

        // All three permits expire at the same instant.
        tokio::time::advance(ms(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(bucket.sem.available_permits(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn all_waiting_requests_are_released_when_their_permits_expire() {
        let bucket = Arc::new(TokenBucket::new(ms(100), 2));

        for _ in 0..2 {
            bucket.acquire().await;
        }

        let mut waiters: Vec<_> = (0..2)
            .map(|_| {
                let bucket = Arc::clone(&bucket);
                tokio::spawn(async move { bucket.acquire().await })
            })
            .collect();

        tokio::time::advance(ms(99)).await;
        tokio::task::yield_now().await;
        assert!(waiters.iter().all(|waiter| !waiter.is_finished()));

        // Both permits expire at the same instant, so both waiting requests
        // are let through together.
        tokio::time::advance(ms(1)).await;
        for waiter in &mut waiters {
            waiter.await.unwrap();
        }
        assert_eq!(bucket.sem.available_permits(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_bucket_stops_returning_permits() {
        let bucket = TokenBucket::new(ms(100), 2);

        let sem = Arc::clone(&bucket.sem);
        bucket.acquire().await;
        assert_eq!(sem.available_permits(), 1);

        drop(bucket);
        // Even after many intervals no permits are returned: the expiry tasks
        // were aborted.
        tokio::time::advance(ms(1_000)).await;
        tokio::task::yield_now().await;
        assert_eq!(sem.available_permits(), 1);
    }
}
