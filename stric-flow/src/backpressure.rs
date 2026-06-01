use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use governor::{Quota, RateLimiter, clock::Clock};
use tokio::sync::RwLock;

type InnerLimiter = RateLimiter<governor::state::direct::NotKeyed, governor::state::InMemoryState, governor::clock::DefaultClock>;

/// Enforces backpressure rate-limiting and pausing/resuming logical streams.
#[derive(Clone)]
pub struct TokenBucketRateLimiter {
    limiter: Arc<RwLock<InnerLimiter>>,
    paused: Arc<RwLock<bool>>,
    max_rate: Arc<AtomicU32>,
    notify: Arc<tokio::sync::Notify>,
}

impl TokenBucketRateLimiter {
    /// Creates a new `TokenBucketRateLimiter` with the given `max_rate` limit in bytes per second.
    ///
    /// If `max_rate` is `0`, rate limiting is disabled, and byte consumption can proceed
    /// without delay (the inner rate limit is set to maximum capacity).
    pub fn new(max_rate: u32) -> Self {
        let rate = if max_rate == 0 { u32::MAX } else { max_rate };
        let quota = Quota::per_second(NonZeroU32::new(rate).unwrap_or(NonZeroU32::new(1).unwrap()));
        let limiter = Arc::new(RwLock::new(RateLimiter::direct(quota)));
        
        Self {
            limiter,
            paused: Arc::new(RwLock::new(false)),
            max_rate: Arc::new(AtomicU32::new(max_rate)),
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Updates the maximum rate limit in bytes per second.
    ///
    /// If `max_rate` is `0`, rate limiting is disabled. Any pending or new calls to
    /// `wait_for_bytes` will not experience rate-limiting delays.
    pub async fn set_rate(&self, max_rate: u32) {
        self.max_rate.store(max_rate, Ordering::SeqCst);
        let rate = if max_rate == 0 { u32::MAX } else { max_rate };
        let quota = Quota::per_second(NonZeroU32::new(rate).unwrap_or(NonZeroU32::new(1).unwrap()));
        let mut limiter = self.limiter.write().await;
        *limiter = RateLimiter::direct(quota);
    }

    /// Returns the current configured maximum rate limit in bytes per second.
    ///
    /// A return value of `0` indicates that rate limiting is disabled.
    pub async fn get_rate(&self) -> u32 {
        self.max_rate.load(Ordering::SeqCst)
    }

    /// Pauses the rate limiter.
    ///
    /// While paused, any calls to `wait_for_bytes` will block asynchronously until
    /// `resume` is called.
    pub async fn pause(&self) {
        let mut paused = self.paused.write().await;
        *paused = true;
    }

    /// Resumes the rate limiter, allowing any pending or new requests to proceed.
    ///
    /// Resuming notifies all waiting tasks that were blocked due to the pause state,
    /// allowing them to continue and acquire tokens.
    pub async fn resume(&self) {
        {
            let mut paused = self.paused.write().await;
            *paused = false;
        }
        self.notify.notify_waiters();
    }

    /// Checks if the rate limiter is currently paused.
    pub async fn is_paused(&self) -> bool {
        *self.paused.read().await
    }

    /// Asynchronously blocks until the specified number of bytes can be processed.
    ///
    /// If the rate limiter is paused, this method blocks until it is resumed.
    /// If the configured maximum rate is `0` (disabled) or `bytes` is `0`, the method
    /// returns immediately.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The number of bytes/tokens to acquire from the token bucket.
    pub async fn wait_for_bytes(&self, bytes: u32) {
        loop {
            if !self.is_paused().await {
                break;
            }
            self.notify.notified().await;
        }

        let max_rate = self.get_rate().await;
        if max_rate == 0 || bytes == 0 {
            return;
        }

        let nz_bytes = match NonZeroU32::new(bytes) {
            Some(val) => val,
            None => return,
        };

        loop {
            let res = {
                let limiter = self.limiter.read().await;
                limiter.check_n(nz_bytes)
            };

            match res {
                Ok(Ok(_)) => break,
                Ok(Err(not_until)) => {
                    let wait_duration = not_until.wait_time_from(governor::clock::DefaultClock::default().now());
                    tokio::time::sleep(wait_duration.into()).await;
                }
                Err(_) => {
                    // InsufficientCapacity: request exceeds maximum possible burst size,
                    // we let it proceed to avoid deadlocks.
                    break;
                }
            }
            
            while self.is_paused().await {
                self.notify.notified().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter_pause_resume() {
        let limiter = TokenBucketRateLimiter::new(1000);
        limiter.pause().await;
        assert!(limiter.is_paused().await);
        limiter.resume().await;
        assert!(!limiter.is_paused().await);
    }
}
