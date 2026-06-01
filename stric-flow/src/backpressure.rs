use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use governor::{Quota, RateLimiter, clock::Clock};
use tokio::sync::RwLock;

/// Enforces backpressure rate-limiting and pausing/resuming logical streams.
pub struct TokenBucketRateLimiter {
    limiter: Arc<RateLimiter<governor::state::direct::NotKeyed, governor::state::InMemoryState, governor::clock::DefaultClock>>,
    paused: Arc<RwLock<bool>>,
    max_rate: u32,
}

impl TokenBucketRateLimiter {
    pub fn new(max_rate: u32) -> Self {
        let rate = if max_rate == 0 { u32::MAX } else { max_rate };
        let quota = Quota::per_second(NonZeroU32::new(rate).unwrap_or(NonZeroU32::new(1).unwrap()));
        let limiter = Arc::new(RateLimiter::direct(quota));
        
        Self {
            limiter,
            paused: Arc::new(RwLock::new(false)),
            max_rate,
        }
    }

    pub async fn set_rate(&mut self, max_rate: u32) {
        self.max_rate = max_rate;
        let rate = if max_rate == 0 { u32::MAX } else { max_rate };
        let quota = Quota::per_second(NonZeroU32::new(rate).unwrap_or(NonZeroU32::new(1).unwrap()));
        self.limiter = Arc::new(RateLimiter::direct(quota));
    }

    pub async fn pause(&self) {
        let mut paused = self.paused.write().await;
        *paused = true;
    }

    pub async fn resume(&self) {
        let mut paused = self.paused.write().await;
        *paused = false;
    }

    pub async fn is_paused(&self) -> bool {
        *self.paused.read().await
    }

    pub async fn wait_for_bytes(&self, bytes: u32) {
        loop {
            if !self.is_paused().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if self.max_rate == 0 || bytes == 0 {
            return;
        }

        let nz_bytes = match NonZeroU32::new(bytes) {
            Some(val) => val,
            None => return,
        };

        loop {
            match self.limiter.check_n(nz_bytes) {
                Ok(Ok(_)) => break,
                Ok(Err(not_until)) => {
                    let wait_duration = not_until.wait_time_from(governor::clock::DefaultClock::default().now());
                    tokio::time::sleep(wait_duration.into()).await;
                }
                Err(_) => {
                    // InsufficientCapacity: request exceeds maximum possible burst size,
                    // we cannot rate limit it properly so we let it proceed to avoid deadlocks.
                    break;
                }
            }
            
            while self.is_paused().await {
                tokio::time::sleep(Duration::from_millis(100)).await;
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
