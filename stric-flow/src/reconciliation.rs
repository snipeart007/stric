use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::Arc;
use rand::Rng;
use dashmap::DashMap;

pub type StateMergeFn = Arc<dyn Fn(&[u8], &[u8]) -> Result<Vec<u8>, String> + Send + Sync>;

pub struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    attempts: u32,
}

impl ExponentialBackoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempts: 0,
        }
    }

    pub fn next_backoff(&mut self) -> Duration {
        self.attempts += 1;
        
        // Calculate raw backoff (base * 2^(attempts-1))
        let factor = 2u32.saturating_pow(self.attempts.saturating_sub(1));
        let mut next = self.base.saturating_mul(factor);
        if next > self.max {
            next = self.max;
        }

        // Apply random jitter: ±10%
        let mut rng = rand::thread_rng();
        let jitter_percent: f64 = rng.gen_range(-0.1..0.1);
        let jitter = next.as_secs_f64() * jitter_percent;
        
        let final_secs = next.as_secs_f64() + jitter;
        let final_secs = if final_secs < 1.0 { 1.0 } else { final_secs };
        Duration::from_secs_f64(final_secs)
    }

    pub fn reset(&mut self) {
        self.attempts = 0;
    }
}

pub struct Session {
    pub session_id: String,
    pub creator_node: String,
    pub flow_ids: Vec<String>,
    pub created_at: u64,
    pub metadata: HashMap<String, String>,
    pub state_data: Vec<u8>,
    pub state_version: u64,
    pub state_timestamp: u64,
}

/// Dynamic garbage collection of inactive sessions.
/// Returns a list of evicted session IDs that should propagate to the rest of the mesh.
pub fn gc_inactive_sessions(
    sessions: &DashMap<String, Session>,
    node_last_seen: &DashMap<String, Instant>,
    session_ttl: Duration,
) -> Vec<String> {
    let mut evicted = Vec::new();
    let now = Instant::now();

    sessions.retain(|session_id, session| {
        if let Some(last_seen) = node_last_seen.get(&session.creator_node) {
            if now.duration_since(*last_seen) > session_ttl {
                evicted.push(session_id.clone());
                false // Evict
            } else {
                true
            }
        } else {
            // Creator node is unknown or hasn't been seen at all.
            // If they remain completely unknown, we eventually evict them.
            // For safety, we keep them unless we explicitly mark them offline.
            true
        }
    });

    evicted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff() {
        let mut eb = ExponentialBackoff::new(Duration::from_secs(1), Duration::from_secs(60));
        let b1 = eb.next_backoff();
        assert!(b1 >= Duration::from_millis(900) && b1 <= Duration::from_millis(1100));
        
        let b2 = eb.next_backoff();
        assert!(b2 >= Duration::from_millis(1800) && b2 <= Duration::from_millis(2200));

        eb.reset();
        let b3 = eb.next_backoff();
        assert!(b3 >= Duration::from_millis(900) && b3 <= Duration::from_millis(1100));
    }

    #[test]
    fn test_session_gc() {
        let sessions = DashMap::new();
        let node_last_seen = DashMap::new();

        sessions.insert(
            "sess1".to_string(),
            Session {
                session_id: "sess1".to_string(),
                creator_node: "node_a".to_string(),
                flow_ids: vec![],
                created_at: 0,
                metadata: HashMap::new(),
                state_data: vec![],
                state_version: 0,
                state_timestamp: 0,
            },
        );

        node_last_seen.insert("node_a".to_string(), Instant::now() - Duration::from_secs(400));

        let evicted = gc_inactive_sessions(&sessions, &node_last_seen, Duration::from_secs(300));
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0], "sess1");
        assert!(sessions.is_empty());
    }
}
