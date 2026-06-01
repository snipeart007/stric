use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::sync::Arc;
use rand::Rng;
use dashmap::DashMap;
use tracing::info;

/// A closure type used to resolve and merge conflicting state updates for a session.
///
/// It accepts the current local state data and the incoming remote state data,
/// returning the successfully merged state data, or a string describing the merge error.
pub type StateMergeFn = Arc<dyn Fn(&[u8], &[u8]) -> Result<Vec<u8>, String> + Send + Sync>;

/// Utility to calculate exponential backoff intervals with random jitter.
pub struct ExponentialBackoff {
    base: Duration,
    max: Duration,
    attempts: u32,
}

impl ExponentialBackoff {
    /// Creates a new `ExponentialBackoff` configuration.
    ///
    /// # Arguments
    ///
    /// * `base` - The initial backoff duration.
    /// * `max` - The maximum cap on the backoff duration.
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            attempts: 0,
        }
    }

    /// Computes the next backoff duration.
    ///
    /// Each call increments the internal attempts counter and calculates the duration as:
    /// `base * 2^(attempts-1)` up to a limit of `max`. A random jitter of ±10% is applied
    /// to the final calculated duration.
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

    /// Resets the internal attempt count to zero, restarting the backoff sequence.
    pub fn reset(&mut self) {
        self.attempts = 0;
    }
}

/// Represents a synchronized logical application session inside the flow mesh.
pub struct Session {
    /// The unique identifier of the session.
    pub session_id: String,
    /// The identifier of the node that created this session.
    pub creator_node: String,
    /// The list of logical flows associated with this session.
    pub flow_ids: Vec<String>,
    /// The creation timestamp in milliseconds since Unix epoch.
    pub created_at: u64,
    /// Key-value metadata associated with the session.
    pub metadata: HashMap<String, String>,
    /// The synchronized application state payload.
    pub state_data: Vec<u8>,
    /// The version number of the current session state.
    pub state_version: u64,
    /// The timestamp in milliseconds of the last state update.
    pub state_timestamp: u64,
}

/// Performs dynamic garbage collection of inactive sessions.
///
/// Evicts sessions where the creator node has not been heard from for longer than `session_ttl`.
///
/// # Arguments
///
/// * `sessions` - The active sessions map.
/// * `node_last_seen` - Map of node identifiers to their last seen times.
/// * `session_ttl` - The maximum allowed duration of inactivity for a creator node before eviction.
///
/// # Returns
///
/// A vector of evicted session IDs that should be propagated to delete the sessions across the mesh.
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
                info!("Session {} evicted due to creator node {} TTL expiry", session_id, session.creator_node);
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
