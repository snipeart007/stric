use std::net::SocketAddr;
use sha2::{Sha256, Digest};
use tracing::debug;

const K: usize = 20;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: String,
    pub addr: SocketAddr,
}

pub struct KBucket {
    pub nodes: Vec<NodeInfo>,
}

impl KBucket {
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }
}

pub struct RoutingTable {
    local_node_id: String,
    local_hash: [u8; 32],
    buckets: Vec<KBucket>,
}

impl RoutingTable {
    pub fn new(local_node_id: String) -> Self {
        let local_hash = sha256_hash(&local_node_id);
        let mut buckets = Vec::with_capacity(256);
        for _ in 0..256 {
            buckets.push(KBucket::new());
        }
        Self {
            local_node_id,
            local_hash,
            buckets,
        }
    }

    pub fn update(&mut self, node_id: String, addr: SocketAddr) {
        if node_id == self.local_node_id {
            return;
        }

        let hash = sha256_hash(&node_id);
        let dist = xor_distance(&self.local_hash, &hash);
        let bucket_idx = leading_zeros(&dist) as usize;
        
        // Ensure bucket_idx is within bounds
        let bucket_idx = if bucket_idx >= 256 { 255 } else { bucket_idx };
        
        let bucket = &mut self.buckets[bucket_idx];
        
        // Check if node is already present
        if let Some(pos) = bucket.nodes.iter().position(|n| n.node_id == node_id) {
            // Move to end (most recently seen)
            let node = bucket.nodes.remove(pos);
            bucket.nodes.push(node);
        } else if bucket.nodes.len() < K {
            // Add to end
            bucket.nodes.push(NodeInfo { node_id, addr });
        } else {
            // Bucket is full. In a full Kademlia node, we would ping the head node.
            // For now, we keep the existing nodes and drop the new one, or can be eviction-based.
            // We will log a warning or simply drop the update.
            debug!("KBucket full for node {}, dropping discovery update for {}", self.local_node_id, node_id);
        }
    }

    pub fn remove(&mut self, node_id: &str) {
        let hash = sha256_hash(node_id);
        let dist = xor_distance(&self.local_hash, &hash);
        let bucket_idx = leading_zeros(&dist) as usize;
        let bucket_idx = if bucket_idx >= 256 { 255 } else { bucket_idx };
        
        let bucket = &mut self.buckets[bucket_idx];
        bucket.nodes.retain(|n| n.node_id != node_id);
    }

    pub fn find_closest_nodes(&self, target_id: &str, count: usize) -> Vec<NodeInfo> {
        let target_hash = sha256_hash(target_id);
        let mut candidates = Vec::new();

        for bucket in &self.buckets {
            for node in &bucket.nodes {
                let node_hash = sha256_hash(&node.node_id);
                let dist = xor_distance(&target_hash, &node_hash);
                candidates.push((node.clone(), dist));
            }
        }

        // Sort by XOR distance
        candidates.sort_by(|a, b| a.1.cmp(&b.1));

        candidates.into_iter().take(count).map(|(node, _)| node).collect()
    }
}

// ─── Helpers ───

fn sha256_hash(id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(id.as_bytes());
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    hash
}

fn xor_distance(a: &[u8; 32], b: &[u8; 32]) -> [u8; 32] {
    let mut dist = [0u8; 32];
    for i in 0..32 {
        dist[i] = a[i] ^ b[i];
    }
    dist
}

fn leading_zeros(bytes: &[u8; 32]) -> u32 {
    let mut count = 0;
    for &b in bytes {
        if b == 0 {
            count += 8;
        } else {
            count += b.leading_zeros();
            break;
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xor_distance_and_leading_zeros() {
        let a = sha256_hash("node_a");
        let b = sha256_hash("node_b");
        
        let dist = xor_distance(&a, &b);
        let lz = leading_zeros(&dist);
        assert!(lz < 256);
    }

    #[test]
    fn test_routing_table() {
        let mut table = RoutingTable::new("local".to_string());
        let addr1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8002".parse().unwrap();

        table.update("peer1".to_string(), addr1);
        table.update("peer2".to_string(), addr2);

        let closest = table.find_closest_nodes("peer1", 1);
        assert_eq!(closest.len(), 1);
        assert_eq!(closest[0].node_id, "peer1");
    }
}
