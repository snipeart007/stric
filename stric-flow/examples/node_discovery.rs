#[path = "common/mod.rs"]
mod common;

use std::net::SocketAddr;
use stric_flow::discovery::RoutingTable;
use tracing::info;

fn main() {
    common::init_logging();
    info!("Initializing Kademlia Routing Table for local_node...");
    let mut table = RoutingTable::new("local_node".to_string());

    let addr_1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
    let addr_2: SocketAddr = "127.0.0.1:8002".parse().unwrap();
    let addr_3: SocketAddr = "127.0.0.1:8003".parse().unwrap();

    info!("Inserting nodes: peer_1, peer_2, peer_3...");
    table.update("peer_1".to_string(), addr_1);
    table.update("peer_2".to_string(), addr_2);
    table.update("peer_3".to_string(), addr_3);

    info!("Searching closest node to 'peer_1'...");
    let closest = table.find_closest_nodes("peer_1", 1);
    
    assert!(!closest.is_empty());
    info!("Closest discovered node ID: '{}', Address: '{}'", closest[0].node_id, closest[0].addr);
    
    assert_eq!(closest[0].node_id, "peer_1");
    assert_eq!(closest[0].addr, addr_1);

    info!("SUCCESS: Kademlia XOR routing table discovery behaves correctly!");
}
