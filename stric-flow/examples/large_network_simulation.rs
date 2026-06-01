#[path = "common/mod.rs"]
mod common;

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use stric_core::NodeConfig;
use stric_flow::node::{FlowNode, FlowHandler, HopCountMetric};
use stric_flow::registry::MessageRegistry;
use stric_flow::proto;
use tokio::sync::mpsc;
use tracing::info;

fn make_node_config(port: u16, cert_der: &[u8], key_der: &[u8]) -> NodeConfig {
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.to_vec())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der.to_vec()).unwrap();
    
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der.to_vec())).unwrap();

    NodeConfig {
        certs: Some(certs),
        key: Some(key),
        socket_addr: SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port),
        alpn_protocol_names: vec![b"stric-flow".to_vec()],
        error_channel_len: 10,
        default_conn_context: stric_core::ConnectionContext {
            id: 0,
            keep_alive: true,
            initiator_uni: true,
            initiator_bi: true,
            responder_uni: true,
            responder_bi: true,
        },
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
        root_cert_store: Some(roots),
        danger_accept_invalid_certs: true,
    }
}

struct FinalHandler {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl FlowHandler for FinalHandler {
    async fn handle_message(
        &self,
        flow_id: &str,
        topic_id: &str,
        message: Box<dyn Any + Send + Sync>,
    ) -> Result<(), String> {
        if let Some(msg_str) = message.downcast_ref::<String>() {
            info!("Final destination handler got message: '{}' from flow '{}' on topic '{}'", msg_str, flow_id, topic_id);
            let _ = self.tx.send(msg_str.clone()).await;
            Ok(())
        } else {
            Err("Failed downcast".to_string())
        }
    }
}

#[tokio::main]
async fn main() {
    common::init_logging();
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    info!("Generating self-signed TLS certificates for nodes...");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let registry = Arc::new({
        let mut r = MessageRegistry::new();
        r.register::<String>("my_app.TextMessage", |bytes| {
            String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
        });
        r
    });

    let mut nodes = Vec::new();
    let mut addrs = Vec::new();

    let num_nodes = 40;
    info!("Creating {} stric-flow nodes in memory...", num_nodes);
    for i in 0..num_nodes {
        if i % 10 == 0 {
            info!("Initializing nodes {} to {}...", i, std::cmp::min(i + 9, num_nodes - 1));
        }
        let config = make_node_config(0, &cert_der, &key_der);
        let node_id = format!("node_{}", i);
        let (node, mut error_rx) = FlowNode::new(node_id, config, Arc::new(HopCountMetric), registry.clone()).unwrap();
        node.start().await;
        
        let addr = node.local_addr().unwrap();
        addrs.push(addr);
        nodes.push(node);

        tokio::spawn(async move { while let Some(_) = error_rx.recv().await {} });
    }

    // Connect them to form a complex mesh graph:
    // 1. Ring connections to guarantee complete network connectivity: node_i <-> node_i+1
    // 2. Chords (+5 and +13) to form a complex mesh network with multiple alternative routes
    info!("Connecting {} nodes to form a complex mesh graph...", num_nodes);
    let mut conn_count = 0;
    for i in 0..(num_nodes - 1) {
        nodes[i].connect(addrs[i + 1], "localhost").await.unwrap();
        conn_count += 1;
    }

    for i in 0..num_nodes {
        // Chord +5 for nodes divisible by 3
        if i % 3 == 0 {
            let target = (i + 5) % num_nodes;
            if target > i {
                nodes[i].connect(addrs[target], "localhost").await.unwrap();
                conn_count += 1;
            }
        }
        // Chord +13 for nodes divisible by 7
        if i % 7 == 0 {
            let target = (i + 13) % num_nodes;
            if target > i {
                nodes[i].connect(addrs[target], "localhost").await.unwrap();
                conn_count += 1;
            }
        }
    }
    info!("Established {} bidirectional links across {} nodes to form the complex mesh.", conn_count, num_nodes);

    // Wait for network handshake negotiation, graph updates, and Dijkstra tree stabilization
    info!("Waiting 8 seconds for network topology to stabilize...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Register a subscription at node_39 (the final node)
    let (tx, mut rx) = mpsc::channel(10);
    nodes[num_nodes - 1].subscribe("chain/data".to_string(), Arc::new(FinalHandler { tx }));

    // Wait for subscription filters to gossip upstream back to node_0
    info!("Gossiping subscription filters back to sender node_0...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    info!("Publishing message from node_0...");
    nodes[0].publish(
        "flow_large".to_string(),
        "chain/data".to_string(),
        "my_app.TextMessage".to_string(),
        proto::Codec::Raw,
        b"Hello from node_0 to node_39 across the mesh!".to_vec(),
    ).await.unwrap();

    // Wait and verify final delivery on node_39
    info!("Awaiting delivery at node_39...");
    let received = tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;
    
    match received {
        Ok(Some(msg)) => {
            info!("SUCCESS: Message successfully routed across the complex mesh: '{}'", msg);
            assert_eq!(msg, "Hello from node_0 to node_39 across the mesh!");
        }
        _ => {
            panic!("FAILED: Message routing timed out or failed!");
        }
    }
}
