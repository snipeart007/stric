#[path = "common/mod.rs"]
mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use stric_core::NodeConfig;
use stric_flow::node::{FlowNode, HopCountMetric};
use stric_flow::registry::MessageRegistry;
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
        error_channel_len: 100,
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

#[tokio::main]
async fn main() {
    common::init_logging();
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let registry = Arc::new(MessageRegistry::new());

    info!("Starting node_a...");
    let config_a = make_node_config(0, &cert_der, &key_der);
    let (node_a, mut error_rx_a) = FlowNode::new("node_a".to_string(), config_a, Arc::new(HopCountMetric), registry.clone()).unwrap();
    node_a.start().await;

    info!("Starting node_b...");
    let config_b = make_node_config(0, &cert_der, &key_der);
    let (node_b, mut error_rx_b) = FlowNode::new("node_b".to_string(), config_b, Arc::new(HopCountMetric), registry.clone()).unwrap();
    node_b.start().await;
    let addr_b = node_b.local_addr().unwrap();

    tokio::spawn(async move { while let Some(_) = error_rx_a.recv().await {} });
    tokio::spawn(async move { while let Some(_) = error_rx_b.recv().await {} });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect node_a to node_b
    node_a.connect(addr_b, "localhost").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 1. Create a session on node_a
    info!("Creating shared session 'sess_lww' on node_a...");
    node_a.create_session("sess_lww".to_string(), vec!["flow1".to_string()], HashMap::new()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify session propagated to node_b
    assert!(node_b.sessions().contains(&"sess_lww".to_string()));

    // 2. Sync state from node_a (Initial state)
    info!("Syncing initial state from node_a: 'initial_value'...");
    node_a.sync_session_state("sess_lww".to_string(), b"initial_value".to_vec()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Check node_b received state
    let state_b = node_b.get_session_state("sess_lww").unwrap();
    assert_eq!(state_b, b"initial_value".to_vec());
    info!("node_b has state: '{}'", String::from_utf8_lossy(&state_b));

    // 3. Sync newer state from node_b (LWW resolution)
    info!("Syncing newer state from node_b: 'updated_value'...");
    node_b.sync_session_state("sess_lww".to_string(), b"updated_value".to_vec()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify state updated on node_a (LWW conflict resolution succeeded)
    let state_a = node_a.get_session_state("sess_lww").unwrap();
    assert_eq!(state_a, b"updated_value".to_vec());
    info!("node_a has updated state: '{}'", String::from_utf8_lossy(&state_a));

    info!("SUCCESS: Session LWW conflict resolution and state sync completed successfully!");
}
