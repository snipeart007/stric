#[path = "common/mod.rs"]
mod common;

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

    info!("Generating self-signed TLS certificates...");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    info!("Starting node_a (listener)...");
    let config_a = make_node_config(0, &cert_der, &key_der);
    let registry = Arc::new(MessageRegistry::new());
    let (node_a, mut error_rx_a) = FlowNode::new(
        "node_a".to_string(),
        config_a,
        Arc::new(HopCountMetric),
        registry.clone(),
    ).unwrap();
    node_a.start().await;
    let addr_a = node_a.local_addr().unwrap();
    info!("node_a is listening on {}", addr_a);

    info!("Starting node_b...");
    let config_b = make_node_config(0, &cert_der, &key_der);
    let (node_b, mut error_rx_b) = FlowNode::new(
        "node_b".to_string(),
        config_b,
        Arc::new(HopCountMetric),
        registry.clone(),
    ).unwrap();
    node_b.start().await;

    // Discard any asynchronous transport errors
    tokio::spawn(async move { while let Some(_) = error_rx_a.recv().await {} });
    tokio::spawn(async move { while let Some(_) = error_rx_b.recv().await {} });

    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("Connecting node_b to node_a...");
    node_b.connect(addr_a, "localhost").await.unwrap();

    info!("Waiting for handshake to complete...");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify connections exist
    assert!(node_a.peer_connections().contains(&"node_b".to_string()));
    assert!(node_b.peer_connections().contains(&"node_a".to_string()));

    info!("SUCCESS: Control stream connection and handshake established successfully!");
}
