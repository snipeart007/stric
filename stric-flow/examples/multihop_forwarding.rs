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

struct ReceiverHandler {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl FlowHandler for ReceiverHandler {
    async fn handle_message(
        &self,
        flow_id: &str,
        topic_id: &str,
        message: Box<dyn Any + Send + Sync>,
    ) -> Result<(), String> {
        if let Some(msg_str) = message.downcast_ref::<String>() {
            info!("Receiver node got message: '{}' from flow '{}' on topic '{}'", msg_str, flow_id, topic_id);
            let _ = self.tx.send(msg_str.clone()).await;
            Ok(())
        } else {
            Err("Failed to downcast".to_string())
        }
    }
}

#[tokio::main]
async fn main() {
    common::init_logging();
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

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

    info!("Starting node_a (Sender)...");
    let config_a = make_node_config(0, &cert_der, &key_der);
    let (node_a, mut error_rx_a) = FlowNode::new("node_a".to_string(), config_a, Arc::new(HopCountMetric), registry.clone()).unwrap();
    node_a.start().await;

    info!("Starting node_b (Transit)...");
    let config_b = make_node_config(0, &cert_der, &key_der);
    let (node_b, mut error_rx_b) = FlowNode::new("node_b".to_string(), config_b, Arc::new(HopCountMetric), registry.clone()).unwrap();
    node_b.start().await;
    let addr_b = node_b.local_addr().unwrap();

    info!("Starting node_c (Receiver)...");
    let config_c = make_node_config(0, &cert_der, &key_der);
    let (node_c, mut error_rx_c) = FlowNode::new("node_c".to_string(), config_c, Arc::new(HopCountMetric), registry.clone()).unwrap();
    node_c.start().await;

    tokio::spawn(async move { while let Some(_) = error_rx_a.recv().await {} });
    tokio::spawn(async move { while let Some(_) = error_rx_b.recv().await {} });
    tokio::spawn(async move { while let Some(_) = error_rx_c.recv().await {} });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect node_a to node_b, and node_c to node_b (forming path node_a <-> node_b <-> node_c)
    info!("Connecting topology node_a <-> node_b <-> node_c...");
    node_a.connect(addr_b, "localhost").await.unwrap();
    node_c.connect(addr_b, "localhost").await.unwrap();

    // Give handshake and routing graph topology sync time to resolve Dijkstra path tree
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Register subscription on node_c
    let (tx, mut rx) = mpsc::channel(10);
    node_c.subscribe("data/topic".to_string(), Arc::new(ReceiverHandler { tx }));

    // Wait for subscription gossip update to propagate to node_a
    tokio::time::sleep(Duration::from_millis(1000)).await;

    info!("Publishing message from node_a...");
    node_a.publish(
        "flow_x".to_string(),
        "data/topic".to_string(),
        "my_app.TextMessage".to_string(),
        proto::Codec::Raw,
        b"Hello through transit node_b!".to_vec(),
    ).await.unwrap();

    // Verify delivery on node_c
    let received = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await.unwrap().unwrap();
    assert_eq!(received, "Hello through transit node_b!");

    info!("SUCCESS: Message statelessly routed via transit node_b and delivered to node_c!");
}
