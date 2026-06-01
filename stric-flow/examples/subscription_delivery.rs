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

// Implement a simple flow handler that sends received string messages to a channel
struct SimpleStringHandler {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl FlowHandler for SimpleStringHandler {
    async fn handle_message(
        &self,
        flow_id: &str,
        topic_id: &str,
        message: Box<dyn Any + Send + Sync>,
    ) -> Result<(), String> {
        if let Some(msg_str) = message.downcast_ref::<String>() {
            println!("Handler received message: '{}' from flow '{}' on topic '{}'", msg_str, flow_id, topic_id);
            let _ = self.tx.send(msg_str.clone()).await;
            Ok(())
        } else {
            Err("Failed to downcast message to String".to_string())
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let config = make_node_config(0, &cert_der, &key_der);
    let mut registry = MessageRegistry::new();
    
    // Register custom String decoder in the message registry
    registry.register::<String>("my_app.TextMessage", |bytes| {
        String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
    });

    let (node, mut error_rx) = FlowNode::new(
        "my_node".to_string(),
        config,
        Arc::new(HopCountMetric),
        Arc::new(registry),
    ).unwrap();
    node.start().await;

    tokio::spawn(async move { while let Some(_) = error_rx.recv().await {} });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create receiver channel to verify handler delivery
    let (tx, mut rx) = mpsc::channel(10);
    let handler = Arc::new(SimpleStringHandler { tx });

    println!("Subscribing to topic 'sensors.temp.#'...");
    node.subscribe("sensors.temp.#".to_string(), handler);

    // Give the async subscription registration task time to run
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("Publishing message to topic 'sensors.temp.room_1'...");
    node.publish(
        "flow_001".to_string(),
        "sensors.temp.room_1".to_string(),
        "my_app.TextMessage".to_string(),
        proto::Codec::Raw,
        b"Temperature: 22.5 C".to_vec(),
    ).await.unwrap();

    // Verify local delivery
    let received = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
    assert_eq!(received, "Temperature: 22.5 C");

    println!("SUCCESS: Message published and local handler delivery verified successfully!");
}
