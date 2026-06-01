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

struct SimpleHandler {
    tx: mpsc::Sender<String>,
}

#[async_trait]
impl FlowHandler for SimpleHandler {
    async fn handle_message(
        &self,
        _flow_id: &str,
        _topic_id: &str,
        message: Box<dyn Any + Send + Sync>,
    ) -> Result<(), String> {
        if let Some(msg_str) = message.downcast_ref::<String>() {
            let _ = self.tx.send(msg_str.clone()).await;
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
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

    println!("Starting node_a (Sender)...");
    let config_a = make_node_config(0, &cert_der, &key_der);
    let (node_a, mut error_rx_a) = FlowNode::new("node_a".to_string(), config_a, Arc::new(HopCountMetric), registry.clone()).unwrap();
    node_a.start().await;

    println!("Starting node_b (Receiver)...");
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

    // Register subscription on node_b
    let (tx, mut rx) = mpsc::channel(10);
    node_b.subscribe("sensors.temp".to_string(), Arc::new(SimpleHandler { tx }));
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Send a backpressure PAUSE signal from node_b to node_a for flow "flow_1"
    println!("Sending backpressure PAUSE from node_b to node_a...");
    node_b.send_backpressure("flow_1".to_string(), proto::BackpressureAction::Pause, 0).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Try to publish a message from node_a (should be blocked / paused)
    println!("Publishing message while paused (should not be received)...");
    node_a.publish(
        "flow_1".to_string(),
        "sensors.temp".to_string(),
        "my_app.TextMessage".to_string(),
        proto::Codec::Raw,
        b"Temp data (paused)".to_vec(),
    ).await.unwrap();

    // Verify no message is received during sleep
    let result = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await;
    assert!(result.is_err(), "Message was received while flow was paused!");
    println!("Confirmed: No message received while paused.");

    // Send a backpressure RESUME signal
    println!("Sending backpressure RESUME from node_b to node_a...");
    node_b.send_backpressure("flow_1".to_string(), proto::BackpressureAction::Resume, 0).await.unwrap();

    // Verify the paused message (or a new message) is delivered
    let result = tokio::time::timeout(Duration::from_secs(8), rx.recv()).await;
    assert!(result.is_ok(), "Message was not received after resume!");
    println!("Confirmed: Message received successfully after resume: '{}'", result.unwrap().unwrap());

    println!("SUCCESS: Backpressure pause and resume functionality verified!");
}
