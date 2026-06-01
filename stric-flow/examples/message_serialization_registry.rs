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

/// A custom domain data structure.
#[derive(Debug, Clone, PartialEq)]
pub struct SensorReading {
    pub device_id: u32,
    pub temperature: f32,
}

impl SensorReading {
    /// Serializes to a compact binary format.
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.device_id.to_be_bytes());
        bytes.extend_from_slice(&self.temperature.to_be_bytes());
        bytes
    }

    /// Deserializes from the custom binary format.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 8 {
            return Err("Payload too short for SensorReading".to_string());
        }
        let device_id = u32::from_be_bytes(bytes[0..4].try_into().map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        let temperature = f32::from_be_bytes(bytes[4..8].try_into().map_err(|e: std::array::TryFromSliceError| e.to_string())?);
        Ok(Self { device_id, temperature })
    }
}

/// Handlers for incoming custom message types downcast to the registered struct type.
struct CustomReadingHandler {
    tx: mpsc::Sender<SensorReading>,
}

#[async_trait]
impl FlowHandler for CustomReadingHandler {
    async fn handle_message(
        &self,
        _flow_id: &str,
        _topic_id: &str,
        message: Box<dyn Any + Send + Sync>,
    ) -> Result<(), String> {
        // Downcast to our custom struct
        if let Some(reading) = message.downcast_ref::<SensorReading>() {
            info!("Handler received decoded SensorReading: {:?}", reading);
            let _ = self.tx.send(reading.clone()).await;
            Ok(())
        } else {
            Err("Failed to downcast received message to SensorReading".to_string())
        }
    }
}

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

    // 1. Instantiate the MessageRegistry and register our custom struct & its decoder
    let registry = Arc::new({
        let mut r = MessageRegistry::new();
        r.register::<SensorReading>("custom.SensorReading", |bytes| {
            SensorReading::deserialize(bytes)
        });
        r
    });

    info!("Starting node_a (Sender)...");
    let config_a = make_node_config(0, &cert_der, &key_der);
    let (node_a, mut error_rx_a) = FlowNode::new(
        "node_a".to_string(),
        config_a,
        Arc::new(HopCountMetric),
        registry.clone(),
    ).unwrap();
    node_a.start().await;

    info!("Starting node_b (Receiver)...");
    let config_b = make_node_config(0, &cert_der, &key_der);
    let (node_b, mut error_rx_b) = FlowNode::new(
        "node_b".to_string(),
        config_b,
        Arc::new(HopCountMetric),
        registry.clone(),
    ).unwrap();
    node_b.start().await;
    let addr_b = node_b.local_addr().unwrap();

    // Discard any asynchronous transport errors
    tokio::spawn(async move { while let Some(_) = error_rx_a.recv().await {} });
    tokio::spawn(async move { while let Some(_) = error_rx_b.recv().await {} });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect node_a to node_b
    info!("Connecting node_a to node_b...");
    node_a.connect(addr_b, "localhost").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 2. Setup subscription on node_b using our custom handler
    let (tx, mut rx) = mpsc::channel(10);
    node_b.subscribe("sensors.readings".to_string(), Arc::new(CustomReadingHandler { tx }));
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 3. Publish message using the custom serialized format
    let sent_reading = SensorReading {
        device_id: 9942,
        temperature: 23.85,
    };
    info!("Publishing SensorReading: {:?}", sent_reading);
    
    node_a.publish(
        "flow_custom_msg".to_string(),
        "sensors.readings".to_string(),
        "custom.SensorReading".to_string(),
        proto::Codec::Raw,
        sent_reading.serialize(),
    ).await.unwrap();

    // 4. Verify successful delivery and deserialization on node_b
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(received.is_ok(), "Failed to receive message!");
    
    let received_reading = received.unwrap().unwrap();
    assert_eq!(received_reading, sent_reading);
    
    info!("SUCCESS: MessageRegistry successfully encoded, decoded, and matched custom message type!");
}
