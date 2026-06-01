use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use async_trait::async_trait;
use stric_core::NodeConfig;
use stric_flow::node::{FlowNode, FlowHandler, HopCountMetric};
use stric_flow::registry::MessageRegistry;
use stric_flow::proto;

/// A custom stateful flow handler that records statistics about received messages.
struct StatefulStatsHandler {
    message_count: Arc<AtomicU32>,
    total_bytes: Arc<AtomicUsize>,
    last_topic: Arc<Mutex<Option<String>>>,
}

#[async_trait]
impl FlowHandler for StatefulStatsHandler {
    async fn handle_message(
        &self,
        _flow_id: &str,
        topic_id: &str,
        message: Box<dyn Any + Send + Sync>,
    ) -> Result<(), String> {
        if let Some(msg_str) = message.downcast_ref::<String>() {
            let len = msg_str.len();
            
            // Increment message count
            self.message_count.fetch_add(1, Ordering::SeqCst);
            // Add to total bytes
            self.total_bytes.fetch_add(len, Ordering::SeqCst);
            // Record last topic
            {
                let mut guard = self.last_topic.lock().unwrap();
                *guard = Some(topic_id.to_string());
            }

            println!(
                "[StatsHandler] Received message (len={}). Count={}, TotalBytes={}, LastTopic={}",
                len,
                self.message_count.load(Ordering::SeqCst),
                self.total_bytes.load(Ordering::SeqCst),
                topic_id
            );
            Ok(())
        } else {
            Err("Expected String message payload".to_string())
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
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    println!("Generating self-signed TLS certificates...");
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
    let (node_a, mut error_rx_a) = FlowNode::new(
        "node_a".to_string(),
        config_a,
        Arc::new(HopCountMetric),
        registry.clone(),
    ).unwrap();
    node_a.start().await;

    println!("Starting node_b (Receiver)...");
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
    println!("Connecting node_a to node_b...");
    node_a.connect(addr_b, "localhost").await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // 1. Initialize stats tracking states
    let msg_count = Arc::new(AtomicU32::new(0));
    let total_bytes = Arc::new(AtomicUsize::new(0));
    let last_topic = Arc::new(Mutex::new(None));

    // 2. Instantiate and register the custom stateful handler
    let stats_handler = Arc::new(StatefulStatsHandler {
        message_count: msg_count.clone(),
        total_bytes: total_bytes.clone(),
        last_topic: last_topic.clone(),
    });
    node_b.subscribe("sensors.alerts".to_string(), stats_handler);
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 3. Publish a sequence of messages from node_a
    let messages = vec!["Danger: Overheating", "Warning: Battery low", "Info: Normal"];
    for (i, msg) in messages.iter().enumerate() {
        println!("Publishing alert {}: '{}'", i + 1, msg);
        node_a.publish(
            "flow_alerts".to_string(),
            "sensors.alerts".to_string(),
            "my_app.TextMessage".to_string(),
            proto::Codec::Raw,
            msg.as_bytes().to_vec(),
        ).await.unwrap();
        
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 4. Verify statistics
    let final_count = msg_count.load(Ordering::SeqCst);
    let final_bytes = total_bytes.load(Ordering::SeqCst);
    let final_topic = last_topic.lock().unwrap().clone().unwrap();

    println!("Final statistics recorded by handler:");
    println!("  Total count: {}", final_count);
    println!("  Total bytes: {}", final_bytes);
    println!("  Last topic:  {}", final_topic);

    assert_eq!(final_count, 3);
    assert_eq!(final_bytes, 19 + 20 + 12); // length of the strings
    assert_eq!(final_topic, "sensors.alerts");

    println!("SUCCESS: Custom stateful FlowHandler configured and executed successfully!");
}
