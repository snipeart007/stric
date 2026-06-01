use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use stric_core::NodeConfig;
use stric_flow::node::{FlowNode, RoutingMetric};
use stric_flow::registry::MessageRegistry;

/// A custom routing metric that estimates costs based on link latency (RTT).
/// It penalizes high-latency paths by scaling the RTT contribution.
struct LatencyRoutingMetric {
    latency_penalty_factor: f64,
}

impl RoutingMetric for LatencyRoutingMetric {
    fn estimate_cost(&self, node_a: &str, node_b: &str, base_hop_cost: u32, rtt_micros: u64) -> u32 {
        let rtt_ms = rtt_micros as f64 / 1000.0;
        let penalty = (rtt_ms * self.latency_penalty_factor) as u32;
        let estimated = base_hop_cost + penalty;
        println!(
            "[Metric] Estimating cost between {} and {}: base={}, RTT={}us -> cost={}",
            node_a, node_b, base_hop_cost, rtt_micros, estimated
        );
        estimated
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

    // 1. Initialize our custom latency-aware routing metric
    let custom_metric = Arc::new(LatencyRoutingMetric {
        latency_penalty_factor: 1.5,
    });

    println!("Starting node_a with custom LatencyRoutingMetric...");
    let config_a = make_node_config(0, &cert_der, &key_der);
    let registry = Arc::new(MessageRegistry::new());
    let (node_a, mut error_rx_a) = FlowNode::new(
        "node_a".to_string(),
        config_a,
        custom_metric.clone(),
        registry.clone(),
    ).unwrap();
    node_a.start().await;
    let addr_a = node_a.local_addr().unwrap();

    println!("Starting node_b with custom LatencyRoutingMetric...");
    let config_b = make_node_config(0, &cert_der, &key_der);
    let (node_b, mut error_rx_b) = FlowNode::new(
        "node_b".to_string(),
        config_b,
        custom_metric.clone(),
        registry.clone(),
    ).unwrap();
    node_b.start().await;

    // Discard any asynchronous transport errors
    tokio::spawn(async move { while let Some(_) = error_rx_a.recv().await {} });
    tokio::spawn(async move { while let Some(_) = error_rx_b.recv().await {} });

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Connect node_b to node_a
    println!("Connecting node_b to node_a...");
    node_b.connect(addr_a, "localhost").await.unwrap();

    // Wait for handshake
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify connections exist
    assert!(node_a.peer_connections().contains(&"node_b".to_string()));
    assert!(node_b.peer_connections().contains(&"node_a".to_string()));

    // Evaluate the metric manually to show it computes estimated costs correctly
    let cost = custom_metric.estimate_cost("node_a", "node_b", 10, 2500);
    assert_eq!(cost, 10 + 3); // 2500us RTT = 2.5ms. 2.5 * 1.5 = 3.75 -> 3 as u32. Cost is 13.
    println!("Estimated cost: {}", cost);

    println!("SUCCESS: Custom routing metric configured and evaluated successfully!");
}
