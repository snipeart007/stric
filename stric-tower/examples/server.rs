use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::task::{Context, Poll};
use std::time::Duration;
use stric_core::connection_wrapper::ConnectionContext;
use stric_core::server::ServerInstance;
use stric_core::server_config::ServerConfig;
use stric_tower::{SerdeCodec, TowerConnectionHandler, TowerError};
use tower::Service;

// 1. Define Request/Response types
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoRequest {
    pub message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoResponse {
    pub message: String,
}

// 2. Define a simple Service
#[derive(Clone)]
struct EchoService;

impl Service<EchoRequest> for EchoService {
    type Response = EchoResponse;
    type Error = anyhow::Error;
    type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EchoRequest) -> Self::Future {
        println!("Received: {}", req.message);
        futures::future::ready(Ok(EchoResponse {
            message: format!("Echo: {}", req.message),
        }))
    }
}

// 3. Define JSON Format for SerdeCodec
#[derive(Clone, Default)]
struct JsonFormat;

impl stric_tower::SerdeFormat for JsonFormat {
    fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>, TowerError> {
        serde_json::to_vec(item).map_err(|e| TowerError::Codec(e.to_string()))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, TowerError> {
        serde_json::from_slice(bytes).map_err(|e| TowerError::Codec(e.to_string()))
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Boilerplate for QUIC crypto (using rustls with ring)
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    // 4. Generate self-signed certificate for development
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der)];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();

    // 5. Configure Server
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433);
    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: Some(Duration::from_secs(60)),
    };

    // 6. Initialize Server and Tower Handler
    let codec = SerdeCodec::<EchoRequest, EchoResponse, JsonFormat>::new();

    // Use Tower ServiceBuilder to add layers
    let service = tower::ServiceBuilder::new()
        .timeout(Duration::from_secs(5))
        .concurrency_limit(100)
        .service(EchoService);

    let tower_handler =
        TowerConnectionHandler::<_, _, EchoRequest, EchoResponse>::new(service, codec);

    let (mut server, mut error_rx) = ServerInstance::<()>::new(config)?;
    server.register_connection_handler(tower_handler.into_handler());

    println!("Server listening on {}", addr);

    // 7. Run Server and log errors
    tokio::select! {
        _ = server.listen_connections() => {},
        Some(err) = error_rx.recv() => {
            eprintln!("Server error: {:?}", err);
        }
    }

    Ok(())
}
