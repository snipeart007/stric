use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::task::{Context, Poll};
use stric_core::connection_wrapper::ConnectionContext;
use stric_core::server::ServerInstance;
use stric_core::server_config::ServerConfig;
use stric_tower::{BincodeFormat, SerdeCodec, TowerClientService, TowerConnectionHandler};
use tower::Service;
use futures::future::BoxFuture;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct EchoRequest {
    message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct EchoResponse {
    message: String,
}

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
        futures::future::ready(Ok(EchoResponse {
            message: req.message,
        }))
    }
}

fn setup_crypto() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
}

#[tokio::test]
async fn test_tower_integration() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    let codec = SerdeCodec::<EchoRequest, EchoResponse, BincodeFormat>::new();
    let service = EchoService;
    let tower_handler = TowerConnectionHandler::<_, _, EchoRequest, EchoResponse>::new(service, codec.clone());

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();

    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    // Client setup
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint.connect(server_addr, "localhost").unwrap().await.unwrap();

    let mut client_service = TowerClientService::new(connection, codec);

    // Test request
    let req = EchoRequest { message: "Hello Tower!".to_string() };
    let res = client_service.call(req).await.unwrap();

    assert_eq!(res.message, "Hello Tower!");
}

#[derive(Clone, Default)]
struct JsonFormat;

impl stric_tower::SerdeFormat for JsonFormat {
    fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>, stric_tower::TowerError> {
        serde_json::to_vec(item).map_err(|e| stric_tower::TowerError::Codec(e.to_string()))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, stric_tower::TowerError> {
        serde_json::from_slice(bytes).map_err(|e| stric_tower::TowerError::Codec(e.to_string()))
    }
}

#[tokio::test]
async fn test_json_tower_integration() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    let codec = SerdeCodec::<EchoRequest, EchoResponse, JsonFormat>::new();
    let service = EchoService;
    let tower_handler = TowerConnectionHandler::<_, _, EchoRequest, EchoResponse>::new(service, codec.clone());

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();

    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    // Client setup
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint.connect(server_addr, "localhost").unwrap().await.unwrap();

    let mut client_service = TowerClientService::new(connection, codec);

    // Test request
    let req = EchoRequest { message: "Hello JSON!".to_string() };
    let res = client_service.call(req).await.unwrap();

    assert_eq!(res.message, "Hello JSON!");
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProstEchoRequest {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ProstEchoResponse {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[derive(Clone)]
struct ProstEchoService;

impl Service<ProstEchoRequest> for ProstEchoService {
    type Response = ProstEchoResponse;
    type Error = anyhow::Error;
    type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ProstEchoRequest) -> Self::Future {
        futures::future::ready(Ok(ProstEchoResponse {
            message: req.message,
        }))
    }
}

#[tokio::test]
async fn test_prost_tower_integration() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    let codec = stric_tower::ProstCodec::<ProstEchoRequest, ProstEchoResponse>::new();
    let service = ProstEchoService;
    let tower_handler = TowerConnectionHandler::<_, _, ProstEchoRequest, ProstEchoResponse>::new(service, codec.clone());

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();
    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap()));
    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint.connect(server_addr, "localhost").unwrap().await.unwrap();
    let mut client_service = TowerClientService::new(connection, codec);

    let req = ProstEchoRequest { message: "Hello Prost!".to_string() };
    let res = client_service.call(req).await.unwrap();
    assert_eq!(res.message, "Hello Prost!");
}

#[tokio::test]
async fn test_tower_layers_timeout() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

    let config = ServerConfig {
        certs,
        key,
        socket_addr: addr,
        alpn_protocol_names: vec![b"h3".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    #[derive(Clone)]
    struct SlowService;
    impl Service<EchoRequest> for SlowService {
        type Response = EchoResponse;
        type Error = anyhow::Error;
        type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> { Poll::Ready(Ok(())) }
        fn call(&mut self, req: EchoRequest) -> Self::Future {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                Ok(EchoResponse { message: req.message })
            })
        }
    }

    // Server-side timeout
    let service = tower::ServiceBuilder::new()
        .timeout(std::time::Duration::from_millis(100))
        .service(SlowService);

    let codec = SerdeCodec::<EchoRequest, EchoResponse, BincodeFormat>::new();
    let tower_handler = TowerConnectionHandler::<_, _, EchoRequest, EchoResponse>::new(service, codec.clone());

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.endpoint.local_addr().unwrap();
    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move { server_clone.listen_connections().await; });

    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(quinn::rustls::pki_types::CertificateDer::from(cert_der)).unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap()));
    let mut client_endpoint = quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint.connect(server_addr, "localhost").unwrap().await.unwrap();
    let mut client_service = TowerClientService::new(connection, codec);

    let req = EchoRequest { message: "Slow".to_string() };
    let res = client_service.call(req).await;

    // The stream should be closed or return an error because the server timed out.
    // In our implementation, handle_stream will return an error, and the stream will be closed.
    assert!(res.is_err());
}
