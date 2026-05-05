use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use stric_core::connection_wrapper::ConnectionContext;
use stric_core::server::ServerInstance;
use stric_core::server_config::ServerConfig;
use stric_tower::{BodyExt, Full, HeaderMap, Json, Request, Router, TowerClientService, TowerConnectionHandler};
use tower::Service;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct EchoRequest {
    message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
struct EchoResponse {
    message: String,
}

async fn echo_handler(Json(req): Json<EchoRequest>) -> Json<EchoResponse> {
    Json(EchoResponse {
        message: req.message,
    })
}

fn setup_crypto() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
}

#[tokio::test]
async fn test_axum_like_tower_integration() {
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

    let app = Router::new().route("/echo", echo_handler);
    let tower_handler = TowerConnectionHandler::new(app);

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

    let mut client_service = TowerClientService::new(connection);

    // Test request
    let payload = EchoRequest { message: "Hello Axum-like!".to_string() };
    let body = serde_json::to_vec(&payload).unwrap();
    let req = Request {
        path: "/echo".to_string(),
        headers: HeaderMap::new(),
        body: Full::new(body.into()),
    };

    let res = client_service.call(req).await.unwrap();
    let body_bytes = res.body.collect().await.unwrap().to_bytes();
    let echo_res: EchoResponse = serde_json::from_slice(&body_bytes).unwrap();

    assert_eq!(echo_res.message, "Hello Axum-like!");
}

#[tokio::test]
async fn test_axum_like_404() {
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

    let app = Router::new().route("/echo", echo_handler);
    let tower_handler = TowerConnectionHandler::new(app);

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
    let mut client_service = TowerClientService::new(connection);

    let req = Request {
        path: "/wrong-path".to_string(),
        headers: HeaderMap::new(),
        body: Full::new(vec![].into()),
    };

    let res = client_service.call(req).await.unwrap();
    assert_eq!(res.status, 404);
}
