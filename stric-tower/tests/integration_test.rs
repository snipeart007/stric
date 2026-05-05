use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use futures::FutureExt;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::Full as HttpFull;
use stric_core::{ConnectionContext, ServerConfig, ServerInstance};
use stric_tower::{BodyExt, Bytes, Full, HeaderMap, HeaderValue, Json, Request, Router, TowerClientService, TowerConnectionHandler, TowerError};
use tower::{Layer, Service};

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

#[derive(Clone)]
struct WrappedBody<B>(B);

impl<B> HttpBody for WrappedBody<B>
where
    B: HttpBody<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.0).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.0.size_hint()
    }
}

#[derive(Clone)]
struct AddHeaderLayer;

#[derive(Clone)]
struct AddHeaderService<S> {
    inner: S,
}

impl<S> Layer<S> for AddHeaderLayer {
    type Service = AddHeaderService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AddHeaderService { inner }
    }
}

impl<S> Service<http::Request<Full<Bytes>>> for AddHeaderService<S>
where
    S: Service<http::Request<Full<Bytes>>, Response = http::Response<Full<Bytes>>, Error = TowerError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<WrappedBody<Full<Bytes>>>;
    type Error = TowerError;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<Full<Bytes>>) -> Self::Future {
        let mut inner = self.inner.clone();

        async move {
            let mut response = inner.call(req).await?;
            response
                .headers_mut()
                .insert("x-layered", HeaderValue::from_static("true"));
            let (parts, body) = response.into_parts();
            Ok(http::Response::from_parts(parts, WrappedBody(body)))
        }
        .boxed()
    }
}

fn build_server_config(
    cert_der: Vec<u8>,
    key_der: Vec<u8>,
    addr: SocketAddr,
) -> ServerConfig {
    ServerConfig {
        certs: vec![quinn::rustls::pki_types::CertificateDer::from(cert_der)],
        key: quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap(),
        socket_addr: addr,
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 10,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    }
}

async fn build_client(cert_der: Vec<u8>, server_addr: SocketAddr) -> TowerClientService {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots
        .add(quinn::rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"stric".to_vec()];
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).unwrap();
    client_endpoint.set_default_client_config(client_config);

    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();

    TowerClientService::new(connection)
}

#[tokio::test]
async fn test_axum_like_tower_integration() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = build_server_config(cert_der.clone(), key_der, addr);

    let app = Router::new().route("/echo", echo_handler);
    let tower_handler = TowerConnectionHandler::new(app);

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();

    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut client_service = build_client(cert_der, server_addr).await;

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
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = build_server_config(cert_der.clone(), key_der, addr);

    let app = Router::new().route("/echo", echo_handler);
    let tower_handler = TowerConnectionHandler::new(app);

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();
    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut client_service = build_client(cert_der, server_addr).await;

    let req = Request {
        path: "/wrong-path".to_string(),
        headers: HeaderMap::new(),
        body: Full::new(vec![].into()),
    };

    let res = client_service.call(req).await.unwrap();
    assert_eq!(res.status, 404);
}

#[tokio::test]
async fn test_invalid_json_returns_bad_request() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = build_server_config(cert_der.clone(), key_der, addr);

    let app = Router::new().route("/echo", echo_handler);
    let tower_handler = TowerConnectionHandler::new(app);

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();
    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut client_service = build_client(cert_der, server_addr).await;
    let req = Request {
        path: "/echo".to_string(),
        headers: HeaderMap::new(),
        body: Full::new(Bytes::from_static(b"{bad-json")),
    };

    let res = client_service.call(req).await.unwrap();
    assert_eq!(res.status, 400);

    let body = res.body.collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("Invalid JSON"));
}

#[tokio::test]
async fn test_standard_layer_can_add_headers_and_wrap_body() {
    setup_crypto();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = build_server_config(cert_der.clone(), key_der, addr);

    let app = Router::new()
        .route("/echo", echo_handler)
        .layer_standard(AddHeaderLayer);
    let tower_handler = TowerConnectionHandler::new(app);

    let (mut server, mut _error_rx) = ServerInstance::<()>::new(config).unwrap();
    let server_addr = server.local_addr().unwrap();
    server.register_connection_handler(tower_handler.into_handler());

    let server_arc = Arc::new(server);
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        server_clone.listen_connections().await;
    });

    let mut client_service = build_client(cert_der, server_addr).await;
    let body = serde_json::to_vec(&EchoRequest {
        message: "layered".to_string(),
    })
    .unwrap();

    let res = client_service
        .call(Request {
            path: "/echo".to_string(),
            headers: HeaderMap::new(),
            body: HttpFull::new(body.into()),
        })
        .await
        .unwrap();

    assert_eq!(res.status, 200);
    assert_eq!(res.headers["x-layered"], "true");

    let body = res.body.collect().await.unwrap().to_bytes();
    let decoded: EchoResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(decoded.message, "layered");
}
