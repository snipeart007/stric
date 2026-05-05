use prost::Message;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use stric_tower::{Json, Protobuf, Request, Router, Server, TowerClientService, HeaderMap, Full, BodyExt};
use tower::Service;
use quinn::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use quinn::rustls::client::danger::{ServerCertVerifier, ServerCertVerified, HandshakeSignatureValid};
use quinn::rustls::{Error, SignatureScheme, DigitallySignedStruct};

// 1. Define JSON request type
#[derive(Debug, Serialize, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    pub limit: u32,
}

// 2. Define Protobuf response type (No .proto files needed)
#[derive(Clone, PartialEq, Message)]
pub struct SearchResult {
    #[prost(string, tag = "1")]
    pub title: String,
    #[prost(string, tag = "2")]
    pub url: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct SearchResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<SearchResult>,
}

// 3. Define handler: Receives JSON, Returns Protobuf
async fn search_handler(Json(req): Json<SearchRequest>) -> Protobuf<SearchResponse> {
    println!("Server received JSON request: {:?}", req);

    let results = vec![
        SearchResult {
            title: format!("Result for {}", req.query),
            url: format!("https://example.com/search?q={}", req.query),
        },
        SearchResult {
            title: "Stric Framework".to_string(),
            url: "https://github.com/stric-rs/stric".to_string(),
        },
    ];

    Protobuf(SearchResponse { results })
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4435);

    // 4. Start Server in the background
    let app = Router::new().route("/search", search_handler);
    let server_handle = tokio::spawn(async move {
        if let Err(e) = Server::bind(addr).unwrap().serve(app).await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Wait a bit for server to start
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 5. Run Client
    run_client(addr).await?;

    server_handle.abort();
    Ok(())
}

async fn run_client(server_addr: SocketAddr) -> Result<(), anyhow::Error> {
    // Boilerplate for QUIC crypto
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    let mut crypto = quinn::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut client_endpoint =
        quinn::Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))?;
    client_endpoint.set_default_client_config(client_config);

    println!("Connecting to {}...", server_addr);
    let connection = client_endpoint.connect(server_addr, "localhost")?.await?;
    println!("Connected!");

    let mut client = TowerClientService::new(connection);

    // 6. Make a JSON request
    let req_payload = SearchRequest {
        query: "rust quic".to_string(),
        limit: 10,
    };
    let body = serde_json::to_vec(&req_payload)?;

    let req = Request {
        path: "/search".to_string(),
        headers: HeaderMap::new(),
        body: Full::new(body.into()),
    };

    println!("Sending JSON request...");
    let res = client.call(req).await?;

    // 7. Decode Protobuf response
    let body_bytes = res.body.collect().await?.to_bytes();
    let res_payload = SearchResponse::decode(body_bytes)?;
    println!("Received Protobuf response with {} results:", res_payload.results.len());
    for (i, result) in res_payload.results.iter().enumerate() {
        println!("  {}. {} ({})", i + 1, result.title, result.url);
    }

    Ok(())
}

// --- Helper to skip verification (For Example Only) ---
#[derive(Debug)]
struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        quinn::rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
