use serde::{Deserialize, Serialize};
use stric_tower::{HeaderMap, IntoResponse, Json, Request, TowerClientService, BodyExt};
use tower::Service;
use std::sync::Arc;
use quinn::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use quinn::rustls::client::danger::{ServerCertVerifier, ServerCertVerified, HandshakeSignatureValid};
use quinn::rustls::{Error, SignatureScheme, DigitallySignedStruct};

#[derive(Serialize, Deserialize, Debug)]
struct EchoRequest {
    message: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct EchoResponse {
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // 1. Setup QUIC crypto
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    // 2. Configure Client
    let _roots = quinn::rustls::RootCertStore::empty();
    
    // Simplest client config that skips cert verification (DO NOT USE IN PRODUCTION)
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"h3".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap(),
    ));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())?;
    endpoint.set_default_client_config(client_config);

    // 3. Connect to Server
    let addr = "127.0.0.1:4433".parse().unwrap();
    let conn = endpoint.connect(addr, "localhost")?.await?;
    println!("Connected to {}", addr);

    // 4. Create Tower Client Service
    let mut client = TowerClientService::new(conn);

    // 5. Build and Send Request
    let req_payload = EchoRequest {
        message: "Hello from Tower Client!".to_string(),
    };

    let req = Request {
        path: "/echo".to_string(),
        headers: HeaderMap::new(),
        body: Json(req_payload).into_response().body,
    };

    println!("Sending request...");
    let res = client.call(req).await?;

    println!("Response Status: {}", res.status);
    
    // res.body is a Full<Bytes>, we can collect it to get the bytes
    let body_bytes = res.body.collect().await?.to_bytes();
    let echo_res: EchoResponse = serde_json::from_slice(&body_bytes)?;
    println!("Echo Response: {}", echo_res.message);

    Ok(())
}

// --- Helper to skip verification for dev ---
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
