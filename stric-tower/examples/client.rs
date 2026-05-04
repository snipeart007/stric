use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use stric_tower::{SerdeCodec, TowerClientService, TowerError};
use tower::Service;

// 1. Define Request/Response types (Should match server)
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoRequest {
    pub message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoResponse {
    pub message: String,
}

// 2. Define JSON Format for SerdeCodec
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
    // Boilerplate for QUIC crypto
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    // 3. Client endpoint configuration
    // Note: In a real app, you would load root CA certs.
    // For this example, we skip certificate verification (DO NOT DO THIS IN PRODUCTION).

    // UNSAFE: Accepting any server certificate for demonstration
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

    // 4. Connect to Server
    let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433);
    println!("Connecting to {}...", server_addr);

    let connection = client_endpoint.connect(server_addr, "localhost")?.await?;
    println!("Connected!");

    // 5. Initialize Tower Client Service
    let codec = SerdeCodec::<EchoRequest, EchoResponse, JsonFormat>::new();
    let mut client = TowerClientService::new(connection, codec);

    // 6. Make Requests
    let messages = vec!["Hello!", "Tower over QUIC", "stric-tower is cool"];

    for msg in messages {
        let req = EchoRequest {
            message: msg.to_string(),
        };
        println!("Sending: {}", msg);

        match client.call(req).await {
            Ok(res) => println!("Received: {}", res.message),
            Err(e) => eprintln!("Error: {:?}", e),
        }
    }

    Ok(())
}

// --- Helper to skip verification (For Example Only) ---

#[derive(Debug)]
struct SkipServerVerification;

impl quinn::rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &quinn::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[quinn::rustls::pki_types::CertificateDer<'_>],
        _server_name: &quinn::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: quinn::rustls::pki_types::UnixTime,
    ) -> Result<quinn::rustls::client::danger::ServerCertVerified, quinn::rustls::Error> {
        Ok(quinn::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(quinn::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(quinn::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        quinn::rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
