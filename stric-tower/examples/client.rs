use serde::{Deserialize, Serialize};
use std::sync::Arc;
use stric_tower::{
    BodyExt, HeaderMap, IntoResponse, Json, Request, SkipServerVerification, TowerClientService,
};
use tower::Service;

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
    crypto.alpn_protocols = vec![b"stric".to_vec()];

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
