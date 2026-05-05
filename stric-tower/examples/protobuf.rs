use prost::Message;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use stric_tower::{
    BodyExt, Full, HeaderMap, Protobuf, Request, Router, Server, SkipServerVerification,
    TowerClientService,
};
use tower::Service;

// 1. Define Protobuf messages using prost macros (No .proto files needed)
#[derive(Clone, PartialEq, Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct HelloResponse {
    #[prost(string, tag = "1")]
    pub greeting: String,
}

// 2. Define handler using Protobuf extractor and response
async fn hello_handler(Protobuf(req): Protobuf<HelloRequest>) -> Protobuf<HelloResponse> {
    println!("Server received Protobuf request: {:?}", req);
    Protobuf(HelloResponse {
        greeting: format!("Hello, {}! This is a Protobuf response.", req.name),
    })
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4434);

    // 3. Start Server in the background
    let app = Router::new().route("/hello", hello_handler);
    let server_handle = tokio::spawn(async move {
        if let Err(e) = Server::bind(addr).serve(app).await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Wait a bit for server to start
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 4. Run Client
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
    crypto.alpn_protocols = vec![b"stric".to_vec()];

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

    // 5. Make a Protobuf request
    let req_payload = HelloRequest {
        name: "Gemini CLI".to_string(),
    };

    let mut body = Vec::with_capacity(req_payload.encoded_len());
    req_payload.encode(&mut body)?;

    let req = Request {
        path: "/hello".to_string(),
        headers: HeaderMap::new(),
        body: Full::new(body.into()),
    };

    println!("Sending Protobuf request...");
    let res = client.call(req).await?;

    // 6. Decode Protobuf response
    let body_bytes = res.body.collect().await?.to_bytes();
    let res_payload = HelloResponse::decode(body_bytes)?;
    println!("Received Protobuf response: {:?}", res_payload);

    Ok(())
}
