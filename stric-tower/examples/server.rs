use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use stric_tower::{Router, Json, Server};

// 1. Define Request/Response types
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoRequest {
    pub message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoResponse {
    pub message: String,
}

// 2. Define handler as an ergonomic async function
async fn echo_handler(Json(req): Json<EchoRequest>) -> Json<EchoResponse> {
    println!("Received: {}", req.message);
    Json(EchoResponse {
        message: format!("Echo: {}", req.message),
    })
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // 3. Define Router and mount routes
    let app = Router::new()
        .route("/echo", echo_handler);

    // 4. Run Server with automatic dev-certificate generation
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433);
    Server::bind(addr).serve(app).await?;

    Ok(())
}
