use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use stric_tower::{Json, Router, Server};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct Message {
    text: String,
}

async fn hello_handler(Json(req): Json<Message>) -> Json<Message> {
    println!("Received: {:?}", req);
    Json(Message {
        text: format!("Hello, {}!", req.text),
    })
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // 1. Initialize Tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .init();

    // 2. Define Router
    let router = Router::new()
        .route("/hello", hello_handler);

    // 3. Apply standard tower-http middleware (TraceLayer)
    // layer_standard converts between stric-tower types and standard http types
    let app = router.layer_standard(TraceLayer::new_for_http());

    // 4. Run Server
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4436);
    tracing::info!("Server starting with tower-http TraceLayer on {}", addr);
    
    Server::bind(addr).serve(app).await?;

    Ok(())
}
