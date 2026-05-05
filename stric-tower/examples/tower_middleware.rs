use futures::future;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use stric_tower::{
    BodyExt, Bytes, Full, HeaderMap, Json, Request, Response, Router, Server,
    SkipServerVerification, TowerClientService,
};
use tokio::time::sleep;
use tower::retry::Policy;
use tower::{ServiceBuilder, ServiceExt};

static UNSTABLE_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EchoRequest {
    message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EchoResponse {
    message: String,
}

async fn echo_handler(Json(req): Json<EchoRequest>) -> Json<EchoResponse> {
    // Add a small delay so the concurrency limit is observable when multiple
    // requests are queued through the buffered client stack.
    sleep(Duration::from_millis(100)).await;

    Json(EchoResponse {
        message: format!("Echo: {}", req.message),
    })
}

async fn unstable_handler() -> Response<Full<Bytes>> {
    let attempt = UNSTABLE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    if attempt == 0 {
        println!("Server: returning 503 from /unstable on first attempt");
        Response::new(503, Full::new(Bytes::from_static(b"retry me")))
    } else {
        println!("Server: succeeding on /unstable retry");
        Response::new(200, Full::new(Bytes::from_static(b"recovered")))
    }
}

async fn slow_handler() -> String {
    sleep(Duration::from_millis(500)).await;
    "slow response".to_string()
}

#[derive(Clone)]
struct RetryOnUnavailable {
    remaining: usize,
}

impl RetryOnUnavailable {
    fn new(remaining: usize) -> Self {
        Self { remaining }
    }
}

impl<E> Policy<Request, Response, E> for RetryOnUnavailable
where
    E: std::fmt::Display,
{
    type Future = future::Ready<()>;

    fn retry(
        &mut self,
        _req: &mut Request,
        result: &mut Result<Response, E>,
    ) -> Option<Self::Future> {
        if self.remaining == 0 {
            return None;
        }

        match result {
            Ok(response) if response.status == 503 => {
                self.remaining -= 1;
                println!(
                    "Client stack: retrying after 503 ({} retries left)",
                    self.remaining
                );
                Some(future::ready(()))
            }
            Err(error) => {
                println!("Client stack: not retrying transport error: {error}");
                None
            }
            _ => None,
        }
    }

    fn clone_request(&mut self, req: &Request) -> Option<Request> {
        Some(req.clone())
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    UNSTABLE_ATTEMPTS.store(0, Ordering::SeqCst);

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4437);
    let app = Router::new()
        .route("/echo", echo_handler)
        .route("/unstable", unstable_handler)
        .route("/slow", slow_handler);

    let server_task = tokio::spawn(async move {
        if let Err(error) = Server::bind(addr).serve(app).await {
            eprintln!("Server error: {error:?}");
        }
    });

    sleep(Duration::from_millis(500)).await;

    let connection = connect_insecure(addr).await?;

    // This stack uses transport-agnostic Tower middleware around the
    // stric-tower client service:
    // - buffer: queue requests while inner layers are busy
    // - concurrency_limit: allow only one in-flight request at a time
    // - retry: repeat transient failures such as the first 503 from /unstable
    // - timeout: fail slow requests regardless of transport details
    let client = ServiceBuilder::new()
        .timeout(Duration::from_millis(250))
        .retry(RetryOnUnavailable::new(2))
        .concurrency_limit(1)
        .buffer::<Request>(8)
        .service(TowerClientService::new(connection));

    run_retry_demo(client.clone()).await?;
    run_buffer_and_limit_demo(client.clone()).await?;
    run_timeout_demo(client).await;

    server_task.abort();
    Ok(())
}

async fn run_retry_demo<S>(client: S) -> Result<(), anyhow::Error>
where
    S: tower::Service<
            Request,
            Response = Response,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    println!("\n== Retry Demo ==");

    let response = client
        .oneshot(Request {
            path: "/unstable".to_string(),
            headers: HeaderMap::new(),
            body: Full::new(Bytes::new()),
        })
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let body = response.body.collect().await?.to_bytes();
    println!(
        "Final /unstable status after retry policy: {} ({})",
        response.status,
        String::from_utf8_lossy(&body)
    );

    Ok(())
}

async fn run_buffer_and_limit_demo<S>(client: S) -> Result<(), anyhow::Error>
where
    S: tower::Service<
            Request,
            Response = Response,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    println!("\n== Buffer + Concurrency Limit Demo ==");

    let request_a = build_echo_request("first buffered request")?;
    let request_b = build_echo_request("second buffered request")?;

    let first = tokio::spawn(client.clone().oneshot(request_a));
    let second = tokio::spawn(client.oneshot(request_b));

    let first = first
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let second = second
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    let first = decode_echo_response(first).await?;
    let second = decode_echo_response(second).await?;

    println!("First response: {}", first.message);
    println!("Second response: {}", second.message);
    println!("Both requests succeeded even though the stack only allows one in-flight call.");

    Ok(())
}

async fn run_timeout_demo<S>(client: S)
where
    S: tower::Service<
            Request,
            Response = Response,
            Error = Box<dyn std::error::Error + Send + Sync>,
        > + Send,
    S::Future: Send,
{
    println!("\n== Timeout Demo ==");

    let result = client
        .oneshot(Request {
            path: "/slow".to_string(),
            headers: HeaderMap::new(),
            body: Full::new(Bytes::new()),
        })
        .await;

    match result {
        Ok(response) => {
            let body = response
                .body
                .collect()
                .await
                .map(|body| body.to_bytes())
                .unwrap_or_else(|_| Bytes::from_static(b"<body collection failed>"));
            println!(
                "Unexpected success: {} ({})",
                response.status,
                String::from_utf8_lossy(&body)
            );
        }
        Err(error) => {
            println!("Timed out as expected: {error}");
        }
    }
}

fn build_echo_request(message: &str) -> Result<Request, anyhow::Error> {
    let body = serde_json::to_vec(&EchoRequest {
        message: message.to_string(),
    })?;

    Ok(Request {
        path: "/echo".to_string(),
        headers: HeaderMap::new(),
        body: Full::new(body.into()),
    })
}

async fn decode_echo_response(response: Response) -> Result<EchoResponse, anyhow::Error> {
    let body = response.body.collect().await?.to_bytes();
    Ok(serde_json::from_slice(&body)?)
}

async fn connect_insecure(addr: SocketAddr) -> Result<quinn::Connection, anyhow::Error> {
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

    Ok(endpoint.connect(addr, "localhost")?.await?)
}
