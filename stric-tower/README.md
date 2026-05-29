# stric-tower

`stric-tower` is the high-level crate for request/response services over Stric QUIC connections. It layers an `axum`-style handler API and Tower integration on top of `stric-core`.

Use this crate when you want:

- path-based routing
- JSON, bincode, protobuf, raw-bytes, or state extractors
- `IntoResponse`-style handler returns
- `TowerClientService` as a Tower client
- standard Tower or `tower-http` middleware through `Router::layer_standard`
- root-level reexports of `HeaderMap`, `HeaderName`, `HeaderValue`, `Bytes`, `Body`, and related HTTP types so you do not need a direct `http` dependency just to build requests

## Public API

Import from the crate root:

```rust,no_run
use stric_tower::{
    Bincode, BincodeFormat, Body, BodyExt, Bytes, FromRequest, Full, HeaderMap,
    HeaderName, HeaderValue, HttpAdapter, HttpError, IntoResponse, Json, Method,
    ProstCodec, Protobuf, RawBytes, Request, Response, Router, SerdeCodec,
    SerdeFormat, Server, SkipServerVerification, State, StatusCode,
    TowerClientService, TowerConnectionHandler, TowerError, Uri,
};
```

What each export is for:

- `Router`
  Main server-side routing API.
- `Json`, `Bincode`, `Protobuf`, `RawBytes`, `State`
  Main extractor and response-wrapper API for handlers.
- `Request`, `Response`
  Low-level request and response types for clients, middleware, and manual service implementations.
- `TowerClientService`
  Client-side Tower `Service` over an established QUIC connection.
- `Server`
  Development-oriented server bootstrap helper. Uses a generated self-signed certificate and the symmetric `QuicNode`.
- `TowerConnectionHandler`
  Low-level bridge from a Stric/Tower service into `stric-core::QuicNode`. Use this for real TLS configuration.
- `HttpAdapter`
  The adapter type returned by `Router::layer_standard`. Treat it as an implementation detail that is public only because it appears in the return type.
- `ServiceCodec`, `ProstCodec`, `SerdeCodec`, `SerdeFormat`, `BincodeFormat`
  Stream-level codec primitives for custom protocols. These are not required for normal router-based request handling.
- `HeaderMap`, `HeaderName`, `HeaderValue`, `Bytes`, `Body`, `BodyExt`, `Full`, `Method`, `Uri`, `StatusCode`, `HttpError`
  Reexports to keep your dependency graph simpler and avoid `http` version mismatches.
- `SkipServerVerification`
  Development-only certificate verifier for examples and local experiments. Never use this in production.
- `Handler`
  Public because `Router::route` is generic over it. Most users should never implement it manually.

Not exported:

- the internal prost wire module
- envelope read/write helpers
- the internal HTTP sandwich shim

## Choose The Right Entry Point

### 1. Development-only server with the smallest API

Use:

- `Router`
- `Server::bind(...).serve(...)`

Do not use:

- this path for production TLS

Reason:

- `Server::serve` always generates a fresh self-signed `localhost` certificate and is intentionally a development helper

### 2. Production-style server with real TLS

Use:

- `Router`
- `TowerConnectionHandler::new(router).into_handler()`
- `stric_core::NodeConfig`
- `stric_core::QuicNode`

Do not use:

- `Server::serve`

Reason:

- `stric-core` is where certificate loading, ALPN selection, and node construction happen

### 3. Client requests over an existing QUIC connection

Use:

- `TowerClientService`
- `Request`
- `HeaderMap`

Do not use:

- `ServiceCodec` unless you are building a custom stream protocol outside the normal Stric request envelope

### 4. Standard Tower middleware

Use:

- `Router::layer_standard`

Do not use:

- `HttpAdapter` directly unless you are doing something unusual and know why you need the concrete type

### 5. Rust-to-Rust compact binary payloads

Use:

- `Bincode<T>`

Do not use:

- `Json<T>` if payload size and Rust-only interoperability matter more than human readability

### 6. Cross-language structured payloads

Use:

- `Json<T>` for human-readable payloads
- `Protobuf<T>` for schema-driven binary payloads

## Quick Start

### Minimal server

```rust,no_run
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use stric_tower::{Json, Router, Server};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct EchoRequest {
    message: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct EchoResponse {
    message: String,
}

async fn echo(Json(req): Json<EchoRequest>) -> Json<EchoResponse> {
    Json(EchoResponse { message: req.message })
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let app = Router::new().route("/echo", echo);
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433);
    Server::bind(addr).serve(app).await?;
    Ok(())
}
```

### Minimal client

```rust,no_run
use std::sync::Arc;

use stric_tower::{BodyExt, HeaderMap, Json, Request, SkipServerVerification, TowerClientService};
use tower::Service;

#[derive(serde::Serialize)]
struct EchoRequest {
    message: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();

    let mut crypto = quinn::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"stric".to_vec()];

    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?,
    ));

    let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    let connection = endpoint.connect("127.0.0.1:4433".parse()?, "localhost")?.await?;
    let mut client = TowerClientService::new(connection);

    let request = Request {
        path: "/echo".to_string(),
        headers: HeaderMap::new(),
        body: Json(EchoRequest {
            message: "hello".to_string(),
        })
        .into_response()
        .body,
    };

    let response = client.call(request).await?;
    let body = response.body.collect().await?.to_bytes();
    println!("{}", String::from_utf8_lossy(&body));
    Ok(())
}
```

## Proper TLS Setup

### Proper server verification on the client

Do this:

- install the rustls crypto provider
- load a CA root or the exact self-signed server certificate into `RootCertStore`
- build the client with `with_root_certificates(...)`
- set ALPN to `b"stric"`
- connect with the same DNS name the certificate covers

Do not do this:

- `dangerous().with_custom_certificate_verifier(Arc::new(SkipServerVerification))`

That API is exported only for local development and examples.

### Proper server configuration

For real TLS, do not use `stric_tower::Server::serve`. Use `stric_core::QuicNode` with your own `NodeConfig` and bridge your router through `TowerConnectionHandler`.

Sketch:

```rust,no_run
use std::sync::Arc;

use stric_core::{ConnectionContext, NodeConfig, QuicNode};
use stric_tower::{Router, TowerConnectionHandler};

let app = Router::new();
let handler = TowerConnectionHandler::new(app);

let config = NodeConfig {
    certs: Some(server_chain),
    key: Some(server_private_key),
    socket_addr: bind_addr,
    alpn_protocol_names: vec![b"stric".to_vec()],
    error_channel_len: 16,
    default_conn_context: ConnectionContext::default(),
    keep_alive_limit_per_thread: 0,
    idle_timeout: None,
    root_cert_store: None,
    danger_accept_invalid_certs: false,
};

let (mut node, mut error_rx) = QuicNode::<()>::new(config)?;
node.on_inbound(handler.into_handler());
```

### Proper client verification by the server

Current limitation:

- `stric_core::QuicNode::new` currently builds rustls with `with_no_client_auth()`
- `stric_tower::Server::serve` therefore also does not support mutual TLS

So today:

- proper client-side verification of the server is supported
- server-side verification of client certificates is not exposed by the current public API

If you need mutual TLS, the crate needs an additional server TLS configuration hook.

## HeaderMap Reexport

`HeaderMap` is intentionally reexported at the crate root:

```rust
use stric_tower::HeaderMap;
```

This is the preferred way to construct request and response headers when using this crate because it avoids a second direct `http` dependency and reduces the risk of version mismatches.

## Error Model

### `TowerClientService`

`TowerClientService` is a Tower `Service<Request, Response = Response, Error = TowerError>`.

Common `TowerError` variants from a client call:

- `TowerError::Connection(quinn::ConnectionError)`
  The QUIC connection closed, timed out, or became unusable.
- `TowerError::Write(quinn::WriteError)`
  Sending the request frame failed.
- `TowerError::Read(quinn::ReadError)`
- `TowerError::ReadExact(quinn::ReadExactError)`
  The response frame could not be read correctly.
- `TowerError::Closed(quinn::ClosedStream)`
  The stream closed before finishing.
- `TowerError::Internal(Box<dyn Error + Send + Sync>)`
  Body collection failed in a generic body implementation.
- `TowerError::ProstDecode`, `TowerError::ProstEncode`
  The Stric request envelope itself could not be encoded or decoded.

### `ServiceCodec` methods

All four codec trait methods return `Result<_, TowerError>`.

Typical variants:

- `ProstCodec` -> `TowerError::ProstEncode`, `TowerError::ProstDecode`, plus stream errors
- `SerdeCodec<_, _, BincodeFormat>` -> `TowerError::Bincode`, plus stream errors
- custom `SerdeFormat` implementations -> whatever `TowerError` your format returns, plus stream errors

### `Server::serve(...) -> Result<(), anyhow::Error>`

Returned error type:

- `anyhow::Error`

Common propagated inner errors:

- `rcgen` certificate generation errors
- `stric_core::QuicNode::new` initialization errors
- async handler failures received from the `stric-core` error channel

Operational meaning:

- either the development server could not start or a background connection task reported a fatal error

### Extractor rejections

Extractor methods are exposed through `FromRequest`, but the concrete built-in extractors reject with `Response<Full<Bytes>>`.

- `Json<T>`
  `400` on invalid JSON, `500` on body collection failure
- `Bincode<T>`
  `400` on invalid bincode, `500` on body collection failure
- `Protobuf<T>`
  `400` on invalid protobuf, `500` on body collection failure
- `RawBytes`
  `500` on body collection failure
- `State<T>`
  no rejection in the current implementation

### `TryFrom<http::Request<B>> for Request<B>`

Returned error type:

- `HttpError`

Current behavior:

- the implementation currently preserves the URI path and returns `Ok(...)`
- the error type remains part of the API because the conversion boundary is intentionally HTTP-flavored

## Edge Cases

- `Router` matches exact paths only. There is no parameter extraction, wildcard matching, or method dispatch.
- `Router::with_state(...)` currently rebuilds the router with the new state and does not preserve existing routes. Call it before attaching routes or treat it as a constructor-style API, not a mutating upgrade.
- `Router` falls back to `Response::empty(404)` for unknown paths.
- Invalid response status codes are allowed inside `Response<u16>` and are only coerced to `500 Internal Server Error` when converted to `http::Response<B>`.
- Header conversion during client/server envelope translation silently drops invalid header names or invalid non-UTF-8 header values because the envelope format stores headers as strings.
- Built-in extractors buffer the entire request body before decoding. Do not use them for truly streaming request bodies.
- `SkipServerVerification` disables server identity checks completely. It is exported only to make example code easy to run.

## Middleware Guidance

Use `Router::layer_standard(layer)` when you want normal Tower middleware such as:

- retries
- buffers
- concurrency limits
- tracing layers
- timeouts

This adapter exists specifically so standard `http::Request` and `http::Response` middleware can wrap a Stric-native router.

Do not use:

- `HttpAdapter::new(...)` manually unless you are doing advanced library integration

Reason:

- the adapter type is public because it appears in the return type, not because it is the preferred entry point

## What To Avoid

- Do not use `Server::serve` for production TLS.
- Do not use `SkipServerVerification` outside local development.
- Do not implement `Handler` manually unless you are building framework-level integrations.
- Do not reach for `ServiceCodec` for normal `Router` plus `TowerClientService` applications.
- Do not add a direct `http` dependency just to get `HeaderMap` unless you already need that crate for other reasons.
