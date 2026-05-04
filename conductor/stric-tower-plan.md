# stric-tower Implementation Plan: Axum-like API with Protobuf Wire Protocol

## Objective
Revamp `stric-tower` to provide an ergonomic, `axum`-like front-facing API for high-performance QUIC services. The new API will abstract away manual `tower::Service` implementations, allowing users to write handlers as simple `async fn`s with Extractors and a `Router`. 

Crucially, the underlying communication over the `BiStream` will use a **Protobuf-based wire protocol envelope** to encapsulate requests (routing paths, headers, payload format, and raw bytes) and responses (status, headers, and raw bytes).

## Architecture & Design

### 1. Protobuf Wire Envelope
To support path-based routing over QUIC streams (which lack HTTP headers natively), we define a standard Protobuf envelope.

```protobuf
// stric_tower_wire.proto
syntax = "proto3";

package stric_tower.wire;

message RequestEnvelope {
    string path = 1;
    map<string, string> headers = 2;
    bytes payload = 3;
}

message ResponseEnvelope {
    uint32 status_code = 1;
    map<string, string> headers = 2;
    bytes payload = 3;
}
```
*   **Transport:** The envelope itself is prefixed with a 4-byte length header over the `BiStream`.
*   **Payload Independence:** The `payload` field contains the raw bytes. These bytes can be encoded/decoded by the specific Extractor used in the handler (e.g., JSON, Protobuf, Bincode).

### 2. The `Request` and `Response` Types
Internal abstractions representing the incoming and outgoing data, heavily inspired by `http::Request` and `http::Response`.

```rust
pub struct Request {
    pub path: String,
    pub headers: std::collections::HashMap<String, String>,
    pub body: Vec<u8>,
}

pub struct Response {
    pub status: u16,
    pub headers: std::collections::HashMap<String, String>,
    pub body: Vec<u8>,
}
```

### 3. Extractors (`FromRequest`) and Responders (`IntoResponse`)
To allow `async fn` handlers, we need traits to convert our internal `Request` into handler arguments, and handler return types into our `Response`.

```rust
use async_trait::async_trait;

#[async_trait]
pub trait FromRequest: Sized {
    type Rejection: IntoResponse;
    async fn from_request(req: &mut Request) -> Result<Self, Self::Rejection>;
}

pub trait IntoResponse {
    fn into_response(self) -> Response;
}
```

**Standard Extractors/Responders provided:**
*   `Json<T>`: Uses `serde_json` to parse/serialize the `Request::body`.
*   `Bincode<T>`: Uses `bincode`.
*   `Protobuf<T>`: Uses `prost::Message`.
*   `State<T>`: Extracts shared application state.
*   `Bytes`: Raw payload extraction.

### 4. The `Handler` Trait
A trait implemented for `async fn`s of various arities.

```rust
pub trait Handler<T, S>: Clone + Send + Sized + 'static {
    type Future: Future<Output = Response> + Send + 'static;
    fn call(self, req: Request, state: S) -> Self::Future;
}
```
*Implemented via macros for `async fn(E1, E2) -> R` where `E` implements `FromRequest` and `R` implements `IntoResponse`.*

### 5. The `Router`
A `tower::Service` implementation that maps paths to handlers.

```rust
pub struct Router<S = ()> {
    routes: HashMap<String, BoxedHandler<S>>,
    state: S,
}

impl<S: Clone + Send + Sync + 'static> Router<S> {
    pub fn new() -> Self { ... }
    
    pub fn route<H, T>(mut self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    { ... }

    pub fn with_state<S2>(self, state: S2) -> Router<S2> { ... }
}

impl<S: Clone + Send + Sync + 'static> tower::Service<Request> for Router<S> {
    type Response = Response;
    // ... routes incoming Request by `req.path` to the correct BoxedHandler.
}
```

### 6. The `TowerConnectionHandler` Integration
The glue code will decode the Protobuf `RequestEnvelope` from the stream, build the internal `Request`, pass it to the `Router` (`tower::Service`), and then encode the resulting `Response` back into a `ResponseEnvelope` Protobuf message over the stream.

## Front-Facing API Showcase

```rust
use serde::{Deserialize, Serialize};
use stric_tower::{Router, Json, State, Server, extract::Path};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
struct EchoPayload {
    msg: String,
}

struct AppState {
    counter: std::sync::atomic::AtomicUsize,
}

// Handler looks exactly like Axum!
async fn echo_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<EchoPayload>,
) -> Json<EchoPayload> {
    state.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Json(EchoPayload {
        msg: format!("Echo: {}", payload.msg),
    })
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState {
        counter: std::sync::atomic::AtomicUsize::new(0),
    });

    let app = Router::new()
        .route("/echo", echo_handler)
        .with_state(state);

    // Build Server (abstracts away ServerInstance config boilerplate)
    let addr = "127.0.0.1:4433".parse().unwrap();
    let mut server = stric_tower::Server::bind(addr).unwrap();
    
    // Mount the router
    server.serve(app).await;
}
```

## Implementation Phases

**Phase 1: Wire Protocol (Protobuf)**
1.  Add `build.rs` and `prost-build` to compile `stric_tower_wire.proto`.
2.  Implement stream read/write helpers in `stric-tower/src/codec.rs` specifically for reading/writing `RequestEnvelope` and `ResponseEnvelope` with a 4-byte length prefix.

**Phase 2: Core Types & Extractors**
1.  Define `Request` and `Response` structs in `stric-tower/src/http.rs` (or similar).
2.  Define `FromRequest` and `IntoResponse` traits.
3.  Implement extractors: `State`, `Json`, `Bincode`, `Bytes`, `Protobuf`.

**Phase 3: The Handler Trait & Router**
1.  Implement the `Handler` trait using a macro to support functions with varying numbers of extractor arguments.
2.  Implement the `Router` struct with a simple `HashMap` or prefix-tree for routing paths to handlers.
3.  Make `Router` implement `tower::Service<Request, Response=Response>`.

**Phase 4: Client & Server Glue**
1.  Update `TowerConnectionHandler` to deserialize the `RequestEnvelope`, create the `Request`, call the `Router`, and serialize the `ResponseEnvelope`.
2.  Implement an ergonomic `stric_tower::Server` wrapper around `stric_core::ServerInstance` to reduce boilerplate (certificate generation for dev, config setup).
3.  Update the Client side to provide an ergonomic interface for building `RequestEnvelope`s and sending them.

**Phase 5: Updates**
1.  Update docs, tests, and binary examples (`examples/server.rs`, `examples/client.rs`) to reflect the new Axum-like API and Protobuf wire protocol.
