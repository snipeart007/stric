# stric-tower Implementation Plan

## Objective
Create a new `stric-tower` crate that integrates the Tower ecosystem (`tower::Service`, `tower::Layer`) with `stric-core`. This will allow users to build highly concurrent, request-response based services using QUIC over our custom `stric-core` primitives (`BiStream`, `ConnectionWrapper`). The solution will rely on a generic wire protocol via length-prefixed messages (supporting both Protobuf and Serde) to enable type-safe RPC-like interactions.

## Architecture & Design
- **Unary Communication (Stream-per-Request):** Idiomatic to QUIC, each Tower `Request` will open a new `BiStream` (or use an accepted one). The client writes the request and reads the response. The server reads the request, executes the `tower::Service`, writes the response, and finishes the stream.
- **Custom BiStream Codec:** Since `stric-core::BiStream` does not natively implement `tokio::io::AsyncRead/Write`, we will define a custom `ServiceCodec` trait that leverages `BiStream`'s inherent `read_exact` and `write_all` methods for fast, manual framing (length-prefixed).
- **Prost Integration:** A default `ProstCodec` will be provided for serializing and deserializing types that implement `prost::Message`.
- **Serde Integration:** A `SerdeCodec` will be provided, featuring a format-agnostic design. Users can plug in any serialization format (e.g., JSON, Bincode, MessagePack, CBOR) to serialize/deserialize any types implementing `serde::Serialize` and `serde::Deserialize`.
- **Agnostic to Connection Management:** `stric-tower` will wrap an existing `ConnectionWrapper` on the server and `quinn::Connection` on the client, leaving connection pooling or multi-connection routing up to the user.

## Planned API Definitions

### 1. The Codec Trait
A generic abstraction for converting raw `BiStream` bytes to/from strongly typed requests and responses.

```rust
use async_trait::async_trait;
use stric_core::stream::BiStream;

#[async_trait]
pub trait ServiceCodec<Req, Res>: Send + Sync + Clone + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn encode_request(&self, req: Req, stream: &mut BiStream) -> Result<(), Self::Error>;
    async fn decode_request(&self, stream: &mut BiStream) -> Result<Req, Self::Error>;
    
    async fn encode_response(&self, res: Res, stream: &mut BiStream) -> Result<(), Self::Error>;
    async fn decode_response(&self, stream: &mut BiStream) -> Result<Res, Self::Error>;
}
```

### 2. Built-in Codecs (Prost & Serde)
Implementations of `ServiceCodec` using a simple 4-byte big-endian length prefix.

```rust
use prost::Message;
use std::marker::PhantomData;
use serde::{Serialize, de::DeserializeOwned};

// --- Prost Codec ---
#[derive(Clone, Default)]
pub struct ProstCodec<Req, Res>(PhantomData<(Req, Res)>);

impl<Req, Res> ProstCodec<Req, Res> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
// Implements ServiceCodec<Req, Res> using u32 length prefixes and prost::Message

// --- Generic Serde Codec ---
// A generic format trait to support JSON, Bincode, MessagePack, etc.
pub trait SerdeFormat: Send + Sync + Clone + 'static {
    fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>, anyhow::Error>;
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, anyhow::Error>;
}

#[derive(Clone, Default)]
pub struct SerdeCodec<Req, Res, Format>(PhantomData<(Req, Res, Format)>);

impl<Req, Res, Format> SerdeCodec<Req, Res, Format> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}
// Implements ServiceCodec<Req, Res> for any Req/Res implementing Serialize/DeserializeOwned
// using the provided Format.
```

### 3. Server-Side API
We will provide a helper function or struct to build a `ConnectionHandlerFn` compatible with `stric-core::ServerInstance`.

```rust
use tower::Service;
use stric_core::connection_wrapper::ConnectionWrapper;

pub struct TowerConnectionHandler<S, C> {
    service: S,
    codec: C,
}

impl<S, C> TowerConnectionHandler<S, C> {
    pub fn new(service: S, codec: C) -> Self { ... }

    /// Creates an Arc'd handler closure suitable for `register_connection_handler`.
    pub fn into_handler<M>(self) -> crate::ConnectionHandlerFn<M> 
    where 
        // appropriate trait bounds for Service, Request, Response, Codec, and Metadata
    { ... }
}
```
Internally, the handler loops over `conn.accept_bi()`. For each stream, it spawns a Tokio task to:
1. `codec.decode_request`
2. `service.call(req).await`
3. `codec.encode_response`
4. `stream.finish()`

### 4. Client-Side API
A wrapper implementing `tower::Service` over a `quinn::Connection`.

```rust
pub struct TowerClientService<C, Req, Res> {
    connection: quinn::Connection,
    codec: C,
    _marker: std::marker::PhantomData<(Req, Res)>,
}

impl<C, Req, Res> tower::Service<Req> for TowerClientService<C, Req, Res>
where
    // appropriate trait bounds
{
    type Response = Res;
    type Error = anyhow::Error; // or specific ClientError
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Checks connection status
    }

    fn call(&mut self, req: Req) -> Self::Future {
        // 1. conn.open_bi().await
        // 2. codec.encode_request
        // 3. codec.decode_response
    }
}
```

## Front-Facing API Showcase

Here is how a user will build a service with `stric-tower` utilizing `tower` layers, showcasing the generic Serde codec.

```rust
// 1. Define Serde Types
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoRequest {
    pub message: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EchoResponse {
    pub message: String,
}

// 2. Define the Base Service
#[derive(Clone)]
struct EchoService;

impl tower::Service<EchoRequest> for EchoService {
    type Response = EchoResponse;
    type Error = anyhow::Error;
    type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EchoRequest) -> Self::Future {
        futures::future::ready(Ok(EchoResponse { message: format!("Echo: {}", req.message) }))
    }
}

// --- SERVER SETUP ---
async fn run_server() {
    // Wrap service in standard Tower layers
    let service = tower::ServiceBuilder::new()
        .timeout(std::time::Duration::from_secs(5))
        .concurrency_limit(100)
        .service(EchoService);

    // Use Serde with Bincode encoding (can swap BincodeFormat with JsonFormat, etc.)
    let codec = SerdeCodec::<EchoRequest, EchoResponse, BincodeFormat>::new();
    let handler = TowerConnectionHandler::new(service, codec).into_handler();

    // Attach to stric-core Server
    let mut server = ServerInstance::new(config).unwrap();
    server.register_connection_handler(handler);
    server.listen_connections().await;
}

// --- CLIENT SETUP ---
async fn run_client(connection: quinn::Connection) {
    let codec = SerdeCodec::<EchoRequest, EchoResponse, BincodeFormat>::new();
    
    // Create base client
    let client = TowerClientService::new(connection, codec);

    // Apply client-side layers
    let mut layered_client = tower::ServiceBuilder::new()
        .timeout(std::time::Duration::from_secs(3))
        .service(client);

    // Use the service
    let req = EchoRequest { message: "Hello from Serde!".into() };
    let res = layered_client.call(req).await.unwrap();
    println!("Response: {}", res.message);
}
```

## Implementation Phases

**Phase 1: Foundation (Codec & Errors)**
- Set up `stric-tower` crate workspace and add dependencies (`tower`, `prost`, `serde`, `bincode`, `async-trait`, `stric-core`).
- Implement `ServiceCodec` trait and custom error types.
- Implement `ProstCodec` and generic `SerdeCodec` (with a format trait) using a 4-byte length prefix protocol. Write unit tests mocking `BiStream`.

**Phase 2: Server-Side Tower Handler**
- Implement `TowerConnectionHandler` to accept `tower::Service`.
- Implement `into_handler` converting it to `stric-core`'s expected `ConnectionHandlerFn`.
- Handle concurrent request spawning safely within the connection loop.

**Phase 3: Client-Side Tower Service**
- Implement `TowerClientService`.
- Implement `tower::Service` for `TowerClientService`, handling `open_bi` and the codec interactions.

**Phase 4: Testing & Examples**
- Write integration tests spinning up a `stric-core` server with `stric-tower` handler and a client executing requests using both Prost and various Serde formats.
- Ensure tower layers (e.g., `Timeout`) function as expected.