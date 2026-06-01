# RFC 2026-0001: stric-core Symmetric Node Architecture

## 1. Objective
This RFC specifies the symmetric peer-to-peer (P2P) architecture of `stric-core`. It transitions the system from a traditional server-client bias to a unified, role-agnostic QUIC node (`QuicNode`) that can simultaneously dial and listen on a single UDP socket. This document provides the complete specification needed to implement or recreate this transport layer.

## 2. Architectural Design

```
                     ┌────────────────────────┐
                     │       QuicNode         │
                     │  (Symmetric Endpoint)  │
                     └───────────┬────────────┘
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
       Inbound Connections             Outbound Connections
          (Responder)                     (Initiator)
                 │                               │
                 └───────────────┬───────────────┘
                                 ▼
                     ┌────────────────────────┐
                     │   ConnectionManager    │
                     │  (Registry of Context) │
                     └────────────────────────┘
```

### 2.1. The Symmetric `QuicNode`
Traditional systems separate servers (listeners) from clients (dialers). In `stric-core`, both roles are consolidated into a single unified `QuicNode` primitives:
* **NAT Traversal:** By sharing a single UDP port for both inbound and outbound traffic, it enables direct hole-punching and avoids firewall NAT mappings changing dynamically for outgoing calls.
* **Unified Identity:** A node is represented by its singular IP/Port endpoint.

### 2.2. Context and Metadata Terminology
To maintain role symmetry:
* **Initiator:** The peer that dialed the connection.
* **Responder:** The peer that accepted the connection.
This removes client/server terminology at the connection level.

---

## 3. Rust Interface and Type Specifications

### 3.1. Traits & Config
```rust
pub trait NodeContext: Send + Sync + Default + 'static {
    fn node_id(&self) -> &str;
    fn capabilities(&self) -> std::collections::HashMap<String, String>;
}

pub struct NodeConfig {
    pub certs: Option<Vec<rustls::Certificate>>,
    pub key: Option<rustls::PrivateKey>,
    pub socket_addr: std::net::SocketAddr,
    pub alpn_protocol_names: Vec<Vec<u8>>,
    pub error_channel_len: usize,
    pub default_conn_context: ConnectionContext,
    pub keep_alive_limit_per_thread: usize,
    pub idle_timeout: Option<std::time::Duration>,
    pub root_cert_store: Option<rustls::RootCertStore>,
    pub danger_accept_invalid_certs: bool,
}
```

### 3.2. Connections
```rust
pub struct ConnectionContext {
    pub id: u64,
    pub keep_alive: bool,
    pub initiator_uni: bool,
    pub responder_bi: bool,
}

pub struct ConnectionWrapper<M> {
    pub connection: quinn::Connection,
    pub context: ConnectionContext,
    pub metadata: M,
}
```

### 3.3. Node APIs
```rust
pub struct QuicNode<M> {
    endpoint: quinn::Endpoint,
    connections: Arc<ConnectionManager<M>>,
    config: NodeConfig,
    inbound_handler: Option<Arc<dyn Fn(ConnectionWrapper<M>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync>>,
    outbound_handler: Option<Arc<dyn Fn(ConnectionWrapper<M>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync>>,
}

impl<M: Send + Sync + 'static> QuicNode<M> {
    pub fn new(config: NodeConfig) -> Result<(Self, tokio::sync::mpsc::Receiver<Error>), Error>;
    
    pub fn on_inbound(&mut self, handler: Arc<dyn Fn(ConnectionWrapper<M>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync>);
    pub fn on_outbound(&mut self, handler: Arc<dyn Fn(ConnectionWrapper<M>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync>);
    
    pub async fn listen(self: Arc<Self>) -> Result<(), Error>;
    pub async fn connect(&self, peer_addr: std::net::SocketAddr, server_name: &str) -> Result<u64, Error>;
}
```

---

## 4. Stream Encapsulation
To decouple applications from `quinn` internal structures, streams are encapsulated into three types:

### 4.1. `ServerUniStream` (Outgoing Unidirectional)
```rust
pub struct ServerUniStream {
    pub(crate) send_stream: quinn::SendStream,
}

impl ServerUniStream {
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, quinn::WriteError>;
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), quinn::WriteError>;
    pub async fn write_chunk(&mut self, buf: bytes::Bytes) -> Result<(), quinn::WriteError>;
    pub async fn finish(&mut self) -> Result<(), quinn::WriteError>;
    pub async fn stopped(&mut self) -> Result<Option<usize>, quinn::StoppedError>;
}
```

### 4.2. `ClientUniStream` (Incoming Unidirectional)
```rust
pub struct ClientUniStream {
    pub(crate) recv_stream: quinn::RecvStream,
}

impl ClientUniStream {
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, quinn::ReadError>;
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), quinn::ReadExactError>;
    pub async fn read_to_end(&mut self, size_limit: usize) -> Result<Vec<u8>, quinn::ReadToEndError>;
    pub async fn read_chunk(&mut self, max_length: usize, ordered: bool) -> Result<Option<quinn::Chunk>, quinn::ReadError>;
    pub async fn stop(&mut self, error_code: quinn::VarInt) -> Result<(), quinn::UnknownStream>;
}
```

### 4.3. `BiStream` (Bidirectional Stream)
```rust
pub struct BiStream {
    pub(crate) send_stream: quinn::SendStream,
    pub(crate) recv_stream: quinn::RecvStream,
}

impl BiStream {
    // Exposes both the read methods (delegated to recv_stream) and write methods (delegated to send_stream).
}
```
