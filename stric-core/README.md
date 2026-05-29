# stric-core

`stric-core` is a robust, high-level, and ergonomic wrapper over the `quinn` QUIC implementation. It provides symmetric peer-to-peer (P2P) capabilities, automated connection tracking, and heartbeat keep-alive mechanisms.

This crate is designed to be agnostic to the application-layer topology, serving as the foundational transport layer for both request-response frameworks (`stric-tower`) and complex distributed mesh networks (`stric-flow`).

## Key Features

- **Symmetric QuicNode:** A unified transport engine that acts as both a listener (Responder) and a dialer (Initiator) on a single UDP endpoint.
- **Role-Agnostic Terminology:** Uses "Initiator" and "Responder" to describe connection origins, removing traditional server-client bias.
- **Connection Management:** Automatic tracking of active connections with support for custom metadata and stable ID lookups.
- **Heartbeat System:** Automated background keep-alive pings to maintain connections and detect silent failures.
- **Optional Security:** Flexible TLS configuration with support for optional certificate verification (e.g., for trusted mesh environments).

## Public API

The crate exposes a simplified, root-level API:

```rust,no_run
use stric_core::{
    BiStream, ConnectionContext, ConnectionHandlerFn, ConnectionManager,
    ConnectionManagerError, ConnectionWrapper, NodeConfig, QuicNode,
    NodeStreamError, ServerUniStream,
};
```

### Core Components

- **`QuicNode<M>`**
  The main engine. Use this to `listen()` for incoming connections and `connect()` to remote peers. It manages the underlying `quinn::Endpoint`.
- **`NodeConfig`**
  Unified configuration for TLS material, bind address, ALPN, and transport parameters (idle timeouts, etc.).
- **`ConnectionManager<M>`**
  A thread-safe registry of active connections. Used for post-registration updates, metadata inspection, and stream opening.
- **`ConnectionWrapper<M>`**
  Passed to connection handlers. Combines the raw `quinn::Connection` with Stric-specific context and user-defined metadata.
- **`ConnectionContext`**
  Capability flags for a connection (e.g., `keep_alive`, `initiator_uni`, `responder_bi`).

## Quick Start (Symmetric Node)

```rust,no_run
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use stric_core::{ConnectionContext, NodeConfig, QuicNode};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // 1. Setup Configuration
    let config = NodeConfig {
        certs: Some(my_certs),
        key: Some(my_key),
        socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433),
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 16,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 10,
        idle_timeout: Some(std::time::Duration::from_secs(60)),
        root_cert_store: Some(my_roots),
        danger_accept_invalid_certs: false,
    };

    // 2. Initialize Node
    let (mut node, mut error_rx) = QuicNode::<()>::new(config)?;

    // 3. Register Handlers
    node.on_inbound(Arc::new(|wrapper| {
        println!("Accepted connection from initiator: {}", wrapper.context.id);
        Box::pin(async move { Ok(()) })
    }));

    node.on_outbound(Arc::new(|wrapper| {
        println!("Connected to responder: {}", wrapper.context.id);
        Box::pin(async move { Ok(()) })
    }));

    // 4. Run Node
    let node_arc = Arc::new(node);
    let node_clone = node_arc.clone();
    tokio::spawn(async move { node_clone.listen().await });

    // 5. Connect to a peer
    let peer_addr = "127.0.0.1:4434".parse()?;
    let conn_id = node_arc.connect(peer_addr, "localhost").await?;

    Ok(())
}
```

## Error Model

### `QuicNode::new(...) -> Result<..., anyhow::Error>`
Fails if TLS configuration is invalid or if the socket cannot be bound.

### `QuicNode::connect(...) -> Result<u64, anyhow::Error>`
Fails if the connection attempt is rejected by the peer or times out.

### `QuicNode::get_unistream` / `get_bistream`
Returns `NodeStreamError` if the connection ID is unknown or if the connection has closed.

## Implementation Notes

- **`stric-core`** is intentionally lean. It does not handle high-level routing, protobuf serialization, or session coordination. These are delegated to **`stric-flow`**.
- **Symmetry:** Every node can act as both an initiator and a responder. The distinction is only used to determine which capability flags (e.g., `initiator_bi`) apply to the connection.
- **Metadata:** The `ConnectionMetadata` type parameter allows you to attach any stateful data to a connection, which can be retrieved via the `ConnectionManager`.
