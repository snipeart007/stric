# Comprehensive Plan: stric-core Node Restructuring & Wrapper Expansion

## 1. System Philosophy & Role of `stric-core`

The fundamental objective of `stric-core` is to serve as a robust, high-level, and ergonomic wrapper over the `quinn` QUIC implementation. It is intentionally designed to be **agnostic to the application-layer topology**. It does not know about flows, topics, HTTP requests, or mesh routing. 

Its primary responsibilities are:
- **Connection Lifecycle Management:** Wrapping the complexity of establishing, maintaining, and tracking QUIC connections.
- **Heartbeat & Keep-Alive:** Providing an automated background system to keep connections alive and detect silent drops.
- **Stream Access:** Exposing simplified, async-friendly wrappers around unidirectional and bidirectional QUIC streams.
- **Symmetric Capability:** Acting as a true peer-to-peer (P2P) engine capable of initiating (dialing) and responding to (listening) connections simultaneously on a single UDP endpoint.

By fulfilling these roles, `stric-core` provides the perfect foundational layer for both request-response frameworks (`stric-tower`) and complex distributed state-machines (`stric-flow-core`, `stric-flow-node`).

---

## 2. The Shift from Server-Client to Symmetric Node

The original architecture relied on a `ServerInstance` that explicitly acted as a listener and treated incoming connections as "clients". This model is insufficient for distributed mesh networks where any node can dial any other node.

### The `QuicNode` Primitive
We are replacing `ServerInstance` with a symmetric `QuicNode`. A `QuicNode` manages a single `quinn::Endpoint` configured with both server (listener) and client (dialer) capabilities. This unified transport approach provides several benefits:
- **NAT Traversal:** Using a single UDP port for both inbound and outbound traffic significantly improves the chances of successful UDP hole-punching in complex network environments.
- **Resource Efficiency:** A single endpoint manages the underlying socket, reducing OS-level overhead.
- **Unified Identity:** The node represents itself to the network via a single `IP:Port` combination, regardless of whether it is initiating or accepting a connection.

### Role-Agnostic Terminology
To completely eradicate the "Client/Server" bias, all connection tracking and capability flags must use role-agnostic terminology based on connection origin:
- **Initiator:** The node that actively dialed the connection (`connect()`).
- **Responder:** The node that passively accepted the connection (`accept()`).

Consequently, the `ConnectionContext` struct, which dictates stream initiation rights, will be updated:
- `client_uni` → `initiator_uni`
- `client_bi`  → `initiator_bi`
- `server_uni` → `responder_uni`
- `server_bi`  → `responder_bi`

The `ConnectionManager` setter methods will be similarly renamed (e.g., `set_initiator_uni`).

---

## 3. Extending the Wrapper API

To fully support the diverse requirements of the upcoming `stric-flow` ecosystem, the `stric-core` API must be expanded.

### 3.1. `NodeConfig` (Consolidated Configuration)
The `ServerConfig` will be replaced by a comprehensive `NodeConfig`. This configuration must include parameters for both listening and dialing:
- **Responder Crypto:** TLS Certificates and Private Keys (e.g., `CertificateDer`, `PrivateKeyDer`).
- **Initiator Crypto:** Root Certificate Stores for verifying peers during outbound dials. Support for custom certificate verifiers (e.g., for self-signed P2P networks or custom CA chains).
- **ALPN Protocols:** Application-Layer Protocol Negotiation identifiers.
- **Transport Parameters:** Idle timeouts, maximum concurrent streams, keep-alive limits, and internal channel buffer sizes.

### 3.2. Dual Connection Handlers
Because a node acts as both initiator and responder, the application layer needs to react differently based on how a connection was established. `QuicNode` will support two distinct handler hooks:
- **`on_inbound`:** A `ConnectionHandlerFn` invoked automatically when a new connection is accepted by the endpoint.
- **`on_outbound`:** A `ConnectionHandlerFn` invoked automatically when a call to `connect()` successfully establishes a connection to a peer.

### 3.3. Connection Identity & The `ConnectionManager`
The `ConnectionManager` is the internal source of truth for active peers.
- **Stable ID Tracking:** QUIC connections are tracked using `quinn`'s `stable_id`. This ID is guaranteed to be unique for the lifetime of the `quinn::Endpoint`.
- **Generic Metadata:** The `ConnectionManager` and `QuicNode` must remain generic over a `ConnectionMetadata: Default + Send + Sync + 'static` type. This allows higher-level crates to attach custom tracking data (like a mapped UUID, auth state, or routing hints) to the connection wrapper.
- **Stream Retrieval:** `QuicNode` will provide direct methods to request new streams on an existing connection using its ID:
  - `node.get_unistream(&id) -> Result<ServerUniStream, NodeStreamError>` (Note: The return type name might need renaming to `SendUniStream` to maintain symmetry).
  - `node.get_bistream(&id) -> Result<BiStream, NodeStreamError>`.

### 3.4. Connection Lifecycle APIs
The `QuicNode` API will expose:
- `listen()`: An async loop that continuously accepts incoming connections, spawns the `on_inbound` handler, and registers the connection with the `ConnectionManager`.
- `connect(addr, server_name)`: An async method that dials a peer. Upon success, it spawns the `on_outbound` handler, registers the connection, and returns the assigned `stable_id`.

---

## 4. Re-wiring `stric-tower`

`stric-tower` provides an HTTP-like, Axum-inspired routing layer over `stric-core`. Modifying `stric-core` requires careful re-wiring of `stric-tower` to ensure no logical or use-case capabilities are lost.

### 4.1. Adapting `TowerConnectionHandler`
The `TowerConnectionHandler` currently implements the logic to convert a QUIC connection into a stream of request/response envelopes. This logic remains sound.
- **Change:** It must be updated to implement the generic `ConnectionHandlerFn` required by `QuicNode`, rather than `ServerInstance`.

### 4.2. Adapting `stric_tower::Server`
The high-level `Server` helper provides ergonomic binding for Tower services.
- **Change:** The `Server::serve()` method will internally instantiate a `QuicNode` using a dynamically generated `NodeConfig`.
- **Binding:** It will register the `TowerConnectionHandler` exclusively using `node.on_inbound(...)`, as the Tower server acts purely as a responder.

### 4.3. Adapting `TowerClientService`
The client implementation in `stric-tower` allows sending requests over a `BiStream`.
- **Change:** The client connection logic (which currently manually utilizes `quinn::Endpoint`) can optionally be updated to utilize a `QuicNode` and `node.connect()`. This unifies the underlying transport mechanics and provides the client with automatic connection tracking and keep-alive benefits if needed.

### 4.4. Maintaining Naming Compatibility
While internal mechanics change, the front-facing API of `stric-tower` (`Router`, `Json`, `State`, `extractors`, etc.) must remain untouched. The user experience of writing an `async fn` handler and mounting it to a server must not degrade.

---

## 5. Execution Roadmap

1. **Refactor `stric-core` Types:** Rename capability flags and configurations. Implement `NodeConfig`.
2. **Implement `QuicNode`:** Build the symmetric endpoint wrapper, dual handlers, and connection tracking logic.
3. **Update `stric-core` Tests:** Ensure the keep-alive mechanism and connection manager work correctly under the new architecture.
4. **Re-wire `stric-tower` Server:** Update the `stric_tower::Server` and `TowerConnectionHandler` to consume the new `QuicNode` API.
5. **Re-wire `stric-tower` Client:** Align the client transport with the new core.
6. **Verify Ecosystem:** Run all `stric-tower` examples and integration tests to guarantee 100% API compatibility for end-users.
