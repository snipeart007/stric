# stric-flow: implementation Blueprint

## 1. Crate Structure & Dependencies

The system is divided into three crates to separate the wire protocol from the routing logic and traits.

### 1.1. `stric-flow-proto` (The Grammar)
- **Role:** Houses all Protobuf definitions and generated Rust code.
- **Core Types:** `FlowHandshake`, `ControlMessage`, `Envelope`, `RoutingHeader`.
- **Dependencies:** `prost`, `prost-build`.

### 1.2. `stric-flow-core` (The Fundamentals)
- **Role:** Defines the traits and state management used by both producers and consumers.
- **Traits:** 
    - `NodeContext`: User-defined node metadata.
    - `FlowHandler`: Async trait for reacting to incoming flows.
- **State:** `SessionRegistry`, `ControlFlowManager`.
- **Dependencies:** `stric-core`, `stric-flow-proto`, `anyhow`, `async-trait`.

### 1.3. `stric-flow-node` (The Engine)
- **Role:** The heavy-lifting mesh engine.
- **Logic:** `GlobalGraph`, `DijkstraRouter`, `ForwardingEngine`, `AggregatorBiasManager`.
- **Dependencies:** `stric-flow-core`, `petgraph` (for graph logic), `dashmap`, `tokio`.

---

## 2. Wire Protocol & Envelopes

### 2.1. The `Envelope`
Every message sent over a stream is a length-prefixed `Envelope` Protobuf:
```protobuf
message Envelope {
    RoutingHeader header = 1;
    string message_type = 2; // The Protobuf name of the payload
    bytes payload = 3;       // The generic MessageType bytes
}

message RoutingHeader {
    string source_node_id = 1;
    repeated string path = 2; // Computed Dijkstra path
    string flow_id = 3;
    string topic_id = 4;
    uint64 timestamp = 5;
    uint64 deadline = 6;
}
```

---

## 3. Core Trait Definitions

### 3.1. `NodeContext`
```rust
pub trait NodeContext: Send + Sync + Default + 'static {
    fn node_id(&self) -> &str;
    fn capabilities(&self) -> HashMap<String, String>;
}
```

### 3.2. `FlowHandler`
```rust
#[async_trait]
pub trait FlowHandler<M>: Send + Sync {
    async fn on_message(&self, msg: M, ctx: &MessageContext);
    async fn on_flow_closed(&self, flow_id: &str);
}
```

---

## 4. The FlowNode Instance

The `FlowNode` wraps `stric-core::QuicNode` and adds the mesh logic.

```rust
pub struct FlowNode<C, M> 
where C: NodeContext, M: MessageType 
{
    core: QuicNode<C>,
    graph: Arc<GlobalGraph>,
    sessions: DashMap<String, Session>,
    topic_handlers: DashMap<String, Arc<dyn FlowHandler<M>>>,
}
```

### 4.1. Connection Handlers
- **`on_inbound`:** Immediately opens the **Control Flow** stream and exchanges `NodeContext` and Topology updates.
- **`on_outbound`:** Same as inbound; symmetry is maintained.

---

## 5. Routing Logic Implementation

1. **Topology Sync:** Every time a node joins/leaves or a subscription changes, a `TopologyUpdate` message is sent on all peer Control Flows.
2. **Dijkstra Tree:** Using `petgraph`, we compute a spanning tree for a topic's subscribers.
3. **Multi-Hop Forwarding:** The `ForwardingEngine` reads the `RoutingHeader.path`. If the next hop is Node B, it looks up Node B's `ConnectionID` in the registry and writes the `Envelope`.

---

## PENDING ARCHITECTURAL QUESTIONS (Must be resolved before implementation)

1.  **Topology Bootstrapping:** How does a node learn about the initial graph? Should we support a "Seed List" in the configuration, or a DHT-like discovery mechanism?
2.  **Dijkstra Metrics:** What defines a "best path" in your vision? Is it purely the lowest number of hops, or should we factor in dynamic metrics like RTT (latency) or current bandwidth usage reported by nodes?
3.  **Message Uniqueness:** To ensure "Exact-Once" delivery in a mesh, how should we handle loop prevention? Should every message have a `Nonce/UUID` that nodes track in a short-term TTL cache to detect duplicates?
4.  **Generic Message Registry:** How should the system map a Protobuf message name (string) from the handshake to a concrete Rust type at runtime? Should we implement a global `MessageRegistry` macro?
5.  **Transit forwarding & Merging:** If two logical paths overlap on one physical connection (Node A -> Node B), should the "Data Merging" happen at the **Source** (merging headers) or should intermediate **Transit Nodes** also attempt to deduplicate if they receive two identical payloads for different paths?
6.  **Aggregator Bias:** How does an `AggregatorNode` declare its subscription bias? Is it a simple wildcard (e.g., "Flow.*") or a more complex predicate?
7.  **Control Flow Priority:** Since the Control Flow handles backpressure (`PAUSE`/`RESUME`), should it use QUIC's stream priority features to ensure it is always processed before data streams?
8.  **Session Conflict Strategy:** For "Conflict Reconciliation" in state synchronization, should we provide a default (like Last-Writer-Wins) or require the user to provide a merge function?
9.  **Transit Immunity:** You mentioned transit nodes must ignore deadlines. Does this mean transit nodes should have **infinite buffers**, or should they drop data if their internal OS-level buffers are full (standard backpressure)?
10. **Node Identity Proof:** Since `stric-core` handles the TLS, should `stric-flow` perform an additional "Identity Handshake" on the Control Flow to verify the permanent `NodeID` against the certificate?

---
**CRITICAL MANDATE:** No implementation code shall be written until all 10 questions above have been moved from this list into the formal sections of this document.
