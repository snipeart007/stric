# stric-flow: implementation Blueprint

## 1. Crate Structure & Dependencies

The system is divided into three crates to separate the wire protocol from the routing logic and traits.

### 1.1. `stric-flow-proto` (The Grammar)
- **Role:** Houses all Protobuf definitions and generated Rust code.
- **Core Types:** `FlowHandshake`, `ControlMessage`, `Envelope`, `RoutingHeader`, `ForwardingTargets`.
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
    Codec  codec = 3;        // How payload is encoded (protobuf, json, bincode, raw)
    bytes  payload = 4;      // The generic MessageType bytes
}

message RoutingHeader {
    string source_node_id = 1;
    string flow_id = 2;
    string topic_id = 3;
    string session_id = 4;       // Optional. Empty if not session-scoped.
    string nonce = 5;            // UUID for duplicate detection.
    uint64 timestamp = 6;
    uint64 deadline = 7;         // 0 = no deadline.
    DeliveryMode delivery_mode = 8;
    map<string, ForwardingTargets> forwarding_table = 9;
}

// Pre-computed by the source. Transit nodes perform an O(1) lookup
// using their own node ID as the key to retrieve their forwarding targets.
message ForwardingTargets {
    repeated string send_to = 1;  // The direct neighbors to forward to.
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
3. **Multi-Hop Forwarding:** The `ForwardingEngine` reads the `RoutingHeader.forwarding_table`. It performs an O(1) lookup using its own node ID as the key, then sends the envelope **unmodified** to each `send_to` neighbor. Transit nodes perform zero graph computation — no header rewriting, no re-serialization.

---

## 6. Resolved Architectural Decisions

1. **Topology Bootstrapping:** We implement a hybrid approach where a static "Seed List" of addresses in the node configuration is used to bootstrap and join a dynamic DHT-like discovery network.
2. **Dijkstra Metrics:** The routing engine initially optimizes based on the lowest number of hops, but routes are computed via a pluggable metric trait so that dynamic pathfinding metrics (e.g. RTT, bandwidth) can be seamlessly integrated later.
3. **Message Uniqueness:** Loop prevention and duplicate message detection strategy is defined via a dedicated setting in the `stric-flow` nodes cluster configuration.
4. **Generic Message Registry:** We implement a compile-time procedural macro/builder API to register Protobuf schemas and construct a global `MessageRegistry` mapping message names to their concrete deserializers.
5. **Transit Forwarding & Merging:** Logical path merging and deduplication are done at the source node (e.g., by combining header destinations/lists) to enable intermediate transit nodes to perform simple, stateless forwarding.
6. **Aggregator Bias:** `AggregatorNode` subscription bias is expressed through wildcard topic pattern matching (e.g., standard MQTT/AMQP-like wildcards: `flow.*`, `sensor/#`).
7. **Control Flow Priority:** Internal control flows (PAUSE/RESUME) utilize QUIC's built-in stream priority features to ensure control messages bypass queued data streams.
8. **Session Conflict Strategy:** State synchronization uses a default Last-Writer-Wins (LWW) conflict reconciliation strategy based on physical timestamps, but allows the developer to register a custom merge function.
9. **Transit Immunity:** Intermediate transit nodes apply standard backpressure upstream when their internal memory buffers/queues fill up, but they do not proactively drop or expire packets based on message deadlines.
10. **Node Identity Proof:** The system supports both verification options: an additional cryptographic handshake on the Control Flow to verify the permanent `NodeID` against the certificate, as well as a TLS-based verification option mapping the certificate CN/SAN to the `NodeID` inside `stric-core`.
