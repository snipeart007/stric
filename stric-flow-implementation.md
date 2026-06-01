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

### 3.3. Pluggable `RoutingMetric`
```rust
pub trait RoutingMetric: Send + Sync {
    /// Computes the cost of traversing the physical connection edge between two nodes.
    /// The routing engine chooses the path with the lowest overall cost.
    fn cost(&self, from: &str, to: &str, graph: &GlobalGraph, env: &Envelope) -> f64;
}

/// Default metric implementation based purely on hop count.
pub struct HopCountMetric;
impl RoutingMetric for HopCountMetric {
    fn cost(&self, _from: &str, _to: &str, _graph: &GlobalGraph, _env: &Envelope) -> f64 {
        1.0
    }
}
```

### 3.4. Dynamic `MessageRegistry`
Provides a central lookup system mapping `message_type` string descriptors to dynamic parser functions for zero-code runtime decoding.
```rust
pub struct MessageRegistry {
    parsers: HashMap<
        String, 
        Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>, String> + Send + Sync>
    >,
}

impl MessageRegistry {
    pub fn new() -> Self {
        Self { parsers: HashMap::new() }
    }

    /// Registers a parser function for a given message type name.
    pub fn register<T>(&mut self, message_type: &str, parser: fn(&[u8]) -> Result<T, String>)
    where 
        T: Send + Sync + 'static 
    {
        self.parsers.insert(
            message_type.to_string(),
            Box::new(move |bytes| parser(bytes).map(|val| Box::new(val) as Box<dyn Any + Send + Sync>))
        );
    }

    /// Decodes raw envelope bytes into the registered concrete Rust type.
    pub fn decode(&self, message_type: &str, data: &[u8]) -> Result<Box<dyn Any + Send + Sync>, String> {
        let parser = self.parsers.get(message_type)
            .ok_or_else(|| format!("No parser registered for type '{}'", message_type))?;
        parser(data)
    }
}
```

### 3.5. Session Conflict Merge Signature
Allows users to register custom state conflict reconciliation logic for session-scoped data.
```rust
pub type StateMergeFn = Box<dyn Fn(&[u8], &[u8]) -> Result<Vec<u8>, String> + Send + Sync>;
```

---

## 4. The FlowNode Instance & API

The `FlowNode` wraps `stric-core::QuicNode` and coordinates the mesh logic, topology state, dynamic sessions, and the routing metrics engine.

```rust
pub struct FlowNode<C, M> 
where C: NodeContext, M: Send + Sync + 'static 
{
    core: QuicNode<C>,
    graph: Arc<RwLock<GlobalGraph>>,
    sessions: DashMap<String, Session>,
    merge_fns: DashMap<String, StateMergeFn>, // Keyed by session payload schema names
    topic_handlers: DashMap<String, Arc<dyn FlowHandler<M>>>,
    metric: Arc<dyn RoutingMetric>,
    registry: Arc<MessageRegistry>,
}
```

### 4.1. Local Application Node API
These methods run on the local `FlowNode` to publish, subscribe, and manage traffic directly.

```rust
impl<C, M> FlowNode<C, M> 
where C: NodeContext, M: Send + Sync + 'static
{
    /// Starts the node, binding to stric-core::QuicNode's incoming connections stream.
    pub async fn start(&self) -> Result<()> {
        let mut connection_events = self.core.incoming_connections().await?;
        
        while let Some(conn) = connection_events.next().await {
            let peer_id = conn.peer_node_id().to_string();
            let graph = self.graph.clone();
            let registry = self.registry.clone();
            
            // Spawn reader/writer tasks for each peer connection
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(conn, graph, registry).await {
                    log::error!("Connection error with peer {}: {}", peer_id, e);
                }
            });
        }
        Ok(())
    }

    /// Subscribes to a topic pattern. Returns a tokio channel stream of incoming messages
    /// and triggers a SubscriptionUpdate broadcast on the mesh control flow.
    pub async fn subscribe(&self, topic_pattern: &str) -> Result<impl tokio_stream::Stream<Item = (M, MessageContext)>> {
        // Add pattern to local subscriptions, send SubscriptionUpdate, return receiver stream.
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    /// Unsubscribes from a topic pattern and broadcasts the subscription deletion.
    pub async fn unsubscribe(&self, topic_pattern: &str) -> Result<()> {
        // Update local maps and send SubscriptionUpdate with UNSUBSCRIBE action.
        Ok(())
    }

    /// Publishes a message to a topic. Computes routing path via Dijkstra metric, 
    /// builds the forwarding map, wraps payload in Envelope, and writes downstream.
    pub async fn publish(&self, flow_id: &str, topic: &str, msg: M, delivery: DeliveryMode) -> Result<()> {
        // Run Dijkstra over the graph using self.metric, construct forwarding map, transmit.
        Ok(())
    }
}
```

### 4.2. Connection Handlers
- **`on_inbound`:** Immediately opens the **Control Flow** stream and exchanges `NodeContext` and Topology updates.
- **`on_outbound`:** Same as inbound; symmetry is maintained.

### 4.3. Concurrency & Internal Task Layout
To prevent latency spikes and lock contention, processing is distributed across specialized background tasks:

1. **Listener Task:** Monitors physical connections and registers reader/writer tasks per peer node connection.
2. **Control Flow Manager Task:** Handles the bidirectional control stream for topology, subscription, and session updates.
3. **Forwarding Engine Task:** Processes incoming `Envelope` packets:
   - Performs a read lock (`.read()`) on the `GlobalGraph` when verifying hops or building routing trees.
   - Performs a quick $O(1)$ key lookup on its own ID in the map-based `forwarding_table`.
   - Clones and dispatches the raw unmodified packet to downstream peer connection writer queues if listed.
   - Decodes via `MessageRegistry` and pushes to the subscriber's tokio channel if subscribed.

```mermaid
graph TD
    IncomingQUIC[Incoming QUIC Stream] -->|Read length-prefixed bytes| ConnTask[Connection Reader Task]
    ConnTask -->|ControlMessage| ControlEngine[Control Flow Manager]
    ConnTask -->|Envelope| ForwardingEngine[Forwarding Engine]
    
    ControlEngine -->|TopologyUpdate| GraphWrite[Acquire Graph Write Lock & Update Graph]
    ControlEngine -->|SubscriptionUpdate| SubsUpdate[Update Topic Subscriptions]
    ControlEngine -->|Backpressure PAUSE/RESUME| BPHandler[Throttle Connection Writer]

    ForwardingEngine -->|O(1) Map Lookup for my_node_id| ForwardCheck{Is Forwarder?}
    ForwardCheck -->|Yes| DownstreamQueue[Clone & Push to Downstream Neighbors Connection Queues]
    ForwardCheck -->|No / Complete| LocalCheck{Is Subscriber?}
    
    LocalCheck -->|Yes| Registry[Decode via MessageRegistry]
    Registry -->|Decoded Message| ClientStream[Push to Subscriber tokio::sync::mpsc channel]
    
    DownstreamQueue -->|QUIC Frame| DirectPeer[Outbound QUIC Connection Writer]
```

---

## 5. Routing Logic Implementation

1. **Topology Sync:** Every time a node joins/leaves or a subscription changes, a `TopologyUpdate` message is sent on all peer Control Flows.
2. **Dijkstra Tree:** Using `petgraph` and `FlowNode.metric`, we compute a spanning tree for a topic's subscribers, passing the envelope metadata to the cost function to determine link preference.
3. **Multi-Hop Forwarding:** The `ForwardingEngine` reads the `RoutingHeader.forwarding_table`. It performs an O(1) lookup using its own node ID as the key, then sends the envelope **unmodified** to each `send_to` neighbor. Transit nodes perform zero graph computation — no header rewriting, no re-serialization.

---

## 6. Partition Recovery & State Reconciliation

To ensure the mesh recovers seamlessly from temporary dropouts or network partitions:

### 6.1. Reconnection Backoff
When a peer connection is interrupted, the node initiates reconnection backoff:
* **Algorithm:** Exponential Backoff with jitter.
* **Timing:** Starts at 1 second, doubling on consecutive failures up to a maximum cap of 60 seconds (e.g., 1s, 2s, 4s, 8s ... 60s).
* **State:** During reconnect attempts, the peer is marked as `unreachable` in the local topology graph, penalizing transit routes without triggering instant cluster-wide deletion.

### 6.2. Full State Reconciliation (On Reconnect)
Upon successful reconnection, delta gossips are bypassed in favor of a **Full Graph State Sync**:
1. The reconnected nodes exchange full `TopologyUpdate` snapshots containing every known node and link descriptor in their graphs.
2. Each node merges descriptors based on physical timestamps (Last-Writer-Wins) or monotonically incremented epoch versions.
3. Both nodes sync active subscriptions via a full `SubscriptionUpdate` exchange to recalculate optimal Dijkstra routing structures.

### 6.3. Session State & Garbage Collection
* **Heartbeat Timeout:** If a node remains disconnected/unreachable for longer than the configured `session_ttl` (default: 300 seconds):
  - Its current active sessions are pruned.
  - The remaining active nodes execute an automatic `SessionLeave` or garbage-collect outstanding stale state records.
  - A clean eviction notification is propagated across the surviving cluster members.

### 6.4. Backpressure & Token-Bucket Rate Limiting
Enforces traffic control policies dynamically when throttling is triggered:
1. **PAUSE / RESUME:** If a `PAUSE` signal is received for a topic/flow, the writer task suspends transmission of matching `Envelope` frames by waiting on a tokio notification flag. Transmission resumes immediately upon receipt of `RESUME`.
2. **THROTTLE:** When `THROTTLE` is received with a `max_rate` (messages/sec or bytes/sec):
   - The outbound connection task initializes a Token-Bucket rate limiter (e.g. using `governor` or a native interval-based credit reload).
   - Before writing each frame, the task must acquire tokens matching the packet size. If insufficient tokens are available, the task asynchronously yields (`tokio::time::sleep`) until enough tokens reload.

### 6.5. Lightweight Kademlia DHT Node Discovery
To discover nodes dynamically in a large mesh:
1. **Bootstrap Seeds:** On startup, a node connects to a pre-configured seed list of static IP/port addresses.
2. **Kademlia Routing Table:** Each node maintains a routing table split into $k$-buckets based on the XOR metric distance between Node IDs (SHA-256 hashes of the permanent `NodeID`).
3. **Query Message Routing:** Nodes find peers by sending `FindNode` queries on the Control Flow. Intermediate nodes route these queries to peers closer to the target key.
4. **Lightweight Execution:** Instead of importing a full external network stack, the discovery queries are packetized as specific control message variants over the `stric-core` control stream.

---

## 7. Resolved Architectural Decisions

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
