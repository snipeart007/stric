# RFC 2026-0007: stric-flow Engine Concurrency and Mesh Reconciliation

## 1. Objective
This RFC specifies the operational mechanics of the `stric-flow` mesh engine (`FlowNode`). It covers internal concurrency boundaries, local node APIs, dynamic deserialization, partition recovery state reconciliation, backpressure rate limiters, and dynamic Kademlia DHT node discovery.

---

## 2. The `FlowNode` API and Concurrency Design

### 2.1. Concurrency Boundaries
To maintain high throughput and low latency, `FlowNode` coordinates independent background tasks separated by tokio channels:
1. **Listener Task:** Monitors physical connections and binds peer-specific tasks.
2. **Control Flow Task:** Orchestrates out-of-band topology updates, subscription maps, and session lifecycles.
3. **Forwarding Task:** Runs high-speed $O(1)$ lookups in the forwarding maps of incoming `Envelope` packets, cloning and writing directly to downstream writer channels.

To handle shared state safely without deadlocks:
* The `GlobalGraph` is wrapped in an `Arc<RwLock<GlobalGraph>>`.
* The Dijkstra routing pathfinder acquires a read lock (`.read()`).
* Incoming topology gossips acquire a write lock (`.write()`).

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

### 2.2. Core Data Struct
```rust
pub struct FlowNode<C, M> 
where C: NodeContext, M: Send + Sync + 'static 
{
    core: QuicNode<C>,
    graph: Arc<RwLock<GlobalGraph>>,
    sessions: DashMap<String, Session>,
    merge_fns: DashMap<String, StateMergeFn>,
    topic_handlers: DashMap<String, Arc<dyn FlowHandler<M>>>,
    metric: Arc<dyn RoutingMetric>,
    registry: Arc<MessageRegistry>,
}
```

---

## 3. Dynamic Message Registry & Sessions

### 3.1. Dynamic Message Registry
Allows applications to dynamically register concrete message parsing functions:
```rust
pub struct MessageRegistry {
    parsers: HashMap<
        String, 
        Box<dyn Fn(&[u8]) -> Result<Box<dyn Any + Send + Sync>, String> + Send + Sync>
    >,
}

impl MessageRegistry {
    pub fn register<T>(&mut self, message_type: &str, parser: fn(&[u8]) -> Result<T, String>)
    where T: Send + Sync + 'static;
    pub fn decode(&self, message_type: &str, data: &[u8]) -> Result<Box<dyn Any + Send + Sync>, String>;
}
```

### 3.2. Custom Merge Signature
```rust
pub type StateMergeFn = Box<dyn Fn(&[u8], &[u8]) -> Result<Vec<u8>, String> + Send + Sync>;
```

---

## 4. Partition Recovery & State Reconciliation

### 4.1. Reconnection Backoff
* When connection to a neighbor fails, it is marked as `unreachable` in the `GlobalGraph` (applying routing cost penalties but keeping it in the graph for delta tolerance).
* Reconnection attempts run on an Exponential Backoff strategy with jitter: starts at 1 second, doubling on consecutive failures up to a cap of 60 seconds.

### 4.2. Full Graph Sync (On Reconnect)
Upon successful reconnection, the partition is reconciled via a **Full Graph State Sync**:
1. Nodes exchange their complete topology graphs via a full `TopologyUpdate` snapshot.
2. Link/node descriptors are merged using Last-Writer-Wins (LWW) epoch timestamps.
3. Both nodes exchange their complete active subscription filters (`SubscriptionUpdate`) to refresh Dijkstra trees.

### 4.3. Session Garbage Collection
If a node remains disconnected for longer than `session_ttl` (default: 300 seconds), its dynamic sessions are garbage collected, and remaining nodes propagate an eviction signal.

---

## 5. Backpressure and Discovery

### 5.1. Token-Bucket Rate Limiter
Enforces backpressure when a `THROTTLE` command is received with a `max_rate`:
* The connection writer task tracks transmission credits using an interval-based Token-Bucket.
* For each written `Envelope`, the task spends tokens corresponding to the frame byte size.
* If empty, the task asynchronously yields (`tokio::time::sleep`) until credits reload.
* `PAUSE` blocks transmission entirely until a `RESUME` is received.

### 5.2. Kademlia DHT Discovery
To bootstrap and discover nodes without external dependencies:
* Nodes track physical addresses using a routing table divided into $k$-buckets based on the XOR metric distance of SHA-256 hashed Node IDs.
* Discovery queries (`FindNode`) are transmitted directly as custom control messages over the `stric-core` control stream.
* Query routing directs requests dynamically to nodes closer to the target key.
