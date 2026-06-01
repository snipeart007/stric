# Stric Flow Crate

`stric-flow` is a modular, high-performance, multi-hop mesh network routing and topic-based messaging engine built on top of the **Stric QUIC network core** (`stric-core`). It coordinates dynamic topology discovery, shortest-path calculation, stateless packet forwarding, flow-level backpressure, and conflict-free state synchronization.

---

## 1. High-Level Architecture

`stric-flow` separates logical communication into two distinct pathways: the **Control Plane** and the **Data Plane**. By utilizing QUIC's multi-streaming capabilities, it prevents control plane bottlenecks (e.g., heartbeats, routing updates) from being blocked by head-of-line congestion on data-heavy streams.

```mermaid
graph TD
    subgraph FlowNode [FlowNode (node.rs)]
        QuicNode[stric-core::QuicNode]
        GlobalGraph[GlobalGraph (routing.rs)]
        RoutingTable[RoutingTable (discovery.rs)]
        SessionsMap[sessions: DashMap<String, Session>]
        MergeFnsMap[merge_fns: DashMap<String, StateMergeFn>]
        TopicHandlers[topic_handlers: DashMap<String, Arc<dyn FlowHandler>>]
        FlowLimiters[flow_limiters: DashMap<String, TokenBucketRateLimiter>]
        Registry[MessageRegistry (registry.rs)]
        ControlLoop[Control Event Loop (run_control_loop)]
    end

    Peer[Remote Peer] <-->|Bidirectional Control Stream| ControlLoop
    Peer <-->|Unidirectional Data Streams| QuicNode
    QuicNode -->|Delivers Payload| TopicHandlers
    QuicNode -->|Routes Forwarding| FlowNode
    ControlLoop -->|Updates Graph| GlobalGraph
    ControlLoop -->|Updates DHT| RoutingTable
    ControlLoop -->|Reconciles State| SessionsMap
```

---

## 2. Core Internal Workings

### A. Connection Negotiation & Handshake
When a new connection is established (either inbound via the listener or outbound via `connect`):
1. **Control Stream Setup**: A single bidirectional QUIC stream is opened between the peers.
2. **Exchange Handshake**: The initiator writes a length-prefixed `FlowHandshake` message containing:
   - Supported protocol version (currently `1`).
   - Permanent `node_id`.
   - Node role (`NodeRole::Flow` or `NodeRole::Aggregator`).
   - Locally subscribed topics and capabilities.
3. **Validation & ACK**: The responder validates the capabilities and version, replying with a `HandshakeAck`. If accepted, connection metadata is initialized, the peer socket address is stored, and the control loop registers the new peer.

### B. Control Plane & Gossip Engine
All control interactions occur asynchronously via the control loop (`run_control_loop`) in [node.rs](file:///home/snipeart007/repos/stric/stric-flow/src/node.rs):
* **Ping / Pong Loop**: To estimate connection quality and RTT, nodes exchange `Ping` and `Pong` control frames every second. The calculated RTT is saved directly into the routing table and used as edge costs in the global graph.
* **Topology Gossip**: When a node joins or leaves, the local node increments its topology epoch and floods a `TopologyUpdate` control message. Nodes track previously seen epochs in `last_epochs` to prevent infinite gossip storms.
* **Subscription Gossip**: When a node subscribes to a topic pattern via `subscribe`, it broadcasts a `SubscriptionUpdate`. An epoch tracking map (`last_subscription_epochs`) dedupes incoming subscription updates to prevent redundant updates.

### C. Mesh Routing & Topic Wildcards
* **DHT Routing Table**: Nodes organize their peer network using a Kademlia-inspired [RoutingTable](file:///home/snipeart007/repos/stric/stric-flow/src/discovery.rs#L23) in [discovery.rs](file:///home/snipeart007/repos/stric/stric-flow/src/discovery.rs). Peer `node_id` strings are hashed via SHA-256 and distance is computed using the XOR metric. Known peers are partitioned into 256 `KBucket`s.
* **Shortest Path Graph**: The [GlobalGraph](file:///home/snipeart007/repos/stric/stric-flow/src/routing.rs#L14) in [routing.rs](file:///home/snipeart007/repos/stric/stric-flow/src/routing.rs) maintains a petgraph directed graph of the entire network. Shortest paths are computed using Dijkstra's algorithm. 
  - To prevent storage-biased transit nodes from being saturated with transit traffic, a path cost penalty (+10,000) is applied to edges leading to nodes with the `NodeRole::Aggregator` role.
* **Topic Wildcard Matching**: Subscriptions support MQTT-style wildcards:
  - `*` matches a single hierarchy level (e.g. `sensor.*` matches `sensor.temp`).
  - `#` matches all remaining levels at the end (e.g. `sensor.#` matches `sensor.temp.celsius`).
  - Implemented in `match_topic`.

### D. Stateless Packet Forwarding
Unlike stateful routing layers that require intermediate nodes to keep track of flow states, `stric-flow` forwards data **statelessly**:
1. When publishing, the source node evaluates `compute_forwarding_table` in `GlobalGraph` to build a directed tree of next-hop targets for all matching subscribers.
2. This tree is serialized into the `forwarding_table` map inside the data [Envelope](file:///home/snipeart007/repos/stric/stric-flow/src/proto.rs)'s `RoutingHeader`.
3. When a relay node receives the envelope on a unidirectional data stream, it extracts its own node's forwarding rules.
4. If it has downstream targets, it opens new unidirectional streams to those targets, copies the envelope, and forwards it.
5. If the topic matches local subscriptions, the message is decoded and delivered to local topic handlers.

### E. Outbound Flow Control & Backpressure
Outbound transmission speeds are managed via a token-bucket rate limiter:
* **TokenBucketRateLimiter**: Implemented in [backpressure.rs](file:///home/snipeart007/repos/stric/stric-flow/src/backpressure.rs) using the `governor` crate.
* **Backpressure Signals**: If a node gets congested or lags behind processing incoming queues, it broadcasts a `BackpressureSignal` containing a `BackpressureAction`:
  - `PAUSE`: Outbound queues block on `wait_for_bytes` indefinitely.
  - `RESUME`: Unblocks the rate limiter via a `tokio::sync::Notify` trigger.
  - `THROTTLE`: Dynamically rebuilds the token quota limit to restrict transmission to `max_rate` bytes/sec.

### F. Shared Sessions & State Reconciliation
* **Reconciliation Engine**: [reconciliation.rs](file:///home/snipeart007/repos/stric/stric-flow/src/reconciliation.rs) supports shared application sessions (`Session`). Changes to session states are propagated via `SessionStateSync` messages.
* **Conflict Resolution**:
  - *Last-Write-Wins (LWW)*: By default, states are overwritten if the incoming version is greater, or if the timestamps are newer.
  - *Pluggable Merging*: Applications can register a [StateMergeFn](file:///home/snipeart007/repos/stric/stric-flow/src/reconciliation.rs#L8) to merge state payloads semantically (e.g., merging key-value maps or delta sets).
* **Garbage Collection**: A background task periodically invokes `gc_inactive_sessions` every 10 seconds. If a session's creator node is undetected/offline for longer than the configured session TTL (default: 300s), the session is evicted locally and a `Close` control event is broadcast.

---

## 3. Module Reference

* **[node.rs](file:///home/snipeart007/repos/stric/stric-flow/src/node.rs)**: The entry point. Manages the QUIC node initialization, connections, control loops, publication pipelines, and session management.
* **[routing.rs](file:///home/snipeart007/repos/stric/stric-flow/src/routing.rs)**: Graph database modeling, Dijkstra calculations, forwarding path mapping, and MQTT wildcard matching.
* **[discovery.rs](file:///home/snipeart007/repos/stric/stric-flow/src/discovery.rs)**: Kademlia XOR routing table logic and bucket maintenance.
* **[reconciliation.rs](file:///home/snipeart007/repos/stric/stric-flow/src/reconciliation.rs)**: Versioned state synchronization, session creation/GC, and exponential backoff configuration.
* **[backpressure.rs](file:///home/snipeart007/repos/stric/stric-flow/src/backpressure.rs)**: Governor token-bucket limits, thread notifications, and pausing.
* **[registry.rs](file:///home/snipeart007/repos/stric/stric-flow/src/registry.rs)**: Generic type-erased registry mapping message name strings to parsing closures.
* **[frame.rs](file:///home/snipeart007/repos/stric/stric-flow/src/frame.rs)**: Length-prefixed stream parser that handles frame safety limits up to 16 MiB.

---

## 4. Simulation Mesh Topology

To verify multi-hop routing at scale, the `large_network_simulation` example builds a complex 40-node mesh based on three main topology rules:
1. **Ring Connections**: Every node `i` is connected to node `i+1` (for `0 <= i < 39`) to guarantee complete network connectivity.
2. **Chord +5**: Every node `i` divisible by 3 is connected to `(i + 5) % 40` (where `target > i`).
3. **Chord +13**: Every node `i` divisible by 7 is connected to `(i + 13) % 40` (where `target > i`).

Below is the visualized network topology for this 40-node simulation:

![Large Network Simulation Mesh](./examples/large_network_simulation_mesh.svg)

