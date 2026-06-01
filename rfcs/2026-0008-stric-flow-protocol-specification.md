# RFC 2026-0008: stric-flow Protocol Specification

This document specifies the design, wire format, algorithms, and state machines of the `stric-flow` protocol. It serves as a language-agnostic specification to enable building compatible implementations in other runtimes or languages (e.g., Go, C++, Python, TypeScript).

---

## 1. Protocol Overview and Invariants

`stric-flow` is a peer-to-peer overlay protocol designed for multi-hop mesh routing, topic-based pub/sub message dissemination, and conflict-free state synchronization. It runs on top of standard QUIC connections.

### Invariants:
1. **Control / Data Stream Separation**: 
   - Control messages (gossip, handshakes, backpressure, session state changes) are multiplexed onto a single **bidirectional** QUIC stream per peer connection.
   - Application data packets are transmitted over dynamically opened **unidirectional** streams.
2. **Stateless Forwarding**:
   - Intermediate nodes (relays) do not maintain per-flow routing tables. The originating node computes the entire forwarding spanning tree and embeds it into the message header.
3. **Loop Prevention via Monotonic Epochs**:
   - All network state dissemination (topology and subscription filters) is versioned using separate monotonic epochs per node, preventing gossip loops.

---

## 2. Wire Format and Framing

### A. Length-Prefixed Framing
All QUIC stream data is framed with a 4-byte length prefix to prevent packet fragmentation issues:
```
+-----------------------------------+-----------------------------------+
|     Length Prefix (4 Bytes)       |     Protobuf Payload (Var Bytes)  |
|     (32-bit Big-Endian uint32)    |                                   |
+-----------------------------------+-----------------------------------+
```
* **Maximum Frame Size**: $16,777,216$ bytes ($16$ MiB). Implementations MUST close the stream with a protocol error if an incoming frame length exceeds this limit.

### B. Serialized Structures
Messages are encoded using Protocol Buffers v3. Refer to the stric-flow protobuf schemas for details.
* **Control Stream**: Expects `ControlMessage` envelopes.
* **Data Stream**: Expects `Envelope` packets containing a `RoutingHeader` and raw payload bytes.

---

## 3. Connection Lifecycle and Handshake

When a QUIC connection is established, the nodes must perform the `stric-flow` handshake before transmitting any other data.

### Handshake State Machine:

```
   [Disconnected]
         | Connect (ALPN "stric-flow")
         v
   [QUIC Handshake Completed]
         | Open Bidirectional Stream
         v
   [Handshake Phase]
         | Send FlowHandshake -> Read FlowHandshake from peer
         | Verify protocol_version == 1
         | Exchange HandshakeAck (accepted = true)
         v
   [Control Session Established]
         - Start Ping/Pong loop (1Hz)
         - Broadcast Initial Topology & Subscription snapshots
```

1. **Verify ALPN**: The QUIC connection MUST negotiate the ALPN protocol identifier `stric-flow`.
2. **Validate Handshake**:
   - Both sides must transmit a `FlowHandshake` frame containing their stable `node_id`, network `role` (`NodeRole::Flow` or `NodeRole::Aggregator`), and capability maps.
   - If a node receives a version higher than it supports, or incompatible ALPN flags, it MUST return a `HandshakeAck` with `accepted = false` and terminate the connection.

---

## 4. Control Plane Mechanisms

### A. Liveness and RTT Measurement (Ping / Pong)
* Each node runs a periodic 1-second interval loop.
* It sends a `Ping` control message containing `sent_at` (Unix epoch milliseconds).
* The peer immediately replies with a `Pong` control message.
* RTT is calculated as:
  $\text{RTT} = \text{now-ms} - \text{ping-sent-at-ms}$
* The calculated RTT is mapped to the peer link in the topology graph to calculate routing costs.

### B. Topology Gossip Deduplication
Topology state is gossiped across the network using `TopologyUpdate` frames:
1. When a node's local link state changes, it increments its topology epoch and creates a `TopologyUpdate` containing added/removed node and link descriptors.
2. It sends this update to all direct peers.
3. Upon receiving a `TopologyUpdate`, a node tracks `last_epochs` mapped by `origin_node_id`.
4. **Deduplication Check**:
   $\text{incoming-epoch} \gt \text{stored-epoch}$
   If true, the node updates its topology graph, updates the stored epoch, and forwards the update to all active peers except the sender and the origin node. Otherwise, the message is ignored.

### C. Subscription Gossip Deduplication
Subscription filters (topic patterns) are gossiped similarly using `SubscriptionUpdate` frames:
1. When a node adds/removes subscription patterns, it increments its subscription epoch.
2. Direct peers receive this update and track epochs in a `last_subscription_epochs` map.
3. **Deduplication Check**:
   $\text{incoming-sub-epoch} \gt \text{stored-sub-epoch}$
   If true, the node updates its graph node capabilities (adding or removing prefix entries like `sub:<pattern>`), stores the new epoch, and forwards the update to all other peers.

---

## 5. Mesh Routing and Spanning Tree Computation

### A. DHT XOR Distance Metric
Nodes maintain a Kademlia DHT routing table for node discovery.
* **ID Space**: Node IDs are hashed into 256-bit space using SHA-256:
  $\text{hash}(ID) \in [0, 2^{256} - 1]$
* **Distance Metric**: The logical distance $d$ between two nodes is defined as the bitwise XOR of their hashes:
  $d(x, y) = \text{hash}(x) \oplus \text{hash}(y)$
* **Bucket Assignment**: The table contains 256 buckets. A peer with distance $d$ goes into bucket index:
  $\text{bucket-idx} = \text{leading-zeros}(d)$
* **Eviction Policy**: Buckets contain at most $K = 20$ nodes. If a bucket is full:
  1. Ping the oldest node in the bucket.
  2. If it responds, keep it, move it to the tail, and drop the new node.
  3. If it fails to respond, evict it and insert the new node at the tail.

### B. Spanning Tree Calculations (Dijkstra)
For a publish request from source $S$ to a set of subscriber nodes $T$:
1. A directed graph $G = (V, E)$ is built from active nodes and links.
2. Edge weight calculation:
   $w(u, v) = \text{hop-cost} + \text{rtt-cost-factor}$
3. **Aggregator Penalty**: If node $v$ has role `NODE_ROLE_AGGREGATOR`, add a cost penalty of $10,000$ to $w(u, v)$ to route traffic around it.
4. Run Dijkstra's algorithm to compute the shortest path tree from source $S$ to all nodes in $T$.
5. Prune all leaves that do not belong to $T$, leaving a minimal forwarding tree.
6. Convert the tree into a map of `forwarding_table` representing parent $\to$ children relationships:
   $\text{forwarding-table}[u] = \{ v_1, v_2, \dots \}$

### C. Wildcard Topic Matching
Topic filters match hierarchical dot-separated patterns:
* `*` matches exactly one level (e.g. `sensor.*` matches `sensor.temperature`).
* `#` matches zero or more trailing levels (e.g. `sensor.#` matches `sensor.temperature.celsius`).
* Match algorithm: Split pattern and topic by `.` and iterate. If `#` is reached in pattern, return `true`. If mismatch occurs, return `false`.

---

## 6. Stateless Packet Forwarding (Data Plane)

### A. Payload Processing and Replication
When publishing a message:
1. Source computes the `forwarding_table`.
2. Encodes the data into an `Envelope` with a `RoutingHeader` containing the table.
3. Sends the envelope over a unidirectional QUIC stream to each next-hop node in `forwarding_table[S]`.

### B. Relay Node Algorithm:
```
On receiving Envelope E on Uni-Stream:
  1. Let self_id = local_node_id.
  2. If E.header.forwarding_table contains self_id:
       Let targets = E.header.forwarding_table[self_id].send_to
       For each next_hop in targets:
         Spawn task to:
           - Wait for backpressure/rate limiting tokens for FlowID
           - Open unidirectional stream to next_hop
           - Write Envelope E to stream
  3. If local subscriptions match E.header.topic_id:
       Decode E.payload using registered codec
       Dispatch to matching local handlers
```

---

## 7. Dynamic Flow Backpressure

To prevent buffer saturation at intermediate hops, nodes enforce token-bucket rate limiting.

### A. Token-Bucket Rate Limiter
* Maintain a token bucket for each flow ID.
* Max rate is configured in bytes/sec. If rate is `0`, rate limiting is disabled.
* `wait_for_bytes(size)` decrements tokens. If insufficient tokens are available, the task yields or sleeps until the bucket replenishes.

### B. Signal Propagation (Pause/Resume/Throttle)
When a receiver detects queue saturation in its handlers:
1. It broadcasts a `BackpressureSignal` containing:
   - `flow_id`
   - `action` (`PAUSE` = 0, `RESUME` = 1, `THROTTLE` = 2)
   - `max_rate` (only used for `THROTTLE`)
2. Upon receiving the signal:
   - `PAUSE`: Set the flow rate limiter state to paused. All calls to `wait_for_bytes` block on a notification channel.
   - `RESUME`: Clear the paused state and trigger waiters via a channel notify.
   - `THROTTLE`: Update the rate limiter's leak rate to `max_rate` bytes/sec.

---

## 8. Shared Sessions and State Reconciliation

### A. Session State Replication
* A logical `Session` has properties: `session_id`, `creator_node`, `flow_ids`, `metadata` map, `state_data` bytes, `state_version` (uint64), and `state_timestamp` (uint64).
* State updates are gossiped as `SessionStateSync` frames.

### B. Conflict Resolution
When a node receives a `SessionStateSync` frame:
1. **Custom Merger**: If a custom StateMergeFn is registered for the session, execute:
   $\text{new-state} = \text{merge-fn}(\text{local-state}, \text{incoming-state})$
   Increment `state_version` and update the timestamp.
2. **LWW (Last-Write-Wins)**: If no custom merger is registered, compare:
   - If $\text{incoming-timestamp} \gt \text{local-timestamp}$, accept the incoming state.
   - If timestamps are equal and $\text{incoming-version} \gt \text{local-version}$, accept the incoming state.
   - Otherwise, reject/drop the update.

### C. Garbage Collection (GC)
To prevent memory exhaustion:
1. Nodes track the last heartbeat/ping received time for all other nodes in `node_last_seen`.
2. A background timer runs every 10 seconds.
3. For each session:
   - Locate `creator_node` in `node_last_seen`.
   - If $\text{now} - \text{last-seen} \gt \text{session-ttl}$ (default: 300 seconds), evict the session.
   - Broadcast a `SessionClose` message to propagate the eviction globally.
