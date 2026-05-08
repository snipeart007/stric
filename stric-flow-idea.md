# The Definitive Architecture of stric-flow

## 1. Executive Summary: The Global Mesh Paradigm

`stric-flow` is not merely a framework for sending messages; it is an overarching, intelligent data-transport fabric. It is designed to act as a **generic, mesh-routed overlay network** built upon the robust peer-to-peer transport primitives provided by `stric-core`. 

The primary mandate of `stric-flow` is **Exact-Once, Optimized Delivery**. It assumes a distributed topology where nodes are interconnected in complex graphs. When data is published, the system calculates the optimal path through the network to ensure every interested node receives the data exactly once, minimizing redundant transmissions and completely abstracting the physical network hops away from the application logic.

To achieve this, the architecture is split into modular layers, primarily realized as distinct crates:
- **`stric-flow-core`**: The fundamental types, protobuf definitions, generic context traits, and the base logic for handling structured streams, control flows, and sessions.
- **`stric-flow-node`**: The heavy-lifting coordination engine. This crate houses the Global Graph topology, the Dijkstra pathfinding algorithms, subscription registries, and the advanced transit and routing logic.

---

## 2. Logical Topology: Flows, Topics, and Nodes

The system operates on a distinct logical hierarchy that separates the data being moved from the physical machines moving it.

### 2.1. The Flow (Logical Topologies)
A **Flow** is the highest level of logical organization. It represents a specific data-exchange topology or "domain" of interaction. 
- A Flow is fundamentally a logical construct superimposed over pre-existing physical connections.
- Flows are deeply generic. They are parameterized over the **MessageType** (the shape of the data) and the **NodeContext** (the metadata describing the participants).
- A single physical cluster of nodes can maintain multiple, completely independent Flows simultaneously.

### 2.2. Topics (Data Entry & Subscription Points)
**Topics** are the specific channels of data nested *within* a Flow. 
- Topics represent the actual data streams.
- A node participates in a Flow by **subscribing** to specific Topics. 
- Topics dictate the routing requirements: if Node A subscribes to "telemetry.v1" within Flow X, the network must ensure any data published to that Topic reaches Node A.

### 2.3. Node Types and Contexts
The network consists of nodes that implement generic traits to define their behavior and capabilities.
- **Generic `NodeContext`:** The system is generic over the `NodeContext`. Each specific network implementation defines its own context to store static, domain-specific information (e.g., geographic region, CPU capacity, hardware capabilities). This context is broadcasted and shared to aid in intelligent pathfinding.
- **The `AggregatorNode`:** A specialized variant of a node explicitly designed for **data collection and storage**. 
  - *Subscription Bias:* AggregatorNodes subscribe to vast amounts of data for archiving or downstream processing.
  - *Transit Avoidance:* Because their primary role is storage, they must not be burdened with the high CPU and bandwidth costs of forwarding packets. The mesh routing algorithms explicitly penalize paths that transit through an AggregatorNode unless it is a dead-end or there is absolutely no other path available.

---

## 3. Mesh Routing and the Global Graph

The defining feature of `stric-flow-node` is its intelligent routing engine. Nodes do not just broadcast data; they calculate deliberate paths.

### 3.1. Topology Awareness
Every node maintains a localized view of the **Global Interconnection Graph**.
- Nodes declare their direct physical connections.
- Nodes declare the Topics they require (subscriptions).
- This state is gossiped or synchronized across the network using the dedicated Control Flows.

### 3.2. Exact-Once Pathfinding (Dijkstra)
When a node produces a message for a Topic:
1. It queries the Global Graph for all nodes subscribed to that Topic.
2. It utilizes Dijkstra's algorithm (or a specialized spanning-tree derivative) to compute a delivery tree that reaches all subscribers.
3. The calculation optimizes for latency, bandwidth, and node types (e.g., avoiding AggregatorNodes for transit).
4. The calculated path (or routing instructions) is embedded into the message's metadata.

### 3.3. Transit Integrity
Nodes play a cooperative role in the mesh:
- **Forwarding Mandate:** If a node receives a message and it is on the calculated transit path for that message, it **MUST** forward the data to the next hop. This mandate applies regardless of whether the transit node itself subscribes to the topic, and regardless of any application-level time limits.

### 3.4. Data Merging and Transmission Efficiency
To conserve bandwidth, the `stric-flow` engine performs aggressive data deduplication at the transmission layer.
- If the routing algorithm dictates that Node A must send Message M to Node B because Node B requires it for Flow X/Topic 1 AND Flow Y/Topic 2, Node A will **transmit the physical bytes over the connection exactly once**.
- The wire-protocol metadata will instruct Node B to dispatch the single payload to the multiple interested handlers internally.

---

## 4. The Wire Protocol and Self-Describing Streams

Data movement over `stric-core`'s raw QUIC streams is highly structured.

### 4.1. Generic Message Wrapping
Every piece of data is encapsulated in a generic wire-protocol envelope (defined in Protobuf).
- **Message Identity:** The envelope contains the string name of the internal Protobuf message, allowing generic deserialization.
- **Routing Metadata:** The envelope contains the computed path, flow ID, topic ID, and source information.
- **Payload:** The raw, serialized bytes of the generic `MessageType`.

### 4.2. The Dedicated Control Flow
Application data and network control signals must never block each other.
- **Out-of-Band Signaling:** Every pair of physically connected nodes opens and maintains exactly **one dedicated, high-priority Bidirectional Stream**. This is the Control Flow.
- **Usage:** This stream is used exclusively for `stric-flow` internals:
  - Topology gossiping (NodeContext updates, connection declarations).
  - Subscription requests (Topic join/leave).
  - Backpressure signaling.

---

## 5. Session Coordination and Time-Awareness

`stric-flow` is designed for stateful, time-sensitive interactions, completely abandoning the stateless request-response model.

### 5.1. Session Persistence
A **Session** binds multiple flows and interactions together.
- Sessions exist independently of connections. They have stable `SessionID`s.
- State can be synchronized across nodes participating in the session (snapshots, incremental diffs, conflict reconciliation).

### 5.2. Application-Level Backpressure
QUIC provides byte-level flow control, but `stric-flow` provides **Intent-Level Flow Control**.
- Using the dedicated Control Flow stream, a receiver can send `PAUSE(FlowID)` or `THROTTLE(FlowID)` messages.
- This allows a node to stop incoming data for a specific logical flow without closing the underlying stream or affecting other flows sharing the physical connection.

### 5.3. Time-Bound Operations & Deadlines
The network supports strict, time-aware semantics.
- **Sender-Enforced Deadlines:** The responsibility for adhering to a deadline lies entirely with the producer (the sender) of the message. If the time limit expires before transmission, the sender aborts the send.
- **Receiver Compliance:** When a deadline or time-window for a specific context/session expires, the receiver simply halts processing or sending messages related to that context.
- **Transit Immunity:** As stated in the routing rules, an intermediate node acting purely as a transit hop ignores these deadlines. Its sole duty is to move the bytes to the next hop; it does not drop packets based on application-level expiry timers.

### 5.4. Partial Reliability
Because `stric-flow` maps over QUIC, it exposes varying levels of delivery guarantees:
- **Guaranteed Delivery:** Data is sent over standard QUIC streams, ensuring ordered, reliable delivery with built-in retries.
- **Best-Effort:** For highly ephemeral data (like high-frequency telemetry), streams can be aggressively reset or QUIC Datagrams can be utilized to drop old data in favor of low latency.
- **Delay-Tolerant:** Messages can be buffered locally and transmitted when intermittent physical connections are re-established.

---

## 6. Summary of Architectural Separation

This design ensures a clean separation of concerns across the ecosystem:

1. **`stric-core` (The Wrapper):** Knows nothing about topology, topics, or protobufs. It only knows `SocketAddr`, `stable_id`, keep-alives, and how to yield raw streams from a symmetric `QuicNode`.
2. **`stric-flow-core` (The Protocol):** Knows how to serialize/deserialize the generic message envelopes, manage the dedicated Control Flow stream, parse handshakes, and manage local Session state.
3. **`stric-flow-node` (The Intelligence):** Knows the entire network graph. Computes Dijkstra routing paths. Understands `AggregatorNode` transit penalties. Orchestrates exact-once delivery and deduplication across physical connections.
