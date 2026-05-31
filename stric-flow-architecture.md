# stric-flow: Exhaustive Architecture Blueprint

## 1. Vision: The Autonomous Flow Mesh

`stric-flow` is a high-level, generic communication engine built on the symmetric `stric-core` transport. It transforms a cluster of independent nodes into a coordinated, mesh-routed fabric where data movement is governed by logical topologies rather than physical addresses.

The core mandate is **Exact-Once, Optimized Delivery** across a distributed graph.

---

## 2. Logical Hierarchy

### 2.1. The Flow (Logical Topology)
A **Flow** is a named, logical graph. It defines a set of nodes that are "in-scope" for a specific domain of data exchange. 
- **Parameterization:** Flows are generic over `NodeContext` (node metadata) and `MessageType` (data schema).
- **Isolation:** Messages in Flow A never leak into Flow B, even if they share the same physical connections.

### 2.2. Topics (The Subscription Child)
**Topics** are the actual entry and exit points for data within a Flow.
- **Subscription Model:** Nodes subscribe to Topics to indicate interest.
- **Data Entry:** Producers push messages into a Topic; the mesh handles the rest.

### 2.3. Sessions (The Coordination Boundary)
A **Session** is a stateful context identified by a `SessionID`.
- It groups multiple flows (logical streams) into a single interaction lifecycle.
- It provides primitives for state synchronization (snapshots/diffs) across the participating nodes.

---

## 3. Mesh Routing Dynamics

### 3.1. Topology Awareness
Every node maintains a local representation of the **Global Interconnection Graph**. 
- Nodes exchange physical connection state and topic subscriptions via the dedicated **Control Flow**.
- The graph is used to compute paths dynamically.

### 3.2. Dijkstra-Based Routing
When a message enters the system at Node A for a set of subscribers {B, C, D}:
1. Node A calculates a **Delivery Tree** using a modified Dijkstra algorithm.
2. The algorithm optimizes for path efficiency while respecting node-type penalties (e.g., avoiding `AggregatorNode` for transit).
3. The routing instructions are embedded in the message header.

### 3.3. Transit Integrity & Forwarding
Nodes operate under a **Forwarding Mandate**:
- If a node is an intermediate hop in a calculated path, it MUST forward the bytes to the next hop.
- **Transit Immunity:** Intermediate nodes ignore application-level deadlines and subscription logic; their only job is movement.

---

## 4. Specialized Node Roles

### 4.1. `FlowNode`
The standard participant. It can produce, consume, and forward data. It maintains the full routing table and participates in session coordination.

### 4.2. `AggregatorNode`
A specialized node designed for massive data collection and archiving.
- **Bias:** Subscribes to wide ranges of data.
- **Transit Avoidance:** The routing engine treats AggregatorNodes as high-cost hops to ensure they are not burdened with the CPU/bandwidth cost of mesh-forwarding for others.

---

## 5. Control & Backpressure

### 5.1. Dedicated Control Flow
Every pair of nodes maintains exactly one high-priority, bidirectional QUIC stream reserved for `stric-flow` internal signals.
- **Signals:** PAUSE/RESUME, Topology Gossiping, Subscription Updates.

### 5.2. Application-Level Backpressure
While QUIC handles congestion at the byte level, `stric-flow` handles it at the **Intent level**. A receiver can signal a producer to "PAUSE" a specific logic flow while keeping the physical connection and other flows healthy.

---

## 6. Time-Awareness Semantics

- **Sender Enforcement:** The producer of a message is responsible for deadlines.
- **Receiver Compliance:** When a time-window expires, the receiver halts transmission and processing for that context.
- **Transit Immunity:** Intermediate nodes do not drop packets based on deadlines to prevent inconsistent state across the mesh.

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
