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
