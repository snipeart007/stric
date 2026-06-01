# stric-flow: Protocol Schemas & Wire Format Specification

> **Status:** Ratified — all architectural questions resolved.
> **Wire Format:** Protocol Buffers v3 (proto3), length-delimited framing over QUIC streams.
> **Crate Home:** `stric-flow-proto` — all `.proto` files live here and are compiled via `prost-build`.

---

## 1. Framing Convention

Every message on every QUIC stream (both data and control) is framed as:

```
┌──────────────────┬────────────────────────┐
│  length (4 bytes)│  protobuf payload      │
│  big-endian u32  │  (length bytes)        │
└──────────────────┴────────────────────────┘
```

- **Data streams** carry `Envelope` messages.
- **The control stream** carries `ControlMessage` messages.
- The maximum single-frame size is **16 MiB** (16,777,216 bytes). Payloads exceeding this must be chunked at the application layer.

---

## 2. File Layout

```
stric-flow-proto/
├── proto/
│   ├── envelope.proto          # Envelope, RoutingHeader, ForwardingTargets
│   ├── control.proto           # ControlMessage (oneof wrapper)
│   ├── handshake.proto         # FlowHandshake, IdentityProof
│   ├── topology.proto          # TopologyUpdate, NodeDescriptor, LinkDescriptor
│   ├── subscription.proto      # SubscriptionUpdate
│   ├── backpressure.proto      # BackpressureSignal
│   ├── session.proto           # Session lifecycle & state sync
│   └── common.proto            # Shared enums and small types
├── build.rs
├── Cargo.toml
└── src/
    └── lib.rs                  # pub mod generated code
```

---

## 3. Shared Types (`common.proto`)

```protobuf
syntax = "proto3";
package stric.flow;

// ─── Codec Hint ───────────────────────────────────────────────
// Tells the receiver how the `payload` bytes in an Envelope are encoded.
// Users of stric-flow can choose any codec; Protobuf is the default.
enum Codec {
  CODEC_PROTOBUF = 0;    // Default. Payload is a serialized Protobuf message.
  CODEC_JSON     = 1;    // Payload is UTF-8 JSON.
  CODEC_BINCODE  = 2;    // Payload is Rust bincode.
  CODEC_RAW      = 3;    // Payload is opaque application bytes (no schema).
}

// ─── Delivery Guarantee ───────────────────────────────────────
// Encoded in the RoutingHeader so transit nodes know how to handle the envelope.
enum DeliveryMode {
  DELIVERY_GUARANTEED      = 0;  // Ordered, reliable QUIC stream. Default.
  DELIVERY_BEST_EFFORT     = 1;  // QUIC datagram or aggressive stream reset.
  DELIVERY_DELAY_TOLERANT  = 2;  // Local buffer; retry when connection returns.
}

// ─── Node Role ────────────────────────────────────────────────
// Advertised during handshake so the routing engine can apply penalties.
enum NodeRole {
  NODE_ROLE_FLOW       = 0;  // Standard participant (produce, consume, forward).
  NODE_ROLE_AGGREGATOR = 1;  // Storage-biased; routing avoids using as transit.
}
```

---

## 4. Data Envelope (`envelope.proto`)

The `Envelope` is the fundamental unit of data movement. Every application message published to a Topic is wrapped in an Envelope.

```protobuf
syntax = "proto3";
package stric.flow;

import "common.proto";

// ─── Envelope ─────────────────────────────────────────────────
// The top-level data frame sent over data streams.
//
// The `message_type` + `codec` fields together tell the receiver how to
// deserialize `payload`. When codec is PROTOBUF, `message_type` is the
// fully-qualified Protobuf message name looked up in the MessageRegistry.
// When codec is JSON/BINCODE/RAW, `message_type` is an application-defined
// string key that the user's handler can switch on.
message Envelope {
  RoutingHeader header       = 1;
  string        message_type = 2;  // e.g. "myapp.SensorReading"
  Codec         codec        = 3;  // How `payload` is encoded.
  bytes         payload      = 4;  // The serialized user message.
}

// ─── Routing Header ───────────────────────────────────────────
// Metadata stamped by the source node. Transit nodes read this to
// decide where to forward; destination nodes read it for context.
message RoutingHeader {
  string           source_node_id = 1;   // Permanent ID of the originating node.
  string           flow_id        = 2;   // Logical Flow this message belongs to.
  string           topic_id       = 3;   // Topic within the Flow.
  string           session_id     = 4;   // Optional. Empty if not session-scoped.
  string           nonce          = 5;   // UUID for duplicate detection (always present).
  uint64           timestamp      = 6;   // Unix epoch milliseconds at send time.
  uint64           deadline       = 7;   // Unix epoch ms. 0 = no deadline.
  DeliveryMode     delivery_mode  = 8;   // How this envelope should be transported.
  map<string, ForwardingTargets> forwarding_table = 9; // Pre-computed forwarding table. Key is forwarder node ID.
}

// ─── Forwarding Targets ───────────────────────────────────────
// Pre-computed forwarding targets for a specific node in the delivery tree.
// The source runs Dijkstra once and embeds ALL instructions for the entire
// delivery tree in the `forwarding_table` map. Transit nodes perform zero graph
// computation and no linear scanning — they perform a simple O(1) lookup on
// their own node ID in the `forwarding_table` map to retrieve their targets,
// then forward the envelope (unmodified) to each neighbor in `send_to`.
//
// Example: A publishes to subscribers {C, D, E} via transit node B:
//
//   forwarding_table: {
//     "A": { send_to: ["B"] },
//     "B": { send_to: ["C", "D"] },
//     "D": { send_to: ["E"] }
//   }
//
// Node B receives the envelope, performs an O(1) lookup in `forwarding_table` for "B",
// finds ["C", "D"], and forwards the envelope byte-for-byte to C and D.
// No graph lookup, no linear scan, no header rewriting, no re-serialization.
message ForwardingTargets {
  repeated string send_to = 1;  // The direct neighbors to forward the envelope to.
}
```

### 4.1. User Extensibility

Users **never modify** `envelope.proto`. Instead, they define their own `.proto` files (or use JSON/bincode structs) and register them:

```rust
// Example: User-defined Protobuf message
// file: my_app/proto/sensor.proto
//   message SensorReading {
//     string sensor_id = 1;
//     double value     = 2;
//     uint64 timestamp = 3;
//   }

// In Rust, register with the MessageRegistry:
use stric_flow_core::MessageRegistry;

let mut registry = MessageRegistry::new();
registry.register::<SensorReading>("myapp.SensorReading");

// Or via the procedural macro:
#[stric_flow_message(name = "myapp.SensorReading")]
struct SensorReading { /* ... */ }
```

When `codec` is `CODEC_JSON` or `CODEC_BINCODE`, no Protobuf schema is needed for the payload — the `message_type` string is simply a user-defined key:

```rust
// JSON example — no .proto file needed for the payload itself
let envelope = Envelope {
    header: routing_header,
    message_type: "myapp.SensorReading".into(),
    codec: Codec::Json,
    payload: serde_json::to_vec(&my_sensor_reading)?,
};
```

---

## 5. Control Message (`control.proto`)

The control stream uses a single `oneof` wrapper so that all control variants share one parsing entrypoint. Every control frame is length-delimited on the dedicated high-priority bidirectional stream.

```protobuf
syntax = "proto3";
package stric.flow;

import "handshake.proto";
import "topology.proto";
import "subscription.proto";
import "backpressure.proto";
import "session.proto";

// ─── ControlMessage ───────────────────────────────────────────
// The single wrapper sent on the control stream. Exactly one variant
// is populated per frame.
message ControlMessage {
  oneof message {
    FlowHandshake       handshake           = 1;
    HandshakeAck        handshake_ack       = 2;
    IdentityProof       identity_proof      = 3;
    IdentityChallenge   identity_challenge  = 4;
    TopologyUpdate      topology_update     = 5;
    SubscriptionUpdate  subscription_update = 6;
    BackpressureSignal  backpressure        = 7;
    SessionControl      session_control     = 8;
    Ping                ping                = 9;
    Pong                pong                = 10;
  }
}

// ─── Control-level heartbeat ──────────────────────────────────
// Separate from stric-core's transport keep-alive. Used to measure
// RTT for the pluggable routing metric trait.
message Ping {
  uint64 sent_at = 1;  // Unix epoch ms when sender dispatched this ping.
}

message Pong {
  uint64 ping_sent_at = 1;  // Echoed from the Ping.
  uint64 pong_sent_at = 2;  // Unix epoch ms when responder dispatched this pong.
}
```

---

## 6. Handshake & Identity (`handshake.proto`)

When two nodes first connect (via `on_inbound` / `on_outbound`), they immediately open the control stream and exchange a `FlowHandshake`, followed by an optional identity verification.

```protobuf
syntax = "proto3";
package stric.flow;

import "common.proto";

// ─── FlowHandshake ────────────────────────────────────────────
// The very first message sent by both sides on the control stream.
message FlowHandshake {
  uint32            protocol_version = 1;  // Current: 1. Reject if unsupported.
  string            node_id          = 2;  // Permanent, stable node identifier.
  NodeRole          role             = 3;  // FLOW or AGGREGATOR.
  map<string, string> capabilities   = 4;  // User-defined NodeContext key-value pairs.
  repeated string   supported_codecs = 5;  // e.g. ["protobuf", "json", "bincode"]
  repeated string   subscribed_topics = 6; // Topics this node is currently subscribed to.
  IdentityMode      identity_mode    = 7;  // How this node wants to verify identity.
}

// ─── HandshakeAck ─────────────────────────────────────────────
// Sent in response to a FlowHandshake. Confirms acceptance or rejection.
message HandshakeAck {
  bool   accepted       = 1;
  string reject_reason  = 2;  // Non-empty only when accepted == false.
  uint32 protocol_version = 3; // The version the responder will use.
}

// ─── Identity Verification ────────────────────────────────────
// Supports two modes (as per architectural decision #10):
//   1. TLS-based: NodeID is derived from the TLS certificate CN/SAN.
//      No additional messages needed — the handshake `node_id` is
//      verified against the already-established TLS session.
//   2. Challenge-Response: An explicit cryptographic proof on the
//      control stream.

enum IdentityMode {
  IDENTITY_TLS_DERIVED          = 0;  // NodeID comes from TLS cert. No extra messages.
  IDENTITY_CHALLENGE_RESPONSE   = 1;  // Explicit challenge-response on control stream.
}

// Sent by the verifier to request proof of identity.
message IdentityChallenge {
  bytes challenge_nonce = 1;  // Random bytes the prover must sign.
}

// Sent by the prover in response to the challenge.
message IdentityProof {
  string node_id   = 1;  // The NodeID being proven.
  bytes  signature = 2;  // Signature over the challenge_nonce using the node's private key.
  bytes  public_key = 3; // The public key corresponding to the node's identity.
}
```

### 6.1. Handshake Sequence

```
  Initiator                          Responder
     │                                   │
     │──── FlowHandshake ──────────────▶│
     │                                   │
     │◀──── FlowHandshake ─────────────│  (both sides send simultaneously)
     │                                   │
     │◀──── HandshakeAck ──────────────│
     │──── HandshakeAck ──────────────▶│
     │                                   │
     │  ┌─── if identity_mode == CHALLENGE_RESPONSE ───┐
     │  │                                               │
     │  │◀── IdentityChallenge ────────│               │
     │  │─── IdentityProof ───────────▶│               │
     │  │                                               │
     │  │──  IdentityChallenge ───────▶│               │
     │  │◀── IdentityProof ────────────│               │
     │  └───────────────────────────────────────────────┘
     │                                   │
     │  ══════ Control stream active ══════
```

---

## 7. Topology Gossiping (`topology.proto`)

Nodes exchange topology state so every node can build a local copy of the Global Interconnection Graph.

```protobuf
syntax = "proto3";
package stric.flow;

import "common.proto";

// ─── TopologyUpdate ───────────────────────────────────────────
// Sent on the control stream whenever a node's view of the network changes.
// Each update is a delta: it describes nodes and links that were added or removed.
message TopologyUpdate {
  string               origin_node_id = 1;  // Who generated this update.
  uint64               epoch          = 2;  // Monotonically increasing version counter.
  repeated NodeDescriptor nodes_added   = 3;
  repeated string         nodes_removed = 4;  // Node IDs that left the network.
  repeated LinkDescriptor links_added   = 5;
  repeated LinkRemoved    links_removed = 6;
}

// ─── NodeDescriptor ───────────────────────────────────────────
// Describes a node in the mesh. Gossiped so all nodes can build the graph.
message NodeDescriptor {
  string              node_id      = 1;
  NodeRole            role         = 2;
  map<string, string> capabilities = 3;  // User-defined NodeContext metadata.
  uint64              last_seen    = 4;  // Unix epoch ms of last heartbeat.
}

// ─── LinkDescriptor ───────────────────────────────────────────
// Describes a physical connection between two nodes.
message LinkDescriptor {
  string node_a     = 1;
  string node_b     = 2;
  uint32 hop_cost   = 3;  // Base cost for Dijkstra (default: 1).
  uint64 rtt_micros = 4;  // Optional. Round-trip time in microseconds (0 = unknown).
}

// ─── LinkRemoved ──────────────────────────────────────────────
message LinkRemoved {
  string node_a = 1;
  string node_b = 2;
}
```

### 7.1. Gossip Propagation

When a node receives a `TopologyUpdate`:
1. It compares the `epoch` against its local knowledge of that `origin_node_id`.
2. If the epoch is newer, it merges the delta into its local graph.
3. It re-broadcasts the update to all other connected peers (except the sender).
4. If the epoch is stale, the update is silently dropped.

---

## 8. Subscription Management (`subscription.proto`)

Nodes subscribe and unsubscribe from Topics. These signals propagate through the mesh so the routing engine knows which nodes need data.

```protobuf
syntax = "proto3";
package stric.flow;

// ─── SubscriptionUpdate ───────────────────────────────────────
message SubscriptionUpdate {
  string                    node_id = 1;  // The node changing its subscriptions.
  repeated SubscriptionEntry entries = 2;
}

message SubscriptionEntry {
  string            flow_id  = 1;
  string            pattern  = 2;  // Topic pattern. Supports wildcards: "*" (one level), "#" (multi-level).
  SubscriptionAction action  = 3;
}

enum SubscriptionAction {
  SUBSCRIBE   = 0;
  UNSUBSCRIBE = 1;
}
```

### 8.1. Wildcard Pattern Matching

Topic patterns follow MQTT-style conventions (as per architectural decision #6):

| Pattern | Matches | Does Not Match |
|:--|:--|:--|
| `sensor.*` | `sensor.temperature`, `sensor.humidity` | `sensor.temperature.celsius` |
| `sensor.#` | `sensor.temperature`, `sensor.temperature.celsius` | `actuator.motor` |
| `sensor.*.celsius` | `sensor.temperature.celsius` | `sensor.temperature` |
| `*` | Any single-level topic | Multi-level topics |
| `#` | Everything | — |

---

## 9. Backpressure Signals (`backpressure.proto`)

Intent-level flow control, sent on the high-priority control stream so signals bypass congested data streams (architectural decision #7).

```protobuf
syntax = "proto3";
package stric.flow;

// ─── BackpressureSignal ───────────────────────────────────────
message BackpressureSignal {
  string              flow_id  = 1;  // The logical Flow being throttled.
  string              topic_id = 2;  // Optional. Empty = entire flow. Non-empty = specific topic.
  BackpressureAction  action   = 3;
}

enum BackpressureAction {
  PAUSE    = 0;  // Stop sending data for this flow/topic.
  RESUME   = 1;  // Resume sending data.
  THROTTLE = 2;  // Reduce send rate. Paired with `max_rate` if applicable.
}
```

---

## 10. Session Lifecycle & State Sync (`session.proto`)

Sessions group flows into stateful interaction boundaries with support for state synchronization.

```protobuf
syntax = "proto3";
package stric.flow;

import "common.proto";

// ─── SessionControl ───────────────────────────────────────────
// Manages session lifecycle and state synchronization.
message SessionControl {
  oneof message {
    SessionCreate      create       = 1;
    SessionJoin        join         = 2;
    SessionLeave       leave        = 3;
    SessionClose       close        = 4;
    SessionStateSync   state_sync   = 5;
  }
}

// ─── Session Lifecycle ────────────────────────────────────────

message SessionCreate {
  string          session_id   = 1;  // Globally unique session identifier.
  string          creator_node = 2;  // Node that initiated the session.
  repeated string flow_ids     = 3;  // Flows grouped under this session.
  uint64          created_at   = 4;  // Unix epoch ms.
  map<string, string> metadata = 5;  // User-defined session metadata.
}

message SessionJoin {
  string session_id = 1;
  string node_id    = 2;  // Node requesting to join.
}

message SessionLeave {
  string session_id = 1;
  string node_id    = 2;
}

message SessionClose {
  string session_id  = 1;
  string closed_by   = 2;  // Node that initiated the close.
  string reason      = 3;  // Optional human-readable reason.
}

// ─── State Synchronization ────────────────────────────────────
// Supports snapshots and incremental diffs.
// Default conflict resolution: Last-Writer-Wins (LWW) based on `timestamp`.
// Users can override with a custom merge function in Rust.

message SessionStateSync {
  string    session_id = 1;
  string    sender     = 2;  // Node sending this state.
  SyncMode  mode       = 3;
  uint64    timestamp  = 4;  // Used for LWW conflict resolution.
  uint64    version    = 5;  // Monotonic state version counter.
  bytes     data       = 6;  // Serialized state (snapshot or diff).
  Codec     codec      = 7;  // How `data` is encoded.
}

enum SyncMode {
  SNAPSHOT = 0;  // Full state replacement.
  DIFF     = 1;  // Incremental delta to apply.
}
```

---

## 11. Complete Message Type Summary

| Stream | Proto Wrapper | Variants / Purpose |
|:--|:--|:--|
| **Data stream** | `Envelope` | User application messages with routing metadata |
| **Control stream** | `ControlMessage` | All control variants via `oneof`: |
| | ↳ `FlowHandshake` | Initial capability exchange |
| | ↳ `HandshakeAck` | Accept/reject handshake |
| | ↳ `IdentityChallenge` | Challenge nonce for identity proof |
| | ↳ `IdentityProof` | Cryptographic identity response |
| | ↳ `TopologyUpdate` | Graph delta gossip |
| | ↳ `SubscriptionUpdate` | Topic subscribe/unsubscribe |
| | ↳ `BackpressureSignal` | PAUSE / RESUME / THROTTLE |
| | ↳ `SessionControl` | Session lifecycle + state sync |
| | ↳ `Ping` / `Pong` | RTT measurement for routing metrics |

---

## 12. Wire Protocol Lifecycle (Complete)

```
Phase 1: Transport (stric-core)
  └─ QUIC handshake + TLS ─── connection established

Phase 2: Control Stream Setup
  └─ Open dedicated BiStream (high priority)
  └─ Exchange FlowHandshake (both directions, simultaneously)
  └─ Exchange HandshakeAck
  └─ Optional: IdentityChallenge / IdentityProof (both directions)

Phase 3: Topology Sync
  └─ Exchange TopologyUpdate (full snapshot of known graph)
  └─ Exchange SubscriptionUpdate (current topic subscriptions)

Phase 4: Steady State
  └─ Control stream: ongoing TopologyUpdate, SubscriptionUpdate,
  │   BackpressureSignal, SessionControl, Ping/Pong
  └─ Data streams: Envelope messages (one stream per flow or topic,
      depending on delivery mode)

Phase 5: Teardown
  └─ SessionClose (if session-scoped)
  └─ QUIC connection close (handled by stric-core)
```

---

## 13. Design Principles for User Extensibility

1. **Users never modify stric-flow `.proto` files.** The `Envelope.payload` field is the extension point. Users define their own messages in their own `.proto` files (or use JSON/bincode structs) and register them with the `MessageRegistry`.

2. **The `codec` field is the escape hatch.** By setting `codec = CODEC_JSON` or `CODEC_BINCODE`, users can bypass Protobuf entirely for their payloads. The stric-flow infrastructure treats the payload as opaque bytes in these cases.

3. **The `message_type` string is the dispatch key.** Whether using Protobuf, JSON, or bincode, the `message_type` field is always a user-defined string that the `FlowHandler` uses to determine how to decode and route the payload to the correct application handler.

4. **`NodeDescriptor.capabilities` is the metadata extension point.** Users implement the `NodeContext` trait and advertise arbitrary key-value metadata that is gossiped through the mesh. The routing engine can use this metadata via the pluggable metric trait.

5. **`SessionCreate.metadata` is the session extension point.** Users can attach arbitrary metadata to sessions for application-specific coordination.

---

## 14. Reserved Field Ranges

To allow future protocol evolution without breaking compatibility, the following field number ranges are reserved:

| Message | Reserved Range | Purpose |
|:--|:--|:--|
| `Envelope` | 100–199 | Future envelope-level extensions |
| `RoutingHeader` | 100–199 | Future routing metadata |
| `ControlMessage` | 50–99 | Future control message variants |
| `FlowHandshake` | 50–99 | Future handshake parameters |
| `TopologyUpdate` | 50–99 | Future topology fields |
| `SessionStateSync` | 50–99 | Future sync strategies |
