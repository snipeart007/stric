# RFC 2026-0006: stric-flow Wire Schema and Protocol Specifications

## 1. Objective
This RFC specifies the Protobuf schemas (proto3) and connection lifecycle protocol stages for `stric-flow`. All data and control stream communication follows these length-delimited structures over QUIC streams.

---

## 2. Framing and File Layout

* **Framing Convention:** Every message is prefixed by a 4-byte big-endian `u32` length header. The maximum allowed frame size is 16 MiB.
* **Stream Allocations:** Out-of-band controls utilize a dedicated bidirectional QUIC Control Stream (high priority). Logical data flows utilize independent unidirectional or bidirectional QUIC streams.

---

## 3. Data Envelope Schema (`envelope.proto`)

```protobuf
syntax = "proto3";
package stric.flow;

import "common.proto";

message Envelope {
  RoutingHeader header       = 1;
  string        message_type = 2;  // Fully-qualified type name
  Codec         codec        = 3;  // Serialization hint (protobuf, json, bincode, raw)
  bytes         payload      = 4;  // Opaque application payload bytes
}

message RoutingHeader {
  string                         source_node_id = 1;
  string                         flow_id        = 2;
  string                         topic_id       = 3;
  string                         session_id     = 4;
  string                         nonce          = 5;  // UUID for deduplication
  uint64                         timestamp      = 6;  // Unix epoch ms
  uint64                         deadline       = 7;  // Unix epoch ms (0 = infinite)
  DeliveryMode                   delivery_mode  = 8;
  map<string, ForwardingTargets> forwarding_table = 9; // Pre-computed table
}

message ForwardingTargets {
  repeated string send_to = 1; // Downstream neighbors to forward to
}
```

---

## 4. Control Stream Messages (`control.proto`)

The Control Stream transmits a union wrapper message `ControlMessage`:

```protobuf
syntax = "proto3";
package stric.flow;

import "handshake.proto";
import "topology.proto";
import "subscription.proto";
import "backpressure.proto";
import "session.proto";

message ControlMessage {
  oneof payload {
    FlowHandshake      handshake           = 1;
    HandshakeAck       handshake_ack       = 2;
    IdentityChallenge  identity_challenge  = 3;
    IdentityProof      identity_proof      = 4;
    TopologyUpdate     topology_update     = 5;
    SubscriptionUpdate subscription_update = 6;
    BackpressureSignal backpressure        = 7;
    SessionControl     session             = 8;
    Ping               ping                = 9;
    Pong               pong                = 10;
  }
}
```

*For brevity, the exact nested Protobuf schemas for Handshake, Topology, Subscriptions, Backpressure, and Sessions are specified in [stric-flow-protocol-schemas.md](file:///home/snipeart007/repos/stric/stric-flow-protocol-schemas.md), which serves as the concrete wire grammar reference.*

---

## 5. Protocol Lifecycle Stages

```
Phase 1: Transport Connection (stric-core)
  └─ QUIC handshake + TLS authentication ─── Established

Phase 2: Control Stream Setup
  └─ Open bidirectional control stream (high priority)
  └─ Exchange FlowHandshake (simultaneous)
  └─ Exchange HandshakeAck (capabilities accepted)
  └─ IdentityChallenge / IdentityProof exchange (cryptographic verification)

Phase 3: Initial Topology Synchronization
  └─ Exchange TopologyUpdate (full graph snapshot)
  └─ Exchange SubscriptionUpdate (active topic filters)

Phase 4: Steady State Operation
  └─ Control stream: ongoing TopologyUpdates (deltas), SubscriptionUpdates,
     BackpressureSignals (PAUSE/RESUME), SessionControl, and Ping/Pongs
  └─ Data streams: Unmodified Envelope messages routed along calculated paths

Phase 5: Teardown
  └─ Clean session closure messages (SessionClose)
  └─ QUIC connection teardown
```

---

## 6. Reserved Fields
To prevent conflicts during future protocol expansions:
* `Envelope`: Fields 100–199 reserved.
* `RoutingHeader`: Fields 100–199 reserved.
* `ControlMessage`: Fields 50–99 reserved.
