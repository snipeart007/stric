# Stric: High-Performance QUIC & Flow Mesh Networking

Stric is a modern networking ecosystem for Rust, built on top of QUIC. It provides a layered approach to distributed systems, from low-level symmetric transport to high-level mesh-routed data flows.

## Ecosystem

- **`stric-core`**: A symmetric P2P wrapper over `quinn`. Handles connection lifecycles, heartbeats, and raw stream management with role-agnostic terminology (Initiator/Responder).
- **`stric-tower`**: An `axum`-like request-response framework built on `stric-core`, supporting Tower middleware and Axum extractors.
- **`stric-flow`**: A modular, peer-to-peer mesh routing and topic-based messaging engine with Dijkstra shortest-path calculations, stateless packet forwarding, dynamic backpressure, and conflict-free session state reconciliation.

## Getting Started

To explore the ecosystem, check out the individual crates:

- [stric-core](./stric-core/README.md): Low-level symmetric QUIC transport.
- [stric-tower](./stric-tower/README.md): High-level request/response framework.
- [stric-flow](./stric-flow/README.md): Peer-to-peer mesh routing and state sync engine.

To see Stric in action, run the examples:

```bash
# Run the stric-tower echo server
cargo run -p stric-tower --example server
```

## Specifications & Design RFCs

The entire system's design and protocols are fully specified under the [rfcs](./rfcs) directory:

* **Transport & Core:**
  * [RFC 2026-0001: stric-core Symmetric Node Architecture](./rfcs/2026-0001-symmetric-node-architecture.md)
  * [RFC 2026-0002: stric-core Automated Heartbeat and Keep-Alive System](./rfcs/2026-0002-automated-heartbeat-keepalive.md)
* **Services & Middleware:**
  * [RFC 2026-0003: stric-tower Request-Response Service Framework](./rfcs/2026-0003-stric-tower-request-response.md)
  * [RFC 2026-0004: stric-tower HTTP Sandwich and Middleware Adapter](./rfcs/2026-0004-stric-tower-sandwich-model.md)
* **Mesh & Flow Routing (stric-flow):**
  * [RFC 2026-0005: stric-flow Mesh Routing and Dijkstra Spanning Tree forwarding](./rfcs/2026-0005-stric-flow-spanning-tree-routing.md)
  * [RFC 2026-0006: stric-flow Wire Schema and Protocol Specifications](./rfcs/2026-0006-stric-flow-wire-schema-specifications.md)
  * [RFC 2026-0007: stric-flow Engine Concurrency and Mesh Reconciliation](./rfcs/2026-0007-stric-flow-engine-reconciliation.md)

## Project Vision

Stric aims to solve the mismatch between local async task models and network communication by treating data as continuous, stateful **flows** rather than discrete, stateless requests.

## Status

`stric-core`, `stric-tower`, and `stric-flow` are all fully implemented, integrated, and verified to build and test successfully. 

- **`stric-core`** provides the symmetric peer-to-peer QUIC node architecture.
- **`stric-tower`** layers request-response routing, extractors, and Tower middleware support on top of the symmetric transport.
- **`stric-flow`** implements dynamic mesh topology discovery, stateless packet forwarding, flow backpressure, and conflict-free session state reconciliation.

---

**GitHub Description:** Symmetric QUIC transport and mesh-flow networking ecosystem for Rust.
