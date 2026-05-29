# Stric: High-Performance QUIC & Flow Mesh Networking

Stric is a modern networking ecosystem for Rust, built on top of QUIC. It provides a layered approach to distributed systems, from low-level symmetric transport to high-level mesh-routed data flows.

## Ecosystem

- **`stric-core`**: A symmetric P2P wrapper over `quinn`. Handles connection lifecycles, heartbeats, and raw stream management with role-agnostic terminology (Initiator/Responder).
- **`stric-tower`**: An `axum`-like request-response framework built on `stric-core`, supporting Tower middleware and Axum extractors.
- **`stric-flow`** (Upcoming): A generic, mesh-routed overlay network for stateful, coordinated data flows with exact-once delivery and Dijkstra-based pathfinding.

## Project Vision

Stric aims to solve the mismatch between local async task models and network communication by treating data as continuous, stateful **flows** rather than discrete, stateless requests.

## Status

`stric-core` has been recently refactored to support symmetric node architectures, enabling true peer-to-peer communication. `stric-tower` is being re-wired to leverage this new core.

---

**GitHub Description:** Symmetric QUIC transport and mesh-flow networking ecosystem for Rust.
