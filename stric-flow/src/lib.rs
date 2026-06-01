//! Stric Flow is a modular, high-performance topic-based messaging and state reconciliation
//! engine built on top of the Stric QUIC network core.
//!
//! It provides:
//! - Mesh graph topology discovery (via Kademlia DHT routing tables).
//! - Shortest-path routing pre-computation (using Dijkstra).
//! - Stateless packet forwarding over unidirectional streams to avoid redundant tree updates.
//! - Dynamic flow backpressure (Pause/Resume/Throttle) to handle network congestion.
//! - Shared session management with Conflict-Free Replicated State Sync (LWW or custom state merges).

/// Generated protobuf types and serialization structs.
pub mod proto;

/// Error types and variants returned by Stric Flow operations.
pub mod error;

/// Dijkstra shortest-path mesh routing and topic subscription wildcard matching.
pub mod routing;

/// Dynamically extensible dynamic decoder mapping for user application message types.
pub mod registry;

/// Token-bucket flow rate-limiting and pausing utilities for congestion management.
pub mod backpressure;

/// Kademlia XOR metric DHT-based node discovery routing tables.
pub mod discovery;

/// Session replication, garbage collection, and state reconciliation utilities.
pub mod reconciliation;

/// The primary coordinator coordinating QUIC listener, handshake negotiation, control engine loops, and publishers.
pub mod node;

/// Length-prefixed framing reader/writer helpers for streams.
pub mod frame;

