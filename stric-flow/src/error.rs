use thiserror::Error;

/// Represents the set of possible errors returned by Stric Flow node operations,
/// covering transport issues, serialization, handshake failures, and routing.
#[derive(Error, Debug)]
pub enum FlowError {
    /// An error originating from the underlying QUIC transport or listener.
    #[error("Transport layer error: {0}")]
    Transport(#[from] stric_core::NodeStreamError),

    /// A failure when decoding a Protobuf message payload.
    #[error("Serialization/Decoding error: {0}")]
    Decode(#[from] prost::DecodeError),

    /// A failure when encoding a Protobuf message payload.
    #[error("Serialization/Encoding error: {0}")]
    Encode(#[from] prost::EncodeError),

    /// The remote peer rejected the handshake or failed to complete control stream negotiation.
    #[error("Handshake error: {0}")]
    Handshake(String),

    /// A failure during TLS client/server identity verification.
    #[error("Identity verification error: {0}")]
    Identity(String),

    /// An error related to shared state sessions or conflict resolution merges.
    #[error("Session management error: {0}")]
    Session(String),

    /// The requested message type is missing from the registry or failed to decode.
    #[error("Message registry error: {0}")]
    Registry(String),

    /// Pathfinding or routing graph pre-computation failed for the mesh topology.
    #[error("Routing/Pathfinding error: {0}")]
    Routing(String),

    /// An internal thread or coordination task failed unexpectedly.
    #[error("Internal engine error: {0}")]
    Internal(String),

    /// Connection tracking or manager manipulation encountered an issue.
    #[error("Connection management error: {0}")]
    Connection(String),

    /// A generic wrapper error for external anyhow diagnostics.
    #[error("Anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}
