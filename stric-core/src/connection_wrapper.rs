/// A wrapper around a QUIC connection that includes context and metadata.
///
/// This struct provides a high-level view of a single connection, combining the underlying
/// `quinn` connection with Stric-specific metadata and context flags.
///
/// `ConnectionWrapper` is intended for use inside connection handlers registered
/// through [`QuicNode`](crate::QuicNode).
/// It should not be treated as a long-lived connection registry entry; use
/// [`ConnectionManager`](crate::ConnectionManager) for post-registration tracking.
#[derive(Clone)]
pub struct ConnectionWrapper<ConnectionMetadata: Default + Send + Sync + 'static> {
    /// The underlying `quinn::Connection`.
    pub conn: quinn::Connection,
    /// Configuration and state flags for this connection.
    pub context: ConnectionContext,
    /// User-defined metadata associated with this connection.
    pub metadata: ConnectionMetadata,
}

/// Configuration and state flags for a connection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionContext {
    /// The stable ID of the connection, typically assigned by `quinn`.
    pub id: u64,
    /// Whether to keep the connection alive using heartbeat pings.
    pub keep_alive: bool,
    /// Whether the initiator of the connection is allowed to open unidirectional streams.
    pub initiator_uni: bool,
    /// Whether the initiator of the connection is allowed to open bidirectional streams.
    pub initiator_bi: bool,
    /// Whether the responder of the connection is allowed to open unidirectional streams.
    pub responder_uni: bool,
    /// Whether the responder of the connection is allowed to open bidirectional streams.
    pub responder_bi: bool,
}
