/// A wrapper around a QUIC connection that includes context and metadata.
///
/// This struct provides a high-level view of a single connection, combining the underlying
/// `quinn` connection with Stric-specific metadata and context flags.
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
#[derive(Clone, Copy, Default)]
pub struct ConnectionContext {
    /// The stable ID of the connection, typically assigned by `quinn`.
    pub id: u64,
    /// Whether to keep the connection alive using heartbeat pings.
    pub keep_alive: bool,
    /// Whether the client initiated a unidirectional stream.
    pub client_uni: bool,
    /// Whether the client initiated a bidirectional stream.
    pub client_bi: bool,
    /// Whether the server initiated a unidirectional stream.
    pub server_uni: bool,
    /// Whether the server initiated a bidirectional stream.
    pub server_bi: bool,
}
