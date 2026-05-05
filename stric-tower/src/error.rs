use thiserror::Error;

/// Errors that can occur in the Stric-Tower integration.
#[derive(Debug, Error)]
pub enum TowerError {
    /// An underlying I/O error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// An error occurred while writing to a QUIC stream.
    #[error("QUIC write error: {0}")]
    Write(#[from] quinn::WriteError),

    /// An error occurred while reading from a QUIC stream.
    #[error("QUIC read error: {0}")]
    Read(#[from] quinn::ReadError),

    /// An error occurred while reading a specific number of bytes from a QUIC stream.
    #[error("QUIC read exact error: {0}")]
    ReadExact(#[from] quinn::ReadExactError),

    /// An error occurred on the underlying QUIC connection.
    #[error("QUIC connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    /// The QUIC stream was closed unexpectedly.
    #[error("QUIC stream closed")]
    Closed(#[from] quinn::ClosedStream),

    /// An error occurred during encoding or decoding (codec-specific).
    #[error("Codec error: {0}")]
    Codec(String),

    /// An error occurred during Protobuf decoding.
    #[error("Prost decode error: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    /// An error occurred during Protobuf encoding.
    #[error("Prost encode error: {0}")]
    ProstEncode(#[from] prost::EncodeError),

    /// An error occurred during Bincode serialization or deserialization.
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    /// An error returned by the Tower service itself.
    #[error("Service error: {0}")]
    Service(String),

    /// A generic error reported via `anyhow`.
    #[error("Anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),

    /// An internal boxed error, typically used for Tower layers that return arbitrary errors.
    #[error("Internal error: {0}")]
    Internal(#[from] Box<dyn std::error::Error + Send + Sync>),

    /// An unknown or unexpected error.
    #[error("Unknown error")]
    Unknown,
}

impl From<std::convert::Infallible> for TowerError {
    fn from(_: std::convert::Infallible) -> Self {
        unreachable!()
    }
}
