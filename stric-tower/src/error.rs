use thiserror::Error;

#[derive(Debug, Error)]
pub enum TowerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("QUIC write error: {0}")]
    Write(#[from] quinn::WriteError),

    #[error("QUIC read error: {0}")]
    Read(#[from] quinn::ReadError),

    #[error("QUIC read exact error: {0}")]
    ReadExact(#[from] quinn::ReadExactError),

    #[error("QUIC connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    #[error("QUIC stream closed")]
    Closed(#[from] quinn::ClosedStream),

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Prost decode error: {0}")]
    ProstDecode(#[from] prost::DecodeError),

    #[error("Prost encode error: {0}")]
    ProstEncode(#[from] prost::EncodeError),

    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),

    #[error("Service error: {0}")]
    Service(String),

    #[error("Anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),

    #[error("Internal error: {0}")]
    Internal(#[from] Box<dyn std::error::Error + Send + Sync>),
}
