use thiserror::Error;

#[derive(Error, Debug)]
pub enum FlowError {
    #[error("Transport layer error: {0}")]
    Transport(#[from] stric_core::NodeStreamError),

    #[error("Serialization/Decoding error: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("Serialization/Encoding error: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("Handshake error: {0}")]
    Handshake(String),

    #[error("Identity verification error: {0}")]
    Identity(String),

    #[error("Session management error: {0}")]
    Session(String),

    #[error("Message registry error: {0}")]
    Registry(String),

    #[error("Routing/Pathfinding error: {0}")]
    Routing(String),

    #[error("Internal engine error: {0}")]
    Internal(String),

    #[error("Connection management error: {0}")]
    Connection(String),

    #[error("Anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}
