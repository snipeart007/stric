use async_trait::async_trait;
use prost::Message;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use stric_core::BiStream;

use crate::error::TowerError;

/// A trait for encoding and decoding requests and responses over a [`BiStream`].
#[async_trait]
pub trait ServiceCodec<Req, Res>: Send + Sync + Clone + 'static {
    /// Encodes a request and writes it to the stream.
    ///
    /// # Errors
    /// Returns [`TowerError`] when serialization fails or when the underlying
    /// QUIC stream cannot be written.
    async fn encode_request(&self, req: Req, stream: &mut BiStream) -> Result<(), TowerError>;

    /// Decodes a request from the stream.
    ///
    /// # Errors
    /// Returns [`TowerError`] when the frame cannot be read completely or when
    /// the payload cannot be decoded into `Req`.
    async fn decode_request(&self, stream: &mut BiStream) -> Result<Req, TowerError>;

    /// Encodes a response and writes it to the stream.
    ///
    /// # Errors
    /// Returns [`TowerError`] when serialization fails or when the underlying
    /// QUIC stream cannot be written.
    async fn encode_response(&self, res: Res, stream: &mut BiStream) -> Result<(), TowerError>;

    /// Decodes a response from the stream.
    ///
    /// # Errors
    /// Returns [`TowerError`] when the frame cannot be read completely or when
    /// the payload cannot be decoded into `Res`.
    async fn decode_response(&self, stream: &mut BiStream) -> Result<Res, TowerError>;
}

// --- Prost Codec ---

/// A codec that uses [Prost](https://github.com/tokio-rs/prost) for Protobuf serialization.
#[derive(Debug, Default)]
pub struct ProstCodec<Req, Res>(PhantomData<(Req, Res)>);

impl<Req, Res> Clone for ProstCodec<Req, Res> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<Req, Res> ProstCodec<Req, Res> {
    /// Creates a new `ProstCodec`.
    ///
    /// Use this codec when both sides exchange `prost::Message` values. It is
    /// not needed for the higher-level `Router` plus `TowerClientService` flow,
    /// which already uses the Stric request envelope format.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[async_trait]
impl<Req, Res> ServiceCodec<Req, Res> for ProstCodec<Req, Res>
where
    Req: Message + Default + Send + Sync + 'static,
    Res: Message + Default + Send + Sync + 'static,
{
    async fn encode_request(&self, req: Req, stream: &mut BiStream) -> Result<(), TowerError> {
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf)?;
        write_length_prefixed(stream, &buf).await
    }

    async fn decode_request(&self, stream: &mut BiStream) -> Result<Req, TowerError> {
        let buf = read_length_prefixed(stream).await?;
        Ok(Req::decode(&buf[..])?)
    }

    async fn encode_response(&self, res: Res, stream: &mut BiStream) -> Result<(), TowerError> {
        let mut buf = Vec::with_capacity(res.encoded_len());
        res.encode(&mut buf)?;
        write_length_prefixed(stream, &buf).await
    }

    async fn decode_response(&self, stream: &mut BiStream) -> Result<Res, TowerError> {
        let buf = read_length_prefixed(stream).await?;
        Ok(Res::decode(&buf[..])?)
    }
}

// --- Generic Serde Codec ---

/// A trait for defining a Serde serialization format.
pub trait SerdeFormat: Send + Sync + Clone + 'static {
    /// Serializes an item into a byte vector.
    ///
    /// # Errors
    /// Returns [`TowerError`] from the concrete format implementation.
    fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>, TowerError>;

    /// Deserializes an item from a byte slice.
    ///
    /// # Errors
    /// Returns [`TowerError`] from the concrete format implementation.
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, TowerError>;
}

/// The [Bincode](https://github.com/bincode-org/bincode) serialization format.
#[derive(Clone, Copy, Debug, Default)]
pub struct BincodeFormat;

impl SerdeFormat for BincodeFormat {
    fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>, TowerError> {
        Ok(bincode::serialize(item)?)
    }

    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, TowerError> {
        Ok(bincode::deserialize(bytes)?)
    }
}

/// A codec that uses [Serde](https://serde.rs/) for serialization and deserialization.
///
/// # Type Parameters
/// * `Req`: The request type.
/// * `Res`: The response type.
/// * `Format`: The serialization format (e.g., `BincodeFormat` or a custom JSON format).
#[derive(Debug, Default)]
pub struct SerdeCodec<Req, Res, Format>(PhantomData<(Req, Res, Format)>);

impl<Req, Res, Format> Clone for SerdeCodec<Req, Res, Format> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<Req, Res, Format> SerdeCodec<Req, Res, Format> {
    /// Creates a new `SerdeCodec`.
    ///
    /// Use this codec for custom stream-level protocols. It should not be used
    /// as a replacement for `Json`, `Bincode`, or `Protobuf` extractors inside
    /// the router API.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

#[async_trait]
impl<Req, Res, Format> ServiceCodec<Req, Res> for SerdeCodec<Req, Res, Format>
where
    Req: Serialize + DeserializeOwned + Send + Sync + 'static,
    Res: Serialize + DeserializeOwned + Send + Sync + 'static,
    Format: SerdeFormat,
{
    async fn encode_request(&self, req: Req, stream: &mut BiStream) -> Result<(), TowerError> {
        let buf = Format::serialize(&req)?;
        write_length_prefixed(stream, &buf).await
    }

    async fn decode_request(&self, stream: &mut BiStream) -> Result<Req, TowerError> {
        let buf = read_length_prefixed(stream).await?;
        Ok(Format::deserialize(&buf)?)
    }

    async fn encode_response(&self, res: Res, stream: &mut BiStream) -> Result<(), TowerError> {
        let buf = Format::serialize(&res)?;
        write_length_prefixed(stream, &buf).await
    }

    async fn decode_response(&self, stream: &mut BiStream) -> Result<Res, TowerError> {
        let buf = read_length_prefixed(stream).await?;
        Ok(Format::deserialize(&buf)?)
    }
}

// --- Helpers ---

/// Writes a byte buffer to the stream, prefixed with its length as a 4-byte big-endian integer.
pub(crate) async fn write_length_prefixed(
    stream: &mut BiStream,
    buf: &[u8],
) -> Result<(), TowerError> {
    let len = buf.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(buf).await?;
    Ok(())
}

/// Reads a byte buffer from the stream, expecting a 4-byte big-endian length prefix first.
pub(crate) async fn read_length_prefixed(stream: &mut BiStream) -> Result<Vec<u8>, TowerError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

// --- Envelope Helpers ---

use crate::wire::proto::{RequestEnvelope, ResponseEnvelope};

pub(crate) async fn write_request_envelope(
    stream: &mut BiStream,
    envelope: RequestEnvelope,
) -> Result<(), TowerError> {
    let mut buf = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut buf)?;
    write_length_prefixed(stream, &buf).await
}

pub(crate) async fn read_request_envelope(
    stream: &mut BiStream,
) -> Result<RequestEnvelope, TowerError> {
    let buf = read_length_prefixed(stream).await?;
    Ok(RequestEnvelope::decode(&buf[..])?)
}

pub(crate) async fn write_response_envelope(
    stream: &mut BiStream,
    envelope: ResponseEnvelope,
) -> Result<(), TowerError> {
    let mut buf = Vec::with_capacity(envelope.encoded_len());
    envelope.encode(&mut buf)?;
    write_length_prefixed(stream, &buf).await
}

pub(crate) async fn read_response_envelope(
    stream: &mut BiStream,
) -> Result<ResponseEnvelope, TowerError> {
    let buf = read_length_prefixed(stream).await?;
    Ok(ResponseEnvelope::decode(&buf[..])?)
}
