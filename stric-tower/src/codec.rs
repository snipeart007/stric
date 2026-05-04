use async_trait::async_trait;
use prost::Message;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use stric_core::stream::BiStream;

use crate::error::TowerError;

#[async_trait]
pub trait ServiceCodec<Req, Res>: Send + Sync + Clone + 'static {
    async fn encode_request(&self, req: Req, stream: &mut BiStream) -> Result<(), TowerError>;
    async fn decode_request(&self, stream: &mut BiStream) -> Result<Req, TowerError>;

    async fn encode_response(&self, res: Res, stream: &mut BiStream) -> Result<(), TowerError>;
    async fn decode_response(&self, stream: &mut BiStream) -> Result<Res, TowerError>;
}

// --- Prost Codec ---
#[derive(Default)]
pub struct ProstCodec<Req, Res>(PhantomData<(Req, Res)>);

impl<Req, Res> Clone for ProstCodec<Req, Res> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<Req, Res> ProstCodec<Req, Res> {
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

pub trait SerdeFormat: Send + Sync + Clone + 'static {
    fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>, TowerError>;
    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, TowerError>;
}

#[derive(Clone, Default)]
pub struct BincodeFormat;

impl SerdeFormat for BincodeFormat {
    fn serialize<T: Serialize>(item: &T) -> Result<Vec<u8>, TowerError> {
        Ok(bincode::serialize(item)?)
    }

    fn deserialize<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, TowerError> {
        Ok(bincode::deserialize(bytes)?)
    }
}

#[derive(Default)]
pub struct SerdeCodec<Req, Res, Format>(PhantomData<(Req, Res, Format)>);

impl<Req, Res, Format> Clone for SerdeCodec<Req, Res, Format> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<Req, Res, Format> SerdeCodec<Req, Res, Format> {
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

async fn write_length_prefixed(stream: &mut BiStream, buf: &[u8]) -> Result<(), TowerError> {
    let len = buf.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(buf).await?;
    Ok(())
}

async fn read_length_prefixed(stream: &mut BiStream) -> Result<Vec<u8>, TowerError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}
