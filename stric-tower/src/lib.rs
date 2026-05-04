//! # stric-tower
//!
//! `stric-tower` provides integration between the Stric network framework and the [Tower](https://github.com/tower-rs/tower) ecosystem.
//! It allows users to build high-performance, request-response based services using QUIC as the transport layer.
//!
//! ## Key Features
//! - **Tower Service Integration:** Easily wrap `stric-core` connections as Tower `Service`s.
//! - **Generic Codecs:** Support for Protobuf (`ProstCodec`) and any Serde format (`SerdeCodec`).
//! - **Stream-per-Request:** Maps each request-response pair to a new bidirectional QUIC stream.

pub mod client;
pub mod codec;
pub mod error;
pub mod handler;
pub mod http;
pub mod routing;
pub mod server;
pub mod wire;

pub use client::TowerClientService;
pub use codec::{BincodeFormat, ProstCodec, SerdeCodec, SerdeFormat, ServiceCodec};
pub use error::TowerError;
pub use http::{
    Bincode, Bytes, FromRequest, IntoResponse, Json, Protobuf, RawBytes, Request, Response, State,
};
pub use routing::Router;
pub use server::{Server, TowerConnectionHandler};
