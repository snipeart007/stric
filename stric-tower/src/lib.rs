//! # stric-tower
//!
//! `stric-tower` provides integration between the Stric network framework and the [Tower](https://github.com/tower-rs/tower) ecosystem.
//! It allows users to build high-performance, request-response based services over QUIC using an ergonomic, `axum`-like API.

pub mod adapter;
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
    Bincode, Body, BodyExt, Bytes, FromRequest, Full, HeaderMap, HeaderName, HeaderValue,
    IntoResponse, Json, Protobuf, RawBytes, Request, Response, State,
};
pub use routing::Router;
pub use server::{Server, TowerConnectionHandler};
