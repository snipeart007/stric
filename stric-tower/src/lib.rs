#![doc = include_str!("../README.md")]

mod adapter;
mod client;
pub mod codec;
mod error;
mod handler;
mod http;
mod routing;
mod server;
mod wire;

pub use adapter::HttpAdapter;
pub use client::{SkipServerVerification, TowerClientService};
pub use codec::{BincodeFormat, ProstCodec, SerdeCodec, SerdeFormat, ServiceCodec};
pub use error::TowerError;
pub use handler::Handler;
pub use http::{
    Bincode, Body, BodyExt, Bytes, FromRequest, Full, HeaderMap, HeaderName, HeaderValue,
    HttpError, IntoResponse, Json, Method, Protobuf, RawBytes, Request, Response, State,
    StatusCode, Uri,
};
pub use routing::Router;
pub use server::{Server, TowerConnectionHandler};
