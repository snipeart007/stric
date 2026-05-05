#![doc = include_str!("../README.md")]

mod connection;
mod connection_wrapper;
mod handler_types;
mod keep_alive;
mod server;
mod server_config;
mod stream;

pub use connection::{ConnectionManager, ConnectionManagerError};
pub use connection_wrapper::{ConnectionContext, ConnectionWrapper};
pub use handler_types::{BoxFuture, ConnectionHandlerFn};
pub use server::{ServerInstance, ServerStreamError};
pub use server_config::ServerConfig;
pub use stream::{BiStream, ClientUniStream, ServerUniStream};
