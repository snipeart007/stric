//! # stric-core
//! 
//! `stric-core` is the foundational crate for the Stric network framework.
//! It provides a high-level, opinionated wrapper around the `quinn` QUIC implementation,
//! focusing on ease of use for building concurrent servers and clients.
//!
//! ## Key Features
//! - **Connection Management:** Efficiently track and manage QUIC connections.
//! - **Stream Abstractions:** Simplified `BiStream` and `UniStream` types.
//! - **Keep-Alive System:** Built-in heartbeat support to prevent idle timeouts.
//! - **Handler-based Architecture:** Register async closures to handle new connections.

pub mod connection;
pub mod connection_wrapper;
pub mod handler_types;
pub mod keep_alive;
pub mod server;
pub mod server_config;
pub mod stream;
