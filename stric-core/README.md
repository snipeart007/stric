# stric-core

`stric-core` is the low-level crate in this workspace. It gives you QUIC server lifecycle management, accepted-connection tracking, and stream wrappers without pulling in the higher-level router and extractor layer from `stric-tower`.

This crate is the right choice when you need one or more of the following:

- custom TLS certificate loading
- direct access to accepted QUIC connections
- server-initiated uni or bi streams
- custom connection metadata
- custom connection policy flags
- integration with another protocol layer besides `stric-tower`

If you want `axum`-style handlers, extractors, and Tower middleware support, use `stric-tower` on top of this crate.

## Public API

The crate intentionally exposes a smaller root-level API now. Import from the crate root instead of module paths:

```rust
use stric_core::{
    BiStream, ConnectionContext, ConnectionHandlerFn, ConnectionManager,
    ConnectionManagerError, ConnectionWrapper, ServerConfig, ServerInstance,
    ServerStreamError, ServerUniStream,
};
```

Public items and intended use:

- `ServerConfig`
  Use this to describe TLS material, bind address, ALPN, keep-alive defaults, and idle timeout policy before starting a server.
- `ServerInstance`
  Use this to construct and run a QUIC server, register a connection handler, inspect the local bind address, access the shared connection manager, and open server-initiated streams.
- `ConnectionWrapper<M>`
  Use this only inside the connection handler passed to `ServerInstance::register_connection_handler`. This is where you can inspect the accepted `quinn::Connection`, adjust `ConnectionContext`, and populate per-connection metadata.
- `ConnectionContext`
  Use this for connection flags such as `keep_alive`, `client_uni`, `client_bi`, `server_uni`, and `server_bi`.
- `ConnectionManager<M>`
  Use this after a connection has already been accepted and registered. It is for policy updates and low-level inspection, not for initial connection setup.
- `ServerUniStream`, `ClientUniStream`, `BiStream`
  Use these wrappers for raw QUIC stream reads and writes.
- `ConnectionHandlerFn` and `BoxFuture`
  These stay public only for low-level integration. Most callers should rely on type inference and pass `Arc::new(|wrapper| Box::pin(async move { ... }))` directly instead of naming them explicitly.

Not exported:

- internal keep-alive worker types
- `ServerInstance::handle_incoming`
- `ConnectionManager::new`
- `ConnectionManager::add_connection`

## Quick Start

```rust
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use stric_core::{ConnectionContext, ServerConfig, ServerInstance};

fn install_crypto() {
    let _ = quinn::rustls::crypto::ring::default_provider().install_default();
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    install_crypto();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let config = ServerConfig {
        certs: vec![quinn::rustls::pki_types::CertificateDer::from(cert_der)],
        key: quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der)?,
        socket_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4433),
        alpn_protocol_names: vec![b"stric".to_vec()],
        error_channel_len: 16,
        default_conn_context: ConnectionContext::default(),
        keep_alive_limit_per_thread: 0,
        idle_timeout: None,
    };

    let (mut server, mut error_rx) = ServerInstance::<()>::new(config)?;

    server.register_connection_handler(Arc::new(|wrapper| {
        println!("accepted connection {}", wrapper.context.id);
        Box::pin(async move { Ok(()) })
    }));

    println!("listening on {}", server.local_addr()?);

    tokio::select! {
        _ = server.listen_connections() => {}
        Some(err) = error_rx.recv() => return Err(err),
    }

    Ok(())
}
```

## Which API For Which Job

### 1. Custom TLS and connection lifecycle

Use:

- `ServerConfig`
- `ServerInstance::new`
- `ServerInstance::register_connection_handler`
- `ServerInstance::listen_connections`

Do not use:

- `ConnectionManager` as a server bootstrap API

Reason:

- the server instance owns endpoint creation, connection acceptance, and error channel wiring

### 2. Mutating per-connection flags or metadata

Use:

- `ConnectionWrapper` inside the connection handler for initial metadata and flag setup
- `ConnectionManager` after registration for later updates

Do not use:

- `ConnectionWrapper` as a globally stored registry entry unless you fully understand the concurrency implications

### 3. Opening follow-up streams after a connection is accepted

Use:

- `ServerInstance::get_unistream`
- `ServerInstance::get_bistream`

Do not use:

- `ConnectionManager::store` mutation directly to fabricate streams

Reason:

- the stream-opening methods apply the supported lookup and error mapping path

### 4. Low-level protocol integration

Use:

- `BiStream`
- `ServerUniStream`
- `ClientUniStream`
- `ConnectionHandlerFn`

Do not use:

- these stream wrappers as substitutes for the higher-level request/response API from `stric-tower`

## Error Model

### `ServerInstance::new(...) -> Result<..., anyhow::Error>`

Returned error type:

- `anyhow::Error`

Common propagated inner errors:

- `quinn::rustls::Error` when the certificate chain or private key is invalid
- `quinn::crypto::rustls::NoInitialCipherSuite` when the rustls crypto provider has not been installed
- the `quinn` timeout conversion error when `idle_timeout` is out of range
- `std::io::Error` when the socket cannot bind

Operational meaning:

- construction failed before the server started accepting connections

### `ServerInstance::local_addr(...) -> Result<SocketAddr, std::io::Error>`

Returned error type:

- `std::io::Error`

Operational meaning:

- Quinn could not report the local socket address

### `ServerInstance::get_unistream(...) -> Result<ServerUniStream, ServerStreamError>`
### `ServerInstance::get_bistream(...) -> Result<BiStream, ServerStreamError>`

Returned error type:

- `ServerStreamError::ConnectionManager(ConnectionManagerError)`
- `ServerStreamError::Open(quinn::ConnectionError)`

Operational meaning:

- `ConnectionManagerError::IdNotFound(id)` means the connection ID is unknown or already gone
- `Open(...)` means the connection was known but Quinn could not open a new stream, usually because the connection closed or became unusable

### `ConnectionManager::{set_keep_alive, set_client_uni, set_client_bi, set_server_uni, set_server_bi} -> Result<(), ConnectionManagerError>`

Returned error type:

- `ConnectionManagerError::IdNotFound(id)`

Operational meaning:

- you tried to update a connection that is not currently registered

### Stream wrapper methods

Returned error types are the underlying Quinn stream errors:

- `ServerUniStream::write`, `write_all` -> `quinn::WriteError`
- `ServerUniStream::finish` -> `quinn::ClosedStream`
- `ServerUniStream::stopped` -> `quinn::StoppedError`
- `ClientUniStream::read` -> `quinn::ReadError`
- `ClientUniStream::read_exact` -> `quinn::ReadExactError`
- `ClientUniStream::read_to_end` -> `quinn::ReadToEndError`
- `ClientUniStream::stop` -> `quinn::ClosedStream`
- `BiStream::write`, `write_all` -> `quinn::WriteError`
- `BiStream::finish` -> `quinn::ClosedStream`
- `BiStream::read` -> `quinn::ReadError`
- `BiStream::read_exact` -> `quinn::ReadExactError`
- `BiStream::read_to_end` -> `quinn::ReadToEndError`

Operational meaning:

- the stream or connection closed
- the peer stopped the stream
- the peer exceeded a configured read limit
- the exact byte count you expected never arrived

## Edge Cases

- `ConnectionContext` is copied from `ServerConfig::default_conn_context` into each accepted connection. Mutating the config later does nothing for existing or future accepted connections because the config is consumed at server creation time.
- `set_keep_alive(id, true)` returns `Ok(())` before the keep-alive stream is actually opened. If the connection closes immediately after the call, no keep-alive stream is created.
- `get_unistream` and `get_bistream` can race with connection shutdown. A successful ID lookup does not guarantee that stream creation will succeed.
- `BiStream::is_server_initiated()` tells you who opened the stream wrapper. It does not imply any access control by itself.
- `read_to_end(size_limit)` fails if the peer sends more than `size_limit` bytes. Pick a real limit; do not use an unbounded value unless you want unbounded buffering.
- `ConnectionManager::store` is public for inspection. Treat it as low-level state, not as the primary application API.

## TLS and ALPN

Use ALPN `b"stric"` consistently on both sides.

Server-side configuration in this crate is certificate-based TLS through `ServerConfig`:

- set `certs` to the server certificate chain you want clients to trust
- set `key` to the matching private key
- set `alpn_protocol_names` to `vec![b"stric".to_vec()]`

Proper server verification on the client side means:

- create a `quinn::rustls::RootCertStore`
- add your CA or self-signed server certificate
- build the client config with `with_root_certificates(...)`
- do not use a dangerous verifier

Important limitation:

- `ServerInstance::new` currently builds rustls with `with_no_client_auth()`
- that means server-side client certificate verification is not exposed by the current public API
- if you need mutual TLS today, the crate needs an API extension to accept a prebuilt server auth policy or prebuilt Quinn/rustls server config

## When To Use `stric-tower` Instead

Use `stric-tower` if you want:

- path-based routing
- request extractors such as JSON or Protobuf
- response wrappers
- Tower middleware adaptation
- the `HeaderMap` reexport and request/response helpers

Stay on `stric-core` if you need:

- raw QUIC streams
- custom handshake material
- connection metadata management
- a protocol that is not request/response shaped
