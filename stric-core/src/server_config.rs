use std::net::SocketAddr;

use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::connection_wrapper::ConnectionContext;

/// Configuration for a [`ServerInstance`](crate::server::ServerInstance).
pub struct ServerConfig {
    /// The certificates to use for TLS.
    pub certs: Vec<CertificateDer<'static>>,
    /// The private key to use for TLS.
    pub key: PrivateKeyDer<'static>,
    /// The socket address to bind the server to.
    pub socket_addr: SocketAddr,
    /// The ALPN (Application-Layer Protocol Negotiation) protocol names to support.
    pub alpn_protocol_names: Vec<Vec<u8>>,
    /// The length of the internal error reporting channel.
    pub error_channel_len: usize,
    /// The default context to apply to all new connections.
    pub default_conn_context: ConnectionContext,
    /// The maximum number of keep-alive streams per worker thread. Set to 0 for no limit.
    pub keep_alive_limit_per_thread: u64,
    /// The maximum duration a connection can be idle before it is timed out.
    pub idle_timeout: Option<std::time::Duration>,
}
