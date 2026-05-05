use std::net::SocketAddr;

use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::ConnectionContext;

/// Configuration for a [`ServerInstance`](crate::ServerInstance).
pub struct ServerConfig {
    /// The certificates to present during the TLS handshake.
    ///
    /// Provide the full server chain expected by clients.
    pub certs: Vec<CertificateDer<'static>>,
    /// The private key paired with `certs`.
    pub key: PrivateKeyDer<'static>,
    /// The socket address to bind the server to.
    pub socket_addr: SocketAddr,
    /// The ALPN (Application-Layer Protocol Negotiation) protocol names to support.
    ///
    /// Clients must advertise at least one matching protocol name or the
    /// handshake will fail.
    pub alpn_protocol_names: Vec<Vec<u8>>,
    /// The length of the internal error reporting channel.
    pub error_channel_len: usize,
    /// The default context copied onto each accepted connection before the user
    /// connection handler runs.
    pub default_conn_context: ConnectionContext,
    /// The maximum number of keep-alive streams per worker thread. Set to 0 for no limit.
    pub keep_alive_limit_per_thread: u64,
    /// The maximum duration a connection can be idle before it is timed out.
    ///
    /// `None` leaves the transport idle timeout at Quinn's default behavior.
    pub idle_timeout: Option<std::time::Duration>,
}
