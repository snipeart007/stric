use crate::connection_wrapper::ConnectionContext;

/// Configuration for a symmetric QUIC node.
///
/// `NodeConfig` encapsulates all parameters required for both responding to
/// incoming connections and initiating outbound connections on a single endpoint.
pub struct NodeConfig {
    /// Certificates for responding to incoming connections.
    ///
    /// If `None`, the node will not be able to accept incoming connections.
    pub certs: Option<Vec<quinn::rustls::pki_types::CertificateDer<'static>>>,
    /// Private key for responding to incoming connections.
    ///
    /// If `None`, the node will not be able to accept incoming connections.
    pub key: Option<quinn::rustls::pki_types::PrivateKeyDer<'static>>,
    /// The local socket address to bind for both listening and dialing.
    pub socket_addr: std::net::SocketAddr,
    /// ALPN protocols supported by this node.
    pub alpn_protocol_names: Vec<Vec<u8>>,
    /// Length of the internal channel used for reporting asynchronous errors.
    pub error_channel_len: usize,
    /// The initial context (capability flags) for every new connection.
    pub default_conn_context: ConnectionContext,
    /// Limit on the number of keep-alive tasks per worker thread.
    pub keep_alive_limit_per_thread: u64,
    /// Duration of inactivity before a connection is considered timed out.
    pub idle_timeout: Option<std::time::Duration>,
    /// Root certificates to trust when initiating outbound connections.
    ///
    /// If `None`, an empty root store is used, which will cause all certificate
    /// verification to fail unless a custom verifier is used.
    pub root_cert_store: Option<quinn::rustls::RootCertStore>,
    /// Whether to skip certificate verification for outbound connections.
    ///
    /// # Warning
    /// Enabling this makes the connection vulnerable to man-in-the-middle attacks.
    /// Use only for development or in trusted networks.
    pub danger_accept_invalid_certs: bool,
}
