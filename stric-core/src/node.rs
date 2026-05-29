use std::sync::Arc;

use crate::{
    connection::{ConnectionManager, ConnectionManagerError},
    connection_wrapper::ConnectionWrapper,
    handler_types::ConnectionHandlerFn,
    node_config::NodeConfig,
    stream::{BiStream, SendUniStream},
};
use quinn::rustls::ServerConfig as RustlsServerConfig;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{debug, error, info};

/// A symmetric QUIC node instance.
///
/// `QuicNode` manages the lifecycle of QUIC connections, both inbound and outbound.
/// It uses a single underlying `quinn::Endpoint` for all communication, enabling
/// symmetric peer-to-peer interactions.
///
/// # Symmetric Architecture
/// In traditional client-server models, only one side initiates connections. In `stric-core`,
/// a `QuicNode` can both `listen()` for incoming connections and `connect()` to peers
/// simultaneously. This symmetry is essential for mesh networks and complex P2P protocols.
///
/// # Type Parameters
/// * `ConnectionMetadata`: A user-defined type for storing custom metadata associated with each connection.
pub struct QuicNode<ConnectionMetadata: Default + Send + Sync + 'static> {
    /// The underlying QUIC endpoint.
    endpoint: quinn::Endpoint,
    /// The manager for active connections.
    conn_manager: Arc<ConnectionManager<ConnectionMetadata>>,
    /// The handler for inbound connections (where this node is the responder).
    inbound_handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
    /// The handler for outbound connections (where this node is the initiator).
    outbound_handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
    /// A sender for reporting asynchronous errors.
    error_tx: Sender<anyhow::Error>,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> QuicNode<ConnectionMetadata> {
    /// Creates a new `QuicNode` with the provided configuration.
    ///
    /// Returns a tuple containing the `QuicNode` and a `Receiver` for asynchronous errors.
    ///
    /// # Errors
    /// Returns `anyhow::Error` when Stric cannot build the TLS configuration
    /// or bind the QUIC endpoint.
    pub fn new(
        config: NodeConfig,
    ) -> Result<(QuicNode<ConnectionMetadata>, Receiver<anyhow::Error>), anyhow::Error> {
        info!("Initializing QuicNode on {}", config.socket_addr);

        // Responder (Server) Configuration
        let quinn_server_config = if let (Some(certs), Some(key)) = (config.certs, config.key) {
            debug!("Configuring responder (server) capabilities");
            let mut server_config = RustlsServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)?;

            server_config.alpn_protocols = config.alpn_protocol_names.clone();

            let mut quinn_config = quinn::ServerConfig::with_crypto(Arc::new(
                quinn::crypto::rustls::QuicServerConfig::try_from(server_config)?,
            ));

            if let Some(timeout) = config.idle_timeout {
                let mut transport_config = quinn::TransportConfig::default();
                transport_config.max_idle_timeout(Some(timeout.try_into()?));
                quinn_config.transport_config(Arc::new(transport_config));
            }
            Some(quinn_config)
        } else {
            debug!("Node initialized without responder capabilities");
            None
        };

        // Initiator (Client) Configuration
        debug!("Configuring initiator (client) capabilities");
        let crypto = quinn::rustls::ClientConfig::builder();

        let mut client_config = if config.danger_accept_invalid_certs {
            info!("WARNING: Insecure certificate verification enabled for outbound connections");
            crypto
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(DangerNoCertificateVerification))
                .with_no_client_auth()
        } else {
            let roots = config
                .root_cert_store
                .unwrap_or_else(quinn::rustls::RootCertStore::empty);
            crypto.with_root_certificates(roots).with_no_client_auth()
        };

        client_config.alpn_protocols = config.alpn_protocol_names;

        let mut quinn_client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client_config)?,
        ));

        if let Some(timeout) = config.idle_timeout {
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_idle_timeout(Some(timeout.try_into()?));
            quinn_client_config.transport_config(Arc::new(transport_config));
        }

        let endpoint = if let Some(server_config) = quinn_server_config {
            let mut ep = quinn::Endpoint::server(server_config, config.socket_addr)?;
            ep.set_default_client_config(quinn_client_config);
            ep
        } else {
            let mut ep = quinn::Endpoint::client(config.socket_addr)?;
            ep.set_default_client_config(quinn_client_config);
            ep
        };

        let (error_tx, error_rx) = mpsc::channel::<anyhow::Error>(config.error_channel_len);
        Ok((
            Self {
                endpoint,
                conn_manager: Arc::new(ConnectionManager::new(
                    config.default_conn_context,
                    config.keep_alive_limit_per_thread,
                    config.idle_timeout,
                )),
                inbound_handler: None,
                outbound_handler: None,
                error_tx,
            },
            error_rx,
        ))
    }

    /// Registers a handler for inbound connections (where this node is the responder).
    ///
    /// Re-registering a handler replaces the previous handler for subsequent
    /// connections only.
    pub fn on_inbound(&mut self, handler: ConnectionHandlerFn<ConnectionMetadata>) {
        debug!("Registered inbound connection handler");
        self.inbound_handler = Some(handler);
    }

    /// Registers a handler for outbound connections (where this node is the initiator).
    ///
    /// The handler is automatically executed in a new task upon successful connection.
    pub fn on_outbound(&mut self, handler: ConnectionHandlerFn<ConnectionMetadata>) {
        debug!("Registered outbound connection handler");
        self.outbound_handler = Some(handler);
    }

    /// Starts listening for incoming QUIC connections.
    ///
    /// This method runs in a loop and spawns a new task for each incoming connection.
    /// It only returns when the endpoint stops accepting connections.
    pub async fn listen(&self) {
        info!("QuicNode listening for incoming connections");
        while let Some(incoming) = self.endpoint.accept().await {
            let manager = self.conn_manager.clone();
            let handler = self.inbound_handler.clone();
            let error_tx = self.error_tx.clone();

            tokio::spawn(Self::handle_incoming(incoming, manager, handler, error_tx));
        }
    }

    /// Connects to a remote node at the specified address.
    ///
    /// Returns the stable ID of the established connection.
    ///
    /// # Errors
    /// Returns `anyhow::Error` if the connection attempt fails or if the remote
    /// certificate cannot be verified (unless `danger_accept_invalid_certs` is true).
    pub async fn connect(
        &self,
        addr: std::net::SocketAddr,
        server_name: &str,
    ) -> Result<u64, anyhow::Error> {
        info!(
            "Initiating outbound connection to {} ({})",
            addr, server_name
        );
        let connection = self.endpoint.connect(addr, server_name)?.await?;
        let id = connection.stable_id() as u64;

        info!("Successfully connected to {} (stable_id: {})", addr, id);

        let manager = self.conn_manager.clone();
        let handler = self.outbound_handler.clone();
        let error_tx = self.error_tx.clone();

        tokio::spawn(Self::setup_connection(
            connection, manager, handler, error_tx,
        ));

        Ok(id)
    }

    /// Returns the local socket address currently bound by the QUIC endpoint.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }

    /// Returns the shared connection manager.
    pub fn connection_manager(&self) -> &Arc<ConnectionManager<ConnectionMetadata>> {
        &self.conn_manager
    }

    /// Internal method to handle an incoming connection attempt.
    pub(crate) async fn handle_incoming(
        incoming: quinn::Incoming,
        manager: Arc<ConnectionManager<ConnectionMetadata>>,
        handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
        error_tx: Sender<anyhow::Error>,
    ) {
        let remote_addr = incoming.remote_address();
        debug!("Accepted incoming connection attempt from {}", remote_addr);

        let Ok(connection) = incoming.await.map_err(|e| {
            error!("Handshake failed with {}: {:?}", remote_addr, e);
            let _ = error_tx.try_send(e.into());
        }) else {
            return;
        };

        info!(
            "Established inbound connection with {} (stable_id: {})",
            remote_addr,
            connection.stable_id()
        );
        Self::setup_connection(connection, manager, handler, error_tx).await;
    }

    /// Internal method to finalize connection setup and register it with the manager.
    async fn setup_connection(
        connection: quinn::Connection,
        manager: Arc<ConnectionManager<ConnectionMetadata>>,
        handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
        error_tx: Sender<anyhow::Error>,
    ) {
        let k = connection.stable_id() as u64;
        let mut context = manager.default_conn_context;
        context.id = k;

        let metadata = ConnectionMetadata::default();
        let mut wrapper = ConnectionWrapper {
            conn: connection,
            context,
            metadata,
        };

        if let Some(func) = handler {
            debug!("Executing connection handler for stable_id: {}", k);
            if let Err(e) = func(&mut wrapper).await {
                error!("Connection handler failed for stable_id {}: {:?}", k, e);
                let _ = error_tx.try_send(e);
                return;
            }
        }

        let keep_alive = wrapper.context.keep_alive;
        manager.add_connection(wrapper);

        if keep_alive {
            debug!("Enabling keep-alive for stable_id: {}", k);
            let _ = manager.set_keep_alive(k, true);
        }
    }

    /// Opens a new unidirectional stream on the connection with the given ID.
    pub async fn get_unistream(&self, id: &u64) -> Result<ServerUniStream, NodeStreamError> {
        let conn = self
            .conn_manager
            .store
            .get(id)
            .ok_or(ConnectionManagerError::IdNotFound(*id))?
            .conn
            .clone();

        debug!("Opening unidirectional stream on connection {}", id);
        let stream = conn.open_uni().await?;
        Ok(ServerUniStream::new(stream))
    }

    /// Opens a new bidirectional stream on the connection with the given ID.
    pub async fn get_bistream(&self, id: &u64) -> Result<BiStream, NodeStreamError> {
        let conn = self
            .conn_manager
            .store
            .get(id)
            .ok_or(ConnectionManagerError::IdNotFound(*id))?
            .conn
            .clone();

        debug!("Opening bidirectional stream on connection {}", id);
        let (send_stream, recv_stream) = conn.open_bi().await?;
        Ok(BiStream::new(true, send_stream, recv_stream))
    }
}

/// Errors returned when opening streams through [`QuicNode`].
#[derive(Debug, thiserror::Error)]
pub enum NodeStreamError {
    /// The requested connection ID is not currently tracked by the connection manager.
    #[error(transparent)]
    ConnectionManager(#[from] ConnectionManagerError),

    /// `quinn` failed to open a new stream.
    #[error(transparent)]
    Open(#[from] quinn::ConnectionError),
}

#[derive(Debug)]
struct DangerNoCertificateVerification;

impl quinn::rustls::client::danger::ServerCertVerifier for DangerNoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &quinn::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[quinn::rustls::pki_types::CertificateDer<'_>],
        _server_name: &quinn::rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: quinn::rustls::pki_types::UnixTime,
    ) -> Result<quinn::rustls::client::danger::ServerCertVerified, quinn::rustls::Error> {
        Ok(quinn::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(quinn::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &quinn::rustls::pki_types::CertificateDer<'_>,
        _dss: &quinn::rustls::DigitallySignedStruct,
    ) -> Result<quinn::rustls::client::danger::HandshakeSignatureValid, quinn::rustls::Error> {
        Ok(quinn::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<quinn::rustls::SignatureScheme> {
        vec![
            quinn::rustls::SignatureScheme::RSA_PSS_SHA256,
            quinn::rustls::SignatureScheme::RSA_PSS_SHA384,
            quinn::rustls::SignatureScheme::RSA_PSS_SHA512,
            quinn::rustls::SignatureScheme::ED25519,
            quinn::rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            quinn::rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            quinn::rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            quinn::rustls::SignatureScheme::RSA_PKCS1_SHA256,
            quinn::rustls::SignatureScheme::RSA_PKCS1_SHA384,
            quinn::rustls::SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}
