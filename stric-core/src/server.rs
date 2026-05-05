use std::sync::Arc;

use crate::{
    connection::{ConnectionManager, ConnectionManagerError},
    connection_wrapper::ConnectionWrapper,
    handler_types::ConnectionHandlerFn,
    server_config::ServerConfig,
    stream::{BiStream, ServerUniStream},
};
use quinn::rustls::ServerConfig as RustlsServerConfig;
use tokio::sync::mpsc::{self, Receiver, Sender};

/// A QUIC server instance.
///
/// `ServerInstance` manages the lifecycle of a QUIC server, including accepting incoming connections,
/// managing connection state via a [`ConnectionManager`], and dispatching connections to a registered handler.
///
/// # Type Parameters
/// * `ConnectionMetadata`: A user-defined type for storing custom metadata associated with each connection.
pub struct ServerInstance<ConnectionMetadata: Default + Send + Sync + 'static> {
    /// The underlying QUIC endpoint.
    endpoint: quinn::Endpoint,
    /// The manager for active connections.
    conn_manager: Arc<ConnectionManager<ConnectionMetadata>>,
    /// The optional handler for new connections.
    conn_handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
    /// A sender for reporting asynchronous errors.
    error_tx: Sender<anyhow::Error>,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ServerInstance<ConnectionMetadata> {
    /// Creates a new `ServerInstance` with the provided configuration.
    ///
    /// Returns a tuple containing the `ServerInstance` and a `Receiver` for asynchronous errors.
    ///
    /// # Errors
    /// Returns `anyhow::Error` when Stric cannot build the TLS server
    /// configuration or bind the QUIC endpoint.
    ///
    /// The propagated source error is typically one of:
    /// - `quinn::rustls::Error` for invalid certificates or key material
    /// - `quinn::crypto::rustls::NoInitialCipherSuite` when the rustls crypto provider is not installed
    /// - the timeout conversion error produced by `quinn` when `idle_timeout` is outside its supported range
    /// - `std::io::Error` when the socket cannot be bound
    ///
    /// # Edge Cases
    /// `ServerInstance::new` does not verify ALPN compatibility with future
    /// clients. A mismatch is reported later during the QUIC handshake instead.
    pub fn new(
        config: ServerConfig,
    ) -> Result<(ServerInstance<ConnectionMetadata>, Receiver<anyhow::Error>), anyhow::Error> {
        let mut server_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(config.certs, config.key)?;

        server_config.alpn_protocols = config.alpn_protocol_names;

        let mut quinn_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_config)?,
        ));

        if let Some(timeout) = config.idle_timeout {
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_idle_timeout(Some(timeout.try_into()?));
            quinn_config.transport_config(Arc::new(transport_config));
        }

        let endpoint = quinn::Endpoint::server(quinn_config, config.socket_addr)?;

        let (error_tx, error_rx) = mpsc::channel::<anyhow::Error>(config.error_channel_len);
        Ok((
            Self {
                endpoint,
                conn_manager: Arc::new(ConnectionManager::new(
                    config.default_conn_context,
                    config.keep_alive_limit_per_thread,
                    config.idle_timeout,
                )),
                conn_handler: None,
                error_tx,
            },
            error_rx,
        ))
    }

    /// Registers a handler function that will be called for every new incoming connection.
    ///
    /// Re-registering a handler replaces the previous handler for subsequent
    /// connections only. Existing accepted connections continue running with the
    /// handler logic that was already spawned for them.
    pub fn register_connection_handler(
        &mut self,
        conn_handler: ConnectionHandlerFn<ConnectionMetadata>,
    ) {
        self.conn_handler = Some(conn_handler);
    }

    /// Starts listening for incoming QUIC connections.
    ///
    /// This method runs in a loop and spawns a new Tokio task for each incoming connection.
    ///
    /// # Edge Cases
    /// This method only returns when the endpoint stops accepting connections.
    /// Per-connection failures are forwarded through the error channel rather
    /// than returned from this function.
    pub async fn listen_connections(&self) {
        while let Some(incoming) = self.endpoint.accept().await {
            let manager = self.conn_manager.clone();
            let conn_handler = self.conn_handler.clone();
            let error_tx = self.error_tx.clone();

            tokio::spawn(Self::handle_incoming(
                incoming,
                manager,
                conn_handler,
                error_tx,
            ));
        }
    }

    /// Returns the local socket address currently bound by the QUIC endpoint.
    ///
    /// # Errors
    /// Propagates the `std::io::Error` returned by `quinn` when the local
    /// socket address is unavailable.
    pub fn local_addr(&self) -> Result<std::net::SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }

    /// Returns the shared connection manager for low-level inspection and policy updates.
    ///
    /// Prefer higher-level APIs unless you need direct access to connection
    /// metadata, keep-alive flags, or per-connection stream opening.
    pub fn connection_manager(&self) -> &Arc<ConnectionManager<ConnectionMetadata>> {
        &self.conn_manager
    }

    /// Internal method to handle an individual incoming connection.
    pub(crate) async fn handle_incoming(
        incoming: quinn::Incoming,
        manager: Arc<ConnectionManager<ConnectionMetadata>>,
        conn_handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
        error_tx: Sender<anyhow::Error>,
    ) {
        let Ok(connection) = incoming.await.map_err(|e| {
            let _ = error_tx.try_send(e.into()); // Send error to channel
        }) else {
            return; // Exit the green thread
        };
        let k = connection.stable_id() as u64;

        let mut context = manager.default_conn_context;
        context.id = k;

        let metadata = ConnectionMetadata::default();

        let mut wrapper = ConnectionWrapper {
            conn: connection,
            context,
            metadata,
        };

        if let Some(func) = conn_handler
            && let Err(e) = func(&mut wrapper).await
        {
            let _ = error_tx.try_send(e);
            return;
        }

        let keep_alive = wrapper.context.keep_alive;
        manager.add_connection(wrapper);

        if keep_alive {
            let _ = manager.set_keep_alive(k, true);
        }
    }

    /// Opens a new unidirectional stream on the connection with the given ID.
    ///
    /// # Errors
    /// Returns [`ServerStreamError::ConnectionManager`] if the connection ID is
    /// unknown and [`ServerStreamError::Open`] if `quinn` cannot open the
    /// stream because the connection is closed or otherwise unusable.
    ///
    /// # Edge Cases
    /// A connection can disappear between looking it up and calling
    /// `open_uni()`. In that case the ID lookup succeeds and the later
    /// `quinn::ConnectionError` is returned as [`ServerStreamError::Open`].
    pub async fn get_unistream(&self, id: &u64) -> Result<ServerUniStream, ServerStreamError> {
        let conn = self
            .conn_manager
            .store
            .get(id)
            .ok_or(ConnectionManagerError::IdNotFound(*id))?
            .conn
            .clone();

        let stream = conn.open_uni().await?;
        Ok(ServerUniStream::new(stream))
    }

    /// Opens a new bidirectional stream on the connection with the given ID.
    ///
    /// # Errors
    /// Returns [`ServerStreamError::ConnectionManager`] if the connection ID is
    /// unknown and [`ServerStreamError::Open`] if `quinn` cannot open the
    /// stream because the connection is closed or otherwise unusable.
    ///
    /// # Edge Cases
    /// The connection may close after lookup but before the stream opens. That
    /// race is surfaced as [`ServerStreamError::Open`].
    pub async fn get_bistream(&self, id: &u64) -> Result<BiStream, ServerStreamError> {
        let conn = self
            .conn_manager
            .store
            .get(id)
            .ok_or(ConnectionManagerError::IdNotFound(*id))?
            .conn
            .clone();

        let (send_stream, recv_stream) = conn.open_bi().await?;
        Ok(BiStream::new(true, send_stream, recv_stream))
    }
}

/// Errors returned when opening server-initiated streams through [`ServerInstance`].
#[derive(Debug, thiserror::Error)]
pub enum ServerStreamError {
    /// The requested connection ID is not currently tracked by the connection manager.
    #[error(transparent)]
    ConnectionManager(#[from] ConnectionManagerError),

    /// `quinn` failed to open a new stream on an otherwise known connection.
    #[error(transparent)]
    Open(#[from] quinn::ConnectionError),
}
