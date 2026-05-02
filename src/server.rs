use std::sync::{Arc, RwLock};

use crate::{
    connection::ConnectionManager, handler_types::ConnectionHandlerFn, server_config::ServerConfig,
};
use quinn::rustls::ServerConfig as RustlsServerConfig;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct ServerInstance<ConnectionMetadata: Send + Sync + 'static> {
    pub endpoint: quinn::Endpoint,
    pub conn_manager: Arc<RwLock<ConnectionManager<ConnectionMetadata>>>,
    pub conn_handler: Option<ConnectionHandlerFn>,
    pub error_tx: Sender<anyhow::Error>,
    pub error_rx: Receiver<anyhow::Error>,
}

impl<ConnectionMetadata: Send + Sync + 'static> ServerInstance<ConnectionMetadata> {
    pub fn new(config: ServerConfig) -> Result<ServerInstance<ConnectionMetadata>, anyhow::Error> {
        let mut server_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(config.certs, config.key)?;

        server_config.alpn_protocols = config.alpn_protocol_names;

        let quinn_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_config)?,
        ));

        let endpoint = quinn::Endpoint::server(quinn_config, config.socket_addr)?;

        let (error_tx, mut error_rx) = mpsc::channel::<anyhow::Error>(config.error_channel_len);
        Ok(Self {
            endpoint,
            conn_manager: Arc::new(RwLock::new(ConnectionManager::new(config.default_conn_context))),
            conn_handler: None,
            error_rx,
            error_tx,
        })
    }
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

    pub async fn handle_incoming(
        incoming: quinn::Incoming,
        manager: Arc<RwLock<ConnectionManager<ConnectionMetadata>>>,
        conn_handler: Option<ConnectionHandlerFn>,
        error_tx: Sender<anyhow::Error>,
    ) {
        let Ok(connection) = incoming.await.map_err(|e| {
            let _ = error_tx.try_send(e.into()); // Send error to channel
        }) else {
            return; // Exit the green thread
        };

        
    }
}
