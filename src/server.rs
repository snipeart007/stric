use std::sync::{Arc, RwLock};

use crate::{
    connection::ConnectionManager, connection_wrapper::ConnectionWrapper,
    handler_types::ConnectionHandlerFn, server_config::ServerConfig,
};
use quinn::rustls::ServerConfig as RustlsServerConfig;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct ServerInstance<ConnectionMetadata: Default + Send + Sync + 'static> {
    pub endpoint: quinn::Endpoint,
    pub conn_manager: Arc<RwLock<ConnectionManager<ConnectionMetadata>>>,
    pub conn_handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
    pub error_tx: Sender<anyhow::Error>,
    pub error_rx: Receiver<anyhow::Error>,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ServerInstance<ConnectionMetadata> {
    pub fn new(config: ServerConfig) -> Result<ServerInstance<ConnectionMetadata>, anyhow::Error> {
        let mut server_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(config.certs, config.key)?;

        server_config.alpn_protocols = config.alpn_protocol_names;

        let quinn_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_config)?,
        ));

        let endpoint = quinn::Endpoint::server(quinn_config, config.socket_addr)?;

        let (error_tx, error_rx) = mpsc::channel::<anyhow::Error>(config.error_channel_len);
        Ok(Self {
            endpoint,
            conn_manager: Arc::new(RwLock::new(ConnectionManager::new(
                config.default_conn_context,
            ))),
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
        manager_lock: Arc<RwLock<ConnectionManager<ConnectionMetadata>>>,
        conn_handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
        error_tx: Sender<anyhow::Error>,
    ) {
        let Ok(connection) = incoming.await.map_err(|e| {
            let _ = error_tx.try_send(e.into()); // Send error to channel
        }) else {
            return; // Exit the green thread
        };
        let k = connection.stable_id() as u64;

        let context = {
            let Ok(manager_read) = manager_lock.read().map_err(|_| {
                let _ = error_tx.try_send(ServerError::ConnManagerPoisoned.into()); // Send error to channel
            }) else {
                return; // Exit the green thread
            };
            let mut context = manager_read.default_conn_context.clone();
            context.uuid = k;
            context
        };

        let metadata = ConnectionMetadata::default();

        let mut wrapper = ConnectionWrapper {
            conn: connection,
            context,
            metadata,
        };

        if let Some(func) = conn_handler {
            if let Err(e) = func(&mut wrapper).await {
                let _ = error_tx.try_send(e);
                return;
            }
        }

        let Ok(mut manager_write) = manager_lock.write().map_err(|_| {
            let _ = error_tx.try_send(ServerError::ConnManagerPoisoned.into()); // Send error to channel
        }) else {
            return; // Exit the green thread
        };

        manager_write.add_connection(wrapper);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("ConnectionManger is poisoned")]
    ConnManagerPoisoned,
}
