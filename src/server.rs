use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    connection::ConnectionManager,
    connection_wrapper::ConnectionWrapper,
    handler_types::ConnectionHandlerFn,
    server_config::ServerConfig,
    stream::{BiStream, ServerUniStream},
};
use quinn::rustls::ServerConfig as RustlsServerConfig;
use tokio::sync::mpsc::{self, Receiver, Sender};

pub struct ServerInstance<ConnectionMetadata: Default + Send + Sync + 'static> {
    pub endpoint: quinn::Endpoint,
    pub conn_manager: Arc<RwLock<ConnectionManager<ConnectionMetadata>>>,
    pub conn_handler: Option<ConnectionHandlerFn<ConnectionMetadata>>,
    pub error_tx: Sender<anyhow::Error>,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ServerInstance<ConnectionMetadata> {
    pub fn new(
        config: ServerConfig,
    ) -> Result<(ServerInstance<ConnectionMetadata>, Receiver<anyhow::Error>), anyhow::Error> {
        let mut server_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(config.certs, config.key)?;

        server_config.alpn_protocols = config.alpn_protocol_names;

        let quinn_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_config)?,
        ));

        let endpoint = quinn::Endpoint::server(quinn_config, config.socket_addr)?;

        let (error_tx, error_rx) = mpsc::channel::<anyhow::Error>(config.error_channel_len);
        Ok((
            Self {
                endpoint,
                conn_manager: Arc::new(RwLock::new(ConnectionManager::new(
                    config.default_conn_context,
                ))),
                conn_handler: None,
                error_tx,
            },
            error_rx,
        ))
    }

    pub fn register_connection_handler(
        &mut self,
        conn_handler: ConnectionHandlerFn<ConnectionMetadata>,
    ) {
        self.conn_handler = Some(conn_handler);
    }

    pub async fn get_manager_read_lock(
        lock: &RwLock<ConnectionManager<ConnectionMetadata>>,
    ) -> RwLockReadGuard<'_, ConnectionManager<ConnectionMetadata>> {
        lock.read().await
    }

    pub async fn get_manager_write_lock(
        lock: &RwLock<ConnectionManager<ConnectionMetadata>>,
    ) -> RwLockWriteGuard<'_, ConnectionManager<ConnectionMetadata>> {
        lock.write().await
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
            let manager_read = Self::get_manager_read_lock(&manager_lock).await;
            let mut context = manager_read.default_conn_context.clone();
            context.id = k;
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

        let mut manager_write = Self::get_manager_write_lock(&manager_lock).await;
        manager_write.add_connection(wrapper);
    }

    pub async fn get_unistream(&self, id: &u64) -> Result<ServerUniStream, anyhow::Error> {
        let manager_read = Self::get_manager_read_lock(&self.conn_manager).await;
        let conn_wrapper = manager_read.get_connection(id).await?;
        let stream = conn_wrapper.conn.open_uni().await?;
        Ok(ServerUniStream { stream })
    }

    pub async fn get_bistream(&self, id: &u64) -> Result<BiStream, anyhow::Error> {
        let manager_read = Self::get_manager_read_lock(&self.conn_manager).await;
        let conn_wrapper = manager_read.get_connection(id).await?;
        let (send_stream, recv_stream) = conn_wrapper.conn.open_bi().await?;
        Ok(BiStream {
            server_initiated: true,
            send_stream,
            recv_stream,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Connection with given ID not found: {0}")]
    ConnNotFound(u64),
}
