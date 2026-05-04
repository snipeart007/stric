use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    connection_wrapper::{ConnectionContext, ConnectionWrapper},
    keep_alive::KeepAlivePool,
    stream::ServerUniStream,
};

/// Manages a collection of active QUIC connections.
///
/// `ConnectionManager` is responsible for storing connections, managing their lifecycle,
/// and providing methods to update connection-specific settings like keep-alive.
pub struct ConnectionManager<ConnectionMetadata: Default + Send + Sync + 'static> {
    /// A thread-safe map storing connections indexed by their stable ID.
    pub store: DashMap<u64, ConnectionWrapper<ConnectionMetadata>>,
    /// The default context to apply to new connections.
    pub default_conn_context: ConnectionContext,
    /// The pool for managing keep-alive heartbeat streams.
    pub keep_alive_pool: Arc<KeepAlivePool>,
    /// The idle timeout duration for connections.
    pub idle_timeout: Duration,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ConnectionManager<ConnectionMetadata> {
    /// Creates a new `ConnectionManager`.
    pub fn new(
        default_conn_context: ConnectionContext,
        keep_alive_limit: u64,
        idle_timeout: Option<Duration>,
    ) -> Self {
        let timeout = idle_timeout.unwrap_or(Duration::from_secs(10));
        Self {
            store: DashMap::new(),
            default_conn_context,
            keep_alive_pool: Arc::new(KeepAlivePool::new(keep_alive_limit)),
            idle_timeout: timeout,
        }
    }

    /// Enables or disables keep-alive for a specific connection.
    ///
    /// If enabled, a background task is spawned to send periodic "ping" messages
    /// over a unidirectional stream.
    ///
    /// # Errors
    /// Returns an error if the connection ID is not found.
    pub fn set_keep_alive(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.keep_alive = val;

        if val {
            let conn = connection.conn.clone();
            let pool = self.keep_alive_pool.clone();
            let interval = self.idle_timeout / 2;
            tokio::spawn(async move {
                if let Ok(stream) = conn.open_uni().await {
                    pool.add_stream(id, ServerUniStream { stream }, interval)
                        .await;
                }
            });
        }

        Ok(())
    }

    /// Sets whether the client is allowed to initiate unidirectional streams.
    pub fn set_client_uni(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.client_uni = val;

        Ok(())
    }

    /// Sets whether the client is allowed to initiate bidirectional streams.
    pub fn set_client_bi(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.client_bi = val;

        Ok(())
    }

    /// Sets whether the server is allowed to initiate unidirectional streams.
    pub fn set_server_uni(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.server_uni = val;

        Ok(())
    }

    /// Sets whether the server is allowed to initiate bidirectional streams.
    pub fn set_server_bi(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.server_bi = val;

        Ok(())
    }

    /// Adds a connection wrapper to the manager's store.
    pub fn add_connection(&self, wrapper: ConnectionWrapper<ConnectionMetadata>) {
        self.store.insert(wrapper.context.id, wrapper);
    }
}

/// Errors related to connection management.
#[derive(Debug, thiserror::Error)]
pub enum ConnectionManagerError {
    /// The specified connection ID was not found in the manager's store.
    #[error("id not found in ConnectionManager store: {0}")]
    IdNotFound(u64),
}
