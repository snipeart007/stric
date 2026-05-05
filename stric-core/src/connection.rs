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
    /// A thread-safe map storing accepted connections indexed by their stable ID.
    ///
    /// This field is public for low-level inspection and diagnostics. Prefer the
    /// higher-level methods on [`ServerInstance`](crate::ServerInstance) and
    /// `ConnectionManager` for ordinary server logic rather than mutating the map directly.
    pub store: DashMap<u64, ConnectionWrapper<ConnectionMetadata>>,
    pub(crate) default_conn_context: ConnectionContext,
    keep_alive_pool: Arc<KeepAlivePool>,
    idle_timeout: Duration,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ConnectionManager<ConnectionMetadata> {
    pub(crate) fn new(
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
    /// Returns [`ConnectionManagerError::IdNotFound`] when `id` does not refer
    /// to a currently tracked connection.
    ///
    /// # Edge Cases
    /// When keep-alive is enabled, stream creation happens in a background
    /// task. If the connection closes before the task opens the stream, the
    /// call still returns `Ok(())` and no keep-alive stream is started.
    pub fn set_keep_alive(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
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
                    pool.add_stream(ServerUniStream::new(stream), interval)
                        .await;
                        }
                    });
        }

        Ok(())
    }

    /// Sets whether the client is allowed to initiate unidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_client_uni(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.client_uni = val;

        Ok(())
    }

    /// Sets whether the client is allowed to initiate bidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_client_bi(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.client_bi = val;

        Ok(())
    }

    /// Sets whether the server is allowed to initiate unidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_server_uni(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.server_uni = val;

        Ok(())
    }

    /// Sets whether the server is allowed to initiate bidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_server_bi(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.server_bi = val;

        Ok(())
    }

    /// Adds a connection wrapper to the manager's store.
    pub(crate) fn add_connection(&self, wrapper: ConnectionWrapper<ConnectionMetadata>) {
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
