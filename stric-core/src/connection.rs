use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use crate::{
    connection_wrapper::{ConnectionContext, ConnectionWrapper},
    keep_alive::KeepAlivePool,
    stream::SendUniStream,
};

/// Manages a collection of active QUIC connections.
///
/// `ConnectionManager` is responsible for storing connections, managing their lifecycle,
/// and providing methods to update connection-specific settings like keep-alive.
///
/// # Type Parameters
/// * `ConnectionMetadata`: A user-defined type for storing custom metadata associated with each connection.
pub struct ConnectionManager<ConnectionMetadata: Default + Send + Sync + 'static> {
    /// A thread-safe map of active connections, keyed by their stable ID.
    pub store: DashMap<u64, ConnectionWrapper<ConnectionMetadata>>,
    /// The default context used when initializing new connections.
    pub(crate) default_conn_context: ConnectionContext,
    /// A background pool for managing keep-alive heartbeats.
    pub(crate) keep_alive_pool: Arc<KeepAlivePool>,
    /// The idle timeout for connections.
    pub(crate) idle_timeout: Duration,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ConnectionManager<ConnectionMetadata> {
    /// Creates a new `ConnectionManager`.
    pub(crate) fn new(
        default_conn_context: ConnectionContext,
        keep_alive_limit_per_thread: u64,
        idle_timeout: Option<Duration>,
    ) -> Self {
        Self {
            store: DashMap::new(),
            default_conn_context,
            keep_alive_pool: Arc::new(KeepAlivePool::new(keep_alive_limit_per_thread)),
            idle_timeout: idle_timeout.unwrap_or(Duration::from_secs(30)),
        }
    }

    /// Sets whether to keep the specified connection alive using heartbeat pings.
    ///
    /// When enabled, Stric will periodically send small "ping" payloads over a
    /// dedicated unidirectional stream to prevent the connection from timing out.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    ///
    /// # Edge Cases
    /// When keep-alive is enabled, stream creation happens in a background
    /// task. If the connection has already closed, the keep-alive task will fail
    /// silently and remove itself from the pool.
    pub fn set_keep_alive(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        debug!("Updating keep-alive for {}: {}", id, val);
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
                debug!("Spawning keep-alive stream for connection {}", id);
                if let Ok(stream) = conn.open_uni().await {
                    pool.add_stream(SendUniStream::new(stream), interval)
                        .await;
                } else {
                    warn!("Failed to open keep-alive stream for connection {}", id);
                }
            });
        }

        Ok(())
    }

    /// Sets whether the connection initiator is allowed to open unidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_initiator_uni(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        debug!("Setting initiator_uni for {}: {}", id, val);
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.initiator_uni = val;

        Ok(())
    }

    /// Sets whether the connection initiator is allowed to open bidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_initiator_bi(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        debug!("Setting initiator_bi for {}: {}", id, val);
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.initiator_bi = val;

        Ok(())
    }

    /// Sets whether the connection responder is allowed to open unidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_responder_uni(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        debug!("Setting responder_uni for {}: {}", id, val);
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.responder_uni = val;

        Ok(())
    }

    /// Sets whether the connection responder is allowed to open bidirectional streams.
    ///
    /// # Errors
    /// Returns [`ConnectionManagerError::IdNotFound`] when the connection has
    /// not been registered or has already been removed.
    pub fn set_responder_bi(&self, id: u64, val: bool) -> Result<(), ConnectionManagerError> {
        debug!("Setting responder_bi for {}: {}", id, val);
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.responder_bi = val;

        Ok(())
    }

    /// Adds a connection wrapper to the manager's store.
    pub(crate) fn add_connection(&self, wrapper: ConnectionWrapper<ConnectionMetadata>) {
        debug!("Adding connection {} to manager", wrapper.context.id);
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
