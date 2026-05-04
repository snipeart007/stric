use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    connection_wrapper::{ConnectionContext, ConnectionWrapper},
    keep_alive::KeepAlivePool,
    stream::ServerUniStream,
};

pub struct ConnectionManager<ConnectionMetadata: Default + Send + Sync + 'static> {
    pub store: DashMap<u64, ConnectionWrapper<ConnectionMetadata>>,
    pub default_conn_context: ConnectionContext,
    pub keep_alive_pool: Arc<KeepAlivePool>,
    pub idle_timeout: Duration,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ConnectionManager<ConnectionMetadata> {
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

    pub fn set_client_uni(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.client_uni = val;

        Ok(())
    }

    pub fn set_client_bi(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.client_bi = val;

        Ok(())
    }

    pub fn set_server_uni(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.server_uni = val;

        Ok(())
    }

    pub fn set_server_bi(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let mut connection = self
            .store
            .get_mut(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        connection.context.server_bi = val;

        Ok(())
    }

    pub fn add_connection(&self, wrapper: ConnectionWrapper<ConnectionMetadata>) {
        self.store.insert(wrapper.context.id, wrapper);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionManagerError {
    #[error("id not found in ConnectionManager store: {0}")]
    IdNotFound(u64),
}
