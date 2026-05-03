use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, MutexGuard};

use crate::connection_wrapper::{ConnectionContext, ConnectionWrapper};

pub struct ConnectionManager<ConnectionMetadata: Default + Send + Sync + 'static> {
    pub store: HashMap<u64, Arc<Mutex<ConnectionWrapper<ConnectionMetadata>>>>,
    pub default_conn_context: ConnectionContext,
}

impl<ConnectionMetadata: Default + Send + Sync + 'static> ConnectionManager<ConnectionMetadata> {
    pub fn new(default_conn_context: ConnectionContext) -> Self {
        Self {
            store: HashMap::new(),
            default_conn_context,
        }
    }

    pub async fn set_client_uni(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        let mut value = connection.lock().await;

        value.context.client_uni = val;

        Ok(())
    }

    pub async fn set_client_bi(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        let mut value = connection.lock().await;

        value.context.client_bi = val;

        Ok(())
    }

    pub async fn set_server_uni(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        let mut value = connection.lock().await;

        value.context.server_uni = val;

        Ok(())
    }

    pub async fn set_server_bi(&self, id: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&id)
            .ok_or(ConnectionManagerError::IdNotFound(id))?;

        let mut value = connection.lock().await;

        value.context.server_bi = val;

        Ok(())
    }

    pub fn add_connection(&mut self, wrapper: ConnectionWrapper<ConnectionMetadata>) {
        self.store
            .insert(wrapper.context.id, Arc::new(Mutex::new(wrapper)));
    }

    pub async fn get_connection(
        &self,
        id: &u64,
    ) -> Result<MutexGuard<'_, ConnectionWrapper<ConnectionMetadata>>, anyhow::Error> {
        if let Some(wrapper_lock) = self.store.get(id) {
            return Ok(wrapper_lock.lock().await);
        }
        Err(ConnectionManagerError::IdNotFound(*id).into())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionManagerError {
    #[error("id not found in ConnectionManager store: {0}")]
    IdNotFound(u64),
}
