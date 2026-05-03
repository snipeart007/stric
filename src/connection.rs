use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

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

    pub fn set_client_uni(&self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?; // Use ? operator for cleaner error propagation

        let mut value = connection
            .lock()
            .map_err(|_| ConnectionManagerError::ThreadPanickedWhileConnectionMutexGuard)?; // Map the poisoned error to your custom error

        value.context.client_uni = val;

        Ok(())
    }

    pub fn set_client_bi(&self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?;

        let mut value = connection
            .lock()
            .map_err(|_| ConnectionManagerError::ThreadPanickedWhileConnectionMutexGuard)?;

        value.context.client_bi = val;

        Ok(())
    }

    pub fn set_server_uni(&self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?;

        let mut value = connection
            .lock()
            .map_err(|_| ConnectionManagerError::ThreadPanickedWhileConnectionMutexGuard)?;

        value.context.server_uni = val;

        Ok(())
    }

    pub fn set_server_bi(&self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let connection = self
            .store
            .get(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?;

        let mut value = connection
            .lock()
            .map_err(|_| ConnectionManagerError::ThreadPanickedWhileConnectionMutexGuard)?;

        value.context.server_bi = val;

        Ok(())
    }

    pub fn add_connection(&mut self, wrapper: ConnectionWrapper<ConnectionMetadata>) {
        self.store.insert(wrapper.context.uuid, Arc::new(Mutex::new(wrapper)));
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionManagerError {
    #[error("UUID not found in ConnectionManager store: {0}")]
    UUIDNotFound(u64),
    #[error("Some thread panicked while holding the mutex to the connection instance")]
    ThreadPanickedWhileConnectionMutexGuard,
}
