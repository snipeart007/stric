use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::connection_wrapper::{ConnectionContext, ConnectionWrapper};

pub struct ConnectionManager<ConnectionMetadata: Send + Sync + 'static> {
    pub store: HashMap<u64, Arc<Mutex<ConnectionWrapper<ConnectionMetadata>>>>,
    pub default_conn_context: ConnectionContext,
}

impl<ConnectionMetadata: Send + Sync + 'static> ConnectionManager<ConnectionMetadata> {
    pub fn new(default_conn_context: ConnectionContext) -> Self {
        Self {
            store: HashMap::new(),
            default_conn_context,
        }
    }

    pub fn set_client_uni(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
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

    pub fn set_client_bi(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
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

    pub fn set_server_uni(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
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

    pub fn set_server_bi(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
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
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionManagerError {
    #[error("UUID not found in ConnectionManager store: {0}")]
    UUIDNotFound(u64),
    #[error("Some thread panicked while holding the mutex to the connection instance")]
    ThreadPanickedWhileConnectionMutexGuard,
}
