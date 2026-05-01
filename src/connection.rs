use std::{collections::HashMap, sync::{Arc, Mutex}};

use crate::connection_wrapper::ConnectionWrapper;

pub struct ConnectionManager<ConnectionMetadata: Send + Sync + 'static> {
    pub store: HashMap<u64, Arc<Mutex<ConnectionWrapper<ConnectionMetadata>>>>,
}

impl<ConnectionMetadata: Send + Sync + 'static> ConnectionManager<ConnectionMetadata> {
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    pub fn set_client_uni(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let value = self
            .store
            .get_mut(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?;
        value.context.client_uni = val;
        Ok(())
    }

    pub fn set_server_uni(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let value = self
            .store
            .get_mut(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?;
        value.context.server_uni = val;
        Ok(())
    }

    pub fn set_client_bi(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let value = self
            .store
            .get_mut(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?;
        value.context.client_bi = val;
        Ok(())
    }

    pub fn set_server_bi(&mut self, uuid: u64, val: bool) -> Result<(), anyhow::Error> {
        let value = self
            .store
            .get_mut(&uuid)
            .ok_or(ConnectionManagerError::UUIDNotFound(uuid))?;
        value.context.server_bi = val;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionManagerError {
    #[error("UUID not found in ConnectionManager store: {0}")]
    UUIDNotFound(u64),
}
