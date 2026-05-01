use std::sync::Arc;

use crate::{connection::ConnectionManager, server_config::ServerConfig};
use quinn::rustls::ServerConfig as RustlsServerConfig;

pub struct ServerInstance<ConnectionMetadata: Send + Sync + 'static> {
    pub endpoint: quinn::Endpoint,
    pub conn_manager: ConnectionManager<ConnectionMetadata>,
    
}

impl<ConnectionMetadata: Send + Sync + 'static> ServerInstance<ConnectionMetadata> {
    pub fn new(config: ServerConfig) -> Result<ServerInstance<ConnectionMetadata>, anyhow::Error> {
        let mut server_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(config.certs, config.key)?;

        server_config.alpn_protocols = config.alpn_protocol_names;

        let quinn_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_config)?,
        ));
        let endpoint = quinn::Endpoint::server(quinn_config, config.socket_addr)?;
        Ok(Self {
            endpoint,
            conn_manager: ConnectionManager::new(),
        })
    }
}
