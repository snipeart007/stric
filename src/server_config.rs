use std::net::SocketAddr;

use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::connection_wrapper::ConnectionContext;

pub struct ServerConfig {
    pub certs: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
    pub socket_addr: SocketAddr,
    pub alpn_protocol_names: Vec<Vec<u8>>,
    pub error_channel_len: usize,
    pub default_conn_context: ConnectionContext,
}
