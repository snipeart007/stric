#[derive(Clone)]
pub struct ConnectionWrapper<ConnectionMetadata: Default + Send + Sync + 'static> {
    pub conn: quinn::Connection,
    pub context: ConnectionContext,
    pub metadata: ConnectionMetadata,
}

#[derive(Clone, Copy)]
pub struct ConnectionContext {
    pub id: u64,
    // Whether to keep the connection alive using Heartbeat pings
    pub keep_alive: bool,
    // Initiate UniStream from Client-side
    pub client_uni: bool,
    // Initiate BiStream from Client-side
    pub client_bi: bool,
    // Initiate UniStream from Server-side
    pub server_uni: bool,
    // Initiate BiStream from Server-side
    pub server_bi: bool,
}

impl Default for ConnectionContext {
    fn default() -> Self {
        ConnectionContext {
            id: 0,
            keep_alive: false,
            client_uni: false,
            client_bi: false,
            server_uni: false,
            server_bi: false,
        }
    }
}
