pub struct ConnectionWrapper<ConnectionMetadata: Send + Sync + 'static> {
    pub conn: quinn::Connection,
    pub context: ConnectionContext,
    pub metadata: ConnectionMetadata,
}

pub struct ConnectionContext {
    pub uuid: u64,
    // Initiate UniStream from Client-side
    pub client_uni: bool,
    // Initiate BiStream from Client-side
    pub client_bi: bool,
    // Initiate UniStream from Server-side
    pub server_uni: bool,
    // Initiate BiStream from Server-side
    pub server_bi: bool,
}
