pub struct ServerUniStream {
    pub stream: quinn::SendStream,
}

pub struct ClientUniStream {
    pub stream: quinn::RecvStream,
}

pub struct BiStream {
    // True if server initiated, false if client initiated
    pub server_initiated: bool,
    pub recv_stream: quinn::RecvStream,
    pub send_stream: quinn::SendStream,
}
