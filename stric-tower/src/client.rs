use std::task::{Context, Poll};
use tower::Service;
use stric_core::stream::BiStream;
use futures::future::BoxFuture;

use crate::error::TowerError;
use crate::http::{Request, Response};
use crate::codec::{write_request_envelope, read_response_envelope};
use crate::wire::proto::{RequestEnvelope};

/// A client-side Tower [`Service`] that sends requests over a QUIC connection.
///
/// `TowerClientService` opens a new bidirectional stream for each request,
/// wraps the request in a `RequestEnvelope`, and decodes the `ResponseEnvelope` from the peer.
pub struct TowerClientService {
    connection: quinn::Connection,
}

impl TowerClientService {
    /// Creates a new `TowerClientService` using an established QUIC connection.
    pub fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
        }
    }
}

impl Service<Request> for TowerClientService {
    type Response = Response;
    type Error = TowerError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check if connection is still alive
        if let Some(e) = self.connection.close_reason() {
            return Poll::Ready(Err(TowerError::from(e)));
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let conn = self.connection.clone();

        Box::pin(async move {
            // 1. Open new BiStream
            let (send, recv) = conn.open_bi().await?;
            let mut stream = BiStream {
                server_initiated: true,
                send_stream: send,
                recv_stream: recv,
            };

            // 2. Encode Request Envelope
            let envelope = RequestEnvelope {
                path: req.path,
                headers: req.headers,
                payload: req.body.into(),
            };
            write_request_envelope(&mut stream, envelope).await?;

            // 3. Decode Response Envelope
            let res_envelope = read_response_envelope(&mut stream).await?;

            Ok(Response {
                status: res_envelope.status_code as u16,
                headers: res_envelope.headers,
                body: res_envelope.payload.into(),
            })
        })
    }
}
