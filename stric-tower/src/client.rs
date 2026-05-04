use std::marker::PhantomData;
use std::task::{Context, Poll};
use tower::Service;
use stric_core::stream::BiStream;
use futures::future::BoxFuture;

use crate::codec::ServiceCodec;
use crate::error::TowerError;

pub struct TowerClientService<C, Req, Res> {
    connection: quinn::Connection,
    codec: C,
    _marker: PhantomData<(Req, Res)>,
}

impl<C, Req, Res> TowerClientService<C, Req, Res> {
    pub fn new(connection: quinn::Connection, codec: C) -> Self {
        Self {
            connection,
            codec,
            _marker: PhantomData,
        }
    }
}

impl<C, Req, Res> Service<Req> for TowerClientService<C, Req, Res>
where
    C: ServiceCodec<Req, Res> + Clone + Send + Sync + 'static,
    Req: Send + 'static,
    Res: Send + 'static,
{
    type Response = Res;
    type Error = TowerError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check if connection is still alive
        if let Some(e) = self.connection.close_reason() {
            return Poll::Ready(Err(TowerError::from(e)));
        }
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let conn = self.connection.clone();
        let codec = self.codec.clone();

        Box::pin(async move {
            // 1. Open new BiStream
            let (send, recv) = conn.open_bi().await?;
            let mut stream = BiStream {
                server_initiated: true,
                send_stream: send,
                recv_stream: recv,
            };

            // 2. Encode Request
            codec.encode_request(req, &mut stream).await?;

            // 3. Decode Response
            let res = codec.decode_response(&mut stream).await?;

            Ok(res)
        })
    }
}
