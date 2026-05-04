use std::sync::Arc;
use tower::{Service, ServiceExt};
use stric_core::connection_wrapper::ConnectionWrapper;
use stric_core::handler_types::ConnectionHandlerFn;
use stric_core::stream::BiStream;

use crate::codec::ServiceCodec;
use crate::error::TowerError;

use std::marker::PhantomData;

/// A handler that bridges Stric connections to a Tower [`Service`].
///
/// `TowerConnectionHandler` implements the Stric `ConnectionHandlerFn` by accepting
/// incoming bidirectional streams and dispatching requests to the wrapped service.
///
/// Each request-response interaction happens on its own bidirectional stream.
pub struct TowerConnectionHandler<S, C, Req, Res> {
    service: S,
    codec: C,
    _marker: PhantomData<(Req, Res)>,
}

impl<S, C, Req, Res> TowerConnectionHandler<S, C, Req, Res>
where
    S: Service<Req, Response = Res> + Clone + Send + Sync + 'static,
    S::Error: Into<TowerError> + Send,
    S::Future: Send,
    C: ServiceCodec<Req, Res> + Clone + Send + Sync + 'static,
    Req: Send + 'static,
    Res: Send + 'static,
{
    /// Creates a new `TowerConnectionHandler` with the given service and codec.
    pub fn new(service: S, codec: C) -> Self {
        Self {
            service,
            codec,
            _marker: PhantomData,
        }
    }

    /// Converts the handler into a Stric-compatible `ConnectionHandlerFn`.
    ///
    /// This function returns a closure that can be registered with a Stric server.
    pub fn into_handler<M>(self) -> ConnectionHandlerFn<M>
    where
        M: Default + Send + Sync + 'static,
    {
        let service = self.service;
        let codec = self.codec;

        Arc::new(move |wrapper: &mut ConnectionWrapper<M>| {
            let conn = wrapper.conn.clone();
            let service = service.clone();
            let codec = codec.clone();

            Box::pin(async move {
                while let Ok((send, recv)) = conn.accept_bi().await {
                    let mut stream = BiStream {
                        server_initiated: false,
                        send_stream: send,
                        recv_stream: recv,
                    };

                    let mut service = service.clone();
                    let codec = codec.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_stream(&mut stream, &mut service, &codec).await {
                            // We might want to log this or send it to an error channel
                            eprintln!("Error handling stream: {:?}", e);
                        }
                    });
                }
                Ok(())
            })
        })
    }
}

/// Internal helper to handle an individual bidirectional stream.
async fn handle_stream<S, C, Req, Res>(
    stream: &mut BiStream,
    service: &mut S,
    codec: &C,
) -> Result<(), TowerError>
where
    S: Service<Req, Response = Res> + Send,
    S::Error: Into<TowerError> + Send,
    C: ServiceCodec<Req, Res>,
{
    // 1. Decode Request
    let req = codec.decode_request(stream).await?;

    // 2. Call Service
    // Wait for service to be ready
    service.ready().await.map_err(|e| e.into())?;
    let res = service.call(req).await.map_err(|e| e.into())?;

    // 3. Encode Response
    codec.encode_response(res, stream).await?;

    // 4. Finish stream
    stream.finish()?;
    
    Ok(())
}
