use std::sync::Arc;
use tower::{Service, ServiceExt};
use stric_core::connection_wrapper::ConnectionWrapper;
use stric_core::handler_types::ConnectionHandlerFn;
use stric_core::stream::BiStream;
use stric_core::server::ServerInstance;
use stric_core::server_config::ServerConfig;
use stric_core::connection_wrapper::ConnectionContext;

use crate::error::TowerError;
use crate::http::{Request, Response};
use crate::codec::{read_request_envelope, write_response_envelope};
use crate::wire::proto::ResponseEnvelope;

/// A handler that bridges Stric connections to a Tower [`Service`] using an Axum-like API.
pub struct TowerConnectionHandler<S> {
    service: S,
}

impl<S> TowerConnectionHandler<S>
where
    S: Service<Request, Response = Response> + Clone + Send + Sync + 'static,
    S::Error: Into<TowerError> + Send,
    S::Future: Send,
{
    /// Creates a new `TowerConnectionHandler` with the given service.
    pub fn new(service: S) -> Self {
        Self { service }
    }

    /// Converts the handler into a Stric-compatible `ConnectionHandlerFn`.
    pub fn into_handler<M>(self) -> ConnectionHandlerFn<M>
    where
        M: Default + Send + Sync + 'static,
    {
        let service = self.service;

        Arc::new(move |wrapper: &mut ConnectionWrapper<M>| {
            let conn = wrapper.conn.clone();
            let service = service.clone();

            Box::pin(async move {
                while let Ok((send, recv)) = conn.accept_bi().await {
                    let mut stream = BiStream {
                        server_initiated: false,
                        send_stream: send,
                        recv_stream: recv,
                    };

                    let mut service = service.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_stream_axum(&mut stream, &mut service).await {
                            eprintln!("Error handling stream: {:?}", e);
                        }
                    });
                }
                Ok(())
            })
        })
    }
}

/// Internal helper to handle an individual bidirectional stream using the axum-like protocol.
async fn handle_stream_axum<S>(
    stream: &mut BiStream,
    service: &mut S,
) -> Result<(), TowerError>
where
    S: Service<Request, Response = Response> + Send,
    S::Error: Into<TowerError> + Send,
{
    // 1. Decode Request Envelope
    let envelope = read_request_envelope(stream).await?;
    let req = Request {
        path: envelope.path,
        headers: envelope.headers,
        body: envelope.payload.into(),
    };

    // 2. Call Service
    service.ready().await.map_err(|e| e.into())?;
    let res = service.call(req).await.map_err(|e| e.into())?;

    // 3. Encode Response Envelope
    let res_envelope = ResponseEnvelope {
        status_code: res.status as u32,
        headers: res.headers,
        payload: res.body.into(),
    };
    write_response_envelope(stream, res_envelope).await?;

    // 4. Finish stream
    stream.finish()?;
    
    Ok(())
}

/// An ergonomic wrapper for starting a Stric server with Tower services.
pub struct Server {
    addr: std::net::SocketAddr,
}

impl Server {
    /// Binds the server to the given address.
    pub fn bind(addr: std::net::SocketAddr) -> Result<Self, anyhow::Error> {
        Ok(Self { addr })
    }

    /// Serves the given Tower service.
    ///
    /// This method sets up a default QUIC configuration with a self-signed certificate.
    pub async fn serve<S>(self, service: S) -> Result<(), anyhow::Error>
    where
        S: Service<Request, Response = Response> + Clone + Send + Sync + 'static,
        S::Error: Into<TowerError> + Send,
        S::Future: Send,
    {
        // Boilerplate for QUIC crypto (using rustls with ring)
        let _ = quinn::rustls::crypto::ring::default_provider().install_default();

        // Generate self-signed certificate for development
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().to_vec();
        let key_der = cert.signing_key.serialize_der();
        let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der)];
        let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();

        let config = ServerConfig {
            certs,
            key,
            socket_addr: self.addr,
            alpn_protocol_names: vec![b"h3".to_vec()],
            error_channel_len: 10,
            default_conn_context: ConnectionContext::default(),
            keep_alive_limit_per_thread: 0,
            idle_timeout: Some(std::time::Duration::from_secs(60)),
        };

        let tower_handler = TowerConnectionHandler::new(service);
        let (mut server, mut error_rx) = ServerInstance::<()>::new(config)?;
        server.register_connection_handler(tower_handler.into_handler());

        println!("Server listening on {}", self.addr);

        tokio::select! {
            _ = server.listen_connections() => {},
            Some(err) = error_rx.recv() => {
                return Err(err);
            }
        }

        Ok(())
    }
}
