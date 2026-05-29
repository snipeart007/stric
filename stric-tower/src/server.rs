use std::sync::Arc;
use std::marker::PhantomData;
use tower::{Service, ServiceExt};
use stric_core::{
    BiStream, ConnectionContext, ConnectionHandlerFn, ConnectionWrapper, NodeConfig,
    QuicNode,
};

use crate::error::TowerError;
use crate::http::{Request as StricRequest, Response, Full, Bytes, HeaderMap, HeaderName, HeaderValue, Body, BodyExt};
use crate::codec::{read_request_envelope, write_response_envelope};
use crate::wire::proto::ResponseEnvelope;

/// A handler that bridges Stric connections to a Tower [`Service`] using an Axum-like API.
///
/// Each accepted bidirectional QUIC stream is decoded into a Stric request,
/// dispatched through the wrapped Tower service, and then encoded back onto the
/// stream as a response envelope.
pub struct TowerConnectionHandler<S, B> {
    service: S,
    _marker: PhantomData<B>,
}

impl<S, B> TowerConnectionHandler<S, B>
where
    S: Service<StricRequest<Full<Bytes>>, Response = Response<B>> + Clone + Send + Sync + 'static,
    S::Error: Into<TowerError> + Send,
    S::Future: Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    /// Creates a new `TowerConnectionHandler` with the given service.
    ///
    /// Use this when you want `stric-tower` routing or middleware behavior on
    /// top of a manually configured [`stric_core::QuicNode`]. If the
    /// development server helper is enough, prefer [`Server::serve`].
    pub fn new(service: S) -> Self {
        Self { service, _marker: PhantomData }
    }

    /// Converts the handler into a Stric-compatible `ConnectionHandlerFn`.
    ///
    /// The returned handler clones the wrapped service per accepted stream so
    /// each request can be processed concurrently.
    ///
    /// This is the low-level bridge for `stric-core` integration. It should not
    /// be used as a general request handler abstraction outside a
    /// `QuicNode::on_inbound` call.
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
                    let mut stream = BiStream::new(false, send, recv);

                    let mut service = service.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_stream_axum::<S, B>(&mut stream, &mut service).await {
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
async fn handle_stream_axum<S, B>(
    stream: &mut BiStream,
    service: &mut S,
) -> Result<(), TowerError>
where
    S: Service<StricRequest<Full<Bytes>>, Response = Response<B>> + Send,
    S::Error: Into<TowerError> + Send,
    B: Body + Send + 'static,
    B::Data: Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    // 1. Decode Request Envelope
    let envelope = read_request_envelope(stream).await?;
    
    // Direct header conversion: Prost HashMap -> HeaderMap
    let mut headers = HeaderMap::with_capacity(envelope.headers.len());
    for (k, v) in envelope.headers {
        if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(&v)) {
            headers.insert(name, value);
        }
    }

    let req = StricRequest {
        path: envelope.path,
        headers,
        body: Full::new(envelope.payload.into()),
    };

    // 2. Call Service
    service.ready().await.map_err(|e| e.into())?;
    let res = service.call(req).await.map_err(|e| e.into())?;

    // 3. Encode Response Envelope
    // Direct header conversion: HeaderMap -> Prost HashMap
    let mut res_headers = std::collections::HashMap::with_capacity(res.headers.len());
    for (name, value) in res.headers {
        if let Some(name) = name {
            if let Ok(val_str) = value.to_str() {
                res_headers.insert(name.to_string(), val_str.to_string());
            }
        }
    }

    let body = res.body.collect().await.map_err(|e| TowerError::Internal(e.into()))?.to_bytes();

    let res_envelope = ResponseEnvelope {
        status_code: res.status as u32,
        headers: res_headers,
        payload: body.into(),
    };
    write_response_envelope(stream, res_envelope).await?;

    // 4. Finish stream
    stream.finish()?;
    
    Ok(())
}

/// An ergonomic wrapper for starting a Stric server with Tower services.
///
/// `Server` hides the QUIC bootstrap details needed for local development and
/// offers a single `serve` entry point for a Stric-native Tower service.
pub struct Server {
    addr: std::net::SocketAddr,
}

impl Server {
    /// Binds the server to the given address.
    ///
    /// This constructor is infallible because it only stores the address. The
    /// actual socket bind happens later in [`serve`](Self::serve).
    pub fn bind(addr: std::net::SocketAddr) -> Self {
        Self { addr }
    }

    /// Serves the given Tower service.
    ///
    /// This method sets up a development-oriented QUIC configuration with a
    /// self-signed certificate and then forwards each accepted request stream to
    /// the supplied service.
    ///
    /// # Errors
    /// Returns `anyhow::Error` when self-signed certificate generation fails,
    /// when the underlying [`stric_core::QuicNode::new`] call fails, or
    /// when the async Stric error channel reports a connection handler failure.
    ///
    /// # Edge Cases
    /// This helper always generates a fresh self-signed certificate for
    /// `localhost`. It is intended for development and examples only. Use
    /// [`TowerConnectionHandler`] together with [`stric_core::QuicNode`]
    /// when you need production TLS or client-certificate verification.
    pub async fn serve<S, B>(self, service: S) -> Result<(), anyhow::Error>
    where
        S: Service<StricRequest<Full<Bytes>>, Response = Response<B>> + Clone + Send + Sync + 'static,
        S::Error: Into<TowerError> + Send,
        S::Future: Send + 'static,
        B: Body + Send + 'static,
        B::Data: Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
    {
        // Boilerplate for QUIC crypto (using rustls with ring)
        let _ = quinn::rustls::crypto::ring::default_provider().install_default();

        // Generate self-signed certificate for development
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.cert.der().to_vec();
        let key_der = cert.signing_key.serialize_der();
        let certs = vec![quinn::rustls::pki_types::CertificateDer::from(cert_der)];
        let key = quinn::rustls::pki_types::PrivateKeyDer::try_from(key_der).unwrap();

        let config = NodeConfig {
            certs: Some(certs),
            key: Some(key),
            socket_addr: self.addr,
            alpn_protocol_names: vec![b"stric".to_vec()],
            error_channel_len: 10,
            default_conn_context: ConnectionContext::default(),
            keep_alive_limit_per_thread: 0,
            idle_timeout: Some(std::time::Duration::from_secs(60)),
            root_cert_store: None,
            danger_accept_invalid_certs: false,
        };

        let tower_handler = TowerConnectionHandler::<S, B>::new(service);
        let (mut node, mut error_rx) = QuicNode::<()>::new(config)?;
        node.on_inbound(tower_handler.into_handler());

        println!("Server listening on {}", self.addr);

        tokio::select! {
            _ = node.listen() => {},
            Some(err) = error_rx.recv() => {
                return Err(err);
            }
        }

        Ok(())
    }
}
