use std::task::{Context, Poll};
use tower::Service;
use futures::future::BoxFuture;
use stric_core::BiStream;

use crate::error::TowerError;
use crate::http::{Request, Response, Full, HeaderMap, HeaderName, HeaderValue, BodyExt};
use crate::codec::{write_request_envelope, read_response_envelope};
use crate::wire::proto::{RequestEnvelope};
use quinn::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use quinn::rustls::client::danger::{ServerCertVerifier, ServerCertVerified, HandshakeSignatureValid};
use quinn::rustls::{Error, SignatureScheme, DigitallySignedStruct};

/// A client-side Tower [`Service`] that sends requests over a QUIC connection.
///
/// `TowerClientService` opens a new bidirectional stream for each request,
/// wraps the request in a `RequestEnvelope`, and decodes the `ResponseEnvelope` from the peer.
#[derive(Clone)]
pub struct TowerClientService {
    connection: quinn::Connection,
}

impl TowerClientService {
    /// Creates a new `TowerClientService` using an established QUIC connection.
    ///
    /// The service is cheap to construct around an already connected
    /// `quinn::Connection` and can then be used like any other Tower client.
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
            let mut stream = BiStream::new(true, send, recv);

            // 2. Encode Request Envelope
            // Direct header conversion: HeaderMap -> Prost HashMap
            let mut req_headers = std::collections::HashMap::with_capacity(req.headers.len());
            for (name, value) in req.headers {
                if let Some(name) = name {
                    if let Ok(val_str) = value.to_str() {
                        req_headers.insert(name.to_string(), val_str.to_string());
                    }
                }
            }

            let body = req.body.collect().await.map_err(|e| TowerError::Internal(e.into()))?.to_bytes();

            let envelope = RequestEnvelope {
                path: req.path,
                headers: req_headers,
                payload: body.into(),
            };
            write_request_envelope(&mut stream, envelope).await?;

            // 3. Decode Response Envelope
            let res_envelope = read_response_envelope(&mut stream).await?;

            // Direct header conversion: Prost HashMap -> HeaderMap
            let mut res_headers = HeaderMap::with_capacity(res_envelope.headers.len());
            for (k, v) in res_envelope.headers {
                if let (Ok(name), Ok(value)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(&v)) {
                    res_headers.insert(name, value);
                }
            }

            Ok(Response {
                status: res_envelope.status_code as u16,
                headers: res_headers,
                body: Full::new(res_envelope.payload.into()),
            })
        })
    }
}

/// A rustls verifier that unconditionally accepts the server certificate.
///
/// This type exists only to make examples and local development setup easier.
/// It disables server identity verification entirely and must not be used in
/// production or on untrusted networks.
#[derive(Clone, Copy, Debug, Default)]
pub struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        quinn::rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
