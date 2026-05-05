use std::task::{Context, Poll};
use futures::future::BoxFuture;
use futures::FutureExt;
use tower::{Layer, Service};
use crate::http::{Request as StricRequest, Response, HttpError};
use crate::error::TowerError;
use http::request; 
use http::response;

/// Bridges a Stric-native service into the standard `http` request/response world.
///
/// `HttpServiceShim` is the inner half of the sandwich model. It lets a
/// standard Tower layer operate on `http::Request` and `http::Response` values
/// even when the real service speaks `stric-tower` request and response types.
struct HttpServiceShim<S> {
    inner: S,
}

impl<S> HttpServiceShim<S> {
    /// Wraps a Stric-native service with HTTP compatibility.
    fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Clone for HttpServiceShim<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S, B1, B2> Service<request::Request<B1>> for HttpServiceShim<S>
where
    S: Service<StricRequest<B1>, Response = Response<B2>, Error = std::convert::Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B1: Send + 'static,
    B2: Send + 'static,
{
    type Response = response::Response<B2>;
    type Error = TowerError; 
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| TowerError::Internal(Box::new(e)))
    }

    fn call(&mut self, http_req: request::Request<B1>) -> Self::Future {
        let mut inner = self.inner.clone();
        
        async move {
            let stric_req: StricRequest<B1> = http_req.try_into().map_err(|e: HttpError| TowerError::Internal(Box::new(e)))?;
            let stric_res = inner.call(stric_req).await.map_err(|_| TowerError::Unknown)?;
            let http_res: response::Response<B2> = stric_res.into();
            Ok(http_res)
        }.boxed()
    }
}


/// Applies a standard Tower layer around a Stric-native service.
///
/// This is the outer half of the sandwich model. The adapter translates the
/// incoming Stric request into an `http::Request`, runs the standard layer, and
/// then converts the layered `http::Response` back into a Stric response.
///
/// The layered response body type is allowed to differ from the inner service
/// body type so middleware such as `tower-http::trace::TraceLayer` can wrap or
/// replace the response body.
pub struct HttpAdapter<S, L> {
    inner: S,
    layer: L,
}

impl<S, L> HttpAdapter<S, L> {
    /// Creates a new adapter from an inner service and a standard Tower layer.
    pub(crate) fn new(inner: S, layer: L) -> Self {
        Self { inner, layer }
    }
}

impl<S, L> Clone for HttpAdapter<S, L>
where
    S: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<S, L, InnerHttpService, B1, B2, B3> Service<StricRequest<B1>> for HttpAdapter<S, L>
where
    S: Service<StricRequest<B1>, Response = Response<B2>, Error = std::convert::Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
    L: Layer<HttpServiceShim<S>, Service = InnerHttpService> + Clone + Send + 'static,
    InnerHttpService: Service<request::Request<B1>, Response = response::Response<B3>, Error = TowerError> + Send + 'static,
    InnerHttpService::Future: Send,
    B1: Send + 'static,
    B2: Send + 'static,
    B3: Send + 'static,
{
    type Response = Response<B3>;
    type Error = TowerError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, stric_req: StricRequest<B1>) -> Self::Future {
        let shim = HttpServiceShim::new(self.inner.clone());
        let mut layered_service = self.layer.layer(shim);

        async move {
            let http_req: request::Request<B1> = stric_req.into();
            let http_res = layered_service.call(http_req).await?;
            let stric_res: Response<B3> = http_res.into();
            Ok(stric_res)
        }.boxed()
    }
}
