use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;
use futures::future::{BoxFuture, FutureExt};
use crate::http::{Request, Response, Full, Bytes};
use crate::handler::Handler;
use crate::adapter::HttpAdapter;


/// A minimal path router for Stric request handlers.
///
/// The router stores handlers keyed by the exact request path carried over the
/// Stric wire protocol. It defaults to a fully buffered byte body but can be
/// parameterized over another request body type when used behind adapters.
pub struct Router<S = (), B = Full<Bytes>> {
    routes: HashMap<String, Arc<dyn HandlerServiceTrait<S, B>>>,
    state: S,
}

impl<S, B> Clone for Router<S, B>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
            state: self.state.clone(),
        }
    }
}

impl Router<(), Full<Bytes>> {
    /// Creates an empty router with unit state and buffered byte bodies.
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            state: (),
        }
    }
}

impl<S, B> Router<S, B>
where
    S: Clone + Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    /// Registers a handler for an exact path match.
    pub fn route<H, T>(mut self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S, B> + Sync,
        T: Send + Sync + 'static,
    {
        let wrapper = HandlerServiceWrapper {
            handler,
            _marker: std::marker::PhantomData,
        };
        self.routes.insert(path.to_string(), Arc::new(wrapper));
        self
    }

    /// Rebuilds the router with a different shared state value.
    ///
    /// Existing routes are not carried across; this mirrors the current
    /// implementation strategy, which constructs a new router instance.
    pub fn with_state<S2>(self, state: S2) -> Router<S2, B>
    where
        S2: Clone + Send + Sync + 'static,
    {
        Router {
            routes: HashMap::new(),
            state,
        }
    }

    /// Wraps the router in a standard Tower layer using the sandwich adapter.
    ///
    /// This allows middleware written for `http::Request` and `http::Response`
    /// to run around a Stric-native router.
    pub fn layer_standard<L>(self, layer: L) -> HttpAdapter<Self, L> {
        HttpAdapter::new(self, layer)
    }
}

trait HandlerServiceTrait<S, B>: Send + Sync {
    fn call(&self, req: Request<B>, state: S) -> BoxFuture<'static, Response<Full<Bytes>>>;
}

struct HandlerServiceWrapper<H, T, S, B> {
    handler: H,
    _marker: std::marker::PhantomData<(T, S, B)>,
}

impl<H, T, S, B> HandlerServiceTrait<S, B> for HandlerServiceWrapper<H, T, S, B>
where
    H: Handler<T, S, B> + Sync,
    S: Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    fn call(&self, req: Request<B>, state: S) -> BoxFuture<'static, Response<Full<Bytes>>> {
        let handler = self.handler.clone();
        handler.call(req, state).boxed()
    }
}

impl<S, B> Service<Request<B>> for Router<S, B>
where
    S: Clone + Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    type Response = Response<Full<Bytes>>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.path.clone();
        let state = self.state.clone();
        if let Some(service) = self.routes.get(&path) {
            let service = service.clone();
            async move {
                Ok(service.call(req, state).await)
            }.boxed()
        } else {
            async move {
                Ok(Response::empty(404))
            }.boxed()
        }
    }
}
