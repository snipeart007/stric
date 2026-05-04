use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::Service;
use futures::future::{BoxFuture, FutureExt};
use crate::http::{Request, Response};
use crate::handler::Handler;

pub struct Router<S = ()> {
    routes: HashMap<String, Arc<dyn HandlerServiceTrait<S>>>,
    state: S,
}

impl<S> Clone for Router<S>
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

impl Router<()> {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            state: (),
        }
    }
}

impl<S> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn route<H, T>(mut self, path: &str, handler: H) -> Self
    where
        H: Handler<T, S> + Sync,
        T: Send + Sync + 'static,
    {
        let wrapper = HandlerServiceWrapper {
            handler,
            _marker: std::marker::PhantomData,
        };
        self.routes.insert(path.to_string(), Arc::new(wrapper));
        self
    }

    pub fn with_state<S2>(self, state: S2) -> Router<S2> {
        Router {
            routes: HashMap::new(),
            state,
        }
    }
}

trait HandlerServiceTrait<S>: Send + Sync {
    fn call(&self, req: Request, state: S) -> BoxFuture<'static, Response>;
}

struct HandlerServiceWrapper<H, T, S> {
    handler: H,
    _marker: std::marker::PhantomData<(T, S)>,
}

impl<H, T, S> HandlerServiceTrait<S> for HandlerServiceWrapper<H, T, S>
where
    H: Handler<T, S> + Sync,
    S: Clone + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    fn call(&self, req: Request, state: S) -> BoxFuture<'static, Response> {
        let handler = self.handler.clone();
        handler.call(req, state).boxed()
    }
}

impl<S> Service<Request> for Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let path = req.path.clone();
        let state = self.state.clone();
        if let Some(service) = self.routes.get(&path) {
            let service = service.clone();
            async move {
                Ok(service.call(req, state).await)
            }.boxed()
        } else {
            async move {
                Ok(Response::new(404))
            }.boxed()
        }
    }
}
