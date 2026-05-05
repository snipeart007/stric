use std::future::Future;
use std::pin::Pin;
use crate::http::{Request, Response, FromRequest, IntoResponse, Full, Bytes};

/// Abstraction used by the router to invoke async handler functions.
///
/// Implementations are generated for plain async functions with up to sixteen
/// extractor arguments. Each extractor is resolved from the request before the
/// handler future is executed.
pub trait Handler<T, S, B = Full<Bytes>>: Clone + Send + Sized + 'static {
    /// The future returned by the handler after extraction succeeds.
    type Future: Future<Output = Response<Full<Bytes>>> + Send + 'static;

    /// Invokes the handler with the request and shared state.
    fn call(self, req: Request<B>, state: S) -> Self::Future;
}

impl<F, Fut, S, R, B> Handler<(), S, B> for F
where
    F: FnOnce() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = R> + Send + 'static,
    R: IntoResponse,
    S: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    type Future = Pin<Box<dyn Future<Output = Response<Full<Bytes>>> + Send>>;

    fn call(self, _req: Request<B>, _state: S) -> Self::Future {
        Box::pin(async move {
            self().await.into_response()
        })
    }
}

macro_rules! impl_handler {
    ( $($ty:ident),* $(,)? ) => {
        #[allow(non_snake_case)]
        impl<F, Fut, S, R, B, $($ty,)*> Handler<($($ty,)*), S, B> for F
        where
            F: FnOnce($($ty,)*) -> Fut + Clone + Send + 'static,
            Fut: Future<Output = R> + Send + 'static,
            R: IntoResponse,
            S: Send + Sync + 'static,
            B: Clone + Send + Sync + 'static,
            $($ty: FromRequest<S, B> + Send,)*
        {
            type Future = Pin<Box<dyn Future<Output = Response<Full<Bytes>>> + Send>>;

            fn call(self, req: Request<B>, state: S) -> Self::Future {
                Box::pin(async move {
                    $(
                        let $ty = match $ty::from_request(req.clone(), &state).await {
                            Ok(value) => value,
                            Err(rejection) => return rejection.into_response(),
                        };
                    )*
                    self($($ty,)*).await.into_response()
                })
            }
        }
    };
}

impl_handler!(T1);
impl_handler!(T1, T2);
impl_handler!(T1, T2, T3);
impl_handler!(T1, T2, T3, T4);
impl_handler!(T1, T2, T3, T4, T5);
impl_handler!(T1, T2, T3, T4, T5, T6);
impl_handler!(T1, T2, T3, T4, T5, T6, T7);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_handler!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);
