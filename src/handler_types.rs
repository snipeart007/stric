use std::{pin::Pin, sync::Arc};


pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;



pub type ConnectionHandlerFn = Arc<dyn Fn(quinn::Connection) -> BoxFuture<'static, Result<(), anyhow::Error>> + Send + Sync>;