use std::{pin::Pin, sync::Arc};

use crate::connection_wrapper::ConnectionWrapper;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[allow(type_alias_bounds)]
pub type ConnectionHandlerFn<ConnectionMetadata: Default + Send + Sync + 'static> = Arc<
    dyn Fn(
            &mut ConnectionWrapper<ConnectionMetadata>,
        ) -> BoxFuture<'static, Result<(), anyhow::Error>>
        + Send
        + Sync,
>;
