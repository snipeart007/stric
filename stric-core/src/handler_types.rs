use std::{pin::Pin, sync::Arc};

use crate::connection_wrapper::ConnectionWrapper;

/// A type alias for a pinned, boxed future that returns a `Result`.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A function type for handling new QUIC connections.
///
/// This handler is called whenever a new connection is established and accepted by the server.
/// It receives a mutable reference to a [`ConnectionWrapper`], which contains the underlying connection,
/// context, and metadata.
///
/// # Type Parameters
/// * `ConnectionMetadata`: The custom metadata type associated with the connection.
#[allow(type_alias_bounds)]
pub type ConnectionHandlerFn<ConnectionMetadata: Default + Send + Sync + 'static> = Arc<
    dyn for<'a> Fn(
            &'a mut ConnectionWrapper<ConnectionMetadata>,
        ) -> BoxFuture<'a, Result<(), anyhow::Error>>
        + Send
        + Sync,
>;
