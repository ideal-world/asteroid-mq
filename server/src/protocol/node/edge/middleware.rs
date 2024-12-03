use std::{future::Future, pin::Pin, sync::Arc};

use asteroid_mq_model::{EdgeError, EdgeRequestEnum, EdgeResponseEnum, NodeId};

use crate::prelude::Node;


pub trait EdgeConnectionHandler: Clone + Send + 'static {
    type Future: Future<Output = Result<EdgeResponseEnum, EdgeError>> + Send;
    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum) -> Self::Future;
}

/// The dynamic object wrapper for [`EdgeConnectionHandler`].
pub struct EdgeConnectionHandlerObject {
    #[allow(clippy::type_complexity)]
    handle: Arc<
        dyn Fn(
                Node,
                NodeId,
                EdgeRequestEnum,
            )
                -> Pin<Box<dyn Future<Output = Result<EdgeResponseEnum, EdgeError>> + Send>>
            + Send
            + Sync,
    >,
}

impl std::fmt::Debug for EdgeConnectionHandlerObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeConnectionHandlerObject").finish()
    }
}

impl Clone for EdgeConnectionHandlerObject {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

impl EdgeConnectionHandlerObject {
    /// Basic handler: call [`Node::handle_edge_request`] directly.
    pub fn basic() -> Self {
        Self {
            handle: Arc::new(|node, from, req| {
                Box::pin(async move { node.handle_edge_request(from, req).await })
            }),
        }
    }
    /// Wrap the handler with a middleware.
    pub fn with_middleware<M>(self, middleware: M) -> Self
    where
        M: EdgeConnectionMiddleware<Self>,
    {
        Self {
            handle: Arc::new(move |node, from, req| {
                let this = self.clone();
                let middleware = middleware.clone();
                Box::pin(async move { middleware.handle(node, from, req, &this).await })
            }),
        }
    }
}

impl EdgeConnectionHandler for EdgeConnectionHandlerObject {
    type Future = Pin<Box<dyn Future<Output = Result<EdgeResponseEnum, EdgeError>> + Send>>;

    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum) -> Self::Future {
        (self.handle)(node, from, req)
    }
}

pub trait EdgeConnectionMiddleware<I>: Clone + Send + Sync + 'static
where
    I: EdgeConnectionHandler,
{
    type Future: Future<Output = Result<EdgeResponseEnum, EdgeError>> + Send;
    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum, inner: &I) -> Self::Future;
}

#[derive(Clone)]
pub struct WithEdgeConnectionMiddleware<I, M> {
    inner: I,
    middleware: M,
}

impl<I, M> EdgeConnectionHandler for WithEdgeConnectionMiddleware<I, M>
where
    I: EdgeConnectionHandler,
    M: EdgeConnectionMiddleware<I>,
{
    type Future = M::Future;

    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum) -> Self::Future {
        self.middleware.handle(node, from, req, &self.inner)
    }
}
#[derive(Clone)]
pub struct FunctionalEdgeConnectionMiddleware<F>(pub F);

impl<F, I> EdgeConnectionMiddleware<I> for FunctionalEdgeConnectionMiddleware<F>
where
    F: Fn(Node, NodeId, EdgeRequestEnum, &I) -> I::Future + Clone + Send + Sync + 'static,
    I: EdgeConnectionHandler,
{
    type Future = I::Future;

    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum, inner: &I) -> Self::Future {
        (self.0)(node, from, req, inner)
    }
}

#[derive(Clone, Debug)]
pub struct BasicHandler;

impl EdgeConnectionHandler for BasicHandler {
    type Future = Pin<Box<dyn Future<Output = Result<EdgeResponseEnum, EdgeError>> + Send>>;

    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum) -> Self::Future {
        Box::pin(async move { node.handle_edge_request(from, req).await })
    }
}

impl Node {
    /// Insert a middleware to the edge connection handler.
    ///
    /// Middlewares may be used to intercept and modify the behavior of the edge connection handler, such as logging, authentication, etc.
    ///
    /// Refer to the [`EdgeConnectionMiddleware`] trait for more information.
    pub async fn insert_edge_connection_middleware<M>(&self, middleware: M)
    where
        M: EdgeConnectionMiddleware<EdgeConnectionHandlerObject>,
    {
        let mut wg = self.edge_handler.write().await;
        *wg = wg.clone().with_middleware(middleware);
    }
    /// Get the current edge connection handler.
    ///
    /// You may access the [`Node::edge_handler`](`crate::protocol::node::NodeInner::edge_handler`) field directly to get the lock.
    pub async fn get_edge_connection_handler(&self) -> EdgeConnectionHandlerObject {
        self.edge_handler.read().await.clone()
    }
    /// Set the edge connection handler.
    pub async fn set_edge_connection_handler(&self, handler: EdgeConnectionHandlerObject) {
        let mut wg = self.edge_handler.write().await;
        *wg = handler;
    }
}
