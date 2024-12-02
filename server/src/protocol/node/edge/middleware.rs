use std::{future::Future, pin::Pin, sync::Arc};

use asteroid_mq_model::{EdgeError, EdgeRequestEnum, EdgeResponseEnum, NodeId};

use crate::prelude::Node;

// EdgeMessage
// EdgeEndpointOnline
// EdgeEndpointOffline
// EndpointInterest
// SetState
pub trait EdgeConnectionHandler: Clone + Send + 'static {
    type Future: Future<Output = Result<EdgeResponseEnum, EdgeError>> + Send;
    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum) -> Self::Future;
}

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
    pub fn basic() -> Self {
        Self {
            handle: Arc::new(|node, from, req| {
                Box::pin(async move { node.handle_edge_request(from, req).await })
            }),
        }
    }
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
#[derive(Clone, Debug)]
pub struct BasicHandler;

impl EdgeConnectionHandler for BasicHandler {
    type Future = Pin<Box<dyn Future<Output = Result<EdgeResponseEnum, EdgeError>> + Send>>;

    fn handle(&self, node: Node, from: NodeId, req: EdgeRequestEnum) -> Self::Future {
        Box::pin(async move { node.handle_edge_request(from, req).await })
    }
}

impl Node {
    pub async fn with_edge_connection_middleware<M>(&self, middleware: M)
    where
        M: EdgeConnectionMiddleware<EdgeConnectionHandlerObject>,
    {
        let mut wg = self.edge_handler.write().await;
        *wg = wg.clone().with_middleware(middleware);
    }
    pub async fn get_edge_connection_handler(&self) -> EdgeConnectionHandlerObject {
        self.edge_handler.read().await.clone()
    }
    pub async fn set_edge_connection_handler(&self, handler: EdgeConnectionHandlerObject) {
        let mut wg = self.edge_handler.write().await;
        *wg = handler;
    }
}
