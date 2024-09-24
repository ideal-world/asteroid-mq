use std::{borrow::Cow, future::Future, sync::Arc};

use crate::prelude::NodeId;

use super::EdgeRequestEnum;
#[derive(Clone)]
pub struct EdgeAuthService {
    inner: Arc<dyn sealed::BoxedEdgeAuth>,
    source: Cow<'static, str>,
}

impl std::fmt::Debug for EdgeAuthService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeAuthService")
            .field("source", &self.source)
            .finish()
    }
}

impl std::fmt::Display for EdgeAuthService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EdgeAuthService: {}", self.source)
    }
}

#[derive(Debug)]
pub struct EdgeAuthError {
    reason: Cow<'static, str>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for EdgeAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EdgeAuthError: {}", self.reason,)?;
        if let Some(source) = &self.source {
            write!(f, "with source: {}", source)?;
        }
        Ok(())
    }
}

impl EdgeAuthService {
    pub fn new<T>(inner: T) -> Self
    where
        T: EdgeAuth,
    {
        Self {
            inner: Arc::new(inner),
            source: std::any::type_name::<T>().into(),
        }
    }

    pub async fn check(
        &self,
        from: NodeId,
        request: &EdgeRequestEnum,
    ) -> Result<(), EdgeAuthError> {
        self.inner.check(from, request).await
    }
}

pub trait EdgeAuth: Send + Sync + 'static {
    fn check<'r>(
        &self,
        from: NodeId,
        request: &'r EdgeRequestEnum,
    ) -> impl Future<Output = Result<(), EdgeAuthError>> + Send + 'r;
}

mod sealed {
    use futures_util::future::BoxFuture;

    use super::{EdgeAuth, EdgeAuthError};
    use crate::protocol::node::edge::EdgeRequestEnum;
    use crate::protocol::node::NodeId;

    pub(super) trait BoxedEdgeAuth: Send + Sync {
        fn check<'r>(
            &'r self,
            from: NodeId,
            request: &'r EdgeRequestEnum,
        ) -> BoxFuture<'r, Result<(), EdgeAuthError>>;
    }

    impl<T> BoxedEdgeAuth for T
    where
        T: EdgeAuth,
    {
        fn check<'r>(
            &'r self,
            from: NodeId,
            request: &'r EdgeRequestEnum,
        ) -> BoxFuture<'r, Result<(), EdgeAuthError>> {
            Box::pin(async move { self.check(from, &request).await })
        }
    }
}
