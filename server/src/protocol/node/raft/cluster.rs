use std::{collections::BTreeMap, future::Future, net::SocketAddr};

use crate::prelude::NodeId;
#[cfg(feature = "cluster-k8s")]
pub(crate) mod k8s;
#[cfg(feature = "cluster-k8s")]
pub use k8s::K8sClusterProvider;
pub(crate) mod r#static;
pub use r#static::StaticClusterProvider;
pub trait ClusterProvider: Send + 'static {
    fn pristine_nodes(
        &mut self,
    ) -> impl Future<Output = crate::Result<BTreeMap<NodeId, SocketAddr>>> + Send {
        self.next_update()
    }
    fn next_update(
        &mut self,
    ) -> impl Future<Output = crate::Result<BTreeMap<NodeId, SocketAddr>>> + Send;
}
