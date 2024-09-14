use std::{collections::BTreeMap, future::Future, net::SocketAddr};

use crate::prelude::NodeId;
pub mod r#static;
#[cfg(feature = "cluster-k8s")]
pub mod k8s;
pub trait ClusterProvider: Send + 'static{
    fn pristine_nodes(
        &mut self,
    ) -> impl Future<Output = crate::Result<BTreeMap<NodeId, SocketAddr>>> + Send {
        self.next_update()
    }
    fn next_update(
        &mut self,
    ) -> impl Future<Output = crate::Result<BTreeMap<NodeId, SocketAddr>>> + Send;
}
