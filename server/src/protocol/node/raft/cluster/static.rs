use std::{collections::BTreeMap, net::SocketAddr};

use futures_util::future::pending;

use crate::prelude::{NodeConfig, NodeId};

use super::ClusterProvider;

#[derive(Clone)]
pub struct StaticClusterProvider {
    nodes: BTreeMap<NodeId, SocketAddr>,
}

impl StaticClusterProvider {
    pub fn new(nodes: BTreeMap<NodeId, SocketAddr>) -> Self {
        Self { nodes }
    }
    pub fn singleton(node_config: &NodeConfig) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(node_config.id, node_config.addr);
        Self { nodes }
    }
}
impl ClusterProvider for StaticClusterProvider {
    async fn next_update(&mut self) -> crate::Result<BTreeMap<NodeId, SocketAddr>> {
        pending().await
    }
    async fn pristine_nodes(&mut self) -> crate::Result<BTreeMap<NodeId, SocketAddr>> {
        Ok(self.nodes.clone())
    }
}
