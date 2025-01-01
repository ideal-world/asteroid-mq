use std::collections::BTreeMap;

use futures_util::future::pending;

use crate::prelude::NodeId;

use super::ClusterProvider;

#[derive(Clone)]
pub struct StaticClusterProvider {
    nodes: BTreeMap<NodeId, String>,
}

impl StaticClusterProvider {
    pub fn new(nodes: BTreeMap<NodeId, String>) -> Self {
        Self { nodes }
    }
    pub fn singleton(id: NodeId, socket_addr: String) -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(id, socket_addr);
        Self { nodes }
    }
}
impl ClusterProvider for StaticClusterProvider {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "static".into()
    }
    async fn next_update(&mut self) -> crate::Result<BTreeMap<NodeId, String>> {
        pending().await
    }
    async fn pristine_nodes(&mut self) -> crate::Result<BTreeMap<NodeId, String>> {
        Ok(self.nodes.clone())
    }
}
