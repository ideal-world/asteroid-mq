use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use asteroid_mq::{prelude::NodeId, protocol::node::raft::cluster::ClusterProvider};
use tokio::sync::{Mutex, Notify};
#[derive(Debug, Clone)]
pub struct TestClusterProvider {
    nodes: Arc<Mutex<BTreeMap<NodeId, SocketAddr>>>,
    notify: Arc<Notify>,
}

impl TestClusterProvider {
    pub fn new(nodes: BTreeMap<NodeId, SocketAddr>) -> Self {
        Self {
            nodes: Arc::new(Mutex::new(nodes)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub async fn update(&self, nodes: BTreeMap<NodeId, SocketAddr>) {
        *self.nodes.lock().await = nodes;
        self.notify.notify_waiters();
    }
}

impl ClusterProvider for TestClusterProvider {
    async fn next_update(&mut self) -> asteroid_mq::Result<BTreeMap<NodeId, SocketAddr>> {
        self.notify.notified().await;
        let nodes = self.nodes.lock().await.clone();
        Ok(nodes)
    }
    async fn pristine_nodes(&mut self) -> asteroid_mq::Result<BTreeMap<NodeId, SocketAddr>> {
        let nodes = self.nodes.lock().await.clone();
        Ok(nodes)
    }
}
#[macro_export]
macro_rules! map {
    ($($key:expr => $value:expr),* $(,)?) => {
        {
            let mut map = std::collections::BTreeMap::new();
            $(
                map.insert($key, $value);
            )*
            map
        }
    };
}
