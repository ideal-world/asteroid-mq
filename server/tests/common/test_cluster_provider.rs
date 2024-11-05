#![allow(dead_code)]
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc},
};

use asteroid_mq::{prelude::NodeId, protocol::node::raft::cluster::ClusterProvider};
use tokio::sync::Mutex;
#[derive(Debug, Clone)]
pub struct TestClusterProvider {
    nodes: Arc<Mutex<BTreeMap<NodeId, SocketAddr>>>,
    latest: Arc<AtomicU64>,
    version: u64,
}

impl TestClusterProvider {
    pub fn new(nodes: BTreeMap<NodeId, SocketAddr>) -> Self {
        Self {
            nodes: Arc::new(Mutex::new(nodes)),
            latest: Arc::new(AtomicU64::new(0)),
            version: 0,
        }
    }

    pub async fn update(&self, nodes: BTreeMap<NodeId, SocketAddr>) {
        *self.nodes.lock().await = nodes;
        self.latest
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl ClusterProvider for TestClusterProvider {
    async fn next_update(&mut self) -> asteroid_mq::Result<BTreeMap<NodeId, SocketAddr>> {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let nodes = self.nodes.lock().await.clone();
        Ok(nodes)
    }
    async fn pristine_nodes(&mut self) -> asteroid_mq::Result<BTreeMap<NodeId, SocketAddr>> {
        let nodes = self.nodes.lock().await.clone();
        Ok(nodes)
    }
    fn name(&self) -> std::borrow::Cow<'static, str> {
        "TestClusterProvider".into()
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
