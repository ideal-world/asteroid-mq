use std::{collections::HashMap, future::Future, time::Duration};

use asteroid_mq::{
    prelude::TopicCode,
    protocol::{
        cluster::{TcpClusterInfo, TcpClusterProvider},
        node::{Node, NodeId, NodeConfig},
    },
};

#[tokio::test(flavor = "multi_thread")]
async fn test_raft() {
    // let console_layer = console_subscriber::spawn();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    #[derive(Debug, Clone)]
    pub struct LocalClusterProvider {
        pub info: TcpClusterInfo,
        next: tokio::time::Instant,
    }
    impl LocalClusterProvider {
        pub fn new(size: u64, from_port: u16) -> Self {
            let mut nodes = HashMap::new();
            for i in 0..size {
                nodes.insert(
                    NodeId::snowflake(),
                    format!("127.0.0.1:{}", from_port + i as u16)
                        .parse()
                        .unwrap(),
                );
            }
            Self {
                info: TcpClusterInfo { size, nodes },
                next: tokio::time::Instant::now(),
            }
        }
    }
    impl TcpClusterProvider for LocalClusterProvider {
        fn next_update(&mut self) -> impl Future<Output = TcpClusterInfo> + Send {
            let next = self.next;
            self.next = next + Duration::from_secs(5);
            let info = self.info.clone();
            async move {
                tokio::time::sleep_until(next).await;
                info
            }
        }
    }

    const CLUSTER_SIZE: u64 = 10;
    let provider = LocalClusterProvider::new(CLUSTER_SIZE, 19000);
    let info = provider.info.clone();
    let mut nodes = Vec::new();
    for (node_id, addr) in info.nodes.iter() {
        let node = Node::new(NodeConfig::new_cluster_by_id(*node_id));
        node.create_cluster(provider.clone(), *addr).await.unwrap();
        tracing::info!("node {:?} listen on {:?}", node_id, addr);
        nodes.push(node)
    }

    for node in nodes.iter() {
        let is_leader = node.wait_raft_cluster_ready().await;
        if is_leader {
            tracing::info!("node {:?} is leader", node.id());
        } else {
            tracing::info!("node {:?} is follower", node.id());
        }
    }
    for node in nodes.iter() {
        node.new_topic(TopicCode::const_new("test")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(10)).await;
}
