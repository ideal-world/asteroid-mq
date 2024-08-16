use std::{future::Future, net::SocketAddr, time::Duration};

use asteroid_mq::protocol::{
    cluster::{TcpClusterInfo, TcpClusterProvider},
    node::{connection::tokio_tcp::TokioTcp, Node},
};

#[tokio::test]
async fn test_raft() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();
    #[derive(Debug, Clone)]
    pub struct LocalClusterProvider {
        size: u64,
        nodes: Vec<SocketAddr>,
        next: tokio::time::Instant,
    }
    impl LocalClusterProvider {
        pub fn new(size: u64, from_port: u16) -> Self {
            let nodes = (0..size)
                .map(|i| format!("0.0.0.0:{}", from_port + i as u16).parse().unwrap())
                .collect();
            Self {
                size,
                nodes,
                next: tokio::time::Instant::now(),
            }
        }
    }
    impl TcpClusterProvider for LocalClusterProvider {
        fn next_update(&mut self) -> impl Future<Output = TcpClusterInfo> + Send {
            let next = self.next;
            self.next = next + Duration::from_secs(5);
            let size = self.size;
            let nodes = self.nodes.clone();
            async move {
                tokio::time::sleep_until(next).await;
                TcpClusterInfo { size, nodes }
            }
        }
    }

    const CLUSTER_SIZE: u64 = 5;
    let provider = LocalClusterProvider::new(CLUSTER_SIZE, 19000);
    for port_bias in 0..CLUSTER_SIZE - 1 {
        let node = Node::default();
        let _handle = node
            .create_cluster(
                provider.clone(),
                format!("0.0.0.0:{}", 19000 + port_bias).parse().unwrap(),
            )
            .unwrap();
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
    let node = Node::default();
    node.create_cluster(
        provider.clone(),
        format!("0.0.0.0:{}", 19000 + CLUSTER_SIZE).parse().unwrap(),
    )
    .unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;
}
