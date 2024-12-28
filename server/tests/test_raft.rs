use asteroid_mq::protocol::node::{Node, NodeConfig, NodeId};
use std::{
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
    time::Duration,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
mod common;

#[tokio::test(flavor = "multi_thread")]
async fn test_raft() {
    // let console_layer = console_subscriber::spawn();
    tracing_subscriber::registry()
        // .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::Directive::from_str("warn").unwrap())
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("asteroid_mq=trace")
                            .unwrap(),
                    )
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("openraft=warn").unwrap(),
                    ),
            ),
        )
        .init();
    fn raft_config() -> openraft::Config {
        openraft::Config {
            cluster_name: "test".to_string(),
            heartbeat_interval: 400,
            election_timeout_max: 1000,
            election_timeout_min: 500,
            ..Default::default()
        }
    }
    const fn node_id(index: usize) -> NodeId {
        NodeId::new_indexed(index as u64)
    }
    const fn node_addr(index: usize) -> SocketAddr {
        SocketAddr::new(
            std::net::IpAddr::V4(Ipv4Addr::LOCALHOST),
            19000 + index as u16,
        )
    }
    let cluster = common::TestClusterProvider::new(map!(
        node_id(2) => node_addr(2),
    ));
    cluster
        .update(map!(
            node_id(2) => node_addr(2),
        ))
        .await;
    let node_1 = Node::new(NodeConfig {
        id: node_id(1),
        addr: node_addr(1),
        raft: raft_config(),
        ..Default::default()
    });

    let node_2 = Node::new(NodeConfig {
        id: node_id(2),
        addr: node_addr(2),
        raft: raft_config(),
        ..Default::default()
    });

    let node_3 = Node::new(NodeConfig {
        id: node_id(3),
        addr: node_addr(3),
        raft: raft_config(),
        ..Default::default()
    });

    node_2.start(cluster.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    cluster
        .update(map!(
            node_id(1) => node_addr(1),
            node_id(2) => node_addr(2),
        ))
        .await;
    // node_1.start(cluster.clone()).await.unwrap();
    // tokio::time::sleep(Duration::from_secs(2)).await;

    // cluster
    //     .update(map!(
    //         node_id(1) => node_addr(1),
    //         node_id(2) => node_addr(2),
    //         node_id(3) => node_addr(3),
    //     ))
    //     .await;
    // let node_3_init_task = {
    //     let node_3 = node_3.clone();
    //     let cluster = cluster.clone();
    //     tokio::spawn(async move { node_3.start(cluster).await })
    // };
    // cluster
    //     .update(map!(
    //         node_id(1) => node_addr(1),
    //         node_id(3) => node_addr(3),
    //     ))
    //     .await;
    // node_3_init_task.await.unwrap().unwrap();
    // tokio::time::sleep(Duration::from_secs(5)).await;

    // tracing::warn!("now shutdown node_2");
    // node_2.shutdown().await;

    // // we should trigger a leader election here
    // tokio::time::sleep(Duration::from_secs(10)).await;

    // let leader_of_1 = node_1.raft().await.current_leader().await;
    // let leader_of_3 = node_3.raft().await.current_leader().await;

    // tracing::warn!("leader of node_1: {:#?}", leader_of_1);
    // tracing::warn!("leader of node_3: {:#?}", leader_of_3);

    // tracing::warn!("now restart node_2");
    // let node_2 = Node::new(NodeConfig {
    //     id: node_id(2),
    //     addr: node_addr(2),
    //     raft: raft_config(),
    //     ..Default::default()
    // });
    // tokio::time::sleep(Duration::from_secs(1)).await;
    // node_2.start(cluster.clone()).await.unwrap();
    // tokio::time::sleep(Duration::from_secs(2)).await;

    // tracing::warn!("now start node_4");
    // let node_4 = Node::new(NodeConfig {
    //     id: node_id(4),
    //     addr: node_addr(4),
    //     raft: raft_config(),
    //     ..Default::default()
    // });
    // node_4.start(cluster.clone()).await.unwrap();
    // cluster
    //     .update(map!(
    //         node_id(1) => node_addr(1),
    //         node_id(3) => node_addr(3),
    //         node_id(4) => node_addr(4),
    //     ))
    //     .await;
    // tokio::time::sleep(Duration::from_secs(2)).await;
    // tracing::warn!("now shutdown node_1");
    // node_1.shutdown().await;
    // cluster
    //     .update(map!(
    //         node_id(3) => node_addr(3),
    //         node_id(4) => node_addr(4),
    //     ))
    //     .await;
    // let node_5 = Node::new(NodeConfig {
    //     id: node_id(5),
    //     addr: node_addr(5),
    //     raft: raft_config(),
    //     ..Default::default()
    // });
    // node_5.start(cluster.clone()).await.unwrap();
    // cluster
    //     .update(map!(
    //         node_id(3) => node_addr(3),
    //         node_id(4) => node_addr(4),
    //         node_id(5) => node_addr(5),
    //     ))
    //     .await;
    // tokio::time::sleep(Duration::from_secs(5)).await;

    // tracing::warn!(
    //     "node_4 leader: {:#?}",
    //     node_4.raft().await.current_leader().await
    // );
    // tracing::warn!(
    //     "node_3 leader: {:#?}",
    //     node_3.raft().await.current_leader().await
    // );
    // tracing::warn!(
    //     "node_5 leader: {:#?}",
    //     node_5.raft().await.current_leader().await
    // );

    tokio::time::sleep(Duration::from_secs(3)).await;
}
