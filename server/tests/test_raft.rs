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
                        tracing_subscriber::filter::Directive::from_str("asteroid_mq=debug")
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
    let cluster = common::TestClusterProvider::new(
        map!(
            node_id(1) => node_addr(1),
            node_id(2) => node_addr(2),
            node_id(3) => node_addr(3),
        ),
        map!(),
    );

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

    cluster
        .update(map!(
            node_id(2) => node_addr(2),
        ))
        .await;
    let node_2_init_task = {
        let cluster = cluster.clone();
        let node_2 = node_2.clone();
        tokio::spawn(async move {
            node_2.start(cluster.clone()).await.unwrap();
        })
    };
    cluster
        .update(map!(
            node_id(1) => node_addr(1),
            node_id(2) => node_addr(2),
        ))
        .await;
    let node_1_init_task = {
        let cluster = cluster.clone();
        let node_1 = node_1.clone();
        tokio::spawn(async move { node_1.start(cluster).await.unwrap() })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;

    let node_3_init_task = {
        let node_3 = node_3.clone();
        let cluster = cluster.clone();
        tokio::spawn(async move { node_3.start(cluster).await.unwrap() })
    };
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (result_1, result_2, result_3) =
        tokio::join!(node_1_init_task, node_2_init_task, node_3_init_task,);
    result_1.expect("node_1 init error");
    result_2.expect("node_2 init error");
    result_3.expect("node_3 init error");

    cluster
        .update(map!(
            node_id(1) => node_addr(1),
            node_id(3) => node_addr(3),
        ))
        .await;
    // now restart node 2
    node_2.shutdown().await;
    cluster
        .update(map!(
            node_id(1) => node_addr(1),
            node_id(2) => node_addr(2),
            node_id(3) => node_addr(3),
        ))
        .await;
    let node_2 = Node::new(NodeConfig {
        id: node_id(2),
        addr: node_addr(2),
        raft: raft_config(),
        ..Default::default()
    });

    node_2.start(cluster.clone()).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // now restart node 1
    cluster
    .update(map!(
        node_id(2) => node_addr(2),
        node_id(3) => node_addr(3),
    ))
    .await;
    node_1.shutdown().await;

    let node_1 = Node::new(NodeConfig {
        id: node_id(1),
        addr: node_addr(1),
        raft: raft_config(),
        ..Default::default()
    });
    cluster
    .update(map!(
        node_id(1) => node_addr(1),
        node_id(2) => node_addr(2),
        node_id(3) => node_addr(3),
    ))
    .await;
    node_1.start(cluster.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // check the leaders is the same

    let leader_1 = node_1.raft().await.current_leader().await.unwrap();
    let leader_2 = node_2.raft().await.current_leader().await.unwrap();
    let leader_3 = node_3.raft().await.current_leader().await.unwrap();
    assert_eq!(leader_1, leader_2);
    assert_eq!(leader_2, leader_3);
}
