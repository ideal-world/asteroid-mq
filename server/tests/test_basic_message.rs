use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::NonZeroU32,
    str::FromStr,
    time::Duration,
};

use asteroid_mq::{
    prelude::{Interest, MessageAckExpectKind, Node, NodeConfig, NodeId, Subject, TopicCode},
    protocol::node::raft::{
        cluster::StaticClusterProvider,
        state_machine::topic::config::{TopicConfig, TopicOverflowConfig, TopicOverflowPolicy},
    },
};
use asteroid_mq_model::EdgeMessage;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[tokio::test()]
async fn test_nodes() {
    // let console_layer = console_subscriber::spawn();
    tracing_subscriber::registry()
        // .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::Directive::from_str("info").unwrap())
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("asteroid_mq=info")
                            .unwrap(),
                    )
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("openraft=info").unwrap(),
                    ),
            ),
        )
        .init();

    let raft_config = openraft::Config {
        cluster_name: "test".to_string(),
        heartbeat_interval: 200,
        election_timeout_max: 2000,
        election_timeout_min: 1000,
        ..Default::default()
    };
    let node_id_1 = NodeId::from(1);
    let node_id_2 = NodeId::from(2);
    let static_nodes = [
        (node_id_1, "127.0.0.1:9559".to_string()),
        (node_id_2, "127.0.0.1:9560".to_string()),
    ];
    let cluster = StaticClusterProvider::new(BTreeMap::from(static_nodes.clone()));
    let mut nodes = HashMap::new();
    let mut init_tasks = tokio::task::JoinSet::new();
    for (id, addr) in static_nodes {
        let node = Node::new(NodeConfig {
            id,
            addr: SocketAddr::from_str(&addr).unwrap(),
            raft: raft_config.clone(),
            ..Default::default()
        });
        nodes.insert(id, node.clone());
        let cluster = cluster.clone();
        init_tasks.spawn(async move {
            node.start(cluster).await?;
            let raft = node.raft().await;
            loop {
                if let Some(leader) = raft.current_leader().await {
                    tracing::error!(?leader, "node is leader");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            asteroid_mq::Result::Ok(())
        });
    }
    while let Some(result) = init_tasks.join_next().await {
        let result = result.unwrap();
        if let Err(err) = result {
            panic!("init node error: {:?}", err);
        }
    }
    drop(init_tasks);
    const CODE: TopicCode = TopicCode::const_new("events");
    fn topic_config() -> TopicConfig {
        TopicConfig {
            code: CODE,
            blocking: false,
            overflow_config: Some(TopicOverflowConfig {
                policy: TopicOverflowPolicy::RejectNew,
                size: NonZeroU32::new(500).unwrap(),
            }),
        }
    }
    let node_server = nodes.get(&node_id_1).unwrap().clone();
    let node_client = nodes.get(&node_id_2).unwrap().clone();

    node_server
        .raft()
        .await
        .with_raft_state(|s| {
            let state = s.server_state;
            tracing::error!(?state, "node server");
        })
        .await
        .unwrap();
    node_client
        .raft()
        .await
        .with_raft_state(|s| {
            let state = s.server_state;
            tracing::error!(?state, "node client");
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    node_server.create_new_topic(topic_config()).await.unwrap();
    let edge_server_node =
        asteroid_mq_sdk::ClientNode::connect_local_without_auth(node_server.clone())
            .await
            .unwrap();

    let mut ep_server = edge_server_node
        .create_endpoint(CODE, vec![Interest::new("events/**")])
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Some(message) = ep_server.next_message().await {
            tracing::info!(?message, "recv message in server node");
            message.ack_processed().await.unwrap();
        }
    });
    let edge_client_node =
        asteroid_mq_sdk::ClientNode::connect_local_without_auth(node_client.clone())
            .await
            .unwrap();

    let mut ep_client = edge_client_node
        .create_endpoint(CODE, vec![Interest::new("events/hello-world")])
        .await
        .unwrap();

    tokio::spawn(async move {
        while let Some(message) = ep_client.next_message().await {
            tracing::info!(?message, "recv message");
            message.ack_processed().await.unwrap();
        }
    });
    let mut handles = vec![];
    for no in 0..10 {
        let ack_handle = edge_server_node
            .send_message(
                EdgeMessage::builder(
                    CODE,
                    [Subject::new("events/hello-world")],
                    format!("Message No.{no} of {}", MessageAckExpectKind::Processed),
                )
                .ack_kind(MessageAckExpectKind::Processed)
                .mode_online()
                .build(),
            )
            .await
            .unwrap();
        handles.push(ack_handle);
    }
    for ack_handle in handles {
        let success = ack_handle.wait().await.unwrap();
        tracing::info!("recv all ack: {success:?}")
    }

    let mut handles = vec![];
    for no in 0..10 {
        let ack_handle = edge_server_node
            .send_message(
                EdgeMessage::builder(
                    CODE,
                    [Subject::new("events/hello-world")],
                    format!("Message No.{no} of {}", MessageAckExpectKind::Sent),
                )
                .ack_kind(MessageAckExpectKind::Sent)
                .mode_push()
                .build(),
            )
            .await
            .unwrap();
        handles.push(ack_handle);
    }
    for ack_handle in handles {
        let success = ack_handle.wait().await.unwrap();
        tracing::info!("recv all ack: {success:?}")
    }

    let mut handles = vec![];
    for no in 0..10 {
        let ack_handle = edge_server_node
            .send_message(
                EdgeMessage::builder(
                    CODE,
                    [Subject::new("events/hello-world")],
                    format!("Message No.{no} of {}", MessageAckExpectKind::Received),
                )
                .ack_kind(MessageAckExpectKind::Received)
                .mode_push()
                .build(),
            )
            .await
            .unwrap();
        handles.push(ack_handle);
    }
    for ack_handle in handles {
        let success = ack_handle.wait().await.unwrap();
        tracing::info!("recv all ack: {success:?}")
    }
}
