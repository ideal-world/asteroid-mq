use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroU32,
    str::FromStr,
    time::Duration,
};

use asteroid_mq::{
    prelude::{Interest, MessageAckExpectKind, Node, NodeConfig, NodeId, Subject, TopicCode, MB},
    protocol::node::raft::{
        cluster::StaticClusterProvider,
        state_machine::topic::config::{TopicConfig, TopicOverflowConfig, TopicOverflowPolicy},
    },
};
use asteroid_mq_model::EdgeMessage;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
/// 1MB message
static BIG_MESSAGE_1MB: &[u8] = &[0; 1024 * 1024];
/// 5MB message
static BIG_MESSAGE_5MB: &[u8] = &[0; 1024 * 1024 * 5];
/// 10MB message
static BIG_MESSAGE_10MB: &[u8] = &[0; 1024 * 1024 * 10];

fn bytes_size_mb<B: AsRef<[u8]>>(bytes: B) -> f64 {
    bytes.as_ref().len() as f64 / 1024.0 / 1024.0
}

#[tokio::test()]
async fn test_big_message() {
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
        heartbeat_interval: 500,
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
            addr: addr.parse().unwrap(),
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
            max_payload_size: 16 * MB as u32,
        }
    }
    let node_sender = nodes.get(&node_id_1).unwrap().clone();
    let node_receiver = nodes.get(&node_id_2).unwrap().clone();
    node_sender
        .raft()
        .await
        .with_raft_state(|s| {
            let state = s.server_state;
            tracing::error!(?state, "node sender");
        })
        .await
        .unwrap();
    node_receiver
        .raft()
        .await
        .with_raft_state(|s| {
            let state = s.server_state;
            tracing::error!(?state, "node receiver");
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    node_sender.create_new_topic(topic_config()).await.unwrap();
    let edge_sender = asteroid_mq_sdk::ClientNode::connect_local_without_auth(node_sender.clone())
        .await
        .unwrap();
    let edge_receiver =
        asteroid_mq_sdk::ClientNode::connect_local_without_auth(node_receiver.clone())
            .await
            .unwrap();
    let mut node_receiver_ep = edge_receiver
        .create_endpoint(CODE, vec![Interest::new("events/big-message/*")])
        .await
        .unwrap();

    tokio::spawn(async move {
        while let Some(message) = node_receiver_ep.next_message().await {
            let size = bytes_size_mb(&message.payload.0);
            tracing::info!(size, "recv message");
            message.ack_processed().await.unwrap();
        }
    });
    let ack_handle = edge_sender
        .send_message(
            EdgeMessage::builder(
                CODE,
                [Subject::new("events/big-message/1mb")],
                BIG_MESSAGE_1MB,
            )
            .ack_kind(MessageAckExpectKind::Processed)
            .mode_online()
            .build(),
        )
        .await
        .unwrap();
    let result = ack_handle.wait().await;
    assert!(result.is_ok());
    let ack_handle = edge_sender
        .send_message(
            EdgeMessage::builder(
                CODE,
                [Subject::new("events/big-message/5mb")],
                BIG_MESSAGE_5MB,
            )
            .ack_kind(MessageAckExpectKind::Processed)
            .mode_online()
            .build(),
        )
        .await
        .unwrap();
    let result = ack_handle.wait().await;
    assert!(result.is_ok());
    let ack_handle = edge_sender
        .send_message(
            EdgeMessage::builder(
                CODE,
                [Subject::new("events/big-message/10mb")],
                BIG_MESSAGE_10MB,
            )
            .ack_kind(MessageAckExpectKind::Processed)
            .mode_online()
            .build(),
        )
        .await
        .unwrap();
    let result = ack_handle.wait().await;
    assert!(result.is_ok());
    let mut handles = Vec::new();
    for _no in 0..10 {
        let ack_handle = edge_sender
            .send_message(
                EdgeMessage::builder(
                    CODE,
                    [Subject::new("events/big-message/1mb")],
                    BIG_MESSAGE_1MB,
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
        tracing::info!("recv all ack: {success:?}");
    }
}
