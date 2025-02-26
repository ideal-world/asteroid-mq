use std::{collections::HashMap, num::NonZeroU32, str::FromStr, time::Duration};
mod common;

use asteroid_mq::{
    prelude::{Interest, MessageAckExpectKind, Node, NodeConfig, NodeId, Subject, TopicCode, MB},
    protocol::node::raft::state_machine::topic::config::{
        TopicConfig, TopicOverflowConfig, TopicOverflowPolicy,
    },
};
use asteroid_mq_model::EdgeMessage;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};
// 64mb，not very big but still big enough.
const TOTAL_PAYLOAD_SIZE: usize = 64 * 1024 * 1024;
const MESSAGE_SIZE: usize = 1024 * 32;
const BATCH_SIZE: usize = TOTAL_PAYLOAD_SIZE / MESSAGE_SIZE;
static MESSAGE: &[u8] = &[0; MESSAGE_SIZE];

#[tokio::test()]
async fn test_install_big_snapshot() {
    // let console_layer = console_subscriber::spawn();
    tracing_subscriber::registry()
        // .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_filter(
                    tracing_subscriber::filter::EnvFilter::from_default_env()
                        .add_directive(
                            tracing_subscriber::filter::Directive::from_str("info").unwrap(),
                        )
                        .add_directive(
                            tracing_subscriber::filter::Directive::from_str("asteroid_mq=info")
                                .unwrap(),
                        )
                        .add_directive(
                            tracing_subscriber::filter::Directive::from_str("openraft=warn")
                                .unwrap(),
                        ),
                ),
        )
        .init();

    let raft_config = openraft::Config {
        cluster_name: "test".to_string(),
        heartbeat_interval: 500,
        election_timeout_max: 2000,
        election_timeout_min: 1000,
        // 1 Mb
        snapshot_max_chunk_size: 1024 * 1024 * 3,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
        install_snapshot_timeout: 1000,
        ..Default::default()
    };
    let node_id_1 = NodeId::from(1);
    let node_id_2 = NodeId::from(2);
    let node_id_3 = NodeId::from(3);
    let node_addr_1 = "127.0.0.1:9001";
    let node_addr_2 = "127.0.0.1:9002";
    let node_addr_3 = "127.0.0.1:9003";
    let cluster = common::TestClusterProvider::new(
        map!(
            node_id_1 => node_addr_1,
            node_id_2 => node_addr_2,
            node_id_3 => node_addr_3,

        ),
        map!(),
    );
    let static_nodes = [
        (node_id_1, node_addr_1.to_string()),
        (node_id_2, node_addr_2.to_string()),
        (node_id_3, node_addr_3.to_string()),
    ];
    cluster
        .update(map!(
            node_id_1 => node_addr_1,
            node_id_2 => node_addr_2,
            node_id_3 => node_addr_3,
        ))
        .await;
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
                size: NonZeroU32::new(99999).unwrap(),
            }),
            max_payload_size: 16 * MB as u32,
        }
    }
    let node_sender = nodes.get(&node_id_1).unwrap().clone();
    let edge_sender = asteroid_mq_sdk::ClientNode::connect_local_without_auth(node_sender.clone())
        .await
        .unwrap();
    let node_receiver = nodes.get(&node_id_2).unwrap().clone();
    let edge_receiver =
        asteroid_mq_sdk::ClientNode::connect_local_without_auth(node_receiver.clone())
            .await
            .unwrap();

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
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut node_receiver_ep = edge_receiver
        .create_endpoint(CODE, vec![Interest::new("events/message")])
        .await
        .unwrap();

    {
        tokio::spawn(async move {
            let mut messages = Vec::with_capacity(BATCH_SIZE);
            loop {
                if messages.len() == BATCH_SIZE {
                    break;
                } else {
                    let message = node_receiver_ep.next_message().await.unwrap();
                    messages.push(message);
                }
            }
            tracing::info!("all messaged received");
            let mut tasks = tokio::task::JoinSet::new();
            for message in messages {
                tasks.spawn(async move { message.ack_processed().await });
            }
            let results = tasks.join_all().await;
            let total = results.len();
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            tracing::info!(total, success_count, "all messaged ack");
        });
    }
    tracing::info!("start sending messages");
    let mut send_tasks = tokio::task::JoinSet::new();
    for idx in 0..BATCH_SIZE {
        let topic_for_spawn = edge_sender.clone();
        #[allow(clippy::async_yields_async)]
        send_tasks.spawn(async move {
            topic_for_spawn
                .send_message(
                    EdgeMessage::builder(CODE, [Subject::new("events/message")], MESSAGE)
                        .ack_kind(MessageAckExpectKind::Processed)
                        .mode_online()
                        .build(),
                )
                .await
        });

        if (idx + 1) % (BATCH_SIZE / 10) == 0 {
            tracing::info!("sent {} messages", idx + 1);
        }
    }
    tracing::info!("sending tasks all spawned");

    let handles = send_tasks.join_all().await;
    let success_count = handles.iter().filter(|r| r.is_ok()).count();
    let handle_size = handles.len();
    tracing::info!(success_count, handle_size, "finished sending messages");

    // and trigger processed ack
    // resolve_message_signal.notify_one();

    // let data = nodes
    //     .get(&node_id_2)
    //     .unwrap()
    //     .snapshot_data()
    //     .await
    //     .expect("no snapshot");
    // tracing::info!("snapshot: {data:#?}");
    // wait all responses
    let mut tasks = tokio::task::JoinSet::new();
    for ack_handle in handles {
        match ack_handle {
            Ok(ack_handle) => {
                tasks.spawn(async move { ack_handle.wait().await });
            }
            Err(err) => {
                tracing::error!("send message failed: {:?}", err);
            }
        }
    }
    let mut count = 0;
    while let Some(Ok(x)) = tasks.join_next().await {
        count += 1;
        if count % 100 == 0 {
            tracing::info!(count, "ack: {:?}", x);
        }
    }
    tracing::info!("recv all ack");
    let data = nodes
        .get(&node_id_2)
        .unwrap()
        .snapshot_data()
        .await
        .expect("no snapshot");
    tracing::info!("snapshot: {data:#?}");
    tokio::time::sleep(Duration::from_millis(1000)).await;
}
