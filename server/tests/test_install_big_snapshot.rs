use std::{collections::HashMap, num::NonZeroU32, str::FromStr, sync::Arc, time::Duration};
mod common;

use asteroid_mq::{
    prelude::{
        Interest, Message, MessageAckExpectKind, MessageHeader, Node, NodeConfig, NodeId, Subject,
        TopicCode,
    },
    protocol::node::raft::state_machine::topic::config::{
        TopicConfig, TopicOverflowConfig, TopicOverflowPolicy,
    },
};
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
            tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::Directive::from_str("info").unwrap())
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("asteroid_mq=info")
                            .unwrap(),
                    )
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("openraft=warn").unwrap(),
                    ),
            ),
        )
        .init();

    let raft_config = openraft::Config {
        cluster_name: "test".to_string(),
        heartbeat_interval: 500,
        election_timeout_max: 2000,
        election_timeout_min: 1000,
        // 64 kb
        snapshot_max_chunk_size: 1024 * 128,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(100),
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
    let event_topic_sender = node_sender.create_new_topic(topic_config()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let event_topic_receiver = node_receiver.get_topic(&CODE).unwrap();
    let node_receiver_ep = event_topic_receiver
        .create_endpoint(vec![Interest::new("events/message")])
        .await
        .unwrap();
    let resolve_message_signal = Arc::new(tokio::sync::Notify::new());

    {
        let resolve_message_signal = resolve_message_signal.clone();
        tokio::spawn(async move {
            let mut messages = Vec::with_capacity(BATCH_SIZE);
            loop {
                let Some(message) = (tokio::select! {
                    _ = resolve_message_signal.notified() => {
                        break
                    }
                    next_message = node_receiver_ep.next_message() => {
                        next_message
                    }
                }) else {
                    break;
                };
                messages.push(message);
            }
            tracing::info!("all messaged received");
            let mut tasks = tokio::task::JoinSet::new();
            for message in messages {
                let ep = node_receiver_ep.clone();
                tasks.spawn(async move { ep.ack_processed(&message.header).await });
            }
            tasks.join_all().await;
            tracing::info!("all messaged ack");
        });
    }
    tracing::info!("start sending messages");
    let mut send_tasks = tokio::task::JoinSet::new();
    for idx in 0..BATCH_SIZE {
        let topic_for_spawn = event_topic_sender.clone();
        #[allow(clippy::async_yields_async)]
        send_tasks.spawn(async move {
            topic_for_spawn
                .send_message(Message::new(
                    MessageHeader::builder([Subject::new("events/message")])
                        .ack_kind(MessageAckExpectKind::Processed)
                        .mode_online()
                        .build(),
                    MESSAGE,
                ))
                .await
                .expect("fail to send")
        });

        if (idx + 1) % (BATCH_SIZE / 10) == 0 {
            tracing::info!("sent {} messages", idx + 1);
        }
    }
    let handles = send_tasks.join_all().await;
    tracing::info!("finished sending messages");

    // restart nodes 3 now
    let node_3 = nodes.remove(&node_id_3).unwrap();
    tracing::info!("shutting down node_3");

    node_3.shutdown().await;

    cluster
        .update(map!(
            node_id_1 => node_addr_1,
            node_id_2 => node_addr_2,
        ))
        .await;
    let node_3 = Node::new(NodeConfig {
        id: node_id_3,
        addr: node_addr_3.parse().unwrap(),
        raft: raft_config.clone(),
        ..Default::default()
    });
    tokio::time::sleep(Duration::from_millis(1000)).await;
    tracing::info!("restarting node_3");
    cluster
        .update(map!(
            node_id_1 => node_addr_1,
            node_id_2 => node_addr_2,
            node_id_3 => node_addr_3,
        ))
        .await;
    node_3.start(cluster).await.expect("restart node 3 failed");
    let node_3_raft = node_3.raft().await;
    loop {
        if let Some(leader) = node_3_raft.current_leader().await {
            tracing::error!(?leader, "node 3 now has a leader");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // and trigger processed ack
    resolve_message_signal.notify_one();

    let data = nodes
        .get(&node_id_2)
        .unwrap()
        .snapshot_data()
        .await
        .expect("no snapshot");
    tracing::info!("snapshot: {data:#?}");
    // wait all responses
    let mut tasks = tokio::task::JoinSet::new();
    for ack_handle in handles {
        tasks.spawn(ack_handle);
    }
    let _results = tasks.join_all().await;
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
