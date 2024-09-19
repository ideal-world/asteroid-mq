//! 1. 按照主题的权限 4
//!   - 1.1. 事件中心接口 TODO  
//!   - 1.2. bios实现 TODO
//! 2. 主题系统 4
//!   - 2.1. 主题的数据流是否阻塞 DONE
//! 3. 主题，消息持久化 3 TO BE CONTINUE
//! 4. AVATAR功能 2 ALTERNATIVE
//! 5. 集群化 1 MAYBE RAFT
//! 6. RUST SDK 1
//! 7. RUST 接入 2
//!

use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::NonZeroU32,
    str::FromStr,
    time::Duration,
};

use asteroid_mq::{
    prelude::{NodeConfig, NodeId},
    protocol::{
        endpoint::{Message, MessageAckExpectKind, MessageHeader},
        interest::{Interest, Subject},
        node::{
            raft::{
                cluster::r#static::StaticClusterProvider,
                state_machine::topic::config::{
                    TopicConfig, TopicOverflowConfig, TopicOverflowPolicy,
                },
            },
            Node,
        },
        topic::TopicCode,
    },
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[tokio::test()]
async fn test_nodes() {
    let console_layer = console_subscriber::spawn();
    tracing_subscriber::registry()
        .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::filter::EnvFilter::from_default_env()
                    .add_directive(tracing_subscriber::filter::Directive::from_str("info").unwrap())
                    .add_directive(
                        tracing_subscriber::filter::Directive::from_str("asteroid_mq=debug")
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
        election_timeout_max: 1000,
        election_timeout_min: 500,
        ..Default::default()
    };
    let node_id_1 = NodeId::from(1);
    let node_id_2 = NodeId::from(2);
    let static_nodes = [
        (node_id_1, SocketAddr::from_str("127.0.0.1:9559").unwrap()),
        (node_id_2, SocketAddr::from_str("127.0.0.1:9560").unwrap()),
    ];
    let cluster = StaticClusterProvider::new(BTreeMap::from(static_nodes));
    let mut nodes = HashMap::new();
    let mut init_tasks = tokio::task::JoinSet::new();
    for (id, addr) in static_nodes {
        let node = Node::new(NodeConfig {
            id,
            addr,
            raft: raft_config.clone(),
        });
        nodes.insert(id, node.clone());
        let cluster = cluster.clone();
        init_tasks.spawn(async move {
            node.init_raft(cluster).await?;
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
            blocking: true,
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
            tracing::error!(?s.membership_state);
        })
        .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let event_topic = node_server.create_new_topic(topic_config()).await.unwrap();
    let node_server_receiver = event_topic
        .create_endpoint(vec![Interest::new("events/**")])
        .await
        .unwrap();
    tokio::spawn(async move {
        loop {
            let message = node_server_receiver.next_message().await;
            tracing::info!(?message, "recv message in server node");
            node_server_receiver
                .ack_processed(&message.header)
                .await
                .unwrap();
        }
    });

    let event_topic = node_client.get_topic(&CODE).unwrap();
    let node_client_receiver = event_topic
        .create_endpoint(vec![Interest::new("events/hello-world")])
        .await
        .unwrap();

    tokio::spawn(async move {
        loop {
            let message = node_client_receiver.next_message().await;
            tracing::info!(?message, "recv message");
            node_client_receiver
                .ack_processed(&message.header)
                .await
                .unwrap();
        }
    });
    let mut handles = vec![];
    for no in 0..10 {
        let ack_handle = event_topic
            .send_message(Message::new(
                MessageHeader::builder([Subject::new("events/hello-world")])
                    .ack_kind(MessageAckExpectKind::Processed)
                    .mode_online()
                    .build(),
                format!("Message No.{no} of {}", MessageAckExpectKind::Processed),
            ))
            .await
            .unwrap();
        handles.push(ack_handle);
    }
    for ack_handle in handles {
        let success = ack_handle.await.unwrap();
        tracing::info!("recv all ack: {success:?}")
    }

    let mut handles = vec![];
    for no in 0..10 {
        let ack_handle = event_topic
            .send_message(Message::new(
                MessageHeader::builder([Subject::new("events/hello-world")])
                    .ack_kind(MessageAckExpectKind::Sent)
                    .mode_push()
                    .build(),
                format!("Message No.{no} of {}", MessageAckExpectKind::Sent),
            ))
            .await
            .unwrap();
        handles.push(ack_handle);
    }
    for ack_handle in handles {
        let id = ack_handle.message_id();
        let success = ack_handle.await.unwrap();
        tracing::info!("recv all ack: {success:?} for {id}")
    }

    let mut handles = vec![];
    for no in 0..10 {
        let ack_handle = event_topic
            .send_message(Message::new(
                MessageHeader::builder([Subject::new("events/hello-world")])
                    .ack_kind(MessageAckExpectKind::Received)
                    .mode_push()
                    .build(),
                format!("Message No.{no} of {}", MessageAckExpectKind::Received),
            ))
            .await
            .unwrap();
        handles.push(ack_handle);
    }
    for ack_handle in handles {
        let id = ack_handle.message_id();
        let success = ack_handle.await.unwrap();
        tracing::info!("recv all ack: {success:?} for {id}")
    }
}
