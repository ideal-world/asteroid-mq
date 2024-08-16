//! 1. 按照主题的权限 4
//!   - 1.1. 事件中心接口
//!   - 1.2. bios实现
//! 2. 主题系统 4
//!   - 2.1. 主题的数据流是否阻塞 DONE
//! 3. 主题，消息持久化 3 TO BE CONTINUE
//! 4. AVATAR功能 2 ALTERNATIVE
//! 5. 集群化 1 MAYBE RAFT
//! 6. RUST SDK 1
//! 7. RUST 接入 2
//!
//! spi-log:需要按照顺序单个接收日志
//!	发送「消息1,消息2,消息3,消息4,消息5,消息6」
//!	服务a:消息1✅,消息2✅,消息5❌
//!	服务 b:消息3✅,消息4✅
//!	消息6阻塞,等待消息5消费
//!

use std::{net::SocketAddr, num::NonZeroU32, str::FromStr, time::Duration};

use asteroid_mq::protocol::{
    endpoint::{Message, MessageAckExpectKind, MessageHeader, MessageId, MessageTargetKind},
    interest::{Interest, Subject},
    node::{connection::tokio_tcp::TokioTcp, Node},
    topic::{
        config::{OverflowPolicy, TopicConfig, TopicOverflowConfig},
        TopicCode,
    },
};

#[tokio::test]
async fn test_nodes() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let node_server = Node::default();
    node_server.set_cluster_size(2);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:10080")
        .await
        .unwrap();
    fn topic_config() -> TopicConfig {
        TopicConfig {
            code: TopicCode::const_new("events"),
            blocking: false,
            overflow_config: Some(TopicOverflowConfig {
                policy: OverflowPolicy::RejectNew,
                size: NonZeroU32::new(500).unwrap(),
            }),
        }
    }

    tokio::spawn(async move {
        {
            let node_server = node_server.clone();
            tokio::spawn(async move {
                while let Ok((stream, peer)) = listener.accept().await {
                    tracing::info!(peer=?peer, "new connection");
                    let node = node_server.clone();
                    tokio::spawn(async move {
                        let conn = TokioTcp::new(stream);
                        node.create_connection(conn).await.unwrap();
                    });
                }
            });
        }
        let event_topic = node_server.new_topic(topic_config()).await.unwrap();
        let node_server_user = event_topic
            .create_endpoint(vec![Interest::new("events/**")])
            .await
            .unwrap();

        loop {
            let message = node_server_user.next_message().await;
            tracing::info!(?message, "recv message in server node");
            node_server_user.ack_processed(&message).await.unwrap();
        }
    });

    let node_client = Node::default();
    node_client.set_cluster_size(2);
    let stream_client = tokio::net::TcpSocket::new_v4()
        .unwrap()
        .connect(SocketAddr::from_str("127.0.0.1:10080").unwrap())
        .await
        .unwrap();

    node_client
        .create_connection(TokioTcp::new(stream_client))
        .await
        .unwrap();

    let event_topic = node_client.new_topic(topic_config()).await.unwrap();
    let node_client_sender = event_topic
        .create_endpoint(vec![Interest::new("events/hello-world")])
        .await
        .unwrap();

    tokio::spawn(async move {
        loop {
            let message = node_client_sender.next_message().await;
            tracing::info!(?message, "recv message");
            node_client_sender.ack_processed(&message).await.unwrap();
        }
    });
    let ep = event_topic.create_endpoint(None).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut handles = vec![];
    for no in 0..10 {
        let ack_handle = ep
            .send_message(Message::new(
                MessageHeader::builder([Subject::new("events/hello-world")])
                    .ack_kind(MessageAckExpectKind::Processed)
                    .mode_online()
                    .build(),
                format!("Message No.{no}"),
            ))
            .await
            .unwrap();
        handles.push(ack_handle);
    }
    for ack_handle in handles {
        let success = ack_handle.await.unwrap();
        tracing::info!("recv all ack: {success:?}")
    }
}
