//! 1. 按照主题的权限 4
//!   - 1.1. 事件中心接口
//!   - 1.2. bios实现
//! 2. 主题系统 4
//!   - 2.1. 主题的数据流是否阻塞
//! 3. 主题，消息持久化 3
//! 4. AVATAR功能 2
//! 5. 集群化 1
//! 6. RUST SDK 1
//! 7. RUST 接入 2
//!
//! spi-log:需要按照顺序单个接收日志
//!	发送「消息1,消息2,消息3,消息4,消息5,消息6」
//!	服务a:消息1✅,消息2✅,消息5❌
//!	服务 b:消息3✅,消息4✅
//!	消息6阻塞,等待消息5消费
//!

use std::{net::SocketAddr, str::FromStr, time::Duration};

use bytes::Bytes;

use asteroid_mq::protocol::{
    endpoint::{Message, MessageAckExpectKind, MessageHeader, MessageId, MessageTargetKind},
    interest::{Interest, Subject},
    node::{connection::tokio_tcp::TokioTcp, Node},
    topic::TopicCode,
};

#[tokio::test]
async fn test_nodes() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let node_server = Node::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:10080")
        .await
        .unwrap();
    tokio::spawn(async move {
        let event_topic = node_server.get_or_init_topic(TopicCode::const_new("events"));
        let node_server_user = event_topic.create_endpoint(vec![Interest::new("events/**")]);

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

        loop {
            let message = node_server_user.next_message().await;
            tracing::info!(?message, "recv message in server node");
            node_server_user.ack_processed(&message);
        }
    });

    let node_client = Node::default();
    let stream_client = tokio::net::TcpSocket::new_v4()
        .unwrap()
        .connect(SocketAddr::from_str("127.0.0.1:10080").unwrap())
        .await
        .unwrap();
    let event_topic = node_client.get_or_init_topic(TopicCode::const_new("events"));

    let node_client_sender =
        event_topic.create_endpoint(vec![Interest::new("cicd/log/task/asudhuiahsdu392")]);
    node_client
        .create_connection(TokioTcp::new(stream_client))
        .await
        .unwrap();

    tokio::spawn(async move {
        loop {
            let message = node_client_sender.next_message().await;
            tracing::info!(?message, "recv message");
            node_client_sender.ack_processed(&message);
        }
    });
    let ep = event_topic.create_endpoint(None);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let ack_handle = ep
        .send_message(Message {
            header: MessageHeader {
                message_id: MessageId::new_snowflake(),
                holder_node: node_client.id(),
                ack_kind: Some(MessageAckExpectKind::Processed),
                target_kind: MessageTargetKind::Online,
                subjects: vec![Subject::new("cicd/log/task/asudhuiahsdu392")].into(),
                topic: event_topic.code().clone(),
            },
            payload: Bytes::from_static(b"Hello every one!"),
        })
        .await
        .unwrap();
    ack_handle.await.unwrap();
    tracing::info!("recv all ack")
}
