use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    str::FromStr,
    time::Duration,
};

use bytes::Bytes;

use asteroid_mq::protocol::{
    endpoint::{MessageAckExpectKind, MessageHeader, MessageId, MessageTargetKind},
    interest::{Interest, Subject},
    node::{connection::tokio_tcp::TokioTcp, Node},
    topic::{
        config::{OverflowPolicy, TopicConfig, TopicOverflowConfig},
        TopicCode,
    },
};

#[tokio::test]
async fn test_raft() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    const CLUSTER_SIZE: u64 = 5;
    let mut node_list: Vec<(Node, u16)> = vec![];
    for port_bias in 0..CLUSTER_SIZE-1 {
        let node = Node::default();
        node.set_cluster_size(CLUSTER_SIZE);
        node_list.push((node, 19000 + port_bias as u16));
    }
    // create listener
    for (node, port) in node_list.clone() {
        let listener = tokio::net::TcpListener::bind(
            format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap(),
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                tracing::info!(peer=?peer, "new connection");
                let node = node.clone();
                tokio::spawn(async move {
                    let conn = TokioTcp::new(stream);
                    node.create_connection(conn).await.unwrap();
                });
            }
        });
    }

    // create connections
    for (i, (node, _)) in node_list.iter().enumerate() {
        for (j, (_, peer_port)) in node_list.iter().enumerate() {
            if i <= j {
                break;
            }
            let stream = tokio::net::TcpSocket::new_v4()
                .unwrap()
                .connect(
                    format!("127.0.0.1:{}", peer_port)
                        .parse::<SocketAddr>()
                        .unwrap(),
                )
                .await
                .unwrap();
            node.create_connection(TokioTcp::new(stream)).await.unwrap();
        }
    }
    
    tokio::time::sleep(Duration::from_secs(5)).await;
    let node = Node::default();
    node.set_cluster_size(CLUSTER_SIZE);
    for (i, (node, port)) in node_list.iter().enumerate() {
        let stream = tokio::net::TcpSocket::new_v4()
            .unwrap()
            .connect(
                format!("127.0.0.1:{}", port)
                    .parse::<SocketAddr>()
                    .unwrap(),
            )
            .await
            .unwrap();
        node.create_connection(TokioTcp::new(stream)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

}
