use std::{net::SocketAddr, str::FromStr, time::Duration};

use bytes::Bytes;

use asteroid_mq::protocol::{
    endpoint::{Message, MessageAckExpectKind, MessageHeader, MessageId, MessageTargetKind},
    interest::{Interest, Subject},
    node::{connection::tokio_tcp::TokioTcp, Node},
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
        let node_server_user = node_server.create_endpoint(vec![Interest::new("events/**")]);
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

    let node_client_sender = node_client.create_endpoint(vec![Interest::new("events/*/user/a")]);
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
    let ep = node_client.create_endpoint(None);
    tokio::time::sleep(Duration::from_secs(1)).await;
    let ack_handle = ep
        .send_message(Message {
            header: MessageHeader {
                message_id: MessageId::new_snowflake(),
                holder_node: node_client.id(),
                ack_kind: Some(MessageAckExpectKind::Processed),
                target_kind: MessageTargetKind::Online,
                subjects: vec![
                    Subject::new("events/hello-world/user/a"),
                    Subject::new("events/hello-world/user/b"),
                ]
                .into(),
            },
            payload: Bytes::from_static(b"Hello every one!"),
        })
        .await
        .unwrap();
    ack_handle.await.unwrap();
    tracing::info!("recv all ack")
}
