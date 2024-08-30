use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};

use std::task::ready;

use bytes::Bytes;
use futures_util::{Sink, Stream};

use asteroid_mq::{
    prelude::{Node, NodeId, NodeInfo},
    protocol::node::event::{N2nPacket, NodeKind},
};

use asteroid_mq::protocol::node::connection::{
    NodeConnection, NodeConnectionError, NodeConnectionErrorKind,
};

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct AxumWs {
        #[pin]
        inner: WebSocket,
    }
}
impl AxumWs {
    pub fn new(inner: WebSocket) -> Self {
        Self { inner }
    }
}
impl Sink<N2nPacket> for AxumWs {
    type Error = NodeConnectionError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(|e| {
            NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll ready failed",
            )
        })
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: N2nPacket) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(Message::Binary(item.to_binary().into()))
            .map_err(|e| {
                NodeConnectionError::new(
                    NodeConnectionErrorKind::Underlying(Box::new(e)),
                    "web socket start send failed",
                )
            })
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(|e| {
            NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll flush failed",
            )
        })
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(|e| {
            NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll close failed",
            )
        })
    }
}

impl Stream for AxumWs {
    type Item = Result<N2nPacket, NodeConnectionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let next = ready!(self.project().inner.poll_next(cx));
        match next {
            Some(Ok(Message::Binary(data))) => {
                let data = Bytes::from(data);
                let packet = N2nPacket::from_binary(data).map_err(|e| {
                    NodeConnectionError::new(
                        NodeConnectionErrorKind::Decode(e),
                        "invalid binary packet",
                    )
                });
                std::task::Poll::Ready(Some(packet))
            }
            Some(Ok(Message::Close(_))) => {
                tracing::debug!("received close message");
                std::task::Poll::Ready(None)
            }
            Some(Ok(p)) => {
                tracing::debug!(?p, "unexpected message type");
                // immediately wake up the task to poll next
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
            Some(Err(e)) => std::task::Poll::Ready(Some(Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll next failed",
            )))),
            None => std::task::Poll::Ready(None),
        }
    }
}

impl NodeConnection for AxumWs {}
async fn handler(ws: WebSocketUpgrade, state: State<Node>) -> Response {
    ws.on_upgrade(|ws| async move { handle_socket(ws, state.0).await })
}

async fn handle_socket(socket: WebSocket, node: Node) {
    let Ok(node_id) = node
        .create_edge_connection(AxumWs::new(socket))
        .await
        .inspect_err(|e| {
            tracing::error!(?e, "failed to create edge connection");
        })
    else {
        return;
    };
    tracing::info!(?node_id, "edge connected");
    let Some(connection) = node.get_edge_connection(node_id) else {
        return;
    };
    let _ = connection.finish_signal.recv_async().await;
    tracing::info!(?node_id, "edge disconnected");
}

#[tokio::test]
async fn test_websocket_server() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();
    let node =
        asteroid_mq::prelude::Node::new(NodeInfo::new(NodeId::snowflake(), NodeKind::Cluster));
    node.set_cluster_size(1);
    let topic = node.new_topic(asteroid_mq::protocol::topic::config::TopicConfig {
        code: asteroid_mq::protocol::topic::TopicCode::const_new("test"),
        blocking: false,
        overflow_config: None,
    }).await.unwrap();
    let receiver_endpoint = topic
        .create_endpoint(vec![asteroid_mq::protocol::interest::Interest::new("*")])
        .await
        .unwrap();
    tokio::spawn(async move {
        loop {
            let message = receiver_endpoint.next_message().await;
            tracing::info!(?message, "recv message in server node");
            receiver_endpoint.ack_processed(&message.header).await.unwrap();
        }
    });
    use axum::serve;
    let tcp_listener = tokio::net::TcpListener::bind("localhost:8080")
        .await
        .unwrap();
    tracing::info!("listening on {}", tcp_listener.local_addr().unwrap());
    let route = axum::Router::new()
        .route("/", axum::routing::get(handler))
        .with_state(node);
    serve(tcp_listener, route).await.unwrap();
}
