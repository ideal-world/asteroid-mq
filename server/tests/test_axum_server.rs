use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use std::task::ready;

use futures_util::{Sink, Stream};

use asteroid_mq::{
    prelude::{Node, NodeConfig, NodeId, TopicCode},
    protocol::node::{
        edge::{
            codec::CodecKind,
            connection::{NodeConnection, NodeConnectionError, NodeConnectionErrorKind},
            packet::{Auth, EdgePacket},
            EdgeConfig,
        },
        raft::cluster::StaticClusterProvider,
    },
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
impl Sink<EdgePacket> for AxumWs {
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

    fn start_send(self: std::pin::Pin<&mut Self>, item: EdgePacket) -> Result<(), Self::Error> {
        self.project()
            .inner
            .start_send(Message::Binary(item.payload.to_vec()))
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
    type Item = Result<EdgePacket, NodeConnectionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let next = ready!(self.project().inner.poll_next(cx));
        match next {
            Some(Ok(Message::Binary(data))) => {
                let packet = EdgePacket::new(CodecKind::JSON, data);
                std::task::Poll::Ready(Some(Ok(packet)))
            }
            Some(Ok(Message::Text(data))) => {
                let packet = EdgePacket::new(CodecKind::JSON, data);
                std::task::Poll::Ready(Some(Ok(packet)))
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectQuery {
    pub node_id: String,
}
async fn handler(
    ws: WebSocketUpgrade,
    query: axum::extract::Query<ConnectQuery>,
    state: State<Node>,
) -> Response {
    use base64::Engine;
    tracing::info!(?query, "new connection");
    let id = base64::engine::general_purpose::URL_SAFE
        .decode(query.0.node_id)
        .unwrap();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&id);
    let config = EdgeConfig {
        peer_id: NodeId { bytes },
        supported_codec_kinds: vec![CodecKind::JSON].into_iter().collect(),
        peer_auth: Auth {},
    };
    tracing::info!(?config, "new edge connection");
    ws.on_upgrade(|ws| async move { handle_socket(ws, state.0, config).await })
}

async fn handle_socket(socket: WebSocket, node: Node, config: EdgeConfig) {
    let Ok(node_id) = node
        .create_edge_connection(AxumWs::new(socket), config)
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

async fn get_node_id() -> String {
    let node_id = NodeId::snowflake().to_base64();
    tracing::info!(?node_id, "new node id");
    node_id
}

#[tokio::test]
async fn test_websocket_server() -> asteroid_mq::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("debug,asteroid_mq=debug,openraft=warn")
        .init();
    let node = Node::new(NodeConfig::default());
    let cluster_provider = StaticClusterProvider::singleton(node.config());
    node.init_raft(cluster_provider).await?;
    let topic = node.create_new_topic(TopicCode::const_new("test")).await?;

    let receiver_endpoint = topic
        .create_endpoint(vec![asteroid_mq::protocol::interest::Interest::new("*")])
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Some(message) = receiver_endpoint.next_message().await {
            let payload: Value = serde_json::from_slice(&message.payload.0).expect("invalid json");
            tracing::info!(%payload, header=?message.header, "recv message in server node");
            receiver_endpoint
                .ack_processed(&message.header)
                .await
                .unwrap();
        }
    });
    use axum::serve;
    let tcp_listener = tokio::net::TcpListener::bind("localhost:8080")
        .await
        .unwrap();
    tracing::info!("listening on {}", tcp_listener.local_addr().unwrap());
    let route = axum::Router::new()
        .route("/connect", axum::routing::get(handler))
        .route("/node_id", axum::routing::put(get_node_id))
        .with_state(node);
    serve(tcp_listener, route).await.unwrap();
    Ok(())
}
