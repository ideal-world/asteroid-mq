use asteroid_mq_model::{
    codec::{Codec, CodecKind, DynCodec},
    connection::EdgeNodeConnection,
    EdgeAuth, EdgePayload,
};
use axum::{
    body::Body,
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
            connection::{EdgeConnectionError, EdgeConnectionErrorKind},
            middleware::{EdgeConnectionHandler, EdgeConnectionMiddleware},
            EdgeConfig,
        },
        raft::cluster::{this_pod_id, K8sClusterProvider, StaticClusterProvider},
    },
    DEFAULT_TCP_PORT,
};

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct AxumWs<C: Codec> {
        #[pin]
        inner: WebSocket,
        codec: C,
    }
}
impl<C: Codec> AxumWs<C> {
    pub fn new(inner: WebSocket, codec: C) -> Self {
        Self { inner, codec }
    }
}
impl<C: Codec> Sink<EdgePayload> for AxumWs<C> {
    type Error = EdgeConnectionError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx).map_err(|e| {
            EdgeConnectionError::new(
                EdgeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll ready failed",
            )
        })
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: EdgePayload) -> Result<(), Self::Error> {
        let this = self.project();
        this.inner
            .start_send(Message::Binary(this.codec.encode(&item).map_err(
                EdgeConnectionError::codec("web socket start send failed"),
            )?))
            .map_err(|e| {
                EdgeConnectionError::new(
                    EdgeConnectionErrorKind::Underlying(Box::new(e)),
                    "web socket start send failed",
                )
            })
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx).map_err(|e| {
            EdgeConnectionError::new(
                EdgeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll flush failed",
            )
        })
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().inner.poll_close(cx).map_err(|e| {
            EdgeConnectionError::new(
                EdgeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll close failed",
            )
        })
    }
}

impl<C: Codec> Stream for AxumWs<C> {
    type Item = Result<EdgePayload, EdgeConnectionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let next = ready!(this.inner.poll_next(cx));
        match next {
            Some(Ok(Message::Binary(data))) => {
                let payload_result = this
                    .codec
                    .decode(&data)
                    .map_err(EdgeConnectionError::codec("axum ws poll next failed"));
                std::task::Poll::Ready(Some(payload_result))
            }
            Some(Ok(Message::Text(data))) => {
                let payload_result = this
                    .codec
                    .decode(data.as_bytes())
                    .map_err(EdgeConnectionError::codec("axum ws poll next failed"));
                std::task::Poll::Ready(Some(payload_result))
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
            Some(Err(e)) => std::task::Poll::Ready(Some(Err(EdgeConnectionError::new(
                EdgeConnectionErrorKind::Underlying(Box::new(e)),
                "web socket poll next failed",
            )))),
            None => std::task::Poll::Ready(None),
        }
    }
}

impl<C: Codec> EdgeNodeConnection for AxumWs<C> {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectQuery {
    pub node_id: String,
    pub codec: String,
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
    let codec = match query.0.codec.as_str() {
        "json" => CodecKind::JSON,
        "bincode" => CodecKind::BINCODE,
        _ => {
            let response = Response::builder()
                .status(axum::http::StatusCode::BAD_REQUEST)
                .body(Body::from("unsupported codec"))
                .expect("response builder failed");
            return response;
        }
    };
    let Some(codec) = DynCodec::form_kind(codec) else {
        let response = Response::builder()
            .status(axum::http::StatusCode::BAD_REQUEST)
            .body(Body::from("unsupported codec"))
            .expect("response builder failed");
        return response;
    };
    let config = EdgeConfig {
        peer_id: NodeId { bytes },
        peer_auth: EdgeAuth::default(),
    };
    tracing::info!(?config, "new edge connection");
    ws.on_upgrade(|ws| async move { handle_socket(ws, state.0, config, codec).await })
}

async fn handle_socket(socket: WebSocket, node: Node, config: EdgeConfig, codec: DynCodec) {
    let Ok(node_id) = node
        .create_edge_connection(AxumWs::new(socket, codec), config)
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
fn is_running_in_k8s() -> bool {
    std::path::Path::new("/var/run/secrets/kubernetes.io").exists()
}
#[tokio::main]
async fn main() -> asteroid_mq::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("debug,kube_client=off,asteroid_mq=debug,openraft=warn,hyper_util=warn,hyper=warn,tower=warn,rustls=off")
        .init();
    let mut node_config = NodeConfig::default();
    if is_running_in_k8s() {
        let node_id = this_pod_id();
        node_config.id = node_id;
    }
    node_config.raft.election_timeout_max = 1000;
    node_config.raft.election_timeout_min = 500;
    node_config.raft.heartbeat_interval = 200;

    let node = Node::new(node_config);
    #[derive(Clone)]
    pub struct TestMiddleware;
    impl<I> EdgeConnectionMiddleware<I> for TestMiddleware
    where
        I: EdgeConnectionHandler,
    {
        type Future = I::Future;
        fn handle(
            &self,
            node: Node,
            from: NodeId,
            req: asteroid_mq_model::EdgeRequestEnum,
            inner: &I,
        ) -> Self::Future {
            tracing::info!(?from, ?req, "log middleware");
            inner.handle(node, from, req)
        }
    }
    node.insert_edge_connection_middleware(TestMiddleware).await;
    if is_running_in_k8s() {
        let cluster_provider = K8sClusterProvider::new(DEFAULT_TCP_PORT).await;
        node.start(cluster_provider).await?;
    } else {
        let cluster_provider =
            StaticClusterProvider::singleton(node.id(), node.config().addr.to_string());
        node.start(cluster_provider).await?;
    };
    node.wait_for_leader().await?;
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
    let http_tcp_listener = tokio::net::TcpListener::bind("localhost:8080")
        .await
        .unwrap();
    tracing::info!("listening on {}", http_tcp_listener.local_addr().unwrap());
    let route = axum::Router::new()
        .route("/connect", axum::routing::get(handler))
        .route("/node_id", axum::routing::put(get_node_id))
        .with_state(node);
    serve(http_tcp_listener, route).await.unwrap();
    Ok(())
}
