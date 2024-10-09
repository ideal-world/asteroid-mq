mod error;
use std::{collections::HashMap, sync::Arc};

use asteroid_mq_model::{
    EdgeError, EdgePayload, EdgePush, EdgeRequest, EdgeResponse, EdgeResponseEnum,
};
pub use error::*;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{oneshot, RwLock};
use tokio_tungstenite::tungstenite;
use tokio_util::sync::CancellationToken;
type Responder = oneshot::Sender<Result<EdgeResponseEnum, EdgeError>>;
type ResponsePool = Arc<RwLock<HashMap<u32, Responder>>>;
pub struct ClientNode {
    sender: tokio::sync::mpsc::UnboundedSender<(EdgeRequest, Responder)>,
}

impl ClientNode {
    pub async fn connect(url: String) -> Result<ClientNode, ClientNodeError> {
        let response_pool: ResponsePool = Default::default();
        let (stream, resp) = tokio_tungstenite::connect_async(url).await?;
        let ct = CancellationToken::new();
        let (request_tx, mut request_rx) = tokio::sync::mpsc::unbounded_channel::<(
            EdgeRequest,
            oneshot::Sender<Result<EdgeResponseEnum, EdgeError>>,
        )>();

        let (mut sink, mut stream) = stream.split();
        let tx_ct = ct.child_token();
        let tx_task = {
            let response_pool = response_pool.clone();
            async move {
                loop {
                    let (request, responder) = tokio::select! {
                        _ = tx_ct.cancelled() => {
                            tracing::debug!("client node tx cancelled");
                            break
                        }
                        message = request_rx.recv() => {
                            match message {
                                Some(message) => {
                                    message
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                    };
                    let seq_id = request.seq_id;
                    response_pool.write().await.insert(seq_id, responder);
                    let payload = serde_json::to_string(&EdgePayload::Request(request))
                        .expect("failed to serialize message");
                    let message = tungstenite::Message::Text(payload);
                    let send_result = sink.send(message).await;
                    if let Err(e) = send_result {
                        tracing::error!("failed to send message: {:?}", e);
                        break;
                    }
                }
            }
        };

        let rx_ct = ct.child_token();
        let rx_task = async move {
            loop {
                let message = tokio::select! {
                    _ = rx_ct.cancelled() => {
                        tracing::debug!("client node rx cancelled");
                        break
                    }
                    received = stream.next() => {
                        match received {
                            Some(Ok(message)) => {
                                message
                            }
                            Some(Err(e)) => {
                                tracing::error!("failed to receive message: {:?}", e);
                                continue
                            }
                            None => {
                                tracing::debug!("stream closed");
                                break;
                            }
                        }
                    }
                };
                let Ok(text) = message.into_text() else {
                    continue;
                };
                let Ok(payload) = serde_json::from_str::<EdgePayload>(&text).inspect_err(|e| {
                    tracing::error!("failed to parse message: {:?}", e);
                }) else {
                    continue;
                };
                match payload {
                    EdgePayload::Push(EdgePush::Message { endpoints, message }) => {}
                    EdgePayload::Response(edge_response) => {
                        let seq_id = edge_response.seq_id;
                        if let Some(responder) = response_pool.write().await.remove(&seq_id) {
                            let _ = responder.send(edge_response.result.into_std());
                        }
                    }
                    _ => {}
                }
            }
            stream.next().await;
        };
        Ok(ClientNode { sender: request_tx })
    }
}
