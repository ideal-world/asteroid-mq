use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU32, Arc},
};

use crate::endpoint::{ClientEndpoint, EndpointMailbox};
pub use crate::error::*;
use asteroid_mq_model::{
    EdgeEndpointOffline, EdgeEndpointOnline, EdgeError, EdgeMessage, EdgePayload, EdgePush,
    EdgeRequest, EdgeRequestEnum, EdgeResponseEnum, EndpointAddr, EndpointInterest, Interest,
    Message, MessageAck, MessageStateUpdate, SetState, TopicCode, WaitAckSuccess,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{oneshot, RwLock};
use tokio_tungstenite::tungstenite;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
type Responder = oneshot::Sender<Result<EdgeResponseEnum, EdgeError>>;
type ResponsePool = Arc<RwLock<HashMap<u32, Responder>>>;
type EndpointMailboxMap = Arc<RwLock<HashMap<EndpointAddr, EndpointMailbox>>>;
#[derive(Debug, Clone)]
pub struct ClientNode {
    pub(crate) inner: Arc<ClientNodeInner>,
}

impl ClientNode {
    pub async fn ack(&self, ack: MessageAck) -> Result<(), ClientNodeError> {
        self.inner.send_single_ack(ack).await
    }
    pub async fn send_message(
        &self,
        message: EdgeMessage,
    ) -> Result<WaitAckSuccess, ClientNodeError> {
        self.inner.send_message(message).await
    }
    pub async fn create_endpoint(
        &self,
        topic_code: TopicCode,
        interests: impl IntoIterator<Item = Interest>,
    ) -> Result<ClientEndpoint, ClientNodeError> {
        let interests = interests.into_iter().collect::<HashSet<_>>();
        let addr = self
            .inner
            .send_ep_online(EdgeEndpointOnline {
                topic_code: topic_code.clone(),
                interests: interests.iter().cloned().collect(),
            })
            .await?;
        let message_rx = self
            .inner
            .ensure_mailbox_and_take_rx(addr)
            .await
            .expect("conflict endpoint addr should not happen");
        Ok(ClientEndpoint {
            addr,
            topic_code,
            interests,
            node: Arc::downgrade(&self.inner),
            message_rx,
        })
    }
    pub async fn connect<R>(request: R) -> Result<Self, ClientNodeError>
    where
        R: tokio_tungstenite::tungstenite::client::IntoClientRequest + Unpin,
    {
        let inner = ClientNodeInner::connect(request).await?;
        Ok(ClientNode {
            inner: Arc::new(inner),
        })
    }
}

#[derive(Debug)]
pub(crate) struct ClientNodeInner {
    pub(crate) sender: tokio::sync::mpsc::UnboundedSender<(EdgeRequest, Responder)>,
    pub(crate) endpoint_map: EndpointMailboxMap,
    pub(crate) seq: AtomicU32,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) _rx_handle: tokio::task::JoinHandle<()>,
    pub(crate) _tx_handle: tokio::task::JoinHandle<()>,
}

impl ClientNodeInner {
    async fn ensure_mailbox_and_take_rx(
        &self,
        addr: EndpointAddr,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<Message>> {
        self.endpoint_map
            .write()
            .await
            .entry(addr)
            .or_insert_with(EndpointMailbox::new)
            .take_rx()
    }
    async fn send_message(&self, message: EdgeMessage) -> Result<WaitAckSuccess, ClientNodeError> {
        let response = self
            .send_request(EdgeRequestEnum::SendMessage(message))
            .await?;
        if let EdgeResponseEnum::SendMessage(edge_result) = response {
            let wait_ack = edge_result.into_std()?;
            Ok(wait_ack)
        } else {
            Err(ClientNodeError::unexpected_response(response))
        }
    }
    async fn send_ep_online(
        &self,
        request: EdgeEndpointOnline,
    ) -> Result<EndpointAddr, ClientNodeError> {
        let response = self
            .send_request(EdgeRequestEnum::EndpointOnline(request))
            .await?;
        if let EdgeResponseEnum::EndpointOnline(ep_addr) = response {
            Ok(ep_addr)
        } else {
            Err(ClientNodeError::unexpected_response(response))
        }
    }
    pub(crate) async fn send_ep_offline(
        &self,
        topic_code: TopicCode,
        endpoint: EndpointAddr,
    ) -> Result<(), ClientNodeError> {
        let response = self
            .send_request(EdgeRequestEnum::EndpointOffline(EdgeEndpointOffline {
                endpoint,
                topic_code,
            }))
            .await?;
        if let EdgeResponseEnum::EndpointOffline = response {
            Ok(())
        } else {
            Err(ClientNodeError::unexpected_response(response))
        }
    }
    pub(crate) async fn send_ep_interests(
        &self,
        topic_code: TopicCode,
        endpoint: EndpointAddr,
        interests: Vec<Interest>,
    ) -> Result<(), ClientNodeError> {
        let response = self
            .send_request(EdgeRequestEnum::EndpointInterest(EndpointInterest {
                topic_code,
                endpoint,
                interests,
            }))
            .await?;
        if let EdgeResponseEnum::EndpointInterest = response {
            Ok(())
        } else {
            Err(ClientNodeError::unexpected_response(response))
        }
    }
    pub(crate) async fn send_single_ack(
        &self,
        MessageAck {
            ack_to,
            topic_code,
            from,
            kind,
        }: MessageAck,
    ) -> Result<(), ClientNodeError> {
        let response = self
            .send_request(EdgeRequestEnum::SetState(SetState {
                topic: topic_code,
                update: MessageStateUpdate {
                    message_id: ack_to,
                    status: HashMap::from_iter([(from, kind)]),
                },
            }))
            .await?;
        if let EdgeResponseEnum::SetState = response {
            Ok(())
        } else {
            Err(ClientNodeError::unexpected_response(response))
        }
    }
    async fn send_request(
        &self,
        request: EdgeRequestEnum,
    ) -> Result<EdgeResponseEnum, ClientNodeError> {
        let (responder, rx) = oneshot::channel();
        let request = EdgeRequest {
            seq_id: self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            request,
        };
        self.sender
            .send((request, responder))
            .map_err(|e| ClientNodeError {
                kind: ClientErrorKind::NoConnection(e.0 .0.request),
            })?;
        let response = rx.await.map_err(|_| ClientNodeError {
            kind: ClientErrorKind::Disconnected,
        })??;
        Ok(response)
    }
    #[tracing::instrument(skip(url))]
    pub(crate) async fn connect<R>(url: R) -> Result<ClientNodeInner, ClientNodeError>
    where
        R: tokio_tungstenite::tungstenite::client::IntoClientRequest + Unpin,
    {
        let response_pool: ResponsePool = Default::default();
        let (stream, resp) = tokio_tungstenite::connect_async(url).await?;
        tracing::debug!(response_code = %resp.status(), "connected to server");
        let endpoint_map = EndpointMailboxMap::default();
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
                            tracing::debug!("task cancelled");
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
            .instrument(tracing::info_span!("client_node_tx"))
        };

        let rx_ct = ct.child_token();
        let rx_task = {
            let endpoints_map = endpoint_map.clone();
            async move {
                loop {
                    let message = tokio::select! {
                        _ = rx_ct.cancelled() => {
                            endpoints_map.write().await.clear();
                            tracing::debug!("task cancelled");
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
                        EdgePayload::Push(EdgePush::Message { endpoints, message }) => {
                            tracing::trace!(endpoints = ?endpoints, ?message, "received message");
                            let mut wg = endpoints_map.write().await;
                            for ep in endpoints {
                                if let Some(mailbox) = wg.get(&ep) {
                                    let send_result = mailbox.message_tx.send(message.clone());
                                    if send_result.is_err() {
                                        tracing::warn!(addr=?ep, "target endpoint is dropped")
                                    }
                                } else {
                                    let mailbox = EndpointMailbox::new();
                                    mailbox
                                        .message_tx
                                        .send(message.clone())
                                        .expect("a brand new channel must have the receiver");
                                    wg.insert(ep, mailbox);
                                    tracing::warn!(addr=?ep, "target endpoint not found")
                                }
                            }
                            drop(wg);
                        }
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
            }
            .instrument(tracing::info_span!("client_node_rx"))
        };
        let tx_handle = tokio::spawn(tx_task);
        let rx_handle = tokio::spawn(rx_task);
        Ok(ClientNodeInner {
            sender: request_tx,
            seq: Default::default(),
            endpoint_map,
            cancellation_token: ct,
            _rx_handle: rx_handle,
            _tx_handle: tx_handle,
        })
    }
}

impl Drop for ClientNodeInner {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}
