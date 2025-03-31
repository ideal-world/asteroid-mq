use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::AtomicU32, Arc},
};

use crate::endpoint::{ClientEndpoint, EndpointMailbox};
pub use crate::error::*;
use asteroid_mq_model::{
    connection::{EdgeConnectionErrorKind, EdgeNodeConnection},
    EdgeEndpointOffline, EdgeEndpointOnline, EdgeError, EdgeMessage, EdgePayload, EdgePush,
    EdgeRequest, EdgeRequestEnum, EdgeResponseEnum, EndpointAddr, EndpointInterest, Interest,
    Message, MessageAck, MessageStateUpdate, SetState, TopicCode, WaitAckSuccess,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{oneshot, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
type Responder = oneshot::Sender<Result<EdgeResponseEnum, EdgeError>>;
type ResponseHandle = oneshot::Receiver<Result<EdgeResponseEnum, EdgeError>>;
type ResponsePool = Arc<RwLock<HashMap<u32, Responder>>>;
type EndpointMailboxMap = Arc<RwLock<HashMap<EndpointAddr, EndpointMailbox>>>;

pub struct MessageAckHandle {
    response_handle: ResponseHandle,
}

impl MessageAckHandle {
    pub async fn wait(self) -> Result<WaitAckSuccess, ClientNodeError> {
        let response = ClientNodeInner::wait_handle(self.response_handle).await?;
        if let EdgeResponseEnum::SendMessage(edge_result) = response {
            let wait_ack = edge_result.into_std()?;
            Ok(wait_ack)
        } else {
            Err(ClientNodeError::unexpected_response(response))
        }
    }
}

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
    ) -> Result<MessageAckHandle, ClientNodeError> {
        self.inner.send_message(message).await
    }
    pub async fn send_message_and_wait(
        &self,
        message: EdgeMessage,
    ) -> Result<WaitAckSuccess, ClientNodeError> {
        let handle = self.inner.send_message(message).await?;
        handle.wait().await
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
    pub async fn connect<C>(connect: C) -> Result<Self, ClientNodeError>
    where
        C: EdgeNodeConnection,
    {
        let inner = ClientNodeInner::connect(connect).await?;
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
    pub fn into_client_node(self: Arc<ClientNodeInner>) -> ClientNode {
        ClientNode {
            inner: self.clone(),
        }
    }
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
    async fn send_message(
        &self,
        message: EdgeMessage,
    ) -> Result<MessageAckHandle, ClientNodeError> {
        let response_handle = self
            .send_request(EdgeRequestEnum::SendMessage(message))
            .await?;
        Ok(MessageAckHandle { response_handle })
    }
    pub(crate) async fn send_ep_online(
        &self,
        request: EdgeEndpointOnline,
    ) -> Result<EndpointAddr, ClientNodeError> {
        let response = self
            .send_request_and_wait(EdgeRequestEnum::EndpointOnline(request))
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
            .send_request_and_wait(EdgeRequestEnum::EndpointOffline(EdgeEndpointOffline {
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
            .send_request_and_wait(EdgeRequestEnum::EndpointInterest(EndpointInterest {
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
            .send_request_and_wait(EdgeRequestEnum::SetState(SetState {
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
    ) -> Result<ResponseHandle, ClientNodeError> {
        let (responder, response_handle) = oneshot::channel();
        let request = EdgeRequest {
            seq_id: self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            request,
        };
        tracing::warn!(?request);
        self.sender
            .send((request, responder))
            .map_err(|e| ClientNodeError {
                kind: ClientErrorKind::NoConnection(e.0 .0.request),
            })?;
        Ok(response_handle)
        // let response = rx.await.map_err(|_| ClientNodeError {
        //     kind: ClientErrorKind::Disconnected,
        // })??;
        // Ok(response)
    }
    async fn wait_handle(
        response_handle: ResponseHandle,
    ) -> Result<EdgeResponseEnum, ClientNodeError> {
        let response = response_handle.await.map_err(|_| ClientNodeError::disconnected("wait handle"))??;
        Ok(response)
    }
    async fn send_request_and_wait(
        &self,
        request: EdgeRequestEnum,
    ) -> Result<EdgeResponseEnum, ClientNodeError> {
        let response_handle = self.send_request(request).await?;
        Self::wait_handle(response_handle).await
    }
    pub(crate) async fn connect<C>(connection: C) -> Result<ClientNodeInner, ClientNodeError>
    where
        C: EdgeNodeConnection,
    {
        let response_pool: ResponsePool = Default::default();

        let endpoint_map = EndpointMailboxMap::default();
        let ct = CancellationToken::new();
        let (request_tx, mut request_rx) = tokio::sync::mpsc::unbounded_channel::<(
            EdgeRequest,
            oneshot::Sender<Result<EdgeResponseEnum, EdgeError>>,
        )>();

        let (mut sink, mut stream) = connection.split();
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
                                    tracing::warn!("tx dropped");
                                    break;
                                }
                            }
                        }
                    };
                    let seq_id = request.seq_id;
                    response_pool.write().await.insert(seq_id, responder);
                    tracing::warn!(seq_id, "[debug] request do send");
                    let send_result = sink.send(EdgePayload::Request(request)).await;
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
                    let edge_pld = tokio::select! {
                        _ = rx_ct.cancelled() => {
                            endpoints_map.write().await.clear();
                            tracing::debug!("task cancelled");
                            break
                        }
                        received = stream.next() => {
                            match received {
                                Some(Ok(edge_pld)) => {
                                    edge_pld
                                }
                                Some(Err(e)) => {
                                    match e.kind {
                                        EdgeConnectionErrorKind::Reconnect | EdgeConnectionErrorKind::Closed  => {
                                            // clear the response pool
                                            response_pool.write().await.clear();
                                            // clear ep map, so the endpoint will know there is going to have a new connection
                                            endpoints_map.write().await.clear();
                                            if matches!(e.kind,EdgeConnectionErrorKind::Closed) {
                                                tracing::info!("connection closed");
                                                break;
                                            } 
                                        },
                                        _ => {
                                            tracing::error!("failed to receive message: {:?}", e);
                                        }
                                    }
                                    continue
                                }
                                None => {
                                    tracing::debug!("stream closed");
                                    break;
                                }
                            }
                        }
                    };

                    match edge_pld {
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
                            tracing::warn!(?edge_response, "edge response");
                            let seq_id = edge_response.seq_id;
                            if let Some(responder) = response_pool.write().await.remove(&seq_id) {
                                tracing::warn!(?edge_response, "[debug] received response from server");
                                let _ = responder.send(edge_response.result.into_std());
                            } else {
                                tracing::error!(seq_id, "response handle not found");
                            }
                        }
                        EdgePayload::Error(e) => {
                            tracing::error!(?e, "received error");
                        }
                        _ => {}
                    }
                }
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
