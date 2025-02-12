use std::{ops::Deref, sync::Arc};

pub use asteroid_mq_model::connection::{
    EdgeConnectionError, EdgeConnectionErrorKind, EdgeNodeConnection,
};
use tracing::warn;

use crate::protocol::{
    endpoint::EndpointAddr,
    message::*,
    node::edge::{middleware::EdgeConnectionHandler, EdgePayload, EdgeResponse},
};

use super::{
    super::{Auth, NodeId, NodeRef},
    EdgePush,
};

// todo
pub mod tokio_channel;
pub mod tokio_tcp;
// pub mod tokio_ws;

use futures_util::{FutureExt, SinkExt, StreamExt};

#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub attached_node: NodeRef,
    pub peer_id: NodeId,
    pub auth: Auth,
}

#[derive(Debug)]
pub struct EdgeConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<EdgePayload>,
    pub alive: Arc<std::sync::atomic::AtomicBool>,
    pub peer_id: NodeId,
    pub finish_signal: flume::Receiver<()>,
}

pub struct EdgeConnectionRef {
    inner: Arc<EdgeConnectionInstance>,
}

impl Deref for EdgeConnectionRef {
    type Target = EdgeConnectionInstance;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl EdgeConnectionInstance {
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn get_connection_ref(self: &Arc<Self>) -> EdgeConnectionRef {
        EdgeConnectionRef {
            inner: Arc::clone(self),
        }
    }
    pub fn send_payload(&self, packet: EdgePayload) -> Result<(), EdgeConnectionError> {
        self.outbound
            .send(packet)
            .map_err(|_e| EdgeConnectionError::new(EdgeConnectionErrorKind::Closed, "send message"))
    }

    pub fn push_message(
        &self,
        endpoint: &EndpointAddr,
        message: Message,
    ) -> Result<(), EdgeConnectionError> {
        self.send_payload(EdgePayload::Push(EdgePush::Message {
            endpoints: vec![*endpoint],
            message,
        }))
    }
    pub async fn init<C: EdgeNodeConnection>(
        config: ConnectionConfig,
        connection: C,
    ) -> Result<Self, EdgeConnectionError> {
        tracing::debug!(?config, "init edge connection");
        let config_clone = config.clone();
        let (mut sink, mut stream) = connection.split();
        let peer_id = config.peer_id;

        let node = config.attached_node.upgrade().ok_or_else(|| {
            EdgeConnectionError::new(EdgeConnectionErrorKind::Closed, "node is already dropped")
        })?;
        let (outbound_tx, outbound_rx) = flume::bounded::<EdgePayload>(1024);
        let task = if true {
            enum PollEvent {
                PacketIn(EdgePayload),
                PacketOut(EdgePayload),
            }
            let internal_outbound_tx = outbound_tx.clone();

            async move {
                loop {
                    let event = futures_util::select! {
                        next_pack = stream.next().fuse() => {
                            let Some(next_event) = next_pack else {
                                break;
                            };
                            PollEvent::PacketIn(next_event?)
                        }
                        packet = outbound_rx.recv_async().fuse() => {
                            let Ok(packet) = packet else {
                                break;
                            };
                            PollEvent::PacketOut(packet)
                        }
                    };
                    match event {
                        PollEvent::PacketIn(payload) => {
                            let outbound = internal_outbound_tx.clone();
                            match payload {
                                EdgePayload::Request(request) => {
                                    let node = node.clone();
                                    tokio::spawn(async move {
                                        let seq_id = request.seq_id;
                                        let handler = node.edge_handler.read().await.clone();
                                        let resp =
                                            handler.handle(node, peer_id, request.request).await;
                                        let resp = EdgeResponse::from_result(seq_id, resp);
                                        let payload = EdgePayload::Response(resp);
                                        outbound.send(payload).unwrap_or_else(|e| {
                                            warn!(?e, "failed to send edge response");
                                        });
                                    });
                                }
                                _ => {
                                    // invalid event, ignore
                                }
                            }
                        }
                        PollEvent::PacketOut(packet) => {
                            sink.send(packet).await?;
                        }
                    }
                }

                Result::<(), EdgeConnectionError>::Ok(())
            }
        } else {
            return Err(EdgeConnectionError::new(
                EdgeConnectionErrorKind::Protocol,
                "peer is not an edge node",
            ));
        };

        let alive_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (finish_notify, finish_signal) = flume::bounded(1);
        let _handle = {
            let alive_flag = Arc::clone(&alive_flag);
            let attached_node = config.attached_node.clone();

            tokio::spawn(async move {
                let result = task.await;
                let node = attached_node.upgrade();
                if let Err(e) = result {
                    if node.is_some() {
                        warn!(?e, "connection task failed");
                    }
                }
                alive_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                let _ = finish_notify.send(());
                if let Some(node) = node {
                    node.remove_edge_connection(peer_id);
                }
            })
        };
        Ok(Self {
            config: config_clone,
            alive: alive_flag,
            outbound: outbound_tx,
            peer_id,
            finish_signal,
        })
    }
}
