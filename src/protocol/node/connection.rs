use std::{borrow::Cow, error::Error, ops::Deref, sync::Arc, time::Instant};

use tracing::warn;

use crate::protocol::{
    codec::{CodecType, DecodeError},
    node::{
        edge::{EdgeRequest, EdgeResponse},
        event::{N2NPayloadKind, N2nAuth},
    },
};

use super::{N2nPacket, NodeAuth, NodeConfig, NodeId, NodeRef};

pub mod tokio_tcp;
pub mod tokio_ws;
#[derive(Debug)]
pub struct NodeConnectionError {
    pub kind: NodeConnectionErrorKind,
    pub context: Cow<'static, str>,
}

impl NodeConnectionError {
    pub fn new(kind: NodeConnectionErrorKind, context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }
}
#[derive(Debug)]
pub enum NodeConnectionErrorKind {
    Send(NodeSendError),
    Decode(DecodeError),
    Io(std::io::Error),
    Underlying(Box<dyn Error + Send>),
    Closed,
    Timeout,
    Protocol,
    Existed,
}
#[derive(Debug)]

pub struct NodeSendError {
    pub raw_packet: N2nPacket,
    pub error: Box<dyn std::error::Error + Send + Sync>,
}
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
pub trait NodeConnection:
    Send
    + 'static
    + Stream<Item = Result<N2nPacket, NodeConnectionError>>
    + Sink<N2nPacket, Error = NodeConnectionError>
{
}
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub attached_node: NodeRef,
    pub auth: NodeAuth,
}


#[derive(Debug)]
pub struct EdgeConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<N2nPacket>,
    pub alive: Arc<std::sync::atomic::AtomicBool>,
    pub peer_id: NodeId,
    pub peer_auth: NodeAuth,
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
    pub async fn init<C: NodeConnection>(
        config: ConnectionConfig,
        connection: C,
    ) -> Result<Self, NodeConnectionError> {
        tracing::debug!(?config, "init edge connection");
        let config_clone = config.clone();
        let (mut sink, mut stream) = connection.split();
        let Some(node) = config.attached_node.upgrade() else {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Closed,
                "node was dropped",
            ));
        };
        let my_auth = config.auth.clone();
        let evt = N2nPacket::auth(N2nAuth {
            id: node.id(),
            auth: my_auth,
        });
        sink.send(evt).await?;
        tracing::trace!("sent auth");
        let auth = stream.next().await.unwrap_or(Err(NodeConnectionError::new(
            NodeConnectionErrorKind::Closed,
            "event stream reached unexpected end when send auth packet",
        )))?;
        if auth.header.kind != N2NPayloadKind::Auth {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Protocol,
                "unexpected event, expect auth",
            ));
        }
        let auth_event = N2nAuth::decode(auth.payload)
            .map_err(|e| {
                NodeConnectionError::new(NodeConnectionErrorKind::Decode(e), "decode auth")
            })?
            .0; 
        let peer_id = auth_event.id;
        tracing::debug!(auth=?auth_event, "received auth");

        if let Some(existed_connection) = node.get_edge_connection(peer_id) {
            if existed_connection.is_alive() {
                return Err(NodeConnectionError::new(
                    NodeConnectionErrorKind::Existed,
                    "connection already exists",
                ));
            } else {
                node.remove_edge_connection(peer_id);
            }
        }
        // if peer is cluster, and we are cluster:

        let (outbound_tx, outbound_rx) = flume::bounded::<N2nPacket>(1024);
        let task = if true {
            enum PollEvent {
                PacketIn(N2nPacket),
                PacketOut(N2nPacket),
            }
            let internal_outbound_tx = outbound_tx.clone();
            let task = async move {
                loop {
                    let event = futures_util::select! {
                        next_pack = stream.next().fuse() => {
                            let Some(next_event) = next_pack else {
                                break;
                            };
                            let Ok(next_event) = next_event else {
                                return Err(NodeConnectionError::new(
                                    NodeConnectionErrorKind::Io(std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "failed to read next event",
                                    )),
                                    "read next event",
                                ));
                            };
                            PollEvent::PacketIn(next_event)
                        }
                        packet = outbound_rx.recv_async().fuse() => {
                            let Ok(packet) = packet else {
                                break;
                            };
                            PollEvent::PacketOut(packet)
                        }
                    };
                    match event {
                        PollEvent::PacketIn(packet) => {
                            match packet.header.kind {
                                N2NPayloadKind::Heartbeat => {}
                                N2NPayloadKind::EdgeRequest => {
                                    let node = node.clone();
                                    let Ok(request) =
                                        EdgeRequest::decode_from_bytes(packet.payload)
                                    else {
                                        continue;
                                    };
                                    let outbound = internal_outbound_tx.clone();
                                    let id = request.id;
                                    tokio::spawn(async move {
                                        let resp = node.handle_edge_request(peer_id, request).await;
                                        let resp = EdgeResponse::from_result(id, resp);
                                        let packet = N2nPacket::edge_response(resp);
                                        outbound.send(packet).unwrap_or_else(|e| {
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

                Ok(())
            };
            task
        } else {
            return Err(NodeConnectionError::new(
                NodeConnectionErrorKind::Protocol,
                "peer is not an edge node",
            ));
        };

        let alive_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let (finish_notify, finish_signal) = flume::bounded(1);
        let _handle = {
            let alive_flag = Arc::clone(&alive_flag);
            let attached_node = config.attached_node.clone();
            let peer_id = auth_event.id;

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
            peer_id: auth_event.id,
            peer_auth: auth_event.auth,
            finish_signal,
        })
    }
}
