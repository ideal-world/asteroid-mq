use std::{borrow::Cow, error::Error, ops::Deref, sync::Arc};

use tracing::warn;

use crate::protocol::{
    codec::{CodecType, DecodeError},
    node::edge::{
        packet::{EdgeAuth, EdgePayloadKind}, EdgePayload, EdgeRequest, EdgeResponse
    },
};

use super::{super::{Auth, EdgePacket, NodeId, NodeRef}, codec::CodecRegistry};

pub mod tokio_tcp;
// pub mod tokio_ws;
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
    pub raw_packet: EdgePacket,
    pub error: Box<dyn std::error::Error + Send + Sync>,
}
use futures_util::{FutureExt, Sink, SinkExt, Stream, StreamExt};
pub trait NodeConnection:
    Send
    + 'static
    + Stream<Item = Result<EdgePacket, NodeConnectionError>>
    + Sink<EdgePacket, Error = NodeConnectionError>
{
}
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub attached_node: NodeRef,
    pub peer_id: NodeId,
    pub codec: Arc<CodecRegistry>,
    pub auth: Auth,
}

#[derive(Debug)]
pub struct EdgeConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<EdgePacket>,
    pub alive: Arc<std::sync::atomic::AtomicBool>,
    pub peer_id: NodeId,
    pub peer_auth: Auth,
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
        let peer_id = config.peer_id;
        let node = config.attached_node.upgrade().ok_or_else(|| {
            NodeConnectionError::new(
                NodeConnectionErrorKind::Closed,
                "node is already dropped",
            )
        })?;
        let (outbound_tx, outbound_rx) = flume::bounded::<EdgePacket>(1024);
        let task = if true {
            enum PollEvent {
                PacketIn(EdgePacket),
                PacketOut(EdgePacket),
            }
            let internal_outbound_tx = outbound_tx.clone();
            let codec_registry = config.codec.clone();
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
                            let Ok(payload) = codec_registry.decode(packet.codec(), &packet.payload) else {
                                warn!("failed to decode packet");
                                continue;
                            };
                            match payload {
                                EdgePayload::Request(request) => {
                                    tokio::spawn(async move {
                                        let resp = node.handle_edge_request(peer_id, request).await;
                                        let resp = EdgeResponse::from_result(id, resp);
                                        let packet = EdgePacket::edge_response(resp);
                                        outbound.send(packet).unwrap_or_else(|e| {
                                            warn!(?e, "failed to send edge response");
                                        });
                                    });
                                },
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
