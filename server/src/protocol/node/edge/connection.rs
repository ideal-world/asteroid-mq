use std::{borrow::Cow, error::Error, ops::Deref, sync::Arc};

use tracing::warn;

use crate::protocol::{
    endpoint::EndpointAddr,
    message::*,
    node::edge::{middleware::EdgeConnectionHandler, EdgePayload, EdgeResponse},
};

use super::{
    super::{Auth, EdgePacket, NodeId, NodeRef},
    codec::{CodecError, CodecKind, CodecRegistry},
    EdgePush,
};

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
    Io(std::io::Error),
    Underlying(Box<dyn Error + Send>),
    Codec(CodecError),
    Closed,
    Timeout,
    Protocol,
    Existed,
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
    pub codec_registry: Arc<CodecRegistry>,
    pub preferred_codec: CodecKind,
    pub auth: Auth,
}

#[derive(Debug)]
pub struct EdgeConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<EdgePacket>,
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
    pub fn send_packet(&self, packet: EdgePacket) -> Result<(), NodeConnectionError> {
        self.outbound
            .send(packet)
            .map_err(|_e| NodeConnectionError::new(NodeConnectionErrorKind::Closed, "send message"))
    }
    pub fn encode_packet(&self, payload: &EdgePayload) -> Result<EdgePacket, NodeConnectionError> {
        let payload = self
            .config
            .codec_registry
            .encode(self.config.preferred_codec, payload)
            .map_err(|e| {
                NodeConnectionError::new(NodeConnectionErrorKind::Codec(e), "encode message")
            })?;
        let packet = EdgePacket::new(self.config.preferred_codec, payload);
        Ok(packet)
    }
    pub fn push_message(
        &self,
        endpoint: &EndpointAddr,
        message: Message,
    ) -> Result<(), NodeConnectionError> {
        let packet = self.encode_packet(&EdgePayload::Push(EdgePush::Message {
            endpoints: vec![*endpoint],
            message,
        }))?;
        self.send_packet(packet)
    }
    pub async fn init<C: NodeConnection>(
        config: ConnectionConfig,
        connection: C,
    ) -> Result<Self, NodeConnectionError> {
        tracing::debug!(?config, "init edge connection");
        let config_clone = config.clone();
        let (mut sink, mut stream) = connection.split();
        let peer_id = config.peer_id;
        let encoder = config
            .codec_registry
            .get(&config.preferred_codec)
            .ok_or_else(|| {
                NodeConnectionError::new(
                    NodeConnectionErrorKind::Protocol,
                    "preferred codec is not registered",
                )
            })?;
        let node = config.attached_node.upgrade().ok_or_else(|| {
            NodeConnectionError::new(NodeConnectionErrorKind::Closed, "node is already dropped")
        })?;
        let (outbound_tx, outbound_rx) = flume::bounded::<EdgePacket>(1024);
        let task = if true {
            enum PollEvent {
                PacketIn(EdgePacket),
                PacketOut(EdgePacket),
            }
            let internal_outbound_tx = outbound_tx.clone();
            let codec_registry = config.codec_registry.clone();
            let preferred_codec = config.preferred_codec;

            async move {
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
                            let Ok(payload) = codec_registry
                                .decode(packet.codec(), &packet.payload)
                                .inspect_err(|err| {
                                    warn!(bytes=?packet.payload, ?err, "failed to decode packet");
                                })
                            else {
                                continue;
                            };
                            let outbound = internal_outbound_tx.clone();
                            match payload {
                                EdgePayload::Request(request) => {
                                    let node = node.clone();
                                    let encoder = encoder.clone();
                                    tokio::spawn(async move {
                                        let seq_id = request.seq_id;
                                        let handler = node.edge_handler.read().await.clone();
                                        let resp =
                                            handler.handle(node, peer_id, request.request).await;
                                        let resp = EdgeResponse::from_result(seq_id, resp);
                                        let payload = encoder.encode(&EdgePayload::Response(resp));
                                        let resp = EdgePacket::new(preferred_codec, payload);
                                        outbound.send(resp).unwrap_or_else(|e| {
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
            }
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
