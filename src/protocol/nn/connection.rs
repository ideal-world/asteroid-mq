use std::{borrow::Cow, ops::Deref, sync::Arc};

use tokio::task::JoinHandle;

use crate::protocol::nn::event::{N2NAuthEvent, N2NEventKind};

use super::{
    codec::{NNCodecType, NNDecodeError},
    N2NAuth, N2NEventPacket, N2NMessageEvent, Node, NodeInfo,
};

pub mod tokio_tcp;
#[derive(Debug)]
pub struct N2NConnectionError {
    pub kind: N2NConnectionErrorKind,
    pub context: Cow<'static, str>,
}

impl N2NConnectionError {
    pub fn new(kind: N2NConnectionErrorKind, context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }
}
#[derive(Debug)]
pub enum N2NConnectionErrorKind {
    Send(N2NSendError),
    Decode(NNDecodeError),
    Io(std::io::Error),
    Closed,
    Protocol,
}
#[derive(Debug)]

pub struct N2NSendError {
    pub raw_event: N2NEventPacket,
    pub error: Box<dyn std::error::Error + Send + Sync>,
}
use futures_util::{Sink, SinkExt, Stream, StreamExt};
pub trait N2NConnection:
    Send
    + 'static
    + Stream<Item = Result<N2NEventPacket, N2NConnectionError>>
    + Sink<N2NEventPacket, Error = N2NConnectionError>
{
}
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    pub attached_node: std::sync::Weak<Node>,
    pub auth: N2NAuth,
}
pub struct N2NConnectionRef {
    inner: Arc<N2NConnectionInstance>,
}

impl Deref for N2NConnectionRef {
    type Target = N2NConnectionInstance;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug)]
pub struct N2NConnectionInstance {
    pub config: ConnectionConfig,
    pub outbound: flume::Sender<N2NEventPacket>,
    pub alive: Arc<std::sync::atomic::AtomicBool>,
    pub peer_info: NodeInfo,
    pub peer_auth: N2NAuth,
}

impl N2NConnectionInstance {
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn get_connection_ref(self: &Arc<Self>) -> N2NConnectionRef {
        N2NConnectionRef {
            inner: Arc::clone(self),
        }
    }
    pub async fn init<C: N2NConnection>(
        config: ConnectionConfig,
        connection: C,
    ) -> Result<Self, N2NConnectionError> {
        let config_clone = config.clone();
        let (mut sink, mut stream) = connection.split();
        let Some(node) = config.attached_node.upgrade() else {
            return Err(N2NConnectionError::new(
                N2NConnectionErrorKind::Closed,
                "node was dropped",
            ));
        };
        let info = NodeInfo {
            id: node.info.id,
            kind: node.info.kind,
        };
        let auth = config.auth.clone();
        let auth_event = N2NAuthEvent { info, auth };
        let evt = N2NEventPacket::auth(auth_event);
        sink.send(evt).await?;
        let auth = stream.next().await.unwrap_or(Err(N2NConnectionError::new(
            N2NConnectionErrorKind::Closed,
            "event stream reached unexpected end when send auth packet",
        )))?;
        if auth.header.kind != N2NEventKind::Auth {
            return Err(N2NConnectionError::new(
                N2NConnectionErrorKind::Protocol,
                "unexpected event, expect auth",
            ));
        }
        let auth_event = N2NAuthEvent::decode(auth.payload)
            .map_err(|e| N2NConnectionError::new(N2NConnectionErrorKind::Decode(e), "decode auth"))?
            .0;

        tracing::debug!(auth=?auth_event, "received auth event");
        let (outbound_tx, outbound_rx) = flume::bounded::<N2NEventPacket>(1024);
        let ob_handle: JoinHandle<Result<_, N2NConnectionError>> = tokio::task::spawn(async move {
            while let Ok(packet) = outbound_rx.recv_async().await {
                sink.send(packet).await?;
            }
            Ok(sink)
        });
        let ib_handle = tokio::task::spawn(async move {
            while let Some(Ok(next_event)) = stream.next().await {
                match next_event.header.kind {
                    N2NEventKind::Message => {
                        let payload = next_event.payload;
                        let message = N2NMessageEvent::decode(payload)
                            .map_err(|e| {
                                N2NConnectionError::new(
                                    N2NConnectionErrorKind::Decode(e),
                                    "decode message event",
                                )
                            })?
                            .0;
                        node.handle_message(message).await;
                        // handle message
                    }
                    N2NEventKind::Unreachable => {

                        // handle unreachable
                    }
                    _ => {
                        return Err(N2NConnectionError::new(
                            N2NConnectionErrorKind::Protocol,
                            "unexpected event",
                        ));
                        // handle unexpected event
                    }
                }
            }
            Ok(stream)
        });
        let alive_flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let _handle = {
            let alive_flag = Arc::clone(&alive_flag);
            let attached_node = config.attached_node.clone();
            let peer_id = auth_event.info.id;
            tokio::spawn(async move {
                let selected = futures_util::future::select(ib_handle, ob_handle).await;
                match selected {
                    futures_util::future::Either::Left(ib) => {
                        // do log stuff here
                    }
                    futures_util::future::Either::Right(ob) => {}
                }
                alive_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                if let Some(node) = attached_node.upgrade() {
                    node.remove_connection(peer_id);
                }
            })
        };
        Ok(Self {
            config: config_clone,
            alive: alive_flag,
            outbound: outbound_tx,
            peer_info: auth_event.info,
            peer_auth: auth_event.auth,
        })
    }
}
