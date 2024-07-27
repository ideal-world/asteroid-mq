pub mod codec;
pub mod connection;

use std::{
    borrow::Cow,
    collections::HashMap,
    mem::size_of,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
    sync::{self, Arc},
};

use bytes::{Buf, Bytes, BytesMut};
use codec::NNCodecType;
use connection::{
    tokio_tcp::TokioTcp, ConnectionConfig, N2NConnection, N2NConnectionError,
    N2NConnectionInstance, N2NConnectionRef,
};
use crossbeam::{epoch::Pointable, sync::ShardedLock};
use serde::{Deserialize, Serialize};

use crate::EndpointAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId {
    pub bytes: [u8; 16],
}
impl NodeId {
    pub fn new() -> NodeId {
        static INSTANCE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dg = crate::util::executor_digest();
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&dg.to_be_bytes());
        bytes[8..16].copy_from_slice(
            &INSTANCE_ID
                .fetch_add(1, sync::atomic::Ordering::SeqCst)
                .to_be_bytes(),
        );
        NodeId { bytes }
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct NodeTrace {
    pub source: NodeId,
    pub hops: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeKind {
    Cluster = 0,
    Edge = 1,
}

impl From<u8> for NodeKind {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeKind::Cluster,
            1 => NodeKind::Edge,
            _ => NodeKind::Edge,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct N2NEventId {
    pub bytes: [u8; 16],
}

impl N2NEventId {
    pub fn gen() -> Self {
        thread_local! {
            static COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        }
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let counter = COUNTER.with(|c| {
            let v = c.get();
            c.set(v.wrapping_add(1));
            v
        });
        let eid = crate::util::executor_digest() as u32;
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&timestamp.to_be_bytes());
        bytes[8..12].copy_from_slice(&counter.to_be_bytes());
        bytes[12..16].copy_from_slice(&eid.to_be_bytes());
        Self { bytes }
    }
}

#[repr(u8)]
pub enum N2NEncodeKind {
    Bincode = 0,
    Json = 1,
    Protobuf = 2,
}
#[derive(Debug)]
pub struct N2NEventPacket {
    pub header: N2NEventPacketHeader,
    pub payload: Bytes,
}
#[derive(Debug)]
pub struct N2NEventPacketHeader {
    pub id: N2NEventId,
    pub kind: N2NEventKind,
    pub payload_size: u32,
}

impl N2NEventPacket {
    pub fn kind(&self) -> N2NEventKind {
        self.header.kind
    }

    pub fn auth(evt: N2NAuthEvent) -> Self {
        let mut payload_buf = BytesMut::with_capacity(size_of::<N2NAuthEvent>());
        evt.encode(&mut payload_buf);
        Self {
            header: N2NEventPacketHeader {
                id: N2NEventId::gen(),
                kind: N2NEventKind::Auth,
                payload_size: payload_buf.len() as u32,
            },
            payload: payload_buf.into(),
        }
    }
    pub fn message(evt: N2NMessageEvent) -> Self {
        let mut payload_buf =
            BytesMut::with_capacity(evt.payload.len() + size_of::<N2NMessageEvent>());
        evt.encode(&mut payload_buf);
        Self {
            header: N2NEventPacketHeader {
                id: N2NEventId::gen(),
                kind: N2NEventKind::Message,
                payload_size: payload_buf.len() as u32,
            },
            payload: payload_buf.into(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum N2NEventKind {
    Auth = 0,
    Message = 1,
    Unreachable = 2,
    Unknown = 255,
}

impl From<u8> for N2NEventKind {
    fn from(value: u8) -> Self {
        match value {
            0 => N2NEventKind::Auth,
            1 => N2NEventKind::Message,
            2 => N2NEventKind::Unreachable,
            _ => N2NEventKind::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct N2NAuthEvent {
    pub info: NodeInfo,
    pub auth: N2NAuth,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct N2NMessageEvent {
    pub to: NodeId,
    pub trace: NodeTrace,
    pub payload: Bytes,
}

pub struct N2NUnreachableEvent {
    pub to: NodeId,
    pub trace: NodeTrace,
}

pub enum N2NEvent {
    Auth(N2NAuthEvent),
    Message(N2NMessageEvent),
    Unreachable(N2NUnreachableEvent),
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct N2NAuth {}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub kind: NodeKind,
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self {
            id: NodeId::new(),
            kind: NodeKind::Cluster,
        }
    }
}

#[derive(Debug, Default)]
pub struct Node {
    info: NodeInfo,
    ep_routing_table: ShardedLock<HashMap<EndpointAddr, NodeId>>,
    n2n_routing_table: ShardedLock<HashMap<NodeId, N2nRoutingInfo>>,
    connections: ShardedLock<HashMap<NodeId, Arc<N2NConnectionInstance>>>,
    auth: N2NAuth,
}

impl Node {
    pub async fn create_connection<C: N2NConnection>(
        self: &Arc<Self>,
        conn: C,
    ) -> Result<(), N2NConnectionError> {
        let config = ConnectionConfig {
            attached_node: Arc::downgrade(self),
            auth: self.auth.clone(),
        };
        let conn_inst = N2NConnectionInstance::init(config, conn).await?;
        self.connections
            .write()
            .unwrap()
            .insert(conn_inst.peer_info.id, Arc::new(conn_inst));
        Ok(())
    }
    pub fn id(&self) -> NodeId {
        self.info.id
    }
    pub async fn handle_message(&self, message_evt: N2NMessageEvent) {
        if message_evt.to == self.id() {
            self.handle_message_as_desitination(message_evt).await;
        } else {
            self.forward_message(message_evt).await;
        }
    }
    pub async fn handle_message_as_desitination(&self, message_evt: N2NMessageEvent) {}
    pub fn get_connection(&self, to: NodeId) -> Option<N2NConnectionRef> {
        let connections = self.connections.read().unwrap();
        connections.get(&to).map(|conn| conn.get_connection_ref())
    }

    pub async fn forward_message(&self, mut message_evt: N2NMessageEvent) {
        let Some(next_jump) = self.get_next_jump(message_evt.to) else {
            todo!("handle unreachable");
            return;
        };
        message_evt.trace.hops.push(self.id());
        let packed = N2NEventPacket::message(message_evt);
    }

    pub fn get_next_jump(&self, to: NodeId) -> Option<NodeId> {
        let routing_table = self.n2n_routing_table.read().unwrap();
        routing_table.get(&to).map(|info| info.next_jump)
    }
}
#[derive(Debug)]
pub struct N2nRoutingInfo {
    next_jump: NodeId,
    hops: u32,
}

pub struct Connection {
    pub attached_node: sync::Weak<Node>,
    pub peer_info: NodeInfo,
}

#[tokio::test]
async fn test_nodes() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    let node_server = <Arc<Node>>::default();
    let server_id = node_server.id();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:10080")
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Ok((stream, peer)) = listener.accept().await {
            tracing::info!(peer=?peer, "new connection");
            let node = Arc::clone(&node_server);
            tokio::spawn(async move {
                let conn = TokioTcp::new(stream);
                node.create_connection(conn).await.unwrap();
            });
        }
    });

    let node_client = <Arc<Node>>::default();
    let client_id = node_client.id();
    let stream_client = tokio::net::TcpSocket::new_v4()
        .unwrap()
        .connect(SocketAddr::from_str("127.0.0.1:10080").unwrap())
        .await
        .unwrap();

    node_client
        .create_connection(TokioTcp::new(stream_client))
        .await
        .unwrap();

    let server_conn = node_client.get_connection(server_id).unwrap();
    server_conn
        .outbound
        .send(N2NEventPacket::message(N2NMessageEvent {
            to: server_id,
            trace: NodeTrace {
                source: client_id,
                hops: vec![],
            },
            payload: Bytes::from_static(b"hello"),
        }))
        .unwrap();
}
