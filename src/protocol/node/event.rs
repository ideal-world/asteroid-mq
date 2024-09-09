use crate::{impl_codec, prelude::DecodeError, protocol::codec::CodecType};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::mem::size_of;

use super::{
    edge::EdgeResponse,
    NodeId, NodeConfig,
};

#[derive(Debug, Clone)]

pub struct NodeTrace {
    pub source: NodeId,
    pub hops: Vec<NodeId>,
}

impl_codec!(
    struct NodeTrace {
        source: NodeId,
        hops: Vec<NodeId>,
    }
);
impl NodeTrace {
    pub fn source(&self) -> NodeId {
        self.source
    }
    pub fn prev_node(&self) -> NodeId {
        self.hops.last().copied().unwrap_or(self.source)
    }
    pub fn trace_back(&self) -> impl Iterator<Item = &'_ NodeId> {
        self.hops.iter().rev().chain(std::iter::once(&self.source))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NodeKind {
    Cluster = 0,
    Edge = 1,
}

impl NodeKind {
    pub fn is_cluster(self) -> bool {
        matches!(self, Self::Cluster)
    }
    pub fn is_edge(self) -> bool {
        matches!(self, Self::Edge)
    }
}

impl_codec!(
    enum NodeKind {
        Cluster = 0,
        Edge = 1,
    }
);

impl From<u8> for NodeKind {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeKind::Cluster,
            1 => NodeKind::Edge,
            _ => NodeKind::Edge,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct N2nPacketId {
    pub bytes: [u8; 16],
}

impl std::fmt::Debug for N2nPacketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("N2NEventId")
            .field(&crate::util::hex(&self.bytes))
            .finish()
    }
}

impl N2nPacketId {
    pub fn new_snowflake() -> Self {
        thread_local! {
            static COUNTER: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        }
        let timestamp = crate::util::timestamp_sec();
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

#[derive(Debug, Clone)]
pub struct N2nPacket {
    pub header: N2nPacketHeader,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct N2nPacketHeader {
    pub id: N2nPacketId,
    pub kind: N2NPayloadKind,
    pub payload_size: u32,
}

impl_codec!(
    struct N2nPacketHeader {
        id: N2nPacketId,
        kind: N2NPayloadKind,
        payload_size: u32,
    }
);



impl N2nPacket {
    pub fn kind(&self) -> N2NPayloadKind {
        self.header.kind
    }
    pub fn id(&self) -> N2nPacketId {
        self.header.id
    }
    pub fn to_binary(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(size_of::<N2nPacketHeader>() + self.header.payload_size as usize);
        self.header.encode(&mut buf);
        buf.extend_from_slice(self.payload.as_ref());
        buf.freeze()
    }
    pub fn from_binary(data: Bytes) -> Result<Self, DecodeError> {
        let (header, payload) = N2nPacketHeader::decode(data)?;
        Ok(Self { header, payload })
    }
    pub fn auth(evt: N2nAuth) -> Self {
        let mut payload_buf = BytesMut::with_capacity(size_of::<N2nAuth>());
        evt.encode(&mut payload_buf);
        Self {
            header: N2nPacketHeader {
                id: N2nPacketId::new_snowflake(),
                kind: N2NPayloadKind::Auth,
                payload_size: payload_buf.len() as u32,
            },
            payload: payload_buf.into(),
        }
    }
    pub fn request_snapshot() -> Self {
        Self {
            header: N2nPacketHeader {
                id: N2nPacketId::new_snowflake(),
                kind: N2NPayloadKind::RequestSnapshot,
                payload_size: 0,
            },
            payload: Bytes::new(),
        }
    }


    pub fn event(evt: N2nEvent) -> Self {
        let payload = evt.encode_to_bytes();
        Self {
            header: N2nPacketHeader {
                id: N2nPacketId::new_snowflake(),
                kind: N2NPayloadKind::Event,
                payload_size: payload.len() as u32,
            },
            payload,
        }
    }

    pub fn unreachable(evt: N2NUnreachableEvent) -> Self {
        let mut payload_buf = BytesMut::with_capacity(size_of::<N2NUnreachableEvent>());
        evt.encode(&mut payload_buf);
        Self {
            header: N2nPacketHeader {
                id: N2nPacketId::new_snowflake(),
                kind: N2NPayloadKind::Unreachable,
                payload_size: payload_buf.len() as u32,
            },
            payload: payload_buf.into(),
        }
    }

    pub fn edge_response(resp: EdgeResponse) -> Self {
        let payload = resp.encode_to_bytes();
        Self {
            header: N2nPacketHeader {
                id: N2nPacketId::new_snowflake(),
                kind: N2NPayloadKind::EdgeResponse,
                payload_size: payload.len() as u32,
            },
            payload,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum N2NPayloadKind {
    Auth = 0x00,
    Event = 0x01,
    Heartbeat = 0x02,
    RequestVote = 0x10,
    Vote = 0x11,
    LogAppend = 0x20,
    LogReplicate = 0x21,
    LogAck = 0x22,
    LogCommit = 0x23,
    Snapshot = 0x24,
    RequestSnapshot = 0x25,
    EdgeRequest = 0x30,
    EdgeResponse = 0x31,
    EdgeMessage = 0x32,
    Unreachable = 0x80,
    Unknown = 0xf0,
}

impl_codec!(
    enum N2NPayloadKind {
        Auth = 0x00,
        Event = 0x01,
        Heartbeat = 0x02,
        RequestVote = 0x10,
        Vote = 0x11,
        LogAppend = 0x20,
        LogReplicate = 0x21,
        LogAck = 0x22,
        LogCommit = 0x23,
        Snapshot = 0x24,
        RequestSnapshot = 0x25,
        EdgeRequest = 0x30,
        EdgeResponse = 0x31,
        EdgeMessage = 0x32,
        Unreachable = 0x80,
        Unknown = 0xf0,
    }
);

impl From<u8> for N2NPayloadKind {
    fn from(value: u8) -> Self {
        match value {
            0x00 => N2NPayloadKind::Auth,
            0x01 => N2NPayloadKind::Event,
            0x02 => N2NPayloadKind::Heartbeat,
            0x10 => N2NPayloadKind::RequestVote,
            0x11 => N2NPayloadKind::Vote,
            0x20 => N2NPayloadKind::LogAppend,
            0x21 => N2NPayloadKind::LogReplicate,
            0x22 => N2NPayloadKind::LogAck,
            0x23 => N2NPayloadKind::LogCommit,
            0x24 => N2NPayloadKind::Snapshot,
            0x25 => N2NPayloadKind::RequestSnapshot,
            0x30 => N2NPayloadKind::EdgeRequest,
            0x31 => N2NPayloadKind::EdgeResponse,
            0x80 => N2NPayloadKind::Unreachable,

            _ => N2NPayloadKind::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct N2nAuth {
    pub info: NodeConfig,
    pub auth: NodeAuth,
}
impl_codec!(
    struct N2nAuth {
        info: NodeConfig,
        auth: NodeAuth,
    }
);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum EventKind {
    /// Hold Message: edge node ask cluster node to hold a message.
    DelegateMessage = 0x10,
    /// Cast Message: distribute message to clusters.
    CastMessage = 0x11,
    /// Ack: ack to the holder node.
    Ack = 0x12,
    /// Ack Report: The delegate cluster node report ack status to the edge node.
    AckReport = 0x13,
    /// Set State: set ack state
    SetState = 0x14,
    /// Load Queue: load messages into the topic queue.
    LoadTopic = 0x15,
    UnloadTopic = 0x16,
    /// En Online: report endpoint online.
    EpOnline = 0x20,
    /// En Offline: report endpoint offline.
    EpOffline = 0x21,
    /// En Sync: sync endpoint info.
    EpSync = 0x22,
    /// En Interest: set endpoint's interests.
    EpInterest = 0x23,
}

impl_codec!(
    enum EventKind {
        DelegateMessage = 0x10,
        CastMessage = 0x11,
        Ack = 0x12,
        AckReport = 0x13,
        SetState = 0x14,
        LoadTopic = 0x15,
        EpOnline = 0x20,
        EpOffline = 0x21,
        EpSync = 0x22,
    }
);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RaftData {
    
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RaftResponse {
    pub result: Result<(), ()>
}

#[derive(Debug, Clone)]
pub struct N2nEvent {
    pub to: NodeId,
    pub trace: NodeTrace,
    pub kind: EventKind,
    pub payload: Bytes,
}

impl_codec!(
    struct N2nEvent {
        to: NodeId,
        trace: NodeTrace,
        kind: EventKind,
        payload: Bytes,
    }
);
pub struct N2NUnreachableEvent {
    pub to: NodeId,
    pub unreachable_target: NodeId,
    pub trace: NodeTrace,
}

impl_codec!(
    struct N2NUnreachableEvent {
        to: NodeId,
        unreachable_target: NodeId,
        trace: NodeTrace,
    }
);

pub enum N2NEvent {
    Auth(N2nAuth),
    Message(N2nEvent),
    Unreachable(N2NUnreachableEvent),
}
#[derive(Debug, Clone, Default)]
pub struct NodeAuth {
}

impl_codec!(
    struct NodeAuth {}
);
