use crate::{impl_codec, protocol::node::codec::CodecType};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::mem::size_of;

use super::{NodeId, NodeInfo};

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct NodeTrace {
    pub source: NodeId,
    pub hops: Vec<NodeId>,
}

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
    pub fn gen() -> Self {
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

#[derive(Debug)]
pub struct N2nPacket {
    pub header: N2nPacketHeader,
    pub payload: Bytes,
}

#[derive(Debug)]
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

    pub fn auth(evt: N2nAuth) -> Self {
        let mut payload_buf = BytesMut::with_capacity(size_of::<N2nAuth>());
        evt.encode(&mut payload_buf);
        Self {
            header: N2nPacketHeader {
                id: N2nPacketId::gen(),
                kind: N2NPayloadKind::Auth,
                payload_size: payload_buf.len() as u32,
            },
            payload: payload_buf.into(),
        }
    }
    pub fn event(evt: N2nEvent) -> Self {
        let payload = evt.encode_to_bytes();
        Self {
            header: N2nPacketHeader {
                id: N2nPacketId::gen(),
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
                id: N2nPacketId::gen(),
                kind: N2NPayloadKind::Unreachable,
                payload_size: payload_buf.len() as u32,
            },
            payload: payload_buf.into(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum N2NPayloadKind {
    Auth = 0,
    Event = 1,
    Unreachable = 2,
    Unknown = 255,
}

impl_codec!(
    enum N2NPayloadKind {
        Auth = 0,
        Event = 1,
        Unreachable = 2,
        Unknown = 255,
    }
);

impl From<u8> for N2NPayloadKind {
    fn from(value: u8) -> Self {
        match value {
            0 => N2NPayloadKind::Auth,
            1 => N2NPayloadKind::Event,
            2 => N2NPayloadKind::Unreachable,
            _ => N2NPayloadKind::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct N2nAuth {
    pub info: NodeInfo,
    pub auth: N2NAuth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum N2nEventKind {
    /// Hold Message: edge node ask cluster node to hold a message.
    HoldMessage = 0x10,
    /// Cast Message: distribute message to clusters.
    CastMessage = 0x11,
    /// Ack: ack to the holder node.
    Ack = 0x12,
    /// Ack Report: The delegate cluster node report ack status to the edge node.
    AckReport = 0x13,
    /// En Online: report endpoint online.
    EpOnline = 0x20,
    /// En Online: report endpoint offline.
    EpOffline = 0x21,
    /// En Online: sync endpoint info.
    EpSync = 0x22,
}

impl_codec!(
    enum N2nEventKind {
        HoldMessage = 0x10,
        CastMessage = 0x11,
        Ack = 0x12,
        AckReport = 0x13,
        EpOnline = 0x20,
        EpOffline = 0x21,
        EpSync = 0x22,
    }
);

#[derive(Debug, Clone)]
pub struct N2nEvent {
    pub to: NodeId,
    pub trace: NodeTrace,
    pub kind: N2nEventKind,
    pub payload: Bytes,
}

impl_codec!(
    struct N2nEvent {
        to: NodeId,
        trace: NodeTrace,
        kind: N2nEventKind,
        payload: Bytes,
    }
);
pub struct N2NUnreachableEvent {
    pub to: NodeId,
    pub unreachable_target: NodeId,
    pub trace: NodeTrace,
}

pub enum N2NEvent {
    Auth(N2nAuth),
    Message(N2nEvent),
    Unreachable(N2NUnreachableEvent),
}
#[derive(Debug, Clone, Default)]
pub struct N2NAuth {}
