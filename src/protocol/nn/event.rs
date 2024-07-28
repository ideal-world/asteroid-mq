use std::mem::size_of;
use crate::protocol::nn::codec::NNCodecType;
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

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
pub struct N2NEventId {
    pub bytes: [u8; 16],
}

impl std::fmt::Debug for N2NEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("N2NEventId")
            .field(&crate::util::hex(&self.bytes))
            .finish()
    }
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
    pub fn id(&self) -> N2NEventId {
        self.header.id
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

    pub fn unreachable(evt: N2NUnreachableEvent) -> Self {
        let mut payload_buf = BytesMut::with_capacity(size_of::<N2NUnreachableEvent>());
        evt.encode(&mut payload_buf);
        Self {
            header: N2NEventPacketHeader {
                id: N2NEventId::gen(),
                kind: N2NEventKind::Unreachable,
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
    pub unreachable_target: NodeId,
    pub trace: NodeTrace,
}

pub enum N2NEvent {
    Auth(N2NAuthEvent),
    Message(N2NMessageEvent),
    Unreachable(N2NUnreachableEvent),
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct N2NAuth {}
