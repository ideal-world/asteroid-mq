use crate::{impl_codec, prelude::{DecodeError, NodeId}, protocol::codec::CodecType};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::mem::size_of;

use super::{codec::CodecKind, EdgeResponse};



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
pub struct EdgePacketId {
    pub bytes: [u8; 16],
}



impl std::fmt::Debug for EdgePacketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("N2NEventId")
            .field(&crate::util::hex(&self.bytes))
            .finish()
    }
}

impl EdgePacketId {
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
pub struct EdgePacket {
    pub header: EdgePacketHeader,
    pub payload: Bytes,
}

#[derive(Debug, Clone)]
pub struct EdgePacketHeader {
    pub id: EdgePacketId,
    pub codec: CodecKind,
}

impl EdgePacket {
    pub fn codec(&self) -> CodecKind {
        self.header.codec
    }
    pub fn id(&self) -> EdgePacketId {
        self.header.id
    }
    pub fn auth(auth: Auth) -> Self {
        let header = EdgePacketHeader {
            id: EdgePacketId::new_snowflake(),
            codec: CodecKind::CBOR,
        };
        let payload = EdgePayload::Auth(EdgeAuth {
            id: NodeId::new_snowflake(),
            auth,
        });
        Self { header, payload: payload.encode() }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgePayloadKind {
    Auth = 0x00,
    Request = 0x30,
    Response = 0x31,
    Push = 0x32,
}

impl_codec!(
    enum EdgePayloadKind {
        Auth = 0x00,
        Request = 0x30,
        Response = 0x31,
        Push = 0x32,

    }
);

impl From<u8> for EdgePayloadKind {
    fn from(value: u8) -> Self {
        match value {
            0x00 => EdgePayloadKind::Auth,
            0x30 => EdgePayloadKind::Request,
            0x31 => EdgePayloadKind::Response,
            _ => EdgePayloadKind::Push,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EdgeAuth {
    pub id: NodeId,
    pub auth: Auth,
}
impl_codec!(
    struct EdgeAuth {
        id: NodeId,
        auth: Auth,
    }
);

#[derive(Debug, Clone, Default)]
pub struct Auth {
}

impl_codec!(
    struct Auth {}
);
