use bytes::Bytes;

use super::codec::CodecKind;

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
    pub fn new(codec: CodecKind, payload: impl Into<Bytes>) -> Self {
        let id = EdgePacketId::new_snowflake();
        let header = EdgePacketHeader { id, codec };
        Self {
            header,
            payload: payload.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Auth {
    pub payload: Bytes,
}

impl Auth {
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
        }
    }
}
