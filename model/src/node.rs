use std::sync;

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

#[derive(Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[typeshare(serialized_as = "string")]
pub struct NodeId {
    pub bytes: [u8; 16],
}

impl Serialize for NodeId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_base64())
        } else {
            <[u8; 16]>::serialize(&self.bytes, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        if deserializer.is_human_readable() {
            let s = <&'de str>::deserialize(deserializer)?;
            NodeId::from_base64(s).map_err(D::Error::custom)
        } else {
            let bytes = <[u8; 16]>::deserialize(deserializer)?;
            Ok(NodeId { bytes })
        }
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        Self::new_indexed(id)
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NodeId")
            .field(&crate::util::dashed(&[
                crate::util::hex(&self.bytes[0..1]),
                crate::util::hex(&self.bytes[1..9]),
                crate::util::hex(&self.bytes[9..10]),
                crate::util::hex(&self.bytes[10..16]),
            ]))
            .finish()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}-{}",
            crate::util::hex(&self.bytes[0..1]),
            crate::util::hex(&self.bytes[1..9]),
            crate::util::hex(&self.bytes[9..10]),
            crate::util::hex(&self.bytes[10..16]),
        )
    }
}

impl NodeId {
    pub const KIND_INDEXED: u8 = 0x00;
    pub const KIND_SHA256: u8 = 0x01;
    pub const KIND_SNOWFLAKE: u8 = 0x02;
    pub const fn new_indexed(id: u64) -> Self {
        let mut bytes = [0; 16];
        bytes[0] = Self::KIND_INDEXED;
        let index_part = id.to_be_bytes();
        bytes[1] = index_part[0];
        bytes[2] = index_part[1];
        bytes[3] = index_part[2];
        bytes[4] = index_part[3];
        bytes[5] = index_part[4];
        bytes[6] = index_part[5];
        bytes[7] = index_part[6];
        bytes[8] = index_part[7];
        NodeId { bytes }
    }
    pub fn sha256(bytes: &[u8]) -> Self {
        let dg = <sha2::Sha256 as sha2::Digest>::digest(bytes);
        let mut bytes = [0; 16];
        bytes[0] = Self::KIND_SHA256;
        bytes[1..16].copy_from_slice(&dg.as_slice()[0..15]);
        NodeId { bytes }
    }
    pub fn snowflake() -> NodeId {
        static INSTANCE_ID: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
        let dg = crate::util::executor_digest();
        let mut bytes = [0; 16];
        bytes[0] = Self::KIND_SNOWFLAKE;
        bytes[1..9].copy_from_slice(&dg.to_be_bytes());
        bytes[9..10].copy_from_slice(
            &INSTANCE_ID
                .fetch_add(1, sync::atomic::Ordering::SeqCst)
                .to_be_bytes(),
        );
        bytes[10..16].copy_from_slice(&(crate::util::timestamp_sec()).to_be_bytes()[2..8]);
        NodeId { bytes }
    }
    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::URL_SAFE.encode(self.bytes)
    }
    pub fn from_base64(s: &str) -> Result<Self, base64::DecodeError> {
        use base64::Engine;
        let id = base64::engine::general_purpose::URL_SAFE.decode(s)?;
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&id);
        Ok(Self { bytes })
    }
}
