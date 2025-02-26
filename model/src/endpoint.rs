use serde::{Deserialize, Serialize};

use typeshare::typeshare;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[typeshare(serialized_as = "String")]
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
pub struct EndpointAddr {
    pub bytes: [u8; 16],
}

impl From<[u8; 16]> for EndpointAddr {
    fn from(bytes: [u8; 16]) -> Self {
        Self { bytes }
    }
}
impl From<EndpointAddr> for [u8; 16] {
    fn from(val: EndpointAddr) -> Self {
        val.bytes
    }
}

impl Serialize for EndpointAddr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            use base64::Engine;
            serializer.serialize_str(&base64::engine::general_purpose::URL_SAFE.encode(self.bytes))
        } else {
            <[u8; 16]>::serialize(&self.bytes, serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EndpointAddr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            use base64::Engine;
            use serde::de::Error;
            let s = String::deserialize(deserializer)?;
            let bytes = base64::engine::general_purpose::URL_SAFE
                .decode(s.as_bytes())
                .map_err(D::Error::custom)?;
            if bytes.len() != 16 {
                return Err(D::Error::custom("invalid length"));
            }
            let mut addr = [0; 16];
            addr.copy_from_slice(&bytes);
            Ok(Self { bytes: addr })
        } else {
            let bytes = <[u8; 16]>::deserialize(deserializer)?;
            Ok(Self { bytes })
        }
    }
}

impl std::fmt::Debug for EndpointAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("EndpointAddr")
            .field(&crate::util::dashed(&[
                crate::util::hex(&self.bytes[0..8]),
                crate::util::hex(&self.bytes[8..12]),
                crate::util::hex(&self.bytes[12..16]),
            ]))
            .finish()
    }
}

impl EndpointAddr {
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
    pub fn hash64(&self) -> u64 {
        use std::hash::{DefaultHasher, Hasher};
        let mut hasher = DefaultHasher::new();
        Hasher::write(&mut hasher, &self.bytes);
        Hasher::finish(&hasher)
    }
}
