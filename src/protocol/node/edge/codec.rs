use std::{borrow::Cow, collections::HashMap, sync::Arc};

use serde::de::DeserializeOwned;

use super::EdgePayload;
pub mod bincode;
pub mod cbor;
pub mod json;
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CodecKind(pub(crate) u8);

impl std::fmt::Display for CodecKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02x}", self.0)
    }
}


impl CodecKind {
    pub const CBOR: Self = Self(0x00);
    pub const BINCODE: Self = Self(0x01);
    pub const JSON: Self = Self(0x40);
}
pub struct CodecError {
    reason: Cow<'static, str>,
}

impl CodecError {
    pub fn decode_error<E: std::fmt::Display>(e: E) -> Self {
        Self {
            reason: format!("decode error: {}", e).into(),
        }
    }
    pub fn unregistered_codec(codec: CodecKind) -> Self {
        Self {
            reason: format!("unregistered codec: {}", codec).into(),
        }
    }
}

pub trait Codec: Send + Sync + 'static {
    fn encode(&self, value: &EdgePayload) -> Vec<u8>;
    fn decode(&self, bytes: &[u8]) -> Result<EdgePayload, CodecError>;
}
pub struct CodecRegistry {
    registry: HashMap<CodecKind, Arc<dyn Codec>>,
}

impl std::fmt::Debug for CodecRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.registry.keys()).finish()
    }
}

impl CodecRegistry {
    pub fn new_empty() -> Self {
        Self {
            registry: HashMap::new(),
        }
    }
    pub fn new_preloaded() -> Self {
        let mut registry = Self::new_empty();
        registry.register(CodecKind::CBOR, cbor::Cbor);
        registry.register(CodecKind::BINCODE, bincode::Bincode);
        registry.register(CodecKind::JSON, json::Json);
        registry
    }
    pub fn register<C: Codec + 'static>(&mut self, kind: CodecKind, codec: C) {
        self.registry.insert(kind, Arc::new(codec));
    }
    pub fn encode(&self, kind: CodecKind, value: &EdgePayload) -> Result<Vec<u8>, CodecError> {
        self.registry
            .get(&kind)
            .ok_or_else(|| CodecError::unregistered_codec(kind))
            .map(|codec| codec.encode(value))
    }
    pub fn decode(&self, kind: CodecKind, bytes: &[u8]) -> Result<EdgePayload, CodecError> {
        self.registry
            .get(&kind)
            .ok_or_else(|| CodecError::unregistered_codec(kind))
            .and_then(|codec| codec.decode(bytes))
    }
}
