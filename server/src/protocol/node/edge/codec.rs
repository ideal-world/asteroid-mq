use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::EdgePayload;
pub(crate) mod bincode;
pub use bincode::*;
#[cfg(feature = "cbor")]
pub(crate) mod cbor;
#[cfg(feature = "cbor")]
pub use cbor::*;
pub(crate) mod json;
pub use asteroid_mq_model::CodecKind;
pub use json::*;

#[derive(Debug)]
pub struct CodecError {
    reason: Cow<'static, str>,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "codec error: {}", self.reason)
    }
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
        #[cfg(feature = "cbor")]
        registry.register(CodecKind::CBOR, cbor::Cbor);
        registry.register(CodecKind::BINCODE, bincode::Bincode);
        registry.register(CodecKind::JSON, json::Json);
        registry
    }
    pub fn register<C: Codec + 'static>(&mut self, kind: CodecKind, codec: C) {
        self.registry.insert(kind, Arc::new(codec));
    }
    pub fn get(&self, kind: &CodecKind) -> Option<Arc<dyn Codec>> {
        self.registry.get(kind).cloned()
    }
    pub fn pick_preferred_codec(&self, supported: &HashSet<CodecKind>) -> Option<CodecKind> {
        supported
            .iter()
            .find(|kind| self.registry.contains_key(kind))
            .copied()
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
