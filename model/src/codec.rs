use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::Display,
    str::FromStr,
    sync::Arc,
};
#[cfg(feature = "bincode")]
pub mod bincode;
#[cfg(feature = "cbor")]
pub mod cbor;
pub mod json;

use crate::EdgePayload;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CodecKind(pub u8);

impl CodecKind {
    pub const CBOR: Self = Self(0x00);
    pub const BINCODE: Self = Self(0x01);
    pub const JSON: Self = Self(0x40);
}

impl FromStr for CodecKind {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cbor" => Ok(Self::CBOR),
            "bincode" => Ok(Self::BINCODE),
            "json" => Ok(Self::JSON),
            _ => u8::from_str_radix(s, 16)
                .map(Self)
                .map_err(|_| "invalid codec kind"),
        }
    }
}

impl Display for CodecKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            CodecKind::CBOR => write!(f, "CBOR"),
            CodecKind::BINCODE => write!(f, "Bincode"),
            CodecKind::JSON => write!(f, "JSON"),
            _ => write!(f, "Unknown({:02x})", self.0),
        }
    }
}

#[derive(Debug)]
pub struct CodecError {
    reason: Cow<'static, str>,
}


impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "codec error: {}", self.reason)
    }
}
impl std::error::Error for CodecError {}

impl CodecError {
    pub fn decode_error<E: std::fmt::Display>(e: E) -> Self {
        Self {
            reason: format!("decode error: {}", e).into(),
        }
    }
    pub fn encode_error<E: std::fmt::Display>(e: E) -> Self {
        Self {
            reason: format!("encode error: {}", e).into(),
        }
    }
    pub fn unregistered_codec(codec: CodecKind) -> Self {
        Self {
            reason: format!("unregistered codec: {}", codec).into(),
        }
    }
}

pub trait Codec: Send + Sync + 'static {
    fn encode(&self, value: &EdgePayload) -> Result<Vec<u8>, CodecError>;
    fn decode(&self, bytes: &[u8]) -> Result<EdgePayload, CodecError>;
    fn kind(&self) -> CodecKind;
}
#[derive(Clone)]
pub struct DynCodec {
    codec: Arc<dyn Codec>,
}

impl std::fmt::Debug for DynCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynCodec")
            .field("kind", &self.codec.kind())
            .finish()
    }
}

impl Codec for DynCodec {
    fn encode(&self, value: &EdgePayload) -> Result<Vec<u8>, CodecError> {
        self.codec.encode(value)
    }
    fn decode(&self, bytes: &[u8]) -> Result<EdgePayload, CodecError> {
        self.codec.decode(bytes)
    }
    fn kind(&self) -> CodecKind {
        self.codec.kind()
    }
}

impl DynCodec {
    pub fn new<C: Codec>(codec: C) -> Self {
        Self {
            codec: Arc::new(codec),
        }
    }
    pub fn form_kind(kind: CodecKind) -> Option<Self> {
        match kind {
            #[cfg(feature = "cbor")]
            CodecKind::CBOR => Some(Self::new(cbor::Cbor)),
            CodecKind::BINCODE => Some(Self::new(bincode::Bincode)),
            CodecKind::JSON => Some(Self::new(json::Json)),
            _ => None,
        }
    }
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
            .and_then(|codec| codec.encode(value))
    }
    pub fn decode(&self, kind: CodecKind, bytes: &[u8]) -> Result<EdgePayload, CodecError> {
        self.registry
            .get(&kind)
            .ok_or_else(|| CodecError::unregistered_codec(kind))
            .and_then(|codec| codec.decode(bytes))
    }
}
