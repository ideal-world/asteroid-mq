#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct CodecKind(pub u8);

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
