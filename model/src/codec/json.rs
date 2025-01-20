use crate::EdgePayload;

use super::Codec;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub struct Json;

impl Codec for Json {
    fn decode(&self, bytes: &[u8]) -> Result<EdgePayload, super::CodecError> {
        serde_json::from_slice(bytes).map_err(super::CodecError::decode_error)
    }
    fn encode(&self, value: &EdgePayload) -> Result<Vec<u8>, super::CodecError> {
        serde_json::to_vec(value).map_err(super::CodecError::encode_error)
    }
    fn kind(&self) -> super::CodecKind {
        super::CodecKind::JSON
    }
}
