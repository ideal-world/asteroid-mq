use crate::EdgePayload;

use super::Codec;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub struct Cbor;

impl Codec for Cbor {
    fn decode(
        &self,
        mut bytes: &[u8],
    ) -> Result<EdgePayload, super::CodecError> {
        ciborium::from_reader(&mut bytes).map_err(super::CodecError::decode_error)
    }
    fn encode(&self, value: &EdgePayload) -> Result<Vec<u8>, super::CodecError> {
        let mut buffer = Vec::new();
        ciborium::into_writer(value, &mut buffer).map_err(super::CodecError::encode_error)?;
        Ok(buffer)
    }
    fn kind(&self) -> super::CodecKind {
        super::CodecKind::CBOR
    }
}
