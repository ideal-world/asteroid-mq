use super::Codec;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Cbor;

impl Codec for Cbor {
    fn decode(&self, mut bytes: &[u8]) -> Result<crate::protocol::node::edge::EdgePayload, super::CodecError> {
        ciborium::from_reader(&mut bytes).map_err(super::CodecError::decode_error)
    }
    fn encode(&self, value: &crate::protocol::node::edge::EdgePayload) -> Vec<u8> {
        let mut buffer = Vec::new();
        ciborium::into_writer(value, &mut buffer).expect("cbor encode failed");
        buffer
    }
}