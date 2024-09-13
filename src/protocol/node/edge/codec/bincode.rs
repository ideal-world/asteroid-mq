use super::Codec;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Bincode;

impl Codec for Bincode {
    fn decode(&self, bytes: &[u8]) -> Result<crate::protocol::node::edge::EdgePayload, super::CodecError> {
        bincode::deserialize(bytes).map_err(super::CodecError::decode_error)
    }
    fn encode(&self, value: &crate::protocol::node::edge::EdgePayload) -> Vec<u8> {
        bincode::serialize(value).expect("bincode encode failed")
    }
}