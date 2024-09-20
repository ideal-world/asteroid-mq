use super::Codec;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Json;

impl Codec for Json {
    fn decode(
        &self,
        bytes: &[u8],
    ) -> Result<crate::protocol::node::edge::EdgePayload, super::CodecError> {
        serde_json::from_slice(bytes).map_err(super::CodecError::decode_error)
    }
    fn encode(&self, value: &crate::protocol::node::edge::EdgePayload) -> Vec<u8> {
        serde_json::to_vec(value).expect("json encode failed")
    }
}
