use std::borrow::Cow;

use bytes::Bytes;

use crate::{
    EdgeError, EdgePayload, Interest, MaybeBase64Bytes, MessageDurableConfig, Subject, TopicCode,
};

use super::Codec;
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Default)]
pub struct Bincode;

pub const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

impl Codec for Bincode {
    fn decode(&self, bytes: &[u8]) -> Result<EdgePayload, super::CodecError> {
        Ok(bincode::decode_from_slice(bytes, BINCODE_CONFIG)
            .map_err(super::CodecError::decode_error)?
            .0)
    }
    fn encode(&self, value: &EdgePayload) -> Result<Vec<u8>, super::CodecError> {
        bincode::encode_to_vec(value, BINCODE_CONFIG).map_err(super::CodecError::encode_error)
    }
    fn kind(&self) -> super::CodecKind {
        super::CodecKind::BINCODE
    }
}

impl bincode::Encode for EdgeError {
    fn encode<W: bincode::enc::Encoder>(
        &self,
        writer: &mut W,
    ) -> Result<(), bincode::error::EncodeError> {
        self.context.encode(writer)?;
        self.message.encode(writer)?;
        self.kind.encode(writer)
    }
}

impl<'de> bincode::BorrowDecode<'de> for EdgeError {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            context: Cow::Owned(String::borrow_decode(decoder)?),
            message: <Option<String>>::borrow_decode(decoder)?.map(Cow::Owned),
            kind: bincode::BorrowDecode::borrow_decode(decoder)?,
        })
    }
}

impl bincode::Decode for EdgeError {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            context: bincode::Decode::decode(decoder)?,
            message: bincode::Decode::decode(decoder)?,
            kind: bincode::Decode::decode(decoder)?,
        })
    }
}

macro_rules! derive_bytes_wrapper {
    ($T: ident) => {
        impl bincode::Encode for $T {
            fn encode<W: bincode::enc::Encoder>(
                &self,
                writer: &mut W,
            ) -> Result<(), bincode::error::EncodeError> {
                self.0.encode(writer)
            }
        }
        impl bincode::Decode for $T {
            fn decode<D: bincode::de::Decoder>(
                decoder: &mut D,
            ) -> Result<Self, bincode::error::DecodeError> {
                Ok(Self(Bytes::from(<Vec<u8>>::decode(decoder)?)))
            }
        }
        impl<'de> bincode::BorrowDecode<'de> for $T {
            fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
                decoder: &mut D,
            ) -> Result<Self, bincode::error::DecodeError> {
                Ok(Self(Bytes::from(<Vec<u8>>::borrow_decode(decoder)?)))
            }
        }
    };
}
derive_bytes_wrapper!(MaybeBase64Bytes);
derive_bytes_wrapper!(TopicCode);
derive_bytes_wrapper!(Interest);
derive_bytes_wrapper!(Subject);

impl bincode::Encode for MessageDurableConfig {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.expire.timestamp().encode(encoder)?;
        self.max_receiver.encode(encoder)?;
        Ok(())
    }
}

impl bincode::Decode for MessageDurableConfig {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            expire: chrono::DateTime::<chrono::Utc>::from_timestamp(i64::decode(decoder)?, 0)
                .ok_or(bincode::error::DecodeError::Other("invalid timestamp"))?,
            max_receiver: Option::<u32>::decode(decoder)?,
        })
    }
}

impl<'de> bincode::BorrowDecode<'de> for MessageDurableConfig {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self {
            expire: chrono::DateTime::<chrono::Utc>::from_timestamp(
                i64::borrow_decode(decoder)?,
                0,
            )
            .ok_or(bincode::error::DecodeError::Other("invalid timestamp"))?,
            max_receiver: Option::<u32>::borrow_decode(decoder)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{EdgePayload, Interest, TopicCode};

    #[test]
    fn test_bincode() {
        let codec = Bincode;
        let ep_online = EdgePayload::Request(crate::EdgeRequest {
            seq_id: 1,
            request: crate::EdgeRequestEnum::EndpointOnline(crate::EdgeEndpointOnline {
                topic_code: TopicCode::const_new("test topic"),
                interests: vec![Interest::new("test/interest")],
            }),
        });

        let encoded = codec.encode(&ep_online).expect("encode failed");
        println!("{:?}", Bytes::copy_from_slice(&encoded));

        let decoded = codec.decode(&encoded).unwrap();
        println!("{:?}", decoded);

        let re_encoded = codec.encode(&decoded).expect("encode failed");
        assert_eq!(encoded, re_encoded);
    }
}
