use std::{borrow::Cow, mem::size_of};

use bytes::{Buf as _, BufMut, Bytes, BytesMut};

use super::{N2NAuth, N2NAuthEvent, N2NMessageEvent, NodeId, NodeInfo, NodeTrace};
#[derive(Debug)]
pub struct NNDecodeError {
    pub parsing_type: &'static str,
    pub context: Cow<'static, str>,
}

impl NNDecodeError {
    pub fn new<T>(context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            parsing_type: std::any::type_name::<T>(),
            context: context.into(),
        }
    }
}

pub trait NNCodecType: Sized {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError>;
    fn encode(&self, buf: &mut BytesMut);
}

impl NNCodecType for () {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        Ok(((), bytes))
    }

    fn encode(&self, _buf: &mut BytesMut) {}
}
impl NNCodecType for Bytes {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        Ok((bytes, Bytes::new()))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(self);
    }
}
impl NNCodecType for u8 {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        if bytes.len() < size_of::<u8>() {
            return Err(NNDecodeError::new::<Self>("too short payload: expect u8"));
        }
        Ok((bytes.get_u8(), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(*self);
    }
}
impl NNCodecType for u32 {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        if bytes.len() < size_of::<u32>() {
            return Err(NNDecodeError::new::<Self>("too short payload: expect u32"));
        }
        Ok((bytes.get_u32(), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(*self);
    }
}

impl<T: NNCodecType> NNCodecType for Vec<T> {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        let (size, mut bytes) = u32::decode(bytes)?;
        let mut vec = Vec::new();
        for _ in 0..size {
            let (item, rest) = T::decode(bytes)?;
            vec.push(item);
            bytes = rest;
        }
        Ok((vec, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        (self.len() as u32).encode(buf);
        for item in self {
            item.encode(buf);
        }
    }
}

impl NNCodecType for NodeId {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        if bytes.len() < size_of::<NodeId>() {
            return Err(NNDecodeError::new::<Self>(
                "too short payload: expect node id",
            ));
        }
        Ok((
            NodeId {
                bytes: bytes
                    .split_to(size_of::<NodeId>())
                    .as_ref()
                    .try_into()
                    .expect("have enough bytes"),
            },
            bytes,
        ))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.bytes);
    }
}

impl NNCodecType for NodeTrace {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        let (source, bytes) = NodeId::decode(bytes)?;
        let (hops, bytes) = Vec::<NodeId>::decode(bytes)?;
        Ok((NodeTrace { source, hops }, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.source.encode(buf);
        self.hops.encode(buf);
    }
}

impl NNCodecType for N2NMessageEvent {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        let (to, bytes) = NodeId::decode(bytes)?;
        let (trace, payload) = NodeTrace::decode(bytes)?;
        Ok((N2NMessageEvent { to, trace, payload }, Bytes::new()))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.to.encode(buf);
        self.trace.encode(buf);
        self.payload.encode(buf);
    }
}

impl NNCodecType for N2NAuth {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        Ok((N2NAuth {}, bytes))
    }

    fn encode(&self, _buf: &mut BytesMut) {}
}

impl NNCodecType for NodeInfo {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        let (id, bytes) = NodeId::decode(bytes)?;
        let (kind, bytes) = u8::decode(bytes)?;

        Ok((
            NodeInfo {
                id,
                kind: kind.into(),
            },
            bytes,
        ))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.id.encode(buf);
        (self.kind as u8).encode(buf);
    }
}

impl NNCodecType for N2NAuthEvent {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), NNDecodeError> {
        let (info, bytes) = NodeInfo::decode(bytes)?;
        let (auth, bytes) = N2NAuth::decode(bytes)?;
        Ok((N2NAuthEvent { info, auth }, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.info.encode(buf);
        self.auth.encode(buf);
    }
}
