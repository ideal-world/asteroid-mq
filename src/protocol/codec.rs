
use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU32;
use std::{borrow::Cow, mem::size_of};

use bytes::{Buf as _, BufMut, Bytes, BytesMut};
use chrono::{TimeZone, Utc};

use crate::protocol::endpoint::EndpointAddr;

use crate::protocol::node::{event::N2nPacketId, NodeId};
#[macro_export]
macro_rules! impl_codec {
    ( struct $ImplTy: ident { $($field:ident: $Type:ty),* $(,)? }) => {
        #[allow(unused_variables)]
        impl $crate::protocol::codec::CodecType for $ImplTy {
            fn decode(bytes: bytes::Bytes) -> Result<(Self, bytes::Bytes), $crate::protocol::codec::DecodeError> {
                $(
                    let ($field, bytes) = <$Type>::decode(bytes)?;
                )*
                let result = Self { $($field),* };
                // enable this to debug decoding
                // tracing::debug!("decoded {:?}: {result:?}", stringify!($ImplTy));
                Ok((result, bytes))
            }

            fn encode(&self, buf: &mut bytes::BytesMut) {
                $(self.$field.encode(buf);)*
            }
        }
    };
    (enum $ImplTy: ident { $($Variant:ident = $val: literal),* $(,)? }) => {
        impl $crate::protocol::codec::CodecType for $ImplTy {
            fn decode(bytes: bytes::Bytes) -> std::result::Result<(Self, bytes::Bytes), $crate::protocol::codec::DecodeError> {
                let (val, bytes) = u8::decode(bytes)?;
                let val = match val {
                    $($val => <$ImplTy>::$Variant,)*
                    _ => return Err($crate::protocol::codec::DecodeError::new::<Self>(format!("invalid kind {val:02x}"))),
                };
                Ok((val, bytes))
            }

            fn encode(&self, buf: &mut bytes::BytesMut) {
                use $crate::bytes::BufMut;
                buf.put_u8(*self as u8);
            }
        }
    };
    (struct $ImplTy: ident ($ProxyTy: ty)) => {
        impl $crate::protocol::codec::CodecType for $ImplTy {
            fn decode(bytes: bytes::Bytes) -> std::result::Result<(Self, bytes::Bytes), $crate::protocol::codec::DecodeError> {
                let (inner, bytes) = <$ProxyTy>::decode(bytes)?;
                Ok(($ImplTy(inner), bytes))
            }

            fn encode(&self, buf: &mut bytes::BytesMut) {
                self.0.encode(buf);
            }
        }
    };


}

#[derive(Debug)]
pub struct DecodeError {
    pub parsing_type: &'static str,
    pub context: Cow<'static, str>,
}

impl DecodeError {
    pub fn new<T>(context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            parsing_type: std::any::type_name::<T>(),
            context: context.into(),
        }
    }
}

impl std::error::Error for DecodeError {}
impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{parsing_type}: {context}", parsing_type = self.parsing_type, context = self.context)
    }
}
pub trait CodecType: Sized {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError>;
    fn encode(&self, buf: &mut BytesMut);
    fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(std::mem::size_of::<Self>() * 2);
        self.encode(&mut buf);
        buf.freeze()
    }
    fn decode_from_bytes(bytes: Bytes) -> Result<Self, DecodeError> {
        let (value, rest) = Self::decode(bytes)?;
        if !rest.is_empty() {
            return Err(DecodeError::new::<Self>("unexpected trailing bytes"));
        }
        Ok(value)
    }
}


/*******************************************************************************************
                                    CODEC FOR PRIMITIVE TYPES
*******************************************************************************************/

impl CodecType for () {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        Ok(((), bytes))
    }

    fn encode(&self, _buf: &mut BytesMut) {}
}

impl CodecType for Bytes {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (size, bytes) = u32::decode(bytes)?;
        if bytes.len() < size as usize {
            return Err(DecodeError::new::<Self>(format!(
                "too short payload: expect {size} bytes, rest bytes: {bytes:?}"
            )));
        }
        Ok((bytes.slice(0..size as usize), bytes.slice(size as usize..)))
    }

    fn encode(&self, buf: &mut BytesMut) {
        (self.len() as u32).encode(buf);
        buf.put_slice(self);
    }
}

impl CodecType for bool {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<bool>() {
            return Err(DecodeError::new::<Self>("too short payload: expect u8"));
        }
        Ok((bytes.get_u8() != 0, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(*self as u8);
    }
}

impl CodecType for u8 {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<u8>() {
            return Err(DecodeError::new::<Self>("too short payload: expect u8"));
        }
        Ok((bytes.get_u8(), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(*self);
    }
}
impl CodecType for u32 {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<u32>() {
            return Err(DecodeError::new::<Self>("too short payload: expect u32"));
        }
        Ok((bytes.get_u32(), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(*self);
    }
}
impl CodecType for u64 {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<u64>() {
            return Err(DecodeError::new::<Self>("too short payload: expect u32"));
        }
        Ok((bytes.get_u64(), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(*self);
    }
}
impl CodecType for i64 {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<u64>() {
            return Err(DecodeError::new::<Self>("too short payload: expect u32"));
        }
        Ok((bytes.get_i64(), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i64(*self);
    }
}

impl CodecType for NonZeroU32 {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (value, bytes) = u32::decode(bytes)?;
        let value = NonZeroU32::new(value).ok_or_else(|| DecodeError::new::<Self>("zero value"))?;
        Ok((value, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.get().encode(buf);
    }
}
impl<T> CodecType for Option<T>
where
    T: CodecType,
{
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (has_value, bytes) = u8::decode(bytes)?;
        if has_value == 0 {
            return Ok((None, bytes));
        }
        let (value, bytes) = T::decode(bytes)?;
        Ok((Some(value), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        if let Some(value) = self {
            1u8.encode(buf);
            value.encode(buf);
        } else {
            0u8.encode(buf);
        }
    }
}

impl<T: CodecType> CodecType for Vec<T> {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
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

impl<K: CodecType + std::hash::Hash + Eq, V: CodecType> CodecType for HashMap<K, V> {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (size, mut bytes) = u32::decode(bytes)?;
        let mut map = HashMap::new();
        for _ in 0..size {
            let (key, rest) = K::decode(bytes)?;
            let (value, rest) = V::decode(rest)?;
            map.insert(key, value);
            bytes = rest;
        }
        Ok((map, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        (self.len() as u32).encode(buf);
        for (key, value) in self.iter() {
            key.encode(buf);
            value.encode(buf);
        }
    }
}

impl<K: CodecType + std::hash::Hash + Eq + Ord, V: CodecType> CodecType for BTreeMap<K, V> {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (size, mut bytes) = u32::decode(bytes)?;
        let mut map = BTreeMap::new();
        for _ in 0..size {
            let (key, rest) = K::decode(bytes)?;
            let (value, rest) = V::decode(rest)?;
            map.insert(key, value);
            bytes = rest;
        }
        Ok((map, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        (self.len() as u32).encode(buf);
        for (key, value) in self.iter() {
            key.encode(buf);
            value.encode(buf);
        }
    }
}

impl<K: CodecType + std::hash::Hash + Eq> CodecType for HashSet<K> {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (size, mut bytes) = u32::decode(bytes)?;
        let mut set = HashSet::new();
        for _ in 0..size {
            let (key, rest) = K::decode(bytes)?;
            set.insert(key);
            bytes = rest;
        }
        Ok((set, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        (self.len() as u32).encode(buf);
        for key in self.iter() {
            key.encode(buf);
        }
    }
}

impl<const N: usize, T> CodecType for [T; N]
where
    T: CodecType,
{
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let arr = unsafe {
            let mut arr = std::mem::MaybeUninit::<[T; N]>::zeroed().assume_init();
            for item in arr.iter_mut() {
                let (value, rest) = T::decode(bytes)?;
                *item = value;
                bytes = rest;
            }
            arr
        };
        Ok((arr, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        for item in self.iter() {
            item.encode(buf);
        }
    }
}

impl<T> CodecType for std::sync::Arc<[T]>
where
    T: CodecType,
{
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (vec, bytes) = <Vec<T>>::decode(bytes)?;
        let arc_arr = vec.into();
        Ok((arc_arr, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        let len = self.len() as u32;
        len.encode(buf);
        for item in self.iter() {
            item.encode(buf);
        }
    }
}

impl<T, E> CodecType for Result<T, E>
where
    T: CodecType,
    E: CodecType,
{
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (ok, bytes) = u8::decode(bytes)?;
        if ok == 0 {
            let (t, bytes) = T::decode(bytes)?;
            Ok((Ok(t), bytes))
        } else {
            let (e, bytes) = E::decode(bytes)?;
            Ok((Err(e), bytes))
        }
    }

    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Ok(t) => {
                0u8.encode(buf);
                t.encode(buf);
            }
            Err(e) => {
                1u8.encode(buf);
                e.encode(buf)
            }
        }
    }
}

impl CodecType for String {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (s, bytes) = Bytes::decode(bytes)?;
        let s =
            String::from_utf8(s.to_vec()).map_err(|e| DecodeError::new::<String>(e.to_string()))?;
        Ok((s, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.as_bytes().to_vec().encode(buf)
    }
}

impl CodecType for Cow<'_, str> {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (s, bytes) = String::decode(bytes)?;
        Ok((Cow::Owned(s), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.to_string().encode(buf)
    }
}

impl<A, B> CodecType for (A, B)
where
    A: CodecType,
    B: CodecType,
{
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (a, bytes) = A::decode(bytes)?;
        let (b, bytes) = B::decode(bytes)?;
        Ok(((a, b), bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.0.encode(buf);
        self.1.encode(buf)
    }
}

/*******************************************************************************************
                                    CODEC FOR CHRONO TYPES
*******************************************************************************************/
impl CodecType for chrono::DateTime<Utc> {
    fn decode(bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        let (secs, bytes) = i64::decode(bytes)?;
        let (nsecs, bytes) = u32::decode(bytes)?;
        let dt = Utc
            .timestamp_opt(secs, nsecs)
            .earliest()
            .unwrap_or_default();
        Ok((dt, bytes))
    }

    fn encode(&self, buf: &mut BytesMut) {
        self.timestamp().encode(buf);
        self.timestamp_subsec_nanos().encode(buf);
    }
}

/*******************************************************************************************
                                    CODEC FOR 16 BYTE IDs
*******************************************************************************************/

impl CodecType for NodeId {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<NodeId>() {
            return Err(DecodeError::new::<Self>(
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
impl CodecType for EndpointAddr {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<EndpointAddr>() {
            return Err(DecodeError::new::<Self>(
                "too short payload: expect EndpointAddr",
            ));
        }
        Ok((
            EndpointAddr {
                bytes: bytes
                    .split_to(size_of::<EndpointAddr>())
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

impl CodecType for N2nPacketId {
    fn decode(mut bytes: Bytes) -> Result<(Self, Bytes), DecodeError> {
        if bytes.len() < size_of::<N2nPacketId>() {
            return Err(DecodeError::new::<Self>(
                "too short payload: expect EndpointAddr",
            ));
        }
        Ok((
            N2nPacketId {
                bytes: bytes
                    .split_to(size_of::<N2nPacketId>())
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
